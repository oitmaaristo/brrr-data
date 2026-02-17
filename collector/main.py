#!/usr/bin/env python3
"""
BRRR Capital Data Collector 🖨️💰

Main entry point. Runs on Kullatera Pi.

Components:
1. WebSocket Live Collector - builds 1min bars from quotes
2. REST Backfill Service - fetches 44 days of history

Usage:
    python main.py              # Run both WebSocket + backfill
    python main.py --backfill   # Only run backfill
    python main.py --websocket  # Only run WebSocket
"""

import asyncio
import os
import sys
import logging
import signal
import argparse
import requests
from datetime import datetime, timezone
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import (
    DB_PATH, LOG_PATH, ENV_PATH, 
    API_BASE_URL, INSTRUMENTS
)
from database import init_database, update_collector_status
from websocket_collector import LiveBarBuilder, MarketHubConnection
from backfill_service import BackfillService

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, mode='a')
    ]
)
logger = logging.getLogger(__name__)


def load_env():
    """Load environment from .env file."""
    if Path(ENV_PATH).exists():
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if line and '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()


def authenticate() -> str:
    """
    Authenticate with TopStepX API.
    
    Returns JWT token.
    """
    username = os.environ.get('PROJECTX_USERNAME')
    api_key = os.environ.get('PROJECTX_API_KEY')
    
    if not username or not api_key:
        raise ValueError("PROJECTX_USERNAME and PROJECTX_API_KEY must be set!")
    
    logger.info(f"Authenticating as {username}...")
    
    response = requests.post(
        f"{API_BASE_URL}/Auth/loginKey",
        json={'userName': username, 'apiKey': api_key},
        headers={'Content-Type': 'application/json'},
        timeout=10
    )
    response.raise_for_status()
    data = response.json()
    
    if data.get('success') or data.get('errorCode') == 0:
        token = data.get('token')
        if token:
            logger.info("✅ Authentication successful!")
            return token
            
    raise Exception(f"Authentication failed: {data.get('errorMessage', 'Unknown error')}")


async def run_collector(token: str, do_backfill: bool = True, do_websocket: bool = True):
    """
    Main collector loop.
    
    Args:
        token: TopStepX JWT token
        do_backfill: Whether to run REST backfill
        do_websocket: Whether to run WebSocket collector
    """
    # Get contract IDs for all symbols
    logger.info("Resolving contract IDs...")
    backfill_service = BackfillService(token)
    contract_ids = backfill_service.get_all_contract_ids(INSTRUMENTS)
    
    logger.info(f"Found {len(contract_ids)} contracts:")
    for symbol, cid in zip(INSTRUMENTS[:len(contract_ids)], contract_ids):
        logger.info(f"  {symbol} -> {cid}")
    
    # Run backfill first (in background or foreground based on mode)
    if do_backfill:
        logger.info("\n" + "=" * 60)
        logger.info("STARTING HISTORICAL BACKFILL")
        logger.info("=" * 60)
        
        if do_websocket:
            # Run backfill in background thread
            loop = asyncio.get_event_loop()
            backfill_task = loop.run_in_executor(
                None, 
                backfill_service.backfill_all, 
                INSTRUMENTS
            )
        else:
            # Run backfill in foreground (blocking)
            backfill_service.backfill_all(INSTRUMENTS)
            logger.info("Backfill complete!")
            return
    
    # Start WebSocket collector
    if do_websocket:
        logger.info("\n" + "=" * 60)
        logger.info("STARTING WEBSOCKET COLLECTOR")
        logger.info("=" * 60)

        bar_builder = LiveBarBuilder()

        # Build reverse mapping: contract_id -> symbol
        contract_to_symbol = {}
        for symbol, cid in zip(INSTRUMENTS[:len(contract_ids)], contract_ids):
            contract_to_symbol[cid] = symbol
        bar_builder.set_contract_mapping(contract_to_symbol)

        hub = MarketHubConnection(token, bar_builder)

        # Subscribe to all contracts
        hub.set_contracts(contract_ids)
        
        # Handle shutdown
        shutdown_event = asyncio.Event()
        
        def handle_shutdown(signum, frame):
            logger.info("\n🛑 Shutdown signal received...")
            shutdown_event.set()
            
        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)
        
        # Start connection
        try:
            hub._build_connection()
            hub._running = True
            hub.connection.start()
            
            logger.info("✅ WebSocket collector running!")
            logger.info(f"   Tracking {len(contract_ids)} instruments")
            logger.info("   Press Ctrl+C to stop\n")
            
            # Keep running until shutdown
            while not shutdown_event.is_set():
                await asyncio.sleep(1)
                
                # Periodic status log (every 5 minutes)
                if datetime.now().second == 0 and datetime.now().minute % 5 == 0:
                    logger.info(f"📊 Status: {bar_builder.bars_saved} bars saved, {len(hub.contract_ids)} subscriptions")
                    
        finally:
            hub.stop()
            if do_backfill:
                # Wait for backfill to complete
                await backfill_task


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='BRRR Capital Data Collector 🖨️💰',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python main.py              # Run both WebSocket + backfill
    python main.py --backfill   # Only run REST backfill (then exit)
    python main.py --websocket  # Only run WebSocket (skip backfill)
    python main.py --status     # Show database status
        """
    )
    parser.add_argument('--backfill', action='store_true', help='Only run REST backfill')
    parser.add_argument('--websocket', action='store_true', help='Only run WebSocket (skip backfill)')
    parser.add_argument('--status', action='store_true', help='Show database status and exit')
    args = parser.parse_args()
    
    print("=" * 60)
    print("  🖨️  BRRR Capital Data Collector  💰")
    print("=" * 60)
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Database: {DB_PATH}")
    print(f"  Log: {LOG_PATH}")
    print("=" * 60)
    print()
    
    # Load environment
    load_env()
    
    # Initialize database
    init_database()
    
    # Status mode
    if args.status:
        from database import get_bar_count, get_oldest_bar_timestamp, get_newest_bar_timestamp
        print("\n📊 DATABASE STATUS:")
        print("-" * 40)
        for symbol in INSTRUMENTS:
            count = get_bar_count(symbol)
            oldest = get_oldest_bar_timestamp(symbol)
            newest = get_newest_bar_timestamp(symbol)
            
            if oldest and newest:
                oldest_dt = datetime.fromtimestamp(oldest, tz=timezone.utc)
                newest_dt = datetime.fromtimestamp(newest, tz=timezone.utc)
                print(f"  {symbol:5} | {count:>8,} bars | {oldest_dt.strftime('%Y-%m-%d')} to {newest_dt.strftime('%Y-%m-%d')}")
            else:
                print(f"  {symbol:5} | {count:>8,} bars | No data")
        print()
        return
    
    # Determine mode
    do_backfill = not args.websocket  # Do backfill unless --websocket specified
    do_websocket = not args.backfill  # Do websocket unless --backfill specified
    
    try:
        # Authenticate
        token = authenticate()
        
        # Run collector
        asyncio.run(run_collector(token, do_backfill, do_websocket))
        
    except KeyboardInterrupt:
        logger.info("\n👋 Goodbye!")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise


if __name__ == '__main__':
    main()
