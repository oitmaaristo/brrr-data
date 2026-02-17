"""
BRRR Capital Pi Data Collector - Backfill Service

Fetches historical bars from TopStepX REST API.
Rate limited: 50 req / 30 sec

Runs once at startup, fills 44 days of history.
"""

import time
import logging
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from config import (
    API_BASE_URL, INSTRUMENTS, 
    REST_RATE_LIMIT, REST_RATE_WINDOW, 
    BARS_PER_REQUEST, BACKFILL_DAYS
)
from database import (
    save_bars_batch, save_contract, get_contract_id,
    update_backfill_status, get_bar_count,
    get_oldest_bar_timestamp, get_newest_bar_timestamp
)

logger = logging.getLogger(__name__)

# Reusable session for connection pooling
_session = requests.Session()


class BackfillService:
    """
    Historical data backfill from TopStepX REST API.
    
    Rate limit aware: 50 requests per 30 seconds.
    """
    
    def __init__(self, token: str):
        self.token = token
        self.request_count = 0
        self.window_start = time.time()
        self.contracts_cache: Dict[str, str] = {}  # symbol -> contract_id
        
    def _get_headers(self):
        """Get auth headers."""
        return {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
    def _rate_limit(self):
        """Enforce rate limit: 50 req / 30 sec."""
        now = time.time()
        
        # Reset window if expired
        if now - self.window_start >= REST_RATE_WINDOW:
            self.request_count = 0
            self.window_start = now
            
        # Wait if at limit
        if self.request_count >= REST_RATE_LIMIT:
            sleep_time = REST_RATE_WINDOW - (now - self.window_start) + 0.5
            if sleep_time > 0:
                logger.info(f"  Rate limit reached, sleeping {sleep_time:.1f}s...")
                time.sleep(sleep_time)
            self.request_count = 0
            self.window_start = time.time()
            
        self.request_count += 1
        
    def get_contract_id(self, symbol: str) -> Optional[str]:
        """
        Get contract ID for symbol.
        
        First checks cache, then database, then API.
        """
        # Check memory cache
        if symbol in self.contracts_cache:
            return self.contracts_cache[symbol]
            
        # Check database cache
        contract_id = get_contract_id(symbol)
        if contract_id:
            self.contracts_cache[symbol] = contract_id
            return contract_id
            
        # Fetch from API
        self._rate_limit()
        
        try:
            response = _session.post(
                f"{API_BASE_URL}/Contract/search",
                headers=self._get_headers(),
                json={'searchText': symbol, 'live': False},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            contracts = data.get('contracts', [])
            if contracts:
                # Find best match (exact symbol match preferred)
                for c in contracts:
                    cid = c.get('id', '')
                    # CON.F.US.MNQH5 -> matches MNQ
                    if f'.{symbol}' in cid.upper() or cid.upper().endswith(symbol):
                        contract_id = cid
                        break
                else:
                    # Take first result
                    contract_id = contracts[0].get('id')
                    
                if contract_id:
                    # Cache it
                    self.contracts_cache[symbol] = contract_id
                    save_contract(
                        symbol=symbol,
                        contract_id=contract_id,
                        full_symbol=contracts[0].get('name'),
                        tick_size=contracts[0].get('tickSize')
                    )
                    logger.info(f"  Found contract: {symbol} -> {contract_id}")
                    return contract_id
                    
            logger.warning(f"  No contract found for {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"  Error searching contract {symbol}: {e}")
            return None
            
    def fetch_bars(self, contract_id: str, start_time: datetime, end_time: datetime, 
                   limit: int = BARS_PER_REQUEST) -> List[Dict[str, Any]]:
        """
        Fetch bars from REST API.
        
        Returns list of bar dicts with keys: t, o, h, l, c, v
        """
        self._rate_limit()
        
        try:
            response = _session.post(
                f"{API_BASE_URL}/History/retrieveBars",
                headers=self._get_headers(),
                json={
                    'contractId': contract_id,
                    'live': False,  # Use sim subscription
                    'startTime': start_time.isoformat(),
                    'endTime': end_time.isoformat(),
                    'unit': 2,  # Minute
                    'unitNumber': 1,  # 1 minute bars
                    'limit': limit,
                    'includePartialBar': False
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get('success'):
                return data.get('bars', [])
            else:
                logger.warning(f"  API error: {data.get('errorMessage')}")
                return []
                
        except Exception as e:
            logger.error(f"  Error fetching bars: {e}")
            return []
            
    def backfill_symbol(self, symbol: str) -> int:
        """
        Backfill 44 days for single symbol.
        
        Returns number of bars saved.
        """
        contract_id = self.get_contract_id(symbol)
        if not contract_id:
            logger.warning(f"Skipping {symbol} - no contract ID")
            return 0
            
        logger.info(f"Backfilling {symbol} ({contract_id})...")
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=BACKFILL_DAYS)
        
        total_saved = 0
        current_end = end_time
        
        while current_end > start_time:
            bars = self.fetch_bars(
                contract_id=contract_id,
                start_time=start_time,
                end_time=current_end,
                limit=BARS_PER_REQUEST
            )
            
            if not bars:
                logger.info(f"  No more bars for {symbol}")
                break
                
            # Convert and save
            db_bars = []
            oldest_ts = None
            newest_ts = None
            
            for bar in bars:
                try:
                    # Parse timestamp
                    ts_str = bar.get('t', '')
                    if ts_str:
                        # Handle various formats
                        ts_str = ts_str.replace('Z', '+00:00')
                        dt = datetime.fromisoformat(ts_str)
                        ts = int(dt.timestamp())
                        
                        db_bars.append({
                            'symbol': symbol,
                            'timestamp': ts,
                            'open': bar['o'],
                            'high': bar['h'],
                            'low': bar['l'],
                            'close': bar['c'],
                            'volume': bar['v'],
                            'source': 'rest_backfill'
                        })
                        
                        if oldest_ts is None or ts < oldest_ts:
                            oldest_ts = ts
                        if newest_ts is None or ts > newest_ts:
                            newest_ts = ts
                            
                except Exception as e:
                    logger.warning(f"  Error parsing bar: {e}")
                    continue
            
            # Save batch
            saved = save_bars_batch(db_bars)
            total_saved += saved
            
            # Update progress
            if oldest_ts:
                oldest_dt = datetime.fromtimestamp(oldest_ts, tz=timezone.utc)
                logger.info(f"  Got {len(bars)} bars, saved {saved}, oldest: {oldest_dt.strftime('%Y-%m-%d %H:%M')}")
                
                # Move window back
                current_end = datetime.fromtimestamp(oldest_ts, tz=timezone.utc) - timedelta(minutes=1)
            else:
                break
                
        # Update status
        update_backfill_status(
            symbol=symbol,
            oldest_bar=get_oldest_bar_timestamp(symbol),
            newest_bar=get_newest_bar_timestamp(symbol),
            total_bars=get_bar_count(symbol),
            topstepx_done=True
        )
        
        logger.info(f"✅ {symbol} backfill complete: {total_saved} new bars")
        return total_saved
        
    def backfill_all(self, symbols: List[str] = None) -> Dict[str, int]:
        """
        Backfill all symbols.
        
        Returns dict of {symbol: bars_saved}.
        """
        if symbols is None:
            symbols = INSTRUMENTS
            
        logger.info(f"Starting backfill for {len(symbols)} symbols...")
        logger.info(f"Symbols: {', '.join(symbols)}")
        logger.info("=" * 50)
        
        results = {}
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"\n[{i}/{len(symbols)}] {symbol}")
            try:
                saved = self.backfill_symbol(symbol)
                results[symbol] = saved
            except Exception as e:
                logger.error(f"Error backfilling {symbol}: {e}")
                results[symbol] = 0
                
        # Summary
        logger.info("\n" + "=" * 50)
        logger.info("BACKFILL COMPLETE!")
        logger.info("=" * 50)
        total = sum(results.values())
        logger.info(f"Total bars saved: {total:,}")
        for symbol, count in results.items():
            status = "✅" if count > 0 else "❌"
            logger.info(f"  {status} {symbol}: {count:,} bars")
            
        return results
    
    def get_all_contract_ids(self, symbols: List[str] = None) -> List[str]:
        """
        Get contract IDs for all symbols.
        
        Useful for WebSocket subscriptions.
        """
        if symbols is None:
            symbols = INSTRUMENTS
            
        contract_ids = []
        for symbol in symbols:
            cid = self.get_contract_id(symbol)
            if cid:
                contract_ids.append(cid)
                
        return contract_ids


def run_backfill(token: str, symbols: List[str] = None):
    """
    Run backfill as standalone process.
    """
    service = BackfillService(token)
    return service.backfill_all(symbols)


if __name__ == '__main__':
    import os
    from dotenv import load_dotenv
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    load_dotenv('/home/kuldar/brrr-data/.env')
    
    token = os.getenv('TOPSTEPX_TOKEN')
    if token:
        run_backfill(token)
    else:
        print("TOPSTEPX_TOKEN not set!")
