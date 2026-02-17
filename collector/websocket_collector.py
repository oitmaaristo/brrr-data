"""
BRRR Capital Pi Data Collector - WebSocket Collector

Connects to TopStepX Market Hub via SignalR.
Builds 1min bars from live quotes.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Optional
import time

from signalrcore.hub_connection_builder import HubConnectionBuilder

from config import MARKET_HUB_URL
from database import (
    save_bar, save_quote, get_all_contracts,
    update_collector_status
)

logger = logging.getLogger(__name__)


class LiveBarBuilder:
    """Builds 1min bars from live quote updates."""

    def __init__(self):
        self.current_bars: Dict[str, dict] = {}
        self.bars_saved = 0
        # Reverse mapping: contract_id -> our symbol name
        self._contract_to_symbol: Dict[str, str] = {}

    def set_contract_mapping(self, mapping: Dict[str, str]):
        """
        Set contract_id -> symbol mapping.

        Args:
            mapping: {contract_id: symbol} e.g. {'CON.F.US.EP.Z25': 'ES'}
        """
        self._contract_to_symbol = mapping
        logger.info(f"Loaded {len(mapping)} contract mappings")

    def on_quote(self, contract_id: str, data: dict):
        """Process incoming GatewayQuote event."""
        try:
            price = data.get('lastPrice', 0)
            volume = data.get('volume', 0)

            if not price:
                return

            # Use contract_id -> symbol mapping (preferred)
            symbol = self._contract_to_symbol.get(contract_id)

            # Fallback: extract from quote's symbol field
            if not symbol:
                symbol_id = data.get('symbol', '')
                symbol = self._extract_symbol(symbol_id)

            if not symbol:
                return
            
            now = datetime.now(timezone.utc)
            minute_ts = int(now.replace(second=0, microsecond=0).timestamp())
            
            # Save quote
            save_quote(
                symbol=symbol,
                contract_id=contract_id,
                bid=data.get('bestBid', 0),
                ask=data.get('bestAsk', 0),
                last=price,
                high=data.get('high', 0),
                low=data.get('low', 0),
                open_price=data.get('open', 0),
                volume=volume
            )
            update_collector_status(last_quote=True)
            
            # Update or create current bar
            if symbol not in self.current_bars:
                self.current_bars[symbol] = {
                    'open': price, 'high': price, 'low': price, 'close': price,
                    'volume': 0, 'minute_ts': minute_ts, 'last_volume': volume
                }
                logger.info(f"Started tracking {symbol} at {price}")
            else:
                bar = self.current_bars[symbol]
                
                if minute_ts > bar['minute_ts']:
                    self._save_completed_bar(symbol, bar)
                    self.current_bars[symbol] = {
                        'open': price, 'high': price, 'low': price, 'close': price,
                        'volume': 0, 'minute_ts': minute_ts, 'last_volume': volume
                    }
                else:
                    bar['high'] = max(bar['high'], price)
                    bar['low'] = min(bar['low'], price)
                    bar['close'] = price
                    if volume > bar['last_volume']:
                        bar['volume'] += volume - bar['last_volume']
                    bar['last_volume'] = volume
                    
        except Exception as e:
            logger.error(f"Error processing quote: {e}")
    
    def _save_completed_bar(self, symbol: str, bar: dict):
        """Save a completed bar to database."""
        try:
            save_bar(
                symbol=symbol, timestamp=bar['minute_ts'],
                o=bar['open'], h=bar['high'], l=bar['low'], c=bar['close'],
                v=bar['volume'], source='websocket'
            )
            self.bars_saved += 1
            update_collector_status(last_bar=True)
            dt = datetime.fromtimestamp(bar['minute_ts'], tz=timezone.utc)
            logger.info(f"Bar: {symbol} {dt.strftime('%H:%M')} O={bar['open']:.2f} C={bar['close']:.2f} V={bar['volume']}")
        except Exception as e:
            logger.error(f"Error saving bar: {e}")
    
    def _extract_symbol(self, symbol_id: str) -> Optional[str]:
        """F.US.MNQ -> MNQ"""
        if not symbol_id:
            return None
        parts = symbol_id.split('.')
        return parts[2] if len(parts) >= 3 else None
    
    def flush_all(self):
        """Save all current bars on shutdown."""
        for symbol, bar in self.current_bars.items():
            self._save_completed_bar(symbol, bar)
        self.current_bars.clear()


class MarketHubConnection:
    """SignalR connection to TopStepX Market Hub."""
    
    def __init__(self, token: str, bar_builder: LiveBarBuilder):
        self.token = token
        self.bar_builder = bar_builder
        self.connection = None
        self.contract_ids = []
        self._running = False
        
    def _build_connection(self):
        """Build SignalR hub connection."""
        url = f"{MARKET_HUB_URL}?access_token={self.token}"
        
        self.connection = HubConnectionBuilder() \
            .with_url(url, options={"skip_negotiation": True}) \
            .with_automatic_reconnect({
                "type": "interval",
                "intervals": [0, 2, 5, 10, 30, 60]
            }) \
            .build()
        
        # Event handlers - args is [contractId, data]
        self.connection.on("GatewayQuote", self._on_quote)
        self.connection.on("GatewayTrade", self._on_trade)
        
        self.connection.on_open(self._on_connected)
        self.connection.on_close(self._on_disconnected)
        self.connection.on_error(self._on_error)
        
    def _on_quote(self, args):
        """Handle GatewayQuote: [contractId, data]"""
        if isinstance(args, list) and len(args) >= 2:
            contract_id, data = args[0], args[1]
            self.bar_builder.on_quote(contract_id, data)
        
    def _on_trade(self, args):
        """Handle GatewayTrade (for logging)."""
        pass
        
    def _on_connected(self):
        """Handle connection established."""
        logger.info("✅ Connected to Market Hub!")
        update_collector_status(websocket_connected=True)
        self._subscribe_all()
            
    def _on_disconnected(self):
        """Handle disconnect."""
        logger.warning("❌ Disconnected from Market Hub")
        update_collector_status(websocket_connected=False)
        
    def _on_error(self, error):
        """Handle error."""
        err_msg = getattr(error, 'error', str(error))
        logger.error(f"❌ Error: {err_msg}")
        
    def _subscribe_all(self):
        """Subscribe to all contracts."""
        for contract_id in self.contract_ids:
            try:
                self.connection.send("SubscribeContractQuotes", [contract_id])
                logger.info(f"  Subscribed: {contract_id}")
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"  Subscribe failed {contract_id}: {e}")
        update_collector_status(symbols_subscribed=len(self.contract_ids))
        
    def set_contracts(self, contract_ids: list):
        """Set contracts to subscribe to."""
        self.contract_ids = contract_ids
        
    def start(self):
        """Start connection (blocking)."""
        self._build_connection()
        self._running = True
        logger.info("Starting Market Hub connection...")
        self.connection.start()
        
    def run_forever(self):
        """Run until stopped."""
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()
            
    def stop(self):
        """Stop connection."""
        self._running = False
        self.bar_builder.flush_all()
        if self.connection:
            self.connection.stop()
        logger.info("Market Hub stopped")
