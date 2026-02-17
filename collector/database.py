"""
BRRR Capital Pi Data Collector - Database Module

SQLite database operations for market data storage.
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from config import DB_PATH


def init_database():
    """Create database and tables if not exist."""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        -- 1min bars (source of truth)
        -- All other timeframes computed from this
        CREATE TABLE IF NOT EXISTS bars_1min (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            source TEXT DEFAULT 'websocket',
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            UNIQUE(symbol, timestamp)
        );
        
        CREATE INDEX IF NOT EXISTS idx_bars_symbol_time 
            ON bars_1min(symbol, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_bars_timestamp 
            ON bars_1min(timestamp DESC);
        
        -- Current quotes (live, overwritten each tick)
        CREATE TABLE IF NOT EXISTS quotes (
            symbol TEXT PRIMARY KEY,
            contract_id TEXT,
            bid REAL,
            ask REAL,
            last REAL,
            high REAL,
            low REAL,
            open REAL,
            volume INTEGER,
            updated_at INTEGER
        );
        
        -- Contract ID cache (symbol -> contract_id mapping)
        CREATE TABLE IF NOT EXISTS contracts (
            symbol TEXT PRIMARY KEY,
            contract_id TEXT NOT NULL,
            full_symbol TEXT,
            tick_size REAL,
            updated_at INTEGER
        );
        
        -- Backfill progress tracking
        CREATE TABLE IF NOT EXISTS backfill_status (
            symbol TEXT PRIMARY KEY,
            oldest_bar INTEGER,
            newest_bar INTEGER,
            total_bars INTEGER DEFAULT 0,
            topstepx_done BOOLEAN DEFAULT FALSE,
            ib_done BOOLEAN DEFAULT FALSE,
            last_updated INTEGER
        );
        
        -- Collector status for monitoring
        CREATE TABLE IF NOT EXISTS collector_status (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            websocket_connected BOOLEAN DEFAULT FALSE,
            last_quote_at INTEGER,
            last_bar_at INTEGER,
            symbols_subscribed INTEGER DEFAULT 0,
            errors_today INTEGER DEFAULT 0,
            updated_at INTEGER
        );
        
        -- Insert initial status row
        INSERT OR IGNORE INTO collector_status (id) VALUES (1);
    """)
    conn.close()
    print(f"✅ Database initialized: {DB_PATH}")


def save_bar(symbol: str, timestamp: int, o: float, h: float, l: float, c: float, v: int, source: str = 'websocket'):
    """Save a single bar to database."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            INSERT OR REPLACE INTO bars_1min 
            (symbol, timestamp, open, high, low, close, volume, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (symbol, timestamp, o, h, l, c, v, source))
        conn.commit()
    finally:
        conn.close()


def save_bars_batch(bars: List[Dict[str, Any]]):
    """Save multiple bars efficiently."""
    if not bars:
        return 0
        
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executemany("""
            INSERT OR IGNORE INTO bars_1min 
            (symbol, timestamp, open, high, low, close, volume, source)
            VALUES (:symbol, :timestamp, :open, :high, :low, :close, :volume, :source)
        """, bars)
        conn.commit()
        return conn.total_changes
    finally:
        conn.close()


def save_quote(symbol: str, contract_id: str, bid: float, ask: float, last: float, 
               high: float, low: float, open_price: float, volume: int):
    """Save current quote (overwrites previous)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            INSERT OR REPLACE INTO quotes 
            (symbol, contract_id, bid, ask, last, high, low, open, volume, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (symbol, contract_id, bid, ask, last, high, low, open_price, volume, 
              int(datetime.now(timezone.utc).timestamp())))
        conn.commit()
    finally:
        conn.close()


def save_contract(symbol: str, contract_id: str, full_symbol: str = None, tick_size: float = None):
    """Cache contract ID for symbol."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            INSERT OR REPLACE INTO contracts 
            (symbol, contract_id, full_symbol, tick_size, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (symbol, contract_id, full_symbol, tick_size, 
              int(datetime.now(timezone.utc).timestamp())))
        conn.commit()
    finally:
        conn.close()


def get_contract_id(symbol: str) -> Optional[str]:
    """Get cached contract ID for symbol."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute(
            "SELECT contract_id FROM contracts WHERE symbol = ?", 
            (symbol,)
        )
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def get_all_contracts() -> Dict[str, str]:
    """Get all cached contracts as {symbol: contract_id}."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute("SELECT symbol, contract_id FROM contracts")
        return {row[0]: row[1] for row in cursor.fetchall()}
    finally:
        conn.close()


def update_backfill_status(symbol: str, oldest_bar: int = None, newest_bar: int = None,
                           total_bars: int = None, topstepx_done: bool = None):
    """Update backfill progress for symbol."""
    conn = sqlite3.connect(DB_PATH)
    try:
        # Get current status
        cursor = conn.execute(
            "SELECT oldest_bar, newest_bar, total_bars, topstepx_done FROM backfill_status WHERE symbol = ?",
            (symbol,)
        )
        row = cursor.fetchone()
        
        if row:
            current = {
                'oldest_bar': row[0],
                'newest_bar': row[1],
                'total_bars': row[2],
                'topstepx_done': row[3]
            }
        else:
            current = {'oldest_bar': None, 'newest_bar': None, 'total_bars': 0, 'topstepx_done': False}
        
        # Update with new values
        new_oldest = oldest_bar if oldest_bar is not None else current['oldest_bar']
        new_newest = newest_bar if newest_bar is not None else current['newest_bar']
        new_total = total_bars if total_bars is not None else current['total_bars']
        new_done = topstepx_done if topstepx_done is not None else current['topstepx_done']
        
        conn.execute("""
            INSERT OR REPLACE INTO backfill_status 
            (symbol, oldest_bar, newest_bar, total_bars, topstepx_done, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (symbol, new_oldest, new_newest, new_total, new_done,
              int(datetime.now(timezone.utc).timestamp())))
        conn.commit()
    finally:
        conn.close()


def update_collector_status(websocket_connected: bool = None, symbols_subscribed: int = None,
                            last_quote: bool = False, last_bar: bool = False, error: bool = False):
    """Update collector status for monitoring."""
    conn = sqlite3.connect(DB_PATH)
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        
        updates = ["updated_at = ?"]
        params = [now]
        
        if websocket_connected is not None:
            updates.append("websocket_connected = ?")
            params.append(websocket_connected)
        
        if symbols_subscribed is not None:
            updates.append("symbols_subscribed = ?")
            params.append(symbols_subscribed)
            
        if last_quote:
            updates.append("last_quote_at = ?")
            params.append(now)
            
        if last_bar:
            updates.append("last_bar_at = ?")
            params.append(now)
            
        if error:
            updates.append("errors_today = errors_today + 1")
        
        conn.execute(f"UPDATE collector_status SET {', '.join(updates)} WHERE id = 1", params)
        conn.commit()
    finally:
        conn.close()


def get_bar_count(symbol: str) -> int:
    """Get total bar count for symbol."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute(
            "SELECT COUNT(*) FROM bars_1min WHERE symbol = ?",
            (symbol,)
        )
        return cursor.fetchone()[0]
    finally:
        conn.close()


def get_oldest_bar_timestamp(symbol: str) -> Optional[int]:
    """Get oldest bar timestamp for symbol."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute(
            "SELECT MIN(timestamp) FROM bars_1min WHERE symbol = ?",
            (symbol,)
        )
        row = cursor.fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()


def get_newest_bar_timestamp(symbol: str) -> Optional[int]:
    """Get newest bar timestamp for symbol."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute(
            "SELECT MAX(timestamp) FROM bars_1min WHERE symbol = ?",
            (symbol,)
        )
        row = cursor.fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()


if __name__ == '__main__':
    init_database()
    print("Database ready!")
