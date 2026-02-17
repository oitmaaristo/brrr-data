"""
IB Historical Data Backfill Script
Loads max available 1-min data for all instruments
"""
import sqlite3
import time
from datetime import datetime, timedelta
from ib_insync import IB, Future
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
log = logging.getLogger(__name__)

# Config
TWS_HOST = '100.86.107.94'
TWS_PORT = 7496
DB_PATH = '/home/brrr/brrr-data/market_data.db'

INSTRUMENTS = {
    'MNQ': 'CME',
    'MES': 'CME', 
    'M2K': 'CME',
    'MYM': 'CBOT',
    'MCL': 'NYMEX',
    'MGC': 'COMEX',
    'ES': 'CME',
    'NQ': 'CME',
    'RTY': 'CME',
    'YM': 'CBOT',
    'GC': 'COMEX',
    'CL': 'NYMEX',
}

def get_front_contract(ib, symbol, exchange):
    """Get front month contract"""
    contract = Future(symbol, exchange=exchange)
    details = ib.reqContractDetails(contract)
    if not details:
        return None
    return details[0].contract

def get_table_range(conn, symbol):
    """Get existing data range for symbol"""
    table = f"ohlcv_{symbol}_1m"
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {table}")
        row = cur.fetchone()
        return row[0], row[1]
    except:
        return None, None

def ensure_table(conn, symbol):
    """Create table if not exists - matches existing schema"""
    table = f"ohlcv_{symbol}_1m"
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table} (
            timestamp TEXT PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
    ''')
    conn.commit()

def save_bars(conn, symbol, bars):
    """Save bars to database - only 6 columns"""
    if not bars:
        return 0
    
    table = f"ohlcv_{symbol}_1m"
    cur = conn.cursor()
    count = 0
    
    for bar in bars:
        ts = bar.date.strftime('%Y-%m-%d %H:%M:%S')
        try:
            cur.execute(f'''
                INSERT OR REPLACE INTO {table} 
                (timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (ts, bar.open, bar.high, bar.low, bar.close, bar.volume))
            count += 1
        except Exception as e:
            log.error(f"Error saving bar: {e}")
    
    conn.commit()
    return count

def download_historical(ib, contract, end_dt, duration='1 D'):
    """Download historical bars"""
    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_dt,
            durationStr=duration,
            barSizeSetting='1 min',
            whatToShow='TRADES',
            useRTH=False,
            formatDate=1
        )
        return bars
    except Exception as e:
        log.error(f"Error downloading: {e}")
        return []

def backfill_symbol(ib, conn, symbol, exchange, days_back=365*5):
    """Backfill single symbol"""
    log.info(f"=== Starting backfill for {symbol} ===")
    
    ensure_table(conn, symbol)
    
    # Get contract
    contract = get_front_contract(ib, symbol, exchange)
    if not contract:
        log.error(f"Could not get contract for {symbol}")
        return
    
    log.info(f"Contract: {contract.localSymbol}")
    
    # Get existing range
    first_ts, last_ts = get_table_range(conn, symbol)
    log.info(f"Existing data: {first_ts} to {last_ts}")
    
    now = datetime.now()
    request_count = 0
    total_bars = 0
    
    # Forward fill (gap to now)
    if last_ts:
        last_dt = datetime.strptime(last_ts, '%Y-%m-%d %H:%M:%S')
        end_dt = now
        
        while end_dt > last_dt:
            end_str = end_dt.strftime('%Y%m%d %H:%M:%S')
            log.info(f"Forward: {symbol} ending {end_str}")
            
            bars = download_historical(ib, contract, end_str, '2 D')
            if bars:
                saved = save_bars(conn, symbol, bars)
                total_bars += saved
                log.info(f"  Got {len(bars)} bars, saved {saved}")
                end_dt = bars[0].date.replace(tzinfo=None) - timedelta(minutes=1)
            else:
                log.warning(f"  No bars returned")
                break
            
            request_count += 1
            time.sleep(2)
            
            if end_dt <= last_dt:
                break
    
    # Backward fill (historical)
    if first_ts:
        first_dt = datetime.strptime(first_ts, '%Y-%m-%d %H:%M:%S')
    else:
        first_dt = now
    
    min_date = now - timedelta(days=days_back)
    end_dt = first_dt
    
    while end_dt > min_date:
        end_str = end_dt.strftime('%Y%m%d %H:%M:%S')
        log.info(f"Backward: {symbol} ending {end_str}")
        
        bars = download_historical(ib, contract, end_str, '2 D')
        if bars:
            saved = save_bars(conn, symbol, bars)
            total_bars += saved
            log.info(f"  Got {len(bars)} bars, saved {saved}")
            end_dt = bars[0].date.replace(tzinfo=None) - timedelta(minutes=1)
        else:
            log.warning(f"  No more data available")
            break
        
        request_count += 1
        time.sleep(2)
        
        # Extra pause every 50 requests
        if request_count % 50 == 0:
            log.info(f"  Extended pause (50 requests)...")
            time.sleep(10)
    
    log.info(f"=== {symbol} complete: {total_bars} bars, {request_count} requests ===")
    return total_bars

def main():
    log.info("Starting IB Historical Backfill")
    
    # Connect
    ib = IB()
    ib.connect(TWS_HOST, TWS_PORT, clientId=50)
    log.info(f"Connected to TWS at {TWS_HOST}:{TWS_PORT}")
    
    # Database
    conn = sqlite3.connect(DB_PATH)
    
    try:
        for symbol, exchange in INSTRUMENTS.items():
            try:
                backfill_symbol(ib, conn, symbol, exchange)
                time.sleep(5)  # Pause between symbols
            except Exception as e:
                log.error(f"Error with {symbol}: {e}")
                continue
    finally:
        conn.close()
        ib.disconnect()
        log.info("Done!")

if __name__ == '__main__':
    main()
