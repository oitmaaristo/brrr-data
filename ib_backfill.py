"""
IB Historical Data Backfill Script - SPECIFIC CONTRACTS VERSION
Uses specific contracts (MNQH6, MNQM6, etc.) instead of CONTFUT.
Handles rollover by chaining contracts backwards in time.

Futures contract months:
- H = March, M = June, U = September, Z = December (financials)
- F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun, N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec (commodities)
"""
import sqlite3
import time
from datetime import datetime, timedelta
from ib_insync import IB, Future
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
log = logging.getLogger(__name__)

# Config
TWS_HOST = '100.86.107.94'  # Risto Windows Tailscale IP
TWS_PORT = 7496
DB_PATH = '/home/brrr/brrr-data/market_data.db'

# Contract month codes
MONTH_CODES = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 
               7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}

# Instrument configs - quarterly (H, M, U, Z) vs monthly
INSTRUMENTS = {
    # QUARTERLY - H (Mar), M (Jun), U (Sep), Z (Dec)
    'MNQ': {'exchange': 'CME', 'months': [3, 6, 9, 12], 'roll_days': 8},
    'MES': {'exchange': 'CME', 'months': [3, 6, 9, 12], 'roll_days': 8},
    'M2K': {'exchange': 'CME', 'months': [3, 6, 9, 12], 'roll_days': 8},
    'MYM': {'exchange': 'CBOT', 'months': [3, 6, 9, 12], 'roll_days': 8},
    'NQ':  {'exchange': 'CME', 'months': [3, 6, 9, 12], 'roll_days': 8},
    'ES':  {'exchange': 'CME', 'months': [3, 6, 9, 12], 'roll_days': 8},
    'RTY': {'exchange': 'CME', 'months': [3, 6, 9, 12], 'roll_days': 8},
    'YM':  {'exchange': 'CBOT', 'months': [3, 6, 9, 12], 'roll_days': 8},
    
    # MONTHLY - every month for commodities  
    'MCL': {'exchange': 'NYMEX', 'months': list(range(1, 13)), 'roll_days': 5},
    'CL':  {'exchange': 'NYMEX', 'months': list(range(1, 13)), 'roll_days': 5},
    'QG':  {'exchange': 'NYMEX', 'months': list(range(1, 13)), 'roll_days': 5},
    'NG':  {'exchange': 'NYMEX', 'months': list(range(1, 13)), 'roll_days': 5},
    
    # METALS - specific months (Feb, Apr, Jun, Aug, Dec for gold)
    'MGC': {'exchange': 'COMEX', 'months': [2, 4, 6, 8, 12], 'roll_days': 3},
    'GC':  {'exchange': 'COMEX', 'months': [2, 4, 6, 8, 12], 'roll_days': 3},
    'SI':  {'exchange': 'COMEX', 'months': [3, 5, 7, 9, 12], 'roll_days': 3},
}

# Priority order for backfill - MNQ ONLY FOR NOW
PRIORITY_ORDER = ['MNQ']


def get_contract_chain(symbol: str, start_date: datetime, end_date: datetime) -> list:
    """
    Generate list of specific contracts needed to cover date range.
    Returns list of (contract_month_str, approx_start, approx_end) tuples.
    
    E.g., for MNQ from 2021-02 to 2026-02:
    [('202603', ...), ('202512', ...), ('202509', ...), ...]
    """
    config = INSTRUMENTS[symbol]
    valid_months = config['months']
    
    contracts = []
    current = end_date
    
    while current > start_date:
        year = current.year
        month = current.month
        
        # Find the active contract for this date
        # Contract is active until ~2 weeks before expiry (3rd Friday of expiry month)
        # So in Feb 2026, the active contract is H6 (March 2026)
        
        # Find next expiry month
        future_months = [m for m in valid_months if m > month]
        if future_months:
            expiry_month = future_months[0]
            expiry_year = year
        else:
            expiry_month = valid_months[0]
            expiry_year = year + 1
        
        # But if we're in an expiry month and past the 15th, use that month
        if month in valid_months and current.day < 15:
            expiry_month = month
            expiry_year = year
        
        # Contract string: YYYYMM
        contract_str = f"{expiry_year}{expiry_month:02d}"
        
        # Find previous expiry month for contract start estimate
        idx = valid_months.index(expiry_month)
        if idx > 0:
            prev_expiry_month = valid_months[idx - 1]
            prev_expiry_year = expiry_year
        else:
            prev_expiry_month = valid_months[-1]
            prev_expiry_year = expiry_year - 1
        
        contract_start = datetime(prev_expiry_year, prev_expiry_month, 20)
        
        if contract_str not in [c[0] for c in contracts]:
            contracts.append((contract_str, contract_start))
            log.info(f"  Adding contract {contract_str}")
        
        # Move to previous contract period
        current = contract_start - timedelta(days=1)
    
    return contracts


def get_specific_contract(ib: IB, symbol: str, contract_month: str, exchange: str):
    """Get a specific futures contract by month (YYYYMM format)."""
    contract = Future(symbol, exchange=exchange, lastTradeDateOrContractMonth=contract_month)
    details = ib.reqContractDetails(contract)
    if not details:
        return None
    return details[0].contract


def ensure_table(conn, symbol):
    """Create table if not exists."""
    table = f"ohlcv_{symbol}_1m"
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table} (
            timestamp TEXT PRIMARY KEY,
            open REAL, high REAL, low REAL, close REAL, volume REAL
        )
    ''')
    conn.commit()


def get_existing_range(conn, symbol):
    """Get existing data range in table."""
    table = f"ohlcv_{symbol}_1m"
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {table}")
        row = cur.fetchone()
        if row[0] and row[1]:
            return datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S'), \
                   datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
    except:
        pass
    return None, None


def save_bars(conn, symbol, bars):
    """Save bars to database."""
    if not bars:
        return 0
    table = f"ohlcv_{symbol}_1m"
    cur = conn.cursor()
    count = 0
    for bar in bars:
        ts = bar.date.strftime('%Y-%m-%d %H:%M:%S') if hasattr(bar.date, 'strftime') else str(bar.date)[:19]
        try:
            cur.execute(f'''
                INSERT OR IGNORE INTO {table} (timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (ts, bar.open, bar.high, bar.low, bar.close, bar.volume))
            if cur.rowcount > 0:
                count += 1
        except Exception as e:
            log.error(f"Save error: {e}")
    conn.commit()
    return count


def backfill_contract(ib: IB, conn, symbol: str, contract, target_start: datetime):
    """
    Backfill data for a specific contract going backwards.
    """
    log.info(f"  Backfilling {contract.localSymbol}...")
    
    total_bars = 0
    current_end = ''  # Empty = now
    
    # Request in 1-week chunks
    for week in range(52):  # Max 1 year per contract
        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=current_end,
                durationStr='1 W',
                barSizeSetting='1 min',
                whatToShow='TRADES',
                useRTH=False,
                formatDate=1
            )
            
            if not bars:
                log.info(f"    No more data for {contract.localSymbol}")
                break
            
            saved = save_bars(conn, symbol, bars)
            total_bars += saved
            
            oldest = bars[0].date
            if hasattr(oldest, 'replace'):
                oldest_naive = oldest.replace(tzinfo=None) if oldest.tzinfo else oldest
            else:
                oldest_naive = datetime.strptime(str(oldest)[:19], '%Y-%m-%d %H:%M:%S')
            
            log.info(f"    +{saved} bars, oldest: {oldest_naive}")
            
            # Check if we've gone back far enough
            if oldest_naive < target_start:
                log.info(f"    Reached target start date")
                break
            
            # Set next request to end at oldest bar
            current_end = oldest_naive.strftime('%Y%m%d %H:%M:%S')
            time.sleep(0.6)  # Rate limit
            
        except Exception as e:
            error_str = str(e)
            if 'pacing' in error_str.lower():
                log.warning(f"    Pacing violation, waiting 60s...")
                time.sleep(60)
                continue
            elif 'No market data' in error_str or 'invalid' in error_str.lower():
                log.info(f"    Contract not available: {e}")
                break
            else:
                log.error(f"    Error: {e}")
                break
    
    return total_bars


def backfill_symbol(ib: IB, conn, symbol: str, start_date: datetime, end_date: datetime):
    """Backfill a symbol using chain of specific contracts."""
    log.info(f"\n{'='*50}")
    log.info(f"BACKFILLING {symbol}")
    log.info(f"{'='*50}")
    
    config = INSTRUMENTS[symbol]
    ensure_table(conn, symbol)
    
    # Check existing data
    existing_start, existing_end = get_existing_range(conn, symbol)
    if existing_start:
        log.info(f"Existing data: {existing_start} to {existing_end}")
    
    # Generate contract chain
    contracts = get_contract_chain(symbol, start_date, end_date)
    log.info(f"Contract chain ({len(contracts)} contracts)")
    
    total_bars = 0
    
    for contract_month, contract_start in contracts:
        # Get the actual contract
        contract = get_specific_contract(ib, symbol, contract_month, config['exchange'])
        
        if not contract:
            log.warning(f"  Could not find contract {symbol} {contract_month}")
            continue
        
        # Backfill this contract
        bars = backfill_contract(ib, conn, symbol, contract, contract_start)
        total_bars += bars
        
        time.sleep(1)  # Pause between contracts
    
    log.info(f"{symbol} COMPLETE: {total_bars} total bars saved")
    return total_bars


def main():
    log.info("="*60)
    log.info("IB BACKFILL - MNQ ONLY")
    log.info("="*60)
    
    # Connect
    ib = IB()
    ib.connect(TWS_HOST, TWS_PORT, clientId=10)
    log.info(f"Connected! Accounts: {ib.managedAccounts()}")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Date range - 5 years back
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 5)
    
    log.info(f"Target range: {start_date.date()} to {end_date.date()}")
    
    # Backfill MNQ only
    for i, symbol in enumerate(PRIORITY_ORDER):
        if symbol not in INSTRUMENTS:
            log.warning(f"Unknown symbol: {symbol}")
            continue
            
        log.info(f"\n[{i+1}/{len(PRIORITY_ORDER)}] {symbol}")
        
        try:
            backfill_symbol(ib, conn, symbol, start_date, end_date)
        except Exception as e:
            log.error(f"Error with {symbol}: {e}")
            continue
    
    conn.close()
    ib.disconnect()
    log.info("\n" + "="*60)
    log.info("ALL DONE!")
    log.info("="*60)


if __name__ == "__main__":
    main()
