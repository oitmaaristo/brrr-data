"""
BRRR Capital Pi Data Collector - Configuration

Instruments, paths, and constants.
"""

# Instruments in priority order (Risto's list)
INSTRUMENTS = [
    'MNQ',  # Micro Nasdaq
    'NQ',   # Nasdaq
    'MGC',  # Micro Gold
    'MBT',  # Micro Bitcoin
    'GC',   # Gold
    'MES',  # Micro S&P
    'ES',   # S&P
    'YM',   # Dow
    'SI',   # Silver
    'SIL',  # Micro Silver
    'MHG',  # Micro Copper
    'MNG',  # Micro Natural Gas
    'MCL',  # Micro Crude Oil
    'MET',  # Micro Ether
]

# Paths
DB_PATH = '/home/kuldar/brrr-data/market_data.db'
LOG_PATH = '/home/kuldar/brrr-data/collector.log'
ENV_PATH = '/home/kuldar/brrr-data/.env'

# TopStepX API
API_BASE_URL = 'https://api.topstepx.com/api'
MARKET_HUB_URL = 'wss://rtc.topstepx.com/hubs/market'

# Rate limits
REST_RATE_LIMIT = 50  # requests per 30 seconds
REST_RATE_WINDOW = 30  # seconds
BARS_PER_REQUEST = 20000  # max bars per REST request

# Backfill settings
BACKFILL_DAYS = 44  # TopStepX has 44 days history
