# config.py
# Central configuration for the Options ETL pipeline.
# In a real environment, sensitive values (API keys, DB creds) would
# live in environment variables or a secrets manager, not here.

TICKERS = ["SPY", "QQQ", "AAPL"]

# U.S. 3-month T-bill rate used as the risk-free rate for Black-Scholes IV.
# Update this periodically. Source: https://fred.stlouisfed.org/series/DTB3
RISK_FREE_RATE = 0.053

# File paths
RAW_DATA_PATH = "data/raw"
OUTPUT_PATH = "data/output"

# Quality check thresholds
IV_NULL_TOLERANCE = 0.20       # Allow up to 20% IV computation failures (deep OTM/ITM contracts commonly fail)
VOLUME_OI_OUTLIER_THRESHOLD = 10.0  # Flag contracts where single-day volume > 10x open interest
MAX_VALID_IV = 5.0             # 500% IV cap - anything above this is almost certainly a bad price
