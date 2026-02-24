# Options ETL Pipeline

End-to-end PySpark pipeline that ingests live options chain data, computes standard options analytics metrics, and writes partitioned Parquet output with integrated data quality checks.

Built as a portfolio project targeting a data engineering role focused on trading analytics at a firm like Chicago Trading Company.

---

## Architecture

```
yfinance API
    │
    ▼
┌─────────────────────────────────────┐
│  INGEST (ingest/fetcher.py)         │  Bronze layer: raw CSVs, one per ticker
│  - Pulls full options chain         │  Preserves source data before any transform
│  - Normalizes column names          │
│  - Attaches spot price + timestamp  │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│  TRANSFORM (transform/pipeline.py)  │  Silver layer: enriched Parquet
│  PySpark local[*] mode              │
│                                     │
│  Metrics computed (metrics.py):     │
│  - DTE (days to expiration)         │
│  - Moneyness (strike / spot)        │
│  - Mid price                        │
│  - Bid-ask spread %                 │
│  - Implied volatility (B-S)         │
│  - Volume / OI ratio                │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│  QUALITY CHECKS (quality/checks.py) │
│  - Null/zero bid check (WARN)       │
│  - Bid > ask inversion (ERROR)      │
│  - IV null rate threshold (ERROR)   │
│  - Volume/OI outlier (WARN)         │
│  - Negative DTE check (ERROR)       │
└─────────────────────────────────────┘
                   │
                   ▼
    data/output/options_chain/
    partitioned by ticker / expiration
```

---

## Key Design Decisions

**Why PySpark in local mode?**
The transform logic is identical whether running locally or on a cluster. Local mode lets you develop and test the full pipeline on a laptop. In production (e.g. on Databricks or EMR), changing `.master("local[*]")` to `.master("yarn")` is the only required change.

**Why separate metric functions from the Spark job?**
`transform/metrics.py` contains pure Python functions with no Spark dependency. This means they are unit-testable without a SparkSession (see `tests/`), and the same logic can be reused in the Dash dashboard (Project 2) without importing PySpark.

**Why Parquet with partition pruning?**
Analytics queries on options data almost always filter by ticker and expiration. Partitioning by these two columns means Spark reads only the relevant directory subtree rather than scanning the full dataset. On a 6-month history of SPY+QQQ+AAPL, this can reduce I/O by 90%+.

**Why mid-price for IV computation, not last_price?**
`last_price` in options data is the price of the most recent trade, which may have occurred hours or days ago. The bid-ask mid reflects the current market consensus. Using `last_price` for IV would produce stale, misleading volatility estimates for illiquid contracts.

---

## Setup

```bash
git clone <repo>
cd options-etl

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

Java is required for PySpark. Verify with `java -version`. Install from https://adoptium.net/ if needed (JDK 11 or 17 recommended).

---

## Usage

```bash
# Run the full pipeline for all configured tickers
python main.py

# Run for specific tickers
python main.py --tickers SPY AAPL

# Re-run transforms on existing raw data (skip the API call)
python main.py --skip-ingest
```

Output is written to `data/output/options_chain/` as Parquet files partitioned by `ticker` and `expiration`.

---

## Running Tests

```bash
pip install pytest
python -m pytest tests/ -v
```

Tests cover the pure metric functions without requiring a SparkSession or network access.

---

## Output Schema

| Column | Type | Description |
|---|---|---|
| contract_symbol | string | OCC standard option symbol |
| ticker | string | Underlying equity ticker |
| expiration | string | Expiration date (partition key) |
| option_type | string | `call` or `put` |
| strike | double | Strike price |
| spot_price | double | Underlying price at fetch time |
| bid | double | Best bid |
| ask | double | Best ask |
| mid_price | double | (bid + ask) / 2 |
| last_price | double | Last traded price |
| volume | double | Contracts traded today |
| open_interest | double | Total open contracts |
| dte | int | Days until expiration |
| moneyness | double | strike / spot |
| bid_ask_spread_pct | double | (ask - bid) / mid |
| implied_volatility | double | Black-Scholes IV (null if unsolvable) |
| volume_oi_ratio | double | volume / open_interest |
| yf_implied_volatility | double | yfinance's own IV estimate (for comparison) |
| fetch_timestamp | string | UTC timestamp of data pull |

---

## References

- yfinance: https://pypi.org/project/yfinance/
- py_vollib: https://pypi.org/project/py-vollib/
- Medallion architecture: https://www.databricks.com/glossary/medallion-architecture
- Black & Scholes (1973): https://www.jstor.org/stable/1831029
- Open Interest definition: https://www.investopedia.com/terms/o/openinterest.asp
