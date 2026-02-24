# main.py
#
# Entry point for the Options ETL pipeline.
# Orchestrates: Ingest -> Transform -> Quality Checks
#
# Usage:
#   python main.py
#
# To run on a subset of tickers:
#   python main.py --tickers SPY QQQ

import argparse
import sys
import time

from config import OUTPUT_PATH, RAW_DATA_PATH, TICKERS
from ingest.fetcher import fetch_options_chain
from quality.checks import run_all_checks
from transform.pipeline import build_spark_session, run_transform


def parse_args():
    parser = argparse.ArgumentParser(description="Options chain ETL pipeline")
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=TICKERS,
        help="Space-separated list of tickers to process (default: from config.py)",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip the ingest step and use existing raw CSV files",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    tickers = [t.upper() for t in args.tickers]

    print("=" * 55)
    print("  Options ETL Pipeline")
    print(f"  Tickers: {', '.join(tickers)}")
    print("=" * 55)

    start = time.time()

    # ------------------------------------------------------------------ #
    # STEP 1: INGEST
    # Pull raw options chains from yfinance and land as CSV.
    # ------------------------------------------------------------------ #
    if not args.skip_ingest:
        print("\n[Step 1/3] Ingesting options chain data...")
        ingest_errors = []
        for ticker in tickers:
            try:
                fetch_options_chain(ticker, RAW_DATA_PATH)
            except Exception as e:
                print(f"  [ingest] ERROR on {ticker}: {e}")
                ingest_errors.append(ticker)

        if ingest_errors:
            print(f"\n  Ingest failed for: {ingest_errors}. Continuing with remaining tickers.")
        if len(ingest_errors) == len(tickers):
            print("  All ingest steps failed. Exiting.")
            sys.exit(1)
    else:
        print("\n[Step 1/3] Skipping ingest (--skip-ingest flag set)")

    # ------------------------------------------------------------------ #
    # STEP 2: TRANSFORM
    # PySpark job: compute metrics, write partitioned Parquet.
    # ------------------------------------------------------------------ #
    print("\n[Step 2/3] Running PySpark transform...")
    try:
        run_transform(RAW_DATA_PATH, OUTPUT_PATH)
    except Exception as e:
        print(f"  [transform] FATAL: {e}")
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # STEP 3: QUALITY CHECKS
    # Read the Parquet output and validate it before declaring success.
    # ------------------------------------------------------------------ #
    print("\n[Step 3/3] Running quality checks on output...")
    spark = build_spark_session()
    try:
        output_df = spark.read.parquet(f"{OUTPUT_PATH}/options_chain")
        results = run_all_checks(output_df)
    finally:
        spark.stop()

    # ------------------------------------------------------------------ #
    # SUMMARY
    # ------------------------------------------------------------------ #
    elapsed = time.time() - start
    error_checks = [r for r in results if not r.passed and r.severity == "ERROR"]

    print(f"\n{'=' * 55}")
    print(f"  Pipeline completed in {elapsed:.1f}s")
    if error_checks:
        failed_names = ", ".join(r.check_name for r in error_checks)
        print(f"  STATUS: DEGRADED - quality checks failed: {failed_names}")
        sys.exit(1)
    else:
        print("  STATUS: SUCCESS")
    print("=" * 55)


if __name__ == "__main__":
    main()
