# transform/pipeline.py
#
# PySpark ETL job: reads raw options CSVs from the landing zone,
# applies metric transformations, and writes partitioned Parquet output.
#
# WHY PYSPARK?
#   In production, options chains across hundreds of tickers with intraday
#   refresh cycles produce millions of rows per day. PySpark's distributed
#   execution model handles this where pandas cannot.
#   Here we run in local[*] mode (all cores on one machine), which is
#   functionally identical to a cluster job - same API, same behavior.
#
# WHY PARQUET OUTPUT?
#   Parquet is a columnar format. Analytics queries typically filter on a
#   few columns (ticker, expiration, strike) and aggregate others (volume, IV).
#   Parquet's columnar storage + partitioning lets queries skip irrelevant
#   files entirely. DuckDB (used in Project 3) reads Parquet natively.
#   Reference: https://parquet.apache.org/docs/

import os
from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from config import RISK_FREE_RATE
from transform.metrics import (
    compute_bid_ask_spread_pct,
    compute_dte,
    compute_implied_volatility,
    compute_moneyness,
)


def build_spark_session(app_name: str = "OptionsETL") -> SparkSession:
    """
    Creates a local SparkSession.

    spark.sql.shuffle.partitions defaults to 200, which is far too many
    for a local job processing a few million rows. Setting it to 2x the
    core count keeps shuffle stages fast.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        # Suppress verbose Spark logs - change to WARN to see more
        .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR,console")
        .getOrCreate()
    )


def run_transform(raw_data_path: str, output_path: str) -> None:
    """
    Full transform job:
      1. Read all raw CSVs from the landing zone
      2. Cast and validate column types
      3. Compute derived metrics via UDFs
      4. Write to Parquet, partitioned by ticker and expiration

    Args:
        raw_data_path: Path to directory containing raw *_options_raw.csv files
        output_path:   Root path for Parquet output
    """
    spark = build_spark_session()

    # ------------------------------------------------------------------ #
    # 1. REGISTER UDFs
    #    Each Python function is wrapped so Spark knows the return type.
    #    UDFs execute row-by-row in the Python process - they bypass Spark's
    #    Catalyst optimizer. For a production system with very high volume,
    #    consider Pandas UDFs (vectorized) for the IV computation which is
    #    the most expensive call here.
    #    Reference: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html
    # ------------------------------------------------------------------ #
    dte_udf = F.udf(compute_dte, IntegerType())
    moneyness_udf = F.udf(compute_moneyness, DoubleType())
    spread_pct_udf = F.udf(compute_bid_ask_spread_pct, DoubleType())

    # Bake risk_free_rate into the IV UDF so it matches the expected
    # (option_type, spot, strike, dte, market_price) signature
    def iv_udf_fn(option_type, spot, strike, dte, market_price):
        return compute_implied_volatility(
            option_type, spot, strike, dte, RISK_FREE_RATE, market_price
        )

    iv_udf = F.udf(iv_udf_fn, DoubleType())

    # ------------------------------------------------------------------ #
    # 2. READ RAW DATA
    # ------------------------------------------------------------------ #
    csv_glob = os.path.join(raw_data_path, "*_options_raw.csv")
    print(f"  [transform] Reading from {csv_glob}")

    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_glob)
    )

    row_count = raw_df.count()
    print(f"  [transform] Loaded {row_count:,} raw rows")

    # ------------------------------------------------------------------ #
    # 3. EXPLICIT TYPE CASTING
    #    inferSchema is convenient but can misfire (e.g. a column with
    #    occasional non-numeric values gets cast to string). Explicit casts
    #    make the contract clear and produce better error messages.
    # ------------------------------------------------------------------ #
    typed_df = (
        raw_df
        .withColumn("strike",           F.col("strike").cast(DoubleType()))
        .withColumn("spot_price",       F.col("spot_price").cast(DoubleType()))
        .withColumn("bid",              F.col("bid").cast(DoubleType()))
        .withColumn("ask",              F.col("ask").cast(DoubleType()))
        .withColumn("last_price",       F.col("last_price").cast(DoubleType()))
        .withColumn("volume",           F.col("volume").cast(DoubleType()))
        .withColumn("open_interest",    F.col("open_interest").cast(DoubleType()))
    )

    # ------------------------------------------------------------------ #
    # 4. DERIVE METRICS
    # ------------------------------------------------------------------ #
    transformed_df = (
        typed_df

        # Time dimension
        .withColumn("dte", dte_udf(F.col("expiration")))

        # Normalized price position
        .withColumn("moneyness", moneyness_udf(F.col("strike"), F.col("spot_price")))

        # Mid price - used as input to IV solver and as a cleaner price reference
        # than last_price which may be stale
        .withColumn("mid_price",
            F.when(
                F.col("bid").isNotNull() & F.col("ask").isNotNull(),
                (F.col("bid") + F.col("ask")) / 2.0
            ).otherwise(None)
        )

        # Liquidity metric
        .withColumn("bid_ask_spread_pct",
            spread_pct_udf(F.col("bid"), F.col("ask"))
        )

        # IV - computationally expensive, runs last
        .withColumn("implied_volatility",
            iv_udf(
                F.col("option_type"),
                F.col("spot_price"),
                F.col("strike"),
                F.col("dte"),
                F.col("mid_price"),
            )
        )

        # Activity ratio - unusually high values warrant investigation
        .withColumn("volume_oi_ratio",
            F.when(
                F.col("open_interest").isNotNull() & (F.col("open_interest") > 0),
                F.col("volume") / F.col("open_interest")
            ).otherwise(None)
        )

        # Put/call flag as integer for easy aggregation downstream
        # (sum of is_put / total = put fraction of volume)
        .withColumn("is_put",
            F.when(F.col("option_type") == "put", 1).otherwise(0)
        )
    )

    # ------------------------------------------------------------------ #
    # 5. WRITE PARTITIONED PARQUET
    #    Partitioning by ticker then expiration means a query like
    #    "give me all SPY contracts expiring 2025-01-17" reads only
    #    two partition directories instead of the entire dataset.
    # ------------------------------------------------------------------ #
    out_path = os.path.join(output_path, "options_chain")

    (
        transformed_df
        .write
        .mode("overwrite")
        .partitionBy("ticker", "expiration")
        .parquet(out_path)
    )

    # Quick sanity check on output
    written_df = spark.read.parquet(out_path)
    written_count = written_df.count()
    print(f"  [transform] Wrote {written_count:,} rows to {out_path}")

    spark.stop()
