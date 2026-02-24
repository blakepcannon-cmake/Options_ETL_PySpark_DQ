# quality/checks.py
#
# Data quality checks that run against the transformed Spark DataFrame
# BEFORE it is considered ready for downstream consumption.
#
# DESIGN PHILOSOPHY:
#   Each check returns a QualityCheckResult rather than raising immediately.
#   This lets us collect ALL failures in a single pass (one Spark action per check)
#   and report them together, rather than failing fast on the first bad check.
#   In a production system, these results would be written to a quality_log table
#   and could trigger alerts or block downstream jobs.
#
# The checks here mirror common patterns in Great Expectations and dbt tests,
# but are implemented directly in PySpark so the project has no extra dependencies.
# Reference: https://docs.greatexpectations.io/docs/conceptual_guides/expectations/

import json
from dataclasses import dataclass, field
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import IV_NULL_TOLERANCE, VOLUME_OI_OUTLIER_THRESHOLD


@dataclass
class QualityCheckResult:
    check_name: str
    description: str
    failed_count: int
    total_count: int
    passed: bool
    severity: str          # "ERROR" blocks downstream; "WARN" is informational
    sample_failures: List[dict] = field(default_factory=list)

    @property
    def failure_rate(self) -> float:
        return self.failed_count / self.total_count if self.total_count > 0 else 0.0

    def summary(self) -> str:
        status = "PASS" if self.passed else self.severity
        return (
            f"[{status:5s}] {self.check_name}: "
            f"{self.failed_count:,}/{self.total_count:,} rows flagged "
            f"({self.failure_rate:.1%})"
        )


def _sample_failures(df: DataFrame, n: int = 3) -> List[dict]:
    """Collect a small sample of failing rows for diagnostic output."""
    return [row.asDict() for row in df.limit(n).collect()]


# ------------------------------------------------------------------ #
# INDIVIDUAL CHECKS
# ------------------------------------------------------------------ #

def check_null_zero_bid(df: DataFrame) -> QualityCheckResult:
    """
    Contracts with a null or zero bid are stale or deeply illiquid.
    Using them for IV computation or spread metrics produces garbage output.

    A high failure rate here typically means the source data feed has
    connectivity issues or the tickers include illiquid names.
    """
    total = df.count()
    failed_df = df.filter(F.col("bid").isNull() | (F.col("bid") <= 0))
    failed_count = failed_df.count()

    return QualityCheckResult(
        check_name="null_zero_bid",
        description="Bid is null or zero - contract is stale or illiquid",
        failed_count=failed_count,
        total_count=total,
        passed=failed_count == 0,
        severity="WARN",  # Common for very deep OTM/ITM, not always an error
        sample_failures=_sample_failures(failed_df.select(
            "ticker", "expiration", "option_type", "strike", "bid", "ask"
        )),
    )


def check_bid_ask_inversion(df: DataFrame) -> QualityCheckResult:
    """
    Bid > Ask is a hard data error. This should never occur in a valid
    market feed - it implies crossed quotes, which are eliminated by
    exchange matching engines before publication.

    In practice this occurs due to:
      - Feed latency (bid/ask captured from slightly different timestamps)
      - Data vendor normalization bugs
    Any inversion count > 0 warrants investigation with the data vendor.
    """
    total = df.count()
    failed_df = df.filter(
        F.col("bid").isNotNull() &
        F.col("ask").isNotNull() &
        (F.col("bid") > F.col("ask"))
    )
    failed_count = failed_df.count()

    return QualityCheckResult(
        check_name="bid_ask_inversion",
        description="Bid price exceeds ask price - crossed quote, hard data error",
        failed_count=failed_count,
        total_count=total,
        passed=failed_count == 0,
        severity="ERROR",
        sample_failures=_sample_failures(failed_df.select(
            "ticker", "expiration", "option_type", "strike", "bid", "ask"
        )),
    )


def check_iv_null_rate(df: DataFrame) -> QualityCheckResult:
    """
    Checks what fraction of contracts have a null implied_volatility.

    Some null IV is expected (deep OTM/ITM contracts where the Black-Scholes
    solver fails to converge). But if the null rate exceeds IV_NULL_TOLERANCE
    it suggests a systemic problem: wrong risk-free rate, bad spot prices,
    or a py_vollib installation issue.

    The threshold is configurable in config.py.
    """
    total = df.count()
    failed_df = df.filter(F.col("implied_volatility").isNull())
    failed_count = failed_df.count()
    failure_rate = failed_count / total if total > 0 else 0.0

    return QualityCheckResult(
        check_name="iv_null_rate",
        description=(
            f"IV computation returned null for >{IV_NULL_TOLERANCE:.0%} of contracts. "
            f"Some nulls are expected for deep OTM/ITM; high rates indicate a systemic issue."
        ),
        failed_count=failed_count,
        total_count=total,
        passed=failure_rate <= IV_NULL_TOLERANCE,
        severity="ERROR",
        sample_failures=_sample_failures(failed_df.select(
            "ticker", "expiration", "option_type", "strike",
            "bid", "ask", "mid_price", "dte", "spot_price"
        )),
    )


def check_volume_oi_outliers(df: DataFrame) -> QualityCheckResult:
    """
    Flags contracts where single-day volume exceeds VOLUME_OI_OUTLIER_THRESHOLD * open_interest.

    Volume > 10x OI is unusual (most of the OI would have to be newly opened today),
    and can indicate:
      - A genuine unusual activity event (earnings play, acquisition rumor)
      - A data error where OI was not updated correctly

    This is a WARNING, not an error. Unusual options activity is itself a
    signal traders track. The point is to surface it for review, not discard it.
    Reference: https://www.investopedia.com/terms/o/openinterest.asp
    """
    total = df.count()
    failed_df = df.filter(
        F.col("open_interest").isNotNull() &
        (F.col("open_interest") > 0) &
        F.col("volume").isNotNull() &
        (F.col("volume") > VOLUME_OI_OUTLIER_THRESHOLD * F.col("open_interest"))
    )
    failed_count = failed_df.count()

    return QualityCheckResult(
        check_name="volume_oi_outlier",
        description=(
            f"Volume > {VOLUME_OI_OUTLIER_THRESHOLD:.0f}x open interest - "
            "unusual activity or stale OI data"
        ),
        failed_count=failed_count,
        total_count=total,
        passed=True,   # Never a hard failure - just surface for awareness
        severity="WARN",
        sample_failures=_sample_failures(
            failed_df
            .withColumn("volume_oi_ratio_display",
                F.round(F.col("volume") / F.col("open_interest"), 1)
            )
            .select(
                "ticker", "expiration", "option_type", "strike",
                "volume", "open_interest", "volume_oi_ratio_display"
            )
            .orderBy(F.desc("volume_oi_ratio_display"))
        ),
    )


def check_negative_dte(df: DataFrame) -> QualityCheckResult:
    """
    DTE should never be negative after transform. A negative DTE means
    the contract is already expired and should have been filtered out
    during the ingest stage.

    If this fires, check the expiration date parsing in metrics.compute_dte().
    """
    total = df.count()
    failed_df = df.filter(F.col("dte").isNotNull() & (F.col("dte") < 0))
    failed_count = failed_df.count()

    return QualityCheckResult(
        check_name="negative_dte",
        description="Expired contracts (DTE < 0) present in output",
        failed_count=failed_count,
        total_count=total,
        passed=failed_count == 0,
        severity="ERROR",
        sample_failures=_sample_failures(failed_df.select(
            "ticker", "expiration", "dte"
        )),
    )


# ------------------------------------------------------------------ #
# RUN ALL CHECKS
# ------------------------------------------------------------------ #

def run_all_checks(df: DataFrame) -> List[QualityCheckResult]:
    """
    Executes all quality checks and prints a summary report.
    Returns the full list of results for the caller to inspect.
    """
    print("\n  [quality] Running data quality checks...")

    checks = [
        check_null_zero_bid(df),
        check_bid_ask_inversion(df),
        check_iv_null_rate(df),
        check_volume_oi_outliers(df),
        check_negative_dte(df),
    ]

    print("\n  ---- Quality Report ----")
    for result in checks:
        print(f"  {result.summary()}")
        if not result.passed and result.sample_failures:
            print(f"    Sample failure: {json.dumps(result.sample_failures[0], default=str)}")

    errors = [r for r in checks if not r.passed and r.severity == "ERROR"]
    warnings = [r for r in checks if not r.passed and r.severity == "WARN"]

    print(f"\n  {len(errors)} ERROR(s), {len(warnings)} WARNING(s)")
    if errors:
        print("  ERROR checks failed - downstream data should NOT be trusted until resolved.")

    return checks
