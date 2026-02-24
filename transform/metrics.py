# transform/metrics.py
#
# Pure Python functions that compute per-contract metrics.
# These are registered as PySpark UDFs in pipeline.py.
#
# Keeping metric logic here (separate from the Spark job) means:
#   1. Each function is unit-testable without a SparkSession
#   2. Logic can be reused outside of Spark (e.g. in the Dash app)
#
# METRIC REFERENCE:
#   - Moneyness: https://www.investopedia.com/terms/m/moneyness.asp
#   - Bid-ask spread as a % of mid: standard market microstructure measure,
#     described in Harris, L. (2003) "Trading and Exchanges", Oxford University Press
#   - IV via Black-Scholes: py_vollib docs at https://pypi.org/project/py-vollib/

from datetime import date, datetime
from typing import Optional


def compute_dte(expiration_str: str) -> Optional[int]:
    """
    Days to expiration (DTE) from today.

    DTE is the most fundamental time dimension for options traders.
    Theta decay accelerates as DTE approaches zero, so it anchors
    nearly every options strategy decision.
    """
    if not expiration_str:
        return None
    try:
        exp_date = datetime.strptime(expiration_str, "%Y-%m-%d").date()
        return max((exp_date - date.today()).days, 0)
    except Exception:
        return None


def compute_moneyness(strike: Optional[float], spot: Optional[float]) -> Optional[float]:
    """
    Moneyness = Strike / Spot.

    Interpretation:
      - moneyness = 1.0  -> at-the-money (ATM)
      - moneyness > 1.0  -> OTM call / ITM put
      - moneyness < 1.0  -> ITM call / OTM put

    This normalized form (as opposed to raw dollar distance) makes
    moneyness comparable across tickers with very different price levels.
    """
    if strike is None or spot is None or spot <= 0:
        return None
    try:
        return round(strike / spot, 6)
    except Exception:
        return None


def compute_bid_ask_spread_pct(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    """
    Relative bid-ask spread = (Ask - Bid) / Mid.

    This is a standard liquidity measure. A spread of 0.10 on a $1.00 option
    means the market maker earns 10% round-trip per contract - very wide.
    On a $50.00 option the same dollar spread is only 0.2%.

    Normalizing by mid price makes liquidity comparable across contracts.
    CTC, as a market maker, would use this to assess where they are most
    competitive and where adverse selection risk is highest.
    """
    if bid is None or ask is None:
        return None
    try:
        mid = (bid + ask) / 2.0
        if mid <= 0:
            return None
        return round((ask - bid) / mid, 6)
    except Exception:
        return None


def compute_implied_volatility(
    option_type: Optional[str],
    spot: Optional[float],
    strike: Optional[float],
    dte: Optional[int],
    risk_free_rate: float,
    market_price: Optional[float],
) -> Optional[float]:
    """
    Implied volatility (IV) computed by inverting the Black-Scholes pricing
    formula using py_vollib's Newton-Raphson solver.

    We use the mid-price as the market price input. Using bid or ask would
    systematically bias IV estimates.

    Returns None when:
      - DTE <= 0 (expired)
      - market_price <= 0 (stale/no trade)
      - The solver fails to converge (deep ITM/OTM, unrealistic prices)
      - Computed IV > 500% (almost certainly a bad input price)

    These None values are surfaced by the quality checks layer.

    Reference: Black & Scholes (1973), "The Pricing of Options and Corporate
    Liabilities", Journal of Political Economy, 81(3), 637-654.
    """
    if None in (option_type, spot, strike, dte, market_price):
        return None
    if dte <= 0 or market_price <= 0 or spot <= 0 or strike <= 0:
        return None

    try:
        from py_vollib.black_scholes.implied_volatility import implied_volatility

        t = dte / 365.0
        flag = "c" if str(option_type).lower().startswith("c") else "p"

        iv = implied_volatility(market_price, spot, strike, t, risk_free_rate, flag)

        # Reject physically unreasonable IV values
        if iv is None or iv <= 0 or iv > 5.0:
            return None

        return round(float(iv), 6)
    except Exception:
        # py_vollib raises when the solver doesn't converge or inputs violate
        # no-arbitrage bounds. This is expected for deep OTM/ITM contracts.
        return None
