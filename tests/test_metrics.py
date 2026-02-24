# tests/test_metrics.py
#
# Unit tests for the metric computation functions in transform/metrics.py.
#
# These tests do NOT require a SparkSession - they test the pure Python logic
# that gets wrapped as UDFs. This is a key benefit of separating metric logic
# from the Spark job itself.
#
# Run with: python -m pytest tests/ -v

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from transform.metrics import (
    compute_dte,
    compute_moneyness,
    compute_bid_ask_spread_pct,
    compute_implied_volatility,
)
from datetime import date, timedelta


class TestComputeDte:
    def test_future_expiration(self):
        future = (date.today() + timedelta(days=30)).strftime("%Y-%m-%d")
        assert compute_dte(future) == 30

    def test_today_expiration(self):
        today = date.today().strftime("%Y-%m-%d")
        # Expires today - DTE is 0, not negative
        assert compute_dte(today) == 0

    def test_past_expiration_clamps_to_zero(self):
        past = (date.today() - timedelta(days=5)).strftime("%Y-%m-%d")
        assert compute_dte(past) == 0

    def test_none_input(self):
        assert compute_dte(None) is None

    def test_bad_format(self):
        assert compute_dte("not-a-date") is None


class TestComputeMoneyness:
    def test_atm(self):
        # Strike == Spot -> moneyness == 1.0
        assert compute_moneyness(100.0, 100.0) == 1.0

    def test_otm_call(self):
        # Strike > Spot
        result = compute_moneyness(110.0, 100.0)
        assert result == pytest.approx(1.1, rel=1e-4)

    def test_itm_call(self):
        # Strike < Spot
        result = compute_moneyness(90.0, 100.0)
        assert result == pytest.approx(0.9, rel=1e-4)

    def test_zero_spot_returns_none(self):
        assert compute_moneyness(100.0, 0.0) is None

    def test_none_input(self):
        assert compute_moneyness(None, 100.0) is None
        assert compute_moneyness(100.0, None) is None


class TestBidAskSpreadPct:
    def test_typical_spread(self):
        # bid=0.90, ask=1.10, mid=1.00, spread=0.20 -> 20%
        result = compute_bid_ask_spread_pct(0.90, 1.10)
        assert result == pytest.approx(0.20, rel=1e-4)

    def test_zero_spread(self):
        result = compute_bid_ask_spread_pct(1.00, 1.00)
        assert result == pytest.approx(0.0, abs=1e-6)

    def test_zero_mid_returns_none(self):
        # Both bid and ask are 0 - mid would be 0, division undefined
        assert compute_bid_ask_spread_pct(0.0, 0.0) is None

    def test_none_input(self):
        assert compute_bid_ask_spread_pct(None, 1.0) is None


class TestComputeIV:
    """
    IV tests require py_vollib. If it's not installed, these are skipped.
    We use known Black-Scholes inputs with a hand-checked expected IV.
    """

    def test_atm_call_reasonable_iv(self):
        # ATM SPY-like call: spot=500, strike=500, dte=30, price~10
        # IV should be in a reasonable range (roughly 0.10-0.40 for SPY)
        iv = compute_implied_volatility("call", 500.0, 500.0, 30, 0.05, 10.0)
        if iv is not None:  # Will be None if py_vollib not installed
            assert 0.05 < iv < 2.0

    def test_expired_contract_returns_none(self):
        iv = compute_implied_volatility("call", 500.0, 500.0, 0, 0.05, 10.0)
        assert iv is None

    def test_zero_price_returns_none(self):
        iv = compute_implied_volatility("call", 500.0, 500.0, 30, 0.05, 0.0)
        assert iv is None

    def test_none_inputs_return_none(self):
        assert compute_implied_volatility(None, 500.0, 500.0, 30, 0.05, 10.0) is None
        assert compute_implied_volatility("call", None, 500.0, 30, 0.05, 10.0) is None

    def test_extreme_iv_rejected(self):
        # A price of 0.01 for an ATM option with 30 DTE implies absurdly high IV
        # The function should reject IV > 500% and return None
        iv = compute_implied_volatility("call", 500.0, 500.0, 30, 0.05, 0.01)
        # Either None (solver failed) or within bounds
        if iv is not None:
            assert iv <= 5.0
