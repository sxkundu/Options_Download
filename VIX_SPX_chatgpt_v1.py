#!/usr/bin/env python3
"""
VIX + SPX Expected Move Bands

- VIX points change (current - previous close)
- SPX spot
- Expected move from VIX: 1σ = SPX * (VIX/100) * sqrt(days/365)
- Bands: SPX ± {1,2,3}σ

Notes:
- VIX is annualized implied volatility for ~30-day options.
- This converts that to a 1-standard-deviation price move over `days`.
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from typing import Optional

import yfinance as yf


@dataclass
class Quote:
    symbol: str
    last: float
    prev_close: float

    @property
    def points_change(self) -> float:
        return self.last - self.prev_close

    @property
    def pct_change(self) -> float:
        if self.prev_close == 0:
            return float("nan")
        return (self.last / self.prev_close - 1.0) * 100.0


def fetch_quote(symbol: str) -> Quote:
    """
    Fetch last price and previous close for `symbol`.

    yfinance can sometimes return different fields depending on the instrument.
    We attempt a few common keys and fall back to recent history when needed.
    """
    t = yf.Ticker(symbol)

    # Prefer fast_info when available
    last = None
    prev = None

    try:
        fi = getattr(t, "fast_info", None)
        if fi:
            last = fi.get("last_price", None)
            prev = fi.get("previous_close", None)
    except Exception:
        pass

    # Try info (sometimes rate-limited / incomplete)
    if last is None or prev is None:
        try:
            info = t.info or {}
            last = last or info.get("regularMarketPrice")
            prev = prev or info.get("regularMarketPreviousClose")
        except Exception:
            pass

    # Fall back to last 2 daily closes from history
    if last is None or prev is None:
        hist = t.history(period="5d", interval="1d")
        if hist is None or hist.empty or len(hist) < 2:
            raise RuntimeError(f"Could not fetch enough price data for {symbol}.")
        # Use last close as "last" if intraday not available
        prev = float(hist["Close"].iloc[-2])
        last = float(hist["Close"].iloc[-1])

    if last is None or prev is None:
        raise RuntimeError(f"Missing last/prev_close for {symbol}.")

    return Quote(symbol=symbol, last=float(last), prev_close=float(prev))


def expected_move_points(spx_level: float, vix_level: float, days: int = 30) -> float:
    """
    Convert VIX (annualized % volatility) into a 1σ expected move in SPX points
    for a given number of days.

    1σ move ≈ S * (VIX/100) * sqrt(days/365)
    """
    if days <= 0:
        raise ValueError("days must be positive.")
    if spx_level <= 0 or vix_level < 0:
        raise ValueError("spx_level must be > 0 and vix_level must be >= 0.")
    return spx_level * (vix_level / 100.0) * math.sqrt(days / 365.0)


def fmt_signed(x: float, decimals: int = 2) -> str:
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.{decimals}f}"


def main(days: int = 30) -> int:
    vix = fetch_quote("^VIX")
    spx = fetch_quote("^GSPC")

    em1 = expected_move_points(spx.last, vix.last, days=days)

    print("\n=== VIX / SPX Volatility Bands ===")
    print(f"Horizon: {days} days\n")

    print("VIX (^VIX)")
    print(f"  Last:       {vix.last:.2f}")
    print(f"  Prev Close: {vix.prev_close:.2f}")
    print(f"  Change:     {fmt_signed(vix.points_change, 2)} pts  ({fmt_signed(vix.pct_change, 2)}%)\n")

    print("SPX (^GSPC)")
    print(f"  Spot:       {spx.last:.2f}")
    print(f"  Prev Close: {spx.prev_close:.2f}")
    print(f"  Change:     {fmt_signed(spx.points_change, 2)} pts  ({fmt_signed(spx.pct_change, 2)}%)\n")

    print("Implied Expected Move (from VIX)")
    print(f"  1σ (EM1):   {em1:.2f} pts\n")

    for n in (1, 2, 3):
        low = spx.last - n * em1
        high = spx.last + n * em1
        print(f"  ±{n}σ band:  {low:.2f}  to  {high:.2f}")

    print()
    return 0


if __name__ == "__main__":
    # Optional CLI: python vix_spx_bands.py 30
    days_arg: Optional[int] = None
    if len(sys.argv) >= 2:
        try:
            days_arg = int(sys.argv[1])
        except ValueError:
            print("Usage: python vix_spx_bands.py [days]", file=sys.stderr)
            sys.exit(2)

    sys.exit(main(days=days_arg or 30))