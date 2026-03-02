#!/usr/bin/env python3
"""
SPX options-derived expected move (ATM straddle) + bands

Data source: Cboe public delayed option quotes JSON
Endpoint pattern (SPX index options use "_SPX"):
  https://cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json

Expected move definition:
  EM ≈ ATM straddle mid = mid(call) + mid(put)   (index points)

Then bands:
  SPX ± {1,2,3} * EM

Optional:
  Convert EM to estimated 1σ using a heuristic factor (default 0.85):
    sigma1 ≈ EM * 0.85
"""

from __future__ import annotations

import datetime as dt
import math
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


CBOE_DELAYED_CHAIN_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class OptionRow:
    option: str
    bid: float
    ask: float
    last: float
    iv: Optional[float]  # may or may not be present
    open_interest: Optional[int]

    @property
    def mid(self) -> Optional[float]:
        # Prefer mid when both sides present; otherwise fall back to last.
        if self.bid is not None and self.ask is not None and self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2.0
        if self.last is not None and self.last > 0:
            return self.last
        return None


@dataclass(frozen=True)
class ParsedContract:
    exp: dt.date
    cp: str              # "C" or "P"
    strike: float        # in index points
    raw_symbol: str


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def fetch_cboe_chain(url: str = CBOE_DELAYED_CHAIN_URL, timeout: int = 20) -> Dict[str, Any]:
    headers = {"User-Agent": UA, "Accept": "application/json,*/*"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def parse_contract_symbol(sym: str) -> ParsedContract:
    """
    Parse Cboe delayed option sym strings.

    Common pattern: UNDERLYING + YYMMDD + [C|P] + strike*1000 (padded)
    Example-ish: SPXW240119C04700000
    For index options, you may still see SPX/SPXW prefixes in the contract string.

    We'll use a robust regex:
      (prefix)(YYMMDD)(C|P)(strikeDigits)
    strikeDigits are usually strike*1000 with no decimal.
    """
    m = re.search(r"(\d{6})([CP])(\d+)$", sym)
    if not m:
        raise ValueError(f"Unrecognized option symbol format: {sym}")

    yymmdd = m.group(1)
    cp = m.group(2)
    strike_digits = m.group(3)

    yy = int(yymmdd[0:2])
    mm = int(yymmdd[2:4])
    dd = int(yymmdd[4:6])

    # Assume 20xx for yy < 80; adjust if needed
    year = 2000 + yy if yy < 80 else 1900 + yy
    exp = dt.date(year, mm, dd)

    # Cboe format typically uses strike * 1000
    strike = int(strike_digits) / 1000.0

    return ParsedContract(exp=exp, cp=cp, strike=strike, raw_symbol=sym)


def build_option_rows(chain_json: Dict[str, Any]) -> Tuple[float, List[OptionRow]]:
    """
    Returns (underlying_price, option_rows)
    """
    data = chain_json.get("data") or chain_json.get("options") or {}
    # Many examples show data["current_price"] and data["options"].
    underlying = _safe_float(data.get("current_price") or data.get("underlying_price") or data.get("spot"))
    if underlying is None:
        raise RuntimeError("Could not find underlying/current_price in response JSON.")

    raw_opts = data.get("options")
    if not isinstance(raw_opts, list) or len(raw_opts) == 0:
        raise RuntimeError("No options list found in response JSON.")

    rows: List[OptionRow] = []
    for o in raw_opts:
        if not isinstance(o, dict):
            continue
        rows.append(
            OptionRow(
                option=str(o.get("option", "")),
                bid=_safe_float(o.get("bid")),
                ask=_safe_float(o.get("ask")),
                last=_safe_float(o.get("last")),
                iv=_safe_float(o.get("iv")),
                open_interest=_safe_int(o.get("open_interest")),
            )
        )
    return underlying, rows


def choose_expiration(rows: List[OptionRow], target_dte: int = 30) -> dt.date:
    today = dt.date.today()
    exps = set()
    for r in rows:
        try:
            exps.add(parse_contract_symbol(r.option).exp)
        except Exception:
            continue

    future_exps = sorted([e for e in exps if e >= today])
    if not future_exps:
        raise RuntimeError("No non-expired expirations found in chain.")

    # Pick expiration with DTE closest to target
    def dte(exp: dt.date) -> int:
        return (exp - today).days

    chosen = min(future_exps, key=lambda e: abs(dte(e) - target_dte))
    return chosen


def find_atm_straddle(
    underlying: float,
    rows: List[OptionRow],
    exp: dt.date,
) -> Tuple[float, float, float, float]:
    """
    Returns (strike, call_mid, put_mid, straddle_mid)
    """
    # Filter rows to this expiration, build dict: strike -> (call, put)
    by_strike: Dict[float, Dict[str, OptionRow]] = {}
    for r in rows:
        try:
            pc = parse_contract_symbol(r.option)
        except Exception:
            continue
        if pc.exp != exp:
            continue
        by_strike.setdefault(pc.strike, {})[pc.cp] = r

    if not by_strike:
        raise RuntimeError(f"No contracts found for expiration {exp}.")

    # Find strike closest to underlying where both call and put exist with usable mid
    best = None
    for strike, legs in by_strike.items():
        c = legs.get("C")
        p = legs.get("P")
        if not c or not p:
            continue
        cm = c.mid
        pm = p.mid
        if cm is None or pm is None:
            continue
        dist = abs(strike - underlying)
        candidate = (dist, strike, cm, pm, cm + pm)
        if best is None or candidate[0] < best[0]:
            best = candidate

    if best is None:
        raise RuntimeError("Could not find an ATM strike with both call and put mids.")

    _, strike, call_mid, put_mid, straddle_mid = best
    return strike, call_mid, put_mid, straddle_mid


def fmt_signed(x: float, decimals: int = 2) -> str:
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.{decimals}f}"


def main(target_dte: int = 30, sigma_factor: float = 0.85) -> int:
    chain = fetch_cboe_chain()
    underlying, rows = build_option_rows(chain)

    exp = choose_expiration(rows, target_dte=target_dte)
    strike, call_mid, put_mid, em = find_atm_straddle(underlying, rows, exp)

    # Optional: convert “expected move” to ~1σ using a heuristic factor.
    sigma1 = em * sigma_factor

    print("\n=== SPX Options-Derived Expected Move (ATM Straddle) ===")
    print(f"Underlying (from feed): {underlying:.2f}")
    print(f"Chosen expiration:      {exp.isoformat()}  (target DTE ~ {target_dte})")
    print(f"ATM strike:             {strike:.2f}")
    print(f"Call mid:               {call_mid:.2f}")
    print(f"Put mid:                {put_mid:.2f}")
    print(f"Expected move (EM):     {em:.2f} points  (ATM straddle mid)\n")

    print(f"Heuristic 1σ estimate:  {sigma1:.2f} points  (EM * {sigma_factor:.2f})\n")

    # Bands based on EM (common “expected move” bands)
    for n in (1, 2, 3):
        low = underlying - n * em
        high = underlying + n * em
        print(f"EM bands ±{n}x:         {low:.2f}  to  {high:.2f}")

    print()

    # Bands based on sigma1 (if you want “standard deviation” style bands)
    for n in (1, 2, 3):
        low = underlying - n * sigma1
        high = underlying + n * sigma1
        print(f"σ bands ±{n}σ:          {low:.2f}  to  {high:.2f}")

    print()
    return 0


if __name__ == "__main__":
    # Usage:
    #   pip install requests
    #   python spx_em.py           # defaults: 30 DTE, sigma_factor=0.85
    #   python spx_em.py 7         # target 7 DTE
    #   python spx_em.py 30 0.80   # custom sigma factor
    dte = 0
    sf = 0.85

    if len(sys.argv) >= 2:
        dte = int(sys.argv[1])
    if len(sys.argv) >= 3:
        sf = float(sys.argv[2])

    raise SystemExit(main(target_dte=dte, sigma_factor=sf))