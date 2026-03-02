#!/usr/bin/env python3
"""
SPX options–derived expected move (ATM straddle) with SPXW weekly preference.

Data source: Cboe public delayed option quotes JSON (typically ~15-min delayed)
Endpoint (SPX index options): https://cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json

What it does:
- Fetches SPX option chain (delayed) + underlying level
- Chooses an expiration closest to target DTE, preferring SPXW (weeklies / typically PM-settled)
- Computes ATM straddle mid = expected move (EM) in SPX points
- Prints EM bands and optional σ bands (heuristic)

Usage:
  pip install requests
  python spxw_expected_move.py
  python spxw_expected_move.py 7
  python spxw_expected_move.py 30 0.85

Args:
  1) target_dte (int) default=30
  2) sigma_factor (float) default=0.85   # heuristic: 1σ ≈ EM * sigma_factor
"""

from __future__ import annotations

import datetime as dt
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
    bid: Optional[float]
    ask: Optional[float]
    last: Optional[float]
    iv: Optional[float]
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
    strike: float        # index points
    raw_symbol: str
    weekly: bool         # True if SPXW


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


def build_option_rows(chain_json: Dict[str, Any]) -> Tuple[float, List[OptionRow]]:
    """
    Returns (underlying_price, option_rows)
    """
    data = chain_json.get("data") or chain_json.get("options") or {}

    underlying = _safe_float(
        data.get("current_price") or data.get("underlying_price") or data.get("spot")
    )
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


def parse_contract_symbol(sym: str) -> ParsedContract:
    """
    Parse Cboe option symbol string and detect SPXW.

    Common Cboe pattern: <ROOT><YYMMDD><C|P><strike*1000>
      e.g. SPXW240119C04700000
           SPX 240119P04700000

    We detect:
    - weekly root by presence of "SPXW" in symbol
    - expiration by YYMMDD
    - type C/P
    - strike by dividing strikeDigits by 1000
    """
    s = sym.upper()
    weekly = "SPXW" in s

    m = re.search(r"(\d{6})([CP])(\d+)$", s)
    if not m:
        raise ValueError(f"Unrecognized option symbol format: {sym}")

    yymmdd = m.group(1)
    cp = m.group(2)
    strike_digits = m.group(3)

    yy = int(yymmdd[0:2])
    mm = int(yymmdd[2:4])
    dd = int(yymmdd[4:6])

    year = 2000 + yy if yy < 80 else 1900 + yy
    exp = dt.date(year, mm, dd)

    strike = int(strike_digits) / 1000.0
    return ParsedContract(exp=exp, cp=cp, strike=strike, raw_symbol=sym, weekly=weekly)


def choose_expiration_prefer_spxw(
    rows: List[OptionRow],
    target_dte: int = 30,
    prefer_weeklies: bool = True,
) -> dt.date:
    """
    Choose expiration closest to target DTE, preferring SPXW expirations (weeklies).

    Steps:
    - Collect future expirations into two buckets: weeklies (SPXW) and non-weeklies.
    - If prefer_weeklies and weeklies exist: choose closest-to-target DTE among weeklies
    - Else choose closest-to-target among non-weeklies (or weeklies as fallback)
    """
    today = dt.date.today()

    weekly_exps = set()
    nonweekly_exps = set()

    for r in rows:
        try:
            pc = parse_contract_symbol(r.option)
        except Exception:
            continue

        if pc.exp < today:
            continue

        (weekly_exps if pc.weekly else nonweekly_exps).add(pc.exp)

    weekly_list = sorted(weekly_exps)
    nonweekly_list = sorted(nonweekly_exps)

    def pick_closest(exps: List[dt.date]) -> dt.date:
        return min(exps, key=lambda e: abs((e - today).days - target_dte))

    if prefer_weeklies and weekly_list:
        return pick_closest(weekly_list)
    if nonweekly_list:
        return pick_closest(nonweekly_list)
    if weekly_list:
        return pick_closest(weekly_list)

    raise RuntimeError("No non-expired expirations found in chain.")


def find_atm_straddle(
    underlying: float,
    rows: List[OptionRow],
    exp: dt.date,
    require_weekly: bool = True,
) -> Tuple[float, float, float, float]:
    """
    Compute ATM straddle for a given expiration.

    Returns (strike, call_mid, put_mid, straddle_mid)

    If require_weekly=True, uses only SPXW contracts for the legs.
    """
    by_strike: Dict[float, Dict[str, OptionRow]] = {}

    for r in rows:
        try:
            pc = parse_contract_symbol(r.option)
        except Exception:
            continue

        if pc.exp != exp:
            continue
        if require_weekly and not pc.weekly:
            continue

        by_strike.setdefault(pc.strike, {})[pc.cp] = r

    if not by_strike:
        raise RuntimeError(f"No contracts found for expiration {exp} (require_weekly={require_weekly}).")

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
        raise RuntimeError("Could not find ATM strike with both call and put mids (check data availability).")

    _, strike, call_mid, put_mid, straddle_mid = best
    return strike, call_mid, put_mid, straddle_mid


def main(target_dte: int = 30, sigma_factor: float = 0.85) -> int:
    chain = fetch_cboe_chain()
    underlying, rows = build_option_rows(chain)

    exp = choose_expiration_prefer_spxw(rows, target_dte=target_dte, prefer_weeklies=True)

    # Require SPXW legs for the straddle; change to False if you want to allow SPX (monthly) legs.
    strike, call_mid, put_mid, em = find_atm_straddle(
        underlying=underlying,
        rows=rows,
        exp=exp,
        require_weekly=True,
    )

    # Optional “1σ” heuristic from straddle
    sigma1 = em * sigma_factor

    today = dt.date.today()
    dte = (exp - today).days

    print("\n=== SPXW Options-Derived Expected Move (ATM Straddle Mid) ===")
    print(f"Underlying (feed):      {underlying:.2f}")
    print(f"Chosen expiration:      {exp.isoformat()} (DTE={dte}, target~{target_dte})")
    print(f"ATM strike (closest):   {strike:.2f}")
    print(f"Call mid:               {call_mid:.2f}")
    print(f"Put mid:                {put_mid:.2f}")
    print(f"Expected move (EM):     {em:.2f} points (call mid + put mid)")

    print(f"\nHeuristic 1σ estimate:  {sigma1:.2f} points  (EM * {sigma_factor:.2f})\n")

    print("Bands using EM multiples (common 'expected move' bands):")
    for n in (1, 2, 3):
        low = underlying - n * em
        high = underlying + n * em
        print(f"  ±{n}x EM:             {low:.2f}  to  {high:.2f}")

    print("\nBands using σ multiples (heuristic):")
    for n in (1, 2, 3):
        low = underlying - n * sigma1
        high = underlying + n * sigma1
        print(f"  ±{n}σ:                {low:.2f}  to  {high:.2f}")

    print()
    return 0


if __name__ == "__main__":
    # CLI:
    #   python spxw_expected_move.py [target_dte] [sigma_factor]
    dte = 1
    sf = 0.90

    if len(sys.argv) >= 2:
        dte = int(sys.argv[1])
    if len(sys.argv) >= 3:
        sf = float(sys.argv[2])

    raise SystemExit(main(target_dte=dte, sigma_factor=sf))