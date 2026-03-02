"""
VIX & SPX Expected Move Calculator
------------------------------------
Fetches live VIX and SPX data, calculates expected moves,
and computes 1, 2, and 3 standard deviation levels.

Requirements:
    pip install yfinance requests
"""

import yfinance as yf
import math
from datetime import datetime, timedelta


def fetch_quote(ticker: str) -> dict:
    """Fetch the latest quote for a ticker using yfinance."""
    t = yf.Ticker(ticker)
    info = t.fast_info
    price = info.last_price
    prev_close = info.previous_close
    return {"price": price, "prev_close": prev_close}


def get_vix():
    """Return VIX value and change from previous close."""
    data = fetch_quote("^VIX")
    vix = data["price"]
    prev = data["prev_close"]
    change = vix - prev
    pct_change = (change / prev) * 100
    return {
        "current": round(vix, 2),
        "prev_close": round(prev, 2),
        "change_pts": round(change, 2),
        "change_pct": round(pct_change, 2),
    }


def get_spx():
    """Return SPX value and change from previous close."""
    data = fetch_quote("^GSPC")
    spx = data["price"]
    prev = data["prev_close"]
    change = spx - prev
    pct_change = (change / prev) * 100
    return {
        "current": round(spx, 2),
        "prev_close": round(prev, 2),
        "change_pts": round(change, 2),
        "change_pct": round(pct_change, 2),
    }


def expected_move(spx_price: float, vix: float, days: int = 1) -> dict:
    """
    Calculate expected move for SPX over `days` trading days.

    Formula:
        Daily Expected Move = SPX * (VIX/100) / sqrt(252)
        N-day Expected Move = Daily EM * sqrt(days)

    Returns the move in points and as a percentage.
    """
    daily_em = spx_price * (vix / 100) / math.sqrt(252)
    em = daily_em * math.sqrt(days)
    em_pct = (em / spx_price) * 100
    return {
        "points": round(em, 2),
        "percent": round(em_pct, 4),
    }


def std_deviation_levels(spx_price: float, em_1sd: float) -> dict:
    """
    Compute 1, 2, and 3 standard deviation price levels (up and down).

    1 SD  = 1x expected move
    2 SD  = 2x expected move
    3 SD  = 3x expected move
    """
    levels = {}
    for n in [1, 2, 3]:
        move = em_1sd * n
        levels[f"{n}SD"] = {
            "move_pts": round(move, 2),
            "move_pct": round((move / spx_price) * 100, 4),
            "upside": round(spx_price + move, 2),
            "downside": round(spx_price - move, 2),
        }
    return levels


def print_separator(char="─", width=60):
    print(char * width)


def format_change(value):
    arrow = "▲" if value >= 0 else "▼"
    sign = "+" if value >= 0 else ""
    return f"{arrow} {sign}{value}"


def main():
    print()
    print_separator("═")
    print("  📊  VIX & SPX EXPECTED MOVE CALCULATOR")
    print(f"  {datetime.now().strftime('%A, %B %d %Y  %H:%M:%S')}")
    print_separator("═")

    # ── VIX ──────────────────────────────────────────────────
    print("\n[ VIX — CBOE Volatility Index ]")
    print_separator()
    vix = get_vix()
    print(f"  Current :  {vix['current']}")
    print(f"  Prev Close:{vix['prev_close']}")
    print(f"  Change  :  {format_change(vix['change_pts'])} pts  "
          f"({format_change(vix['change_pct'])}%)")

    # Interpret VIX level
    v = vix["current"]
    if v < 15:
        sentiment = "😌 Low volatility — markets are complacent"
    elif v < 20:
        sentiment = "🟡 Moderate — normal trading environment"
    elif v < 30:
        sentiment = "🟠 Elevated — uncertainty in the market"
    elif v < 40:
        sentiment = "🔴 High — significant fear / risk-off"
    else:
        sentiment = "🚨 Extreme — crisis-level volatility"
    print(f"  Sentiment: {sentiment}")

    # ── SPX ──────────────────────────────────────────────────
    print("\n[ SPX — S&P 500 Index ]")
    print_separator()
    spx = get_spx()
    print(f"  Current :  {spx['current']:,.2f}")
    print(f"  Prev Close:{spx['prev_close']:,.2f}")
    print(f"  Change  :  {format_change(spx['change_pts'])} pts  "
          f"({format_change(spx['change_pct'])}%)")

    # ── Expected Move ─────────────────────────────────────────
    print("\n[ Expected Move (based on VIX) ]")
    print_separator()

    for label, days in [("Daily (1 day)", 1), ("Weekly (5 days)", 5),
                        ("Monthly (21 days)", 21)]:
        em = expected_move(spx["current"], vix["current"], days)
        print(f"  {label:<20}  ±{em['points']:>8,.2f} pts  "
              f"(±{em['percent']:.2f}%)")

    # ── Standard Deviation Levels (Daily) ────────────────────
    print("\n[ Daily Standard Deviation Levels ]")
    print_separator()
    em_1d = expected_move(spx["current"], vix["current"], 1)
    sd_levels = std_deviation_levels(spx["current"], em_1d["points"])

    header = f"  {'SD':<6}  {'Move Pts':>10}  {'Move %':>8}  "
    header += f"{'Upside':>10}  {'Downside':>10}"
    print(header)
    print_separator("-")

    for label, data in sd_levels.items():
        print(
            f"  {label:<6}  "
            f"±{data['move_pts']:>9,.2f}  "
            f"±{data['move_pct']:>7.2f}%  "
            f"{data['upside']:>10,.2f}  "
            f"{data['downside']:>10,.2f}"
        )

    # ── Weekly Standard Deviation Levels ─────────────────────
    print("\n[ Weekly Standard Deviation Levels (5 days) ]")
    print_separator()
    em_1w = expected_move(spx["current"], vix["current"], 5)
    sd_levels_w = std_deviation_levels(spx["current"], em_1w["points"])

    print(header)
    print_separator("-")

    for label, data in sd_levels_w.items():
        print(
            f"  {label:<6}  "
            f"±{data['move_pts']:>9,.2f}  "
            f"±{data['move_pct']:>7.2f}%  "
            f"{data['upside']:>10,.2f}  "
            f"{data['downside']:>10,.2f}"
        )

    print()
    print_separator("═")
    print("  Data via Yahoo Finance (yfinance).  For reference only.")
    print_separator("═")
    print()


if __name__ == "__main__":
    main()
