"""
VIX & SPY Expected Move Calculator
------------------------------------------
Fetches live VIX and SPY (SPDR S&P 500 ETF) data,
calculates expected moves, and computes 1, 2, and 3 standard deviation levels.

Requirements:
    pip install yfinance
"""

import yfinance as yf
import math
from datetime import datetime


def round_to_5(value: float) -> int:
    """Round a price level to the nearest 5 points (e.g. 5, 10, 15, 20...)."""
    return int(round(value / 5) * 5)


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


def get_spy():
    """Return SPY ETF value and change from previous close."""
    data = fetch_quote("SPY")
    spy = data["price"]
    prev = data["prev_close"]
    change = spy - prev
    pct_change = (change / prev) * 100
    return {
        "current": round(spy, 2),
        "prev_close": round(prev, 2),
        "change_pts": round(change, 2),
        "change_pct": round(pct_change, 2),
    }


def expected_move(price: float, vix: float, days: int = 1) -> dict:
    """
    Calculate expected move over `days` trading days.

    Formula:
        Daily Expected Move = Price * (VIX/100) / sqrt(252)
        N-day Expected Move = Daily EM * sqrt(days)

    Returns the move in points and as a percentage.
    """
    daily_em = price * (vix / 100) / math.sqrt(252)
    em = daily_em * math.sqrt(days)
    em_pct = (em / price) * 100
    return {
        "points": round(em, 2),
        "percent": round(em_pct, 4),
    }


def std_deviation_levels(price: float, em_1sd: float) -> dict:
    """
    Compute 1, 2, and 3 standard deviation price levels (up and down).

    1 SD  = 1x expected move
    2 SD  = 2x expected move
    3 SD  = 3x expected move
    """
    levels = {}
    for n in [1, 2, 3]:
        move = em_1sd * n
        upside = round_to_5(price + move)
        downside = round_to_5(price - move)
        levels[f"{n}SD"] = {
            "move_pts": round(move, 2),
            "move_pct": round((move / price) * 100, 4),
            "upside": upside,
            "downside": downside,
            "upside_plus50": upside + 50,
            "downside_minus50": downside - 50,
        }
    return levels


def print_separator(char="─", width=80):
    print(char * width)


def format_change(value):
    arrow = "▲" if value >= 0 else "▼"
    sign = "+" if value >= 0 else ""
    return f"{arrow} {sign}{value}"


def main():
    print()
    print_separator("═", width=80)
    print("  📊  VIX & SPY EXPECTED MOVE CALCULATOR")
    print(f"  {datetime.now().strftime('%A, %B %d %Y  %H:%M:%S')}")
    print_separator("═", width=80)

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

    # ── SPY ETF ────────────────────────────────────────────
    print("\n[ SPY — SPDR S&P 500 ETF ]")
    print_separator()
    spy = get_spy()
    print(f"  Current :  {spy['current']:,.2f}")
    print(f"  Prev Close:{spy['prev_close']:,.2f}")
    print(f"  Change  :  {format_change(spy['change_pts'])} pts  "
          f"({format_change(spy['change_pct'])}%)")

    # ── Expected Move ─────────────────────────────────────────
    print("\n[ Expected Move (based on VIX) ]")
    print_separator()

    for label, days in [("Daily (1 day)", 1), ("Weekly (5 days)", 5),
                        ("Monthly (21 days)", 21)]:
        em = expected_move(spy["current"], vix["current"], days)
        print(f"  {label:<20}  ±{em['points']:>8,.2f} pts  "
              f"(±{em['percent']:.2f}%)")

    # ── Standard Deviation Levels (Daily) ────────────────────
    print("\n[ Daily Standard Deviation Levels ]")
    print_separator()
    em_1d = expected_move(spy["current"], vix["current"], 1)
    sd_levels = std_deviation_levels(spy["current"], em_1d["points"])

    header = (
        f"  {'SD':<6}  {'Move Pts':>10}  {'Move %':>8}  "
        f"{'Upside':>10}  {'Up+50':>10}  "
        f"{'Downside':>10}  {'Down-50':>10}"
    )
    print(header)
    print_separator("-", width=80)

    def print_sd_row(label, data):
        print(
            f"  {label:<6}  "
            f"±{int(round(data['move_pts'])):>9,}  "
            f"±{data['move_pct']:>7.1f}%  "
            f"{data['upside']:>10,}  "
            f"{data['upside_plus50']:>10,}  "
            f"{data['downside']:>10,}  "
            f"{data['downside_minus50']:>10,}"
        )

    for label, data in sd_levels.items():
        print_sd_row(label, data)

    # ── Weekly Standard Deviation Levels ─────────────────────
    print("\n[ Weekly Standard Deviation Levels (5 days) ]")
    print_separator(width=80)
    em_1w = expected_move(spy["current"], vix["current"], 5)
    sd_levels_w = std_deviation_levels(spy["current"], em_1w["points"])

    print(header)
    print_separator("-", width=80)

    for label, data in sd_levels_w.items():
        print_sd_row(label, data)

    print()
    print_separator("═", width=80)
    print("  Data via Yahoo Finance (yfinance).  For reference only.")
    print_separator("═", width=80)
    print()


if __name__ == "__main__":
    main()
