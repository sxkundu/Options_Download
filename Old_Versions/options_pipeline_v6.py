"""
options_pipeline.py
Usage: python options_pipeline.py

Reads a list of tickers from dbo.TickerList in SQL Server, then fetches
option chains for each ticker and upserts the results into dbo.OptionsData.

Dependencies:
    pip install yfinance pandas pyodbc scipy numpy

Greeks (delta, theta) are calculated using Black-Scholes since yfinance
does not provide them directly.
"""

import math
import datetime
import logging
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import pyodbc
import yfinance as yf
import pandas as pd
import numpy as np
from scipy.stats import norm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
SQL_SERVER_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Options;"
    "Trusted_Connection=yes;"
    # For SQL auth replace the line above with:
    # "UID=sa;PWD=YourPassword;"
)

RISK_FREE_RATE        = 0.05    # annualised
TRADING_DAYS_PER_YEAR = 252
MIN_VOLUME            = 0    # rows with volume <= this are skipped
TICKER_TABLE          = "dbo.TickerList"

# ── Column precision limits (must match the DDL below) ────────────
# DECIMAL(18,4)  → max 99_999_999_999_999.9999
# DECIMAL(10,6)  → max 9999.999999
MAX_DECIMAL_18_4 = 99_999_999_999_999.9999
MAX_DECIMAL_10_6 = 9_999.999999


def to_sql_decimal(val, precision: int, scale: int):
    """
    Convert val to a Python Decimal rounded to `scale` places.
    Returns None for NaN / Inf / values exceeding the column's integer-part width.
    Pyodbc maps Python Decimal directly to SQL DECIMAL without type inference issues.
    """
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None

    max_integer_digits = precision - scale
    max_val = 10 ** max_integer_digits - (10 ** -scale)
    if abs(f) > max_val:
        log.warning("Value %.8g exceeds DECIMAL(%d,%d) range – stored as NULL", f, precision, scale)
        return None

    quantize_str = "1." + "0" * scale  # e.g. "1.000000" for scale=6
    try:
        return Decimal(str(f)).quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        log.warning("Could not convert %.8g to Decimal – stored as NULL", f)
        return None


# ─────────────────────────────────────────────
# SAFE NUMERIC HELPERS
# ─────────────────────────────────────────────
def _safe_float(val, max_abs=None) -> float | None:
    """Return a clean float or None if the value is missing/infinite/NaN."""
    try:
        v = float(val)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    if max_abs is not None and abs(v) > max_abs:
        log.warning("Value %.6g exceeds column limit %.6g – set to NULL", v, max_abs)
        return None
    return v


def _safe_int(val) -> int | None:
    try:
        v = float(val)
        if math.isnan(v) or math.isinf(v):
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────
# BLACK-SCHOLES GREEKS
# ─────────────────────────────────────────────
def bs_greeks(S: float, K: float, T: float, r: float, sigma: float, option_type: str):
    """Return (delta, theta) or (None, None) on any bad input."""
    if any(v is None for v in [S, K, T, sigma]):
        return (None, None)
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return (None, None)

    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)

        if option_type == "call":
            delta = norm.cdf(d1)
            theta = (
                -(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
                - r * K * math.exp(-r * T) * norm.cdf(d2)
            ) / TRADING_DAYS_PER_YEAR
        else:
            delta = norm.cdf(d1) - 1
            theta = (
                -(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
                + r * K * math.exp(-r * T) * norm.cdf(-d2)
            ) / TRADING_DAYS_PER_YEAR

        # Clamp to column precision before returning
        delta = _safe_float(delta, max_abs=MAX_DECIMAL_10_6)
        theta = _safe_float(theta, max_abs=MAX_DECIMAL_10_6)
        return (round(delta, 6) if delta is not None else None,
                round(theta, 6) if theta is not None else None)

    except Exception as exc:
        log.debug("bs_greeks failed (S=%s K=%s T=%s σ=%s): %s", S, K, T, sigma, exc)
        return (None, None)


# ─────────────────────────────────────────────
# FETCH OPTIONS DATA FOR A SINGLE TICKER
# ─────────────────────────────────────────────
def fetch_options(ticker_symbol: str) -> pd.DataFrame:
    ticker = yf.Ticker(ticker_symbol)

    spot = _safe_float(ticker.fast_info.last_price)
    if spot is None or spot <= 0:
        raise ValueError(f"Could not retrieve a valid spot price for {ticker_symbol}")
    log.info("  Spot price: %.4f", spot)

    today        = datetime.date.today()
    one_year_out = today + datetime.timedelta(days=365)
    expirations  = [
        d for d in ticker.options
        if datetime.date.fromisoformat(d) <= one_year_out
    ]
    log.info("  Expiry dates within 1 year: %d", len(expirations))

    skipped_vol   = 0
    skipped_bad   = 0
    rows          = []

    for exp_str in expirations:
        exp_date = datetime.date.fromisoformat(exp_str)
        T        = (exp_date - today).days / 365.0
        chain    = ticker.option_chain(exp_str)

        for opt_type, df_raw in [("call", chain.calls), ("put", chain.puts)]:
            # Coerce numeric columns to float so pandas never tries to cast
            # NaN to int -- eliminates the "cannot convert NaN to integer" warning
            df = df_raw.copy()
            for col in ["strike", "volume", "openInterest", "impliedVolatility"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            for _, row in df.iterrows():

                # ── Raw values ────────────────────────────────────
                strike        = _safe_float(row.get("strike"),           max_abs=MAX_DECIMAL_18_4)
                volume        = _safe_int(row.get("volume"))
                open_interest = _safe_int(row.get("openInterest"))
                iv            = _safe_float(row.get("impliedVolatility"), max_abs=MAX_DECIMAL_10_6)
                spot_safe     = _safe_float(spot,                         max_abs=MAX_DECIMAL_18_4)

                # ── Skip if essential fields are missing/bad ──────
                if strike is None or spot_safe is None:
                    skipped_bad += 1
                    continue

                # ── Volume filter ─────────────────────────────────
                if (volume or 0) <= MIN_VOLUME:
                    skipped_vol += 1
                    continue

                # ── Greeks ────────────────────────────────────────
                delta, theta = bs_greeks(
                    S=spot_safe, K=strike, T=T,
                    r=RISK_FREE_RATE,
                    sigma=iv if iv is not None else 0.0,
                    option_type=opt_type,
                )

                rows.append({
                    "ticker":        ticker_symbol.upper(),
                    "expiry_date":   exp_date,
                    "option_type":   opt_type,
                    "strike_price":  to_sql_decimal(strike,    18, 4),
                    "volume":        volume,
                    "open_interest": open_interest,
                    "implied_vol":   to_sql_decimal(iv,        10, 6),
                    "delta":         to_sql_decimal(delta,     10, 6),
                    "theta":         to_sql_decimal(theta,     10, 6),
                    "spot_price":    to_sql_decimal(spot_safe, 18, 4),
                    "as_of_date":    today,
                })

    log.info("  Rows kept: %d  |  skipped (volume): %d  |  skipped (bad data): %d",
             len(rows), skipped_vol, skipped_bad)
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# SQL SERVER - SCHEMA DDL
# ─────────────────────────────────────────────
TICKER_TABLE_DDL = """\
IF OBJECT_ID('dbo.TickerList', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.TickerList
    (
        TickerId    INT             IDENTITY(1,1) NOT NULL,
        Ticker      NVARCHAR(10)    NOT NULL,
        IsActive    BIT             NOT NULL DEFAULT 1,
        AddedDate   DATE            NOT NULL DEFAULT CAST(GETDATE() AS DATE),
        Notes       NVARCHAR(255)   NULL,
        CONSTRAINT PK_TickerList        PRIMARY KEY (TickerId),
        CONSTRAINT UQ_TickerList_Ticker UNIQUE (Ticker)
    )
    INSERT INTO dbo.TickerList (Ticker, IsActive) VALUES
        ('AAPL', 1), ('MSFT', 1), ('TSLA', 1), ('SPY', 1), ('NVDA', 1)
END
"""

OPTIONS_TABLE_DDL = """\
IF OBJECT_ID('dbo.OptionsData', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.OptionsData
    (
        OptionId        BIGINT           IDENTITY(1,1)   NOT NULL,

        -- Business key
        Ticker          NVARCHAR(10)     NOT NULL,
        AsOfDate        DATE             NOT NULL,
        ExpiryDate      DATE             NOT NULL,
        OptionType      NCHAR(4)         NOT NULL,        -- 'call' | 'put '
        StrikePrice     DECIMAL(18, 4)   NOT NULL,        -- up to 99 trillion

        -- Market data
        Volume          INT              NULL,
        OpenInterest    INT              NULL,
        ImpliedVol      DECIMAL(10, 6)   NULL,            -- e.g. 0.250000 = 25%
        SpotPrice       DECIMAL(18, 4)   NOT NULL,

        -- Greeks  (DECIMAL(10,6) => max ±9999.999999)
        Delta           DECIMAL(10, 6)   NULL,
        Theta           DECIMAL(10, 6)   NULL,

        -- Temporal columns – maintained automatically by SQL Server
        SysStartTime    DATETIME2(7)     GENERATED ALWAYS AS ROW START NOT NULL,
        SysEndTime      DATETIME2(7)     GENERATED ALWAYS AS ROW END   NOT NULL,

        CONSTRAINT PK_OptionsData        PRIMARY KEY CLUSTERED (OptionId),
        CONSTRAINT UQ_OptionsData_BizKey UNIQUE (Ticker, AsOfDate, ExpiryDate, OptionType, StrikePrice),
        PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)
    )
    WITH (SYSTEM_VERSIONING = ON
          (HISTORY_TABLE = dbo.OptionsDataHistory, DATA_CONSISTENCY_CHECK = ON))

    CREATE NONCLUSTERED INDEX IX_OptionsData_Ticker_AsOf
        ON dbo.OptionsData (Ticker, AsOfDate)
        INCLUDE (ExpiryDate, OptionType, StrikePrice, Delta, Theta)

    CREATE NONCLUSTERED INDEX IX_OptionsData_Expiry
        ON dbo.OptionsData (ExpiryDate)
        INCLUDE (Ticker, OptionType, StrikePrice)
END
"""

UPSERT_SQL = """\
MERGE dbo.OptionsData AS target
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?))
    AS source (Ticker, AsOfDate, ExpiryDate, OptionType, StrikePrice,
               Volume, OpenInterest, ImpliedVol, SpotPrice, Delta, Theta)
ON  target.Ticker      = source.Ticker
AND target.AsOfDate    = source.AsOfDate
AND target.ExpiryDate  = source.ExpiryDate
AND target.OptionType  = source.OptionType
AND target.StrikePrice = source.StrikePrice
WHEN MATCHED THEN
    UPDATE SET
        Volume        = source.Volume,
        OpenInterest  = source.OpenInterest,
        ImpliedVol    = source.ImpliedVol,
        SpotPrice     = source.SpotPrice,
        Delta         = source.Delta,
        Theta         = source.Theta
WHEN NOT MATCHED THEN
    INSERT (Ticker, AsOfDate, ExpiryDate, OptionType, StrikePrice,
            Volume, OpenInterest, ImpliedVol, SpotPrice, Delta, Theta)
    VALUES (source.Ticker, source.AsOfDate, source.ExpiryDate,
            source.OptionType, source.StrikePrice,
            source.Volume, source.OpenInterest, source.ImpliedVol,
            source.SpotPrice, source.Delta, source.Theta);
"""


# ─────────────────────────────────────────────
# SQL SERVER - HELPERS
# ─────────────────────────────────────────────
def create_schema(conn):
    cursor = conn.cursor()
    for ddl in [TICKER_TABLE_DDL, OPTIONS_TABLE_DDL]:
        cursor.execute(ddl)
    conn.commit()
    log.info("Schema created / verified.")


def fetch_tickers(conn) -> list:
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT Ticker FROM {TICKER_TABLE} WHERE IsActive = 1 ORDER BY Ticker"
    )
    tickers = [row[0].strip().upper() for row in cursor.fetchall()]
    if not tickers:
        raise ValueError(
            f"No active tickers in {TICKER_TABLE}. Set IsActive = 1 for at least one row."
        )
    log.info("Loaded %d ticker(s): %s", len(tickers), ", ".join(tickers))
    return tickers


def insert_data(conn, df: pd.DataFrame):
    """Upsert a DataFrame of validated option rows into dbo.OptionsData."""
    cursor = conn.cursor()
    cursor.fast_executemany = True

    records = []
    for _, row in df.iterrows():
        records.append((
            str(row["ticker"]),                          # NVARCHAR
            row["as_of_date"],                           # DATE
            row["expiry_date"],                          # DATE
            str(row["option_type"]),                     # NCHAR(4)
            row["strike_price"],                         # Decimal(18,4)
            int(row["volume"])        if row["volume"]        is not None else None,  # INT
            int(row["open_interest"]) if row["open_interest"] is not None else None,  # INT
            row["implied_vol"],                          # Decimal(10,6)
            row["spot_price"],                           # Decimal(18,4)
            row["delta"],                                # Decimal(10,6)
            row["theta"],                                # Decimal(10,6)
        ))

    cursor.executemany(UPSERT_SQL, records)
    conn.commit()
    log.info("  Upserted %d rows into dbo.OptionsData.", len(records))


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    log.info("=== Connecting to SQL Server ===")
    conn = pyodbc.connect(SQL_SERVER_CONN)

    create_schema(conn)

    log.info("=== Loading tickers from %s ===", TICKER_TABLE)
    tickers = fetch_tickers(conn)

    total_rows = 0
    failed     = []

    for i, ticker_symbol in enumerate(tickers, start=1):
        log.info("[%d/%d] Processing %s ...", i, len(tickers), ticker_symbol)
        try:
            df = fetch_options(ticker_symbol)
            if df.empty:
                log.warning("  No rows passed filters for %s – skipping insert.", ticker_symbol)
                continue
            insert_data(conn, df)
            total_rows += len(df)
        except Exception as exc:
            log.error("  FAILED %s: %s", ticker_symbol, exc)
            failed.append((ticker_symbol, str(exc)))

    conn.close()

    log.info("=" * 52)
    log.info("Pipeline complete.")
    log.info("  Tickers attempted  : %d", len(tickers))
    log.info("  Tickers succeeded  : %d", len(tickers) - len(failed))
    log.info("  Total rows upserted: %d", total_rows)
    if failed:
        log.warning("  Failed tickers (%d):", len(failed))
        for sym, err in failed:
            log.warning("    %s: %s", sym, err)
    log.info("=" * 52)


if __name__ == "__main__":
    main()
