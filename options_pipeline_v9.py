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
def _clean_chain(df_raw: pd.DataFrame, opt_type: str,
                 exp_date, T: float, spot: float, today,
                 ticker_symbol: str) -> pd.DataFrame:
    """
    Fully vectorized chain cleaner.  Never calls iterrows() or any
    per-cell Python cast, so nullable Int64 / pd.NA values cannot
    trigger "cannot convert float NaN to integer".

    Steps
    -----
    1. Hard-cast every column we care about to float64 via numpy.
       np.array(..., dtype=float) turns pd.NA / None / NaN all into np.nan.
    2. Apply volume filter as a boolean mask.
    3. Compute Greeks row-wise with numpy ufuncs.
    4. Return a tidy DataFrame with SQL-ready Python-native types.
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    def to_f64(series_or_col: str):
        """Extract column as a plain numpy float64 array (NaN for missing)."""
        col = df_raw[series_or_col] if isinstance(series_or_col, str) else series_or_col
        return np.array(col.tolist(), dtype=float)   # .tolist() converts pd.NA -> None -> np.nan

    # ── 1. Extract columns as clean float64 arrays ────────────────
    strike = to_f64("strike")
    volume = to_f64("volume")
    oi     = to_f64("openInterest")
    iv     = to_f64("impliedVolatility")

    n = len(strike)
    if n == 0:
        return pd.DataFrame()

    # ── 2. Volume filter (vectorized) ─────────────────────────────
    vol_ok = np.where(np.isnan(volume), 0, volume) > MIN_VOLUME
    if not vol_ok.any():
        return pd.DataFrame()

    strike = strike[vol_ok]
    volume = volume[vol_ok]
    oi     = oi[vol_ok]
    iv     = iv[vol_ok]
    n      = len(strike)

    # ── 3. Compute Black-Scholes Greeks (vectorized) ───────────────
    sigma = np.where(np.isnan(iv) | (iv <= 0), np.nan, iv)
    valid = (~np.isnan(sigma)) & (~np.isnan(strike)) & (strike > 0) & (T > 0)

    delta_arr = np.full(n, np.nan)
    theta_arr = np.full(n, np.nan)

    if valid.any():
        S  = spot
        K  = strike[valid]
        s  = sigma[valid]
        r  = RISK_FREE_RATE

        d1 = (np.log(S / K) + (r + 0.5 * s**2) * T) / (s * np.sqrt(T))
        d2 = d1 - s * np.sqrt(T)

        from scipy.stats import norm as _norm
        if opt_type == "call":
            d = _norm.cdf(d1)
            th = (-(S * _norm.pdf(d1) * s) / (2 * np.sqrt(T))
                  - r * K * np.exp(-r * T) * _norm.cdf(d2)) / TRADING_DAYS_PER_YEAR
        else:
            d = _norm.cdf(d1) - 1
            th = (-(S * _norm.pdf(d1) * s) / (2 * np.sqrt(T))
                  + r * K * np.exp(-r * T) * _norm.cdf(-d2)) / TRADING_DAYS_PER_YEAR

        delta_arr[valid] = d
        theta_arr[valid] = th

    # ── 4. Clamp extreme values ────────────────────────────────────
    def clamp(arr, limit):
        return np.where(np.abs(arr) > limit, np.nan, arr)

    strike    = clamp(strike,    MAX_DECIMAL_18_4)
    iv        = clamp(iv,        MAX_DECIMAL_10_6)
    delta_arr = clamp(delta_arr, MAX_DECIMAL_10_6)
    theta_arr = clamp(theta_arr, MAX_DECIMAL_10_6)

    # ── 5. Build output DataFrame with Python-native types ─────────
    def to_decimal_col(arr, p, s):
        return [to_sql_decimal(v, p, s) for v in arr]

    def to_int_col(arr):
        # np.nan  -> None,  valid float -> int
        return [None if np.isnan(v) else int(v) for v in arr]

    df_out = pd.DataFrame({
        "ticker":        ticker_symbol,
        "as_of_date":    today,
        "expiry_date":   exp_date,
        "option_type":   opt_type,
        "strike_price":  to_decimal_col(strike,    18, 4),
        "volume":        to_int_col(volume),
        "open_interest": to_int_col(oi),
        "implied_vol":   to_decimal_col(iv,        10, 6),
        "delta":         to_decimal_col(delta_arr, 10, 6),
        "theta":         to_decimal_col(theta_arr, 10, 6),
        "spot_price":    to_sql_decimal(spot, 18, 4),
    })
    return df_out


# ticker_symbol is used inside _clean_chain via closure — keep it as
# a parameter threaded through fetch_options below.
def fetch_options(ticker_symbol: str) -> pd.DataFrame:
    yf_ticker = yf.Ticker(ticker_symbol)

    spot = _safe_float(yf_ticker.fast_info.last_price)
    if spot is None or spot <= 0:
        raise ValueError(f"Could not retrieve a valid spot price for {ticker_symbol}")
    log.info("  Spot price: %.4f", spot)

    today        = datetime.date.today()
    one_year_out = today + datetime.timedelta(days=365)
    expirations  = [
        d for d in yf_ticker.options
        if datetime.date.fromisoformat(d) <= one_year_out
    ]
    log.info("  Expiry dates within 1 year: %d", len(expirations))

    frames = []
    for exp_str in expirations:
        exp_date = datetime.date.fromisoformat(exp_str)
        T        = max((exp_date - today).days / 365.0, 1e-6)
        chain    = yf_ticker.option_chain(exp_str)

        for opt_type, df_raw in [("call", chain.calls), ("put", chain.puts)]:
            df = _clean_chain(df_raw, opt_type, exp_date, T, spot, today, ticker_symbol)
            if not df.empty:
                frames.append(df)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    log.info("  Rows to insert: %d", len(result))
    return result

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


def _validate_record(r: dict) -> tuple | None:
    """
    Build one insert tuple from a record dict, validating every value.
    Returns None and logs a warning if any non-nullable field is bad.
    All Decimal columns are re-validated here as a final safety net —
    this catches anything that slipped through _clean_chain.
    """
    ticker     = str(r.get("ticker", "") or "").strip()
    as_of_date = r.get("as_of_date")
    exp_date   = r.get("expiry_date")
    opt_type   = str(r.get("option_type", "") or "").strip()
    strike     = r.get("strike_price")
    spot       = r.get("spot_price")

    # Non-nullable fields must be present
    if not ticker or not as_of_date or not exp_date or not opt_type:
        log.warning("Skipping row – missing key field: %s", r)
        return None
    if strike is None or spot is None:
        log.warning("Skipping row – NULL strike or spot: ticker=%s exp=%s strike=%s",
                    ticker, exp_date, strike)
        return None

    # Re-run to_sql_decimal on every Decimal column as a final guard.
    # If a value is already a valid Decimal this is a cheap no-op.
    def _dec(val, p, s):
        if isinstance(val, Decimal):
            # Still check it fits the column
            limit = Decimal(10 ** (p - s)) - Decimal(10 ** -s)
            if abs(val) > limit:
                log.warning("Decimal %s exceeds DECIMAL(%d,%d) – NULL", val, p, s)
                return None
            return val
        return to_sql_decimal(val, p, s)

    strike_d = _dec(strike, 18, 4)
    spot_d   = _dec(spot,   18, 4)
    iv_d     = _dec(r.get("implied_vol"), 10, 6)
    delta_d  = _dec(r.get("delta"),       10, 6)
    theta_d  = _dec(r.get("theta"),       10, 6)

    if strike_d is None or spot_d is None:
        log.warning("Skipping row – strike/spot out of DECIMAL range: %s %s", strike, spot)
        return None

    # Integer columns: must be Python int or None — never numpy int / pd.NA
    def _int(val):
        if val is None:
            return None
        try:
            v = float(val)
            return None if (math.isnan(v) or math.isinf(v)) else int(v)
        except (TypeError, ValueError):
            return None

    return (
        ticker,
        as_of_date,
        exp_date,
        opt_type,
        strike_d,
        _int(r.get("volume")),
        _int(r.get("open_interest")),
        iv_d,
        spot_d,
        delta_d,
        theta_d,
    )


def insert_data(conn, df: pd.DataFrame):
    """Upsert a DataFrame of validated option rows into dbo.OptionsData."""
    # Build and validate every record BEFORE touching the cursor.
    records = []
    skipped = 0
    for r in df.to_dict("records"):
        rec = _validate_record(r)
        if rec is None:
            skipped += 1
        else:
            records.append(rec)

    if skipped:
        log.warning("  Skipped %d rows that failed final validation.", skipped)
    if not records:
        log.warning("  No valid records to insert.")
        return

    cursor = conn.cursor()
    # fast_executemany = True makes pyodbc infer SQL types from the FIRST row only.
    # If the first row has None in a Decimal column pyodbc picks the wrong type and
    # later rows with real values cause error 22003.
    # Solution: disable fast_executemany and insert in chunks instead.
    cursor.fast_executemany = False

    CHUNK = 500
    total = 0
    for i in range(0, len(records), CHUNK):
        chunk = records[i : i + CHUNK]
        try:
            cursor.executemany(UPSERT_SQL, chunk)
            conn.commit()
            total += len(chunk)
        except Exception as exc:
            conn.rollback()
            log.error("  Chunk %d–%d failed: %s", i, i + len(chunk), exc)
            # Row-by-row fallback so one bad row does not lose the whole chunk
            for j, rec in enumerate(chunk):
                try:
                    cursor.execute(UPSERT_SQL, rec)
                    conn.commit()
                    total += 1
                except Exception as row_exc:
                    conn.rollback()
                    log.error("  Row %d skipped – %s | values: %s", i + j, row_exc, rec)

    log.info("  Upserted %d / %d rows into dbo.OptionsData.", total, len(records))


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
