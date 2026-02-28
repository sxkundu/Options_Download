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
import pyodbc
import yfinance as yf
import pandas as pd
import numpy as np
from scipy.stats import norm

# ─────────────────────────────────────────────
# CONFIG – update connection string as needed
# ─────────────────────────────────────────────
SQL_SERVER_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Options;"
    "Trusted_Connection=yes;"
    # For SQL auth, replace the two lines above with:
    # "UID=sa;PWD=YourPassword;"
)

RISK_FREE_RATE        = 0.05    # annualised, e.g. 5%
TRADING_DAYS_PER_YEAR = 252
MIN_VOLUME            = 0    # only include options with volume > this threshold
TICKER_TABLE          = "dbo.TickerList"   # table that holds the tickers to process


# ─────────────────────────────────────────────
# BLACK-SCHOLES GREEKS
# ─────────────────────────────────────────────
def bs_greeks(S: float, K: float, T: float, r: float, sigma: float, option_type: str):
    """
    Returns (delta, theta) for a European option.
    S           - spot price
    K           - strike
    T           - time to expiry in years (> 0)
    r           - risk-free rate
    sigma       - implied volatility (decimal)
    option_type - 'call' or 'put'
    """
    if T <= 0 or sigma <= 0 or S <= 0:
        return (None, None)

    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)

    if option_type == "call":
        delta = norm.cdf(d1)
        theta = (
            -(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
            - r * K * math.exp(-r * T) * norm.cdf(d2)
        ) / TRADING_DAYS_PER_YEAR           # per calendar day
    else:
        delta = norm.cdf(d1) - 1
        theta = (
            -(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
            + r * K * math.exp(-r * T) * norm.cdf(-d2)
        ) / TRADING_DAYS_PER_YEAR

    return (round(delta, 6), round(theta, 6))


# ─────────────────────────────────────────────
# FETCH OPTIONS DATA FOR A SINGLE TICKER
# ─────────────────────────────────────────────
def fetch_options(ticker_symbol: str) -> pd.DataFrame:
    ticker = yf.Ticker(ticker_symbol)

    # Current spot price
    info = ticker.fast_info
    spot = info.last_price
    if spot is None or spot <= 0:
        raise ValueError(f"Could not retrieve spot price for {ticker_symbol}")

    print(f"  Spot price: {spot:.2f}")

    # Filter expiry dates within 1 year
    today        = datetime.date.today()
    one_year_out = today + datetime.timedelta(days=365)
    expirations  = [
        d for d in ticker.options
        if datetime.date.fromisoformat(d) <= one_year_out
    ]
    print(f"  Expiry dates within 1 year: {len(expirations)}")

    rows = []
    for exp_str in expirations:
        exp_date = datetime.date.fromisoformat(exp_str)
        T        = (exp_date - today).days / 365.0
        chain    = ticker.option_chain(exp_str)

        for opt_type, df in [("call", chain.calls), ("put", chain.puts)]:
            for _, row in df.iterrows():
                strike        = row.get("strike")
                volume        = row.get("volume")
                open_interest = row.get("openInterest")
                iv            = row.get("impliedVolatility")

                # Volume filter
                vol_value = volume if (volume and not math.isnan(float(volume))) else 0
                if vol_value <= MIN_VOLUME:
                    continue

                delta, theta = bs_greeks(
                    S=spot, K=strike, T=T,
                    r=RISK_FREE_RATE,
                    sigma=iv if (iv and not math.isnan(iv)) else 0.0,
                    option_type=opt_type,
                )

                rows.append({
                    "ticker":        ticker_symbol.upper(),
                    "expiry_date":   exp_date,
                    "option_type":   opt_type,
                    "strike_price":  strike,
                    "volume":        int(volume)        if (volume        and not math.isnan(volume))        else None,
                    "open_interest": int(open_interest) if (open_interest and not math.isnan(open_interest)) else None,
                    "implied_vol":   round(iv, 6)       if (iv            and not math.isnan(iv))            else None,
                    "delta":         delta,
                    "theta":         theta,
                    "spot_price":    round(spot, 4),
                    "as_of_date":    today,
                })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# SQL SERVER - SCHEMA DDL
# ─────────────────────────────────────────────

# Creates dbo.TickerList if it doesn't exist, and seeds it with examples
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
        CONSTRAINT PK_TickerList PRIMARY KEY (TickerId),
        CONSTRAINT UQ_TickerList_Ticker UNIQUE (Ticker)
    )
    INSERT INTO dbo.TickerList (Ticker, IsActive) VALUES
        ('AAPL', 1), ('MSFT', 1), ('TSLA', 1), ('SPY', 1), ('NVDA', 1)
END
"""

# Creates the temporal OptionsData table
OPTIONS_TABLE_DDL = """\
IF OBJECT_ID('dbo.OptionsData', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.OptionsData
    (
        OptionId        BIGINT          IDENTITY(1,1)   NOT NULL,
        Ticker          NVARCHAR(10)    NOT NULL,
        AsOfDate        DATE            NOT NULL,
        ExpiryDate      DATE            NOT NULL,
        OptionType      NCHAR(4)        NOT NULL,
        StrikePrice     DECIMAL(18, 4)  NOT NULL,
        Volume          INT             NULL,
        OpenInterest    INT             NULL,
        ImpliedVol      DECIMAL(10, 6)  NULL,
        SpotPrice       DECIMAL(18, 4)  NOT NULL,
        Delta           DECIMAL(10, 6)  NULL,
        Theta           DECIMAL(10, 6)  NULL,
        SysStartTime    DATETIME2       GENERATED ALWAYS AS ROW START NOT NULL,
        SysEndTime      DATETIME2       GENERATED ALWAYS AS ROW END   NOT NULL,
        CONSTRAINT PK_OptionsData     PRIMARY KEY (OptionId),
        CONSTRAINT UQ_OptionsData_Key UNIQUE (Ticker, AsOfDate, ExpiryDate, OptionType, StrikePrice),
        PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)
    )
    WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.OptionsDataHistory, DATA_CONSISTENCY_CHECK = ON))

    CREATE INDEX IX_OptionsData_Ticker_AsOf ON dbo.OptionsData (Ticker, AsOfDate)
    CREATE INDEX IX_OptionsData_Expiry      ON dbo.OptionsData (ExpiryDate)
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
    """Create TickerList and OptionsData tables if they do not already exist."""
    cursor = conn.cursor()
    for ddl in [TICKER_TABLE_DDL, OPTIONS_TABLE_DDL]:
        cursor.execute(ddl)
    conn.commit()
    print("Schema created / verified.")


def fetch_tickers(conn) -> list:
    """Read the list of active tickers from dbo.TickerList."""
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT Ticker FROM {TICKER_TABLE} WHERE IsActive = 1 ORDER BY Ticker"
    )
    tickers = [row[0].strip().upper() for row in cursor.fetchall()]
    if not tickers:
        raise ValueError(
            f"No active tickers found in {TICKER_TABLE}. "
            "Set IsActive = 1 for at least one row."
        )
    print(f"Loaded {len(tickers)} ticker(s): {', '.join(tickers)}")
    return tickers


def insert_data(conn, df: pd.DataFrame):
    """Upsert a DataFrame of option rows into dbo.OptionsData."""
    cursor = conn.cursor()
    cursor.fast_executemany = True

    records = [
        (
            row["ticker"],
            row["as_of_date"],
            row["expiry_date"],
            row["option_type"],
            row["strike_price"],
            row["volume"],
            row["open_interest"],
            row["implied_vol"],
            row["spot_price"],
            row["delta"],
            row["theta"],
        )
        for _, row in df.iterrows()
    ]

    cursor.executemany(UPSERT_SQL, records)
    conn.commit()
    print(f"  Upserted {len(records)} rows into dbo.OptionsData.")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    # 1. Open a single connection reused throughout the run
    print("\n=== Connecting to SQL Server ===")
    conn = pyodbc.connect(SQL_SERVER_CONN)

    # 2. Ensure schema exists (safe to re-run)
    create_schema(conn)

    # 3. Load ticker list from dbo.TickerList
    print("\n=== Loading tickers from dbo.TickerList ===")
    tickers = fetch_tickers(conn)

    # 4. Loop through each ticker
    total_rows = 0
    failed     = []

    for i, ticker_symbol in enumerate(tickers, start=1):
        print(f"\n[{i}/{len(tickers)}] Processing {ticker_symbol} ...")
        try:
            df = fetch_options(ticker_symbol)
            if df.empty:
                print(f"  No rows passed the volume filter (>{MIN_VOLUME}) – skipping insert.")
                continue
            insert_data(conn, df)
            total_rows += len(df)
        except Exception as exc:
            print(f"  ERROR: {exc}")
            failed.append((ticker_symbol, str(exc)))

    conn.close()

    # 5. Run summary
    print("\n" + "=" * 52)
    print("Pipeline complete.")
    print(f"  Tickers attempted : {len(tickers)}")
    print(f"  Tickers succeeded : {len(tickers) - len(failed)}")
    print(f"  Total rows upserted: {total_rows}")
    if failed:
        print(f"\n  Failed tickers ({len(failed)}):")
        for sym, err in failed:
            print(f"    {sym}: {err}")
    print("=" * 52)


if __name__ == "__main__":
    main()
