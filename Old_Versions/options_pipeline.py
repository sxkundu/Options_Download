"""
options_pipeline.py
Usage: python options_pipeline.py <TICKER>
Example: python options_pipeline.py AAPL

Dependencies:
    pip install yfinance pandas pyodbc scipy numpy

Greeks (delta, theta) are calculated using Black-Scholes since yfinance
does not provide them directly.
"""

import sys
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
    "DATABASE=OptionsDB;"
    "Trusted_Connection=yes;"
    # For SQL auth, replace the line above with:
    # "UID=sa;PWD=YourPassword;"
)

RISK_FREE_RATE = 0.05          # annualised, e.g. 5%
TRADING_DAYS_PER_YEAR = 252


# ─────────────────────────────────────────────
# BLACK-SCHOLES GREEKS
# ─────────────────────────────────────────────
def bs_greeks(S: float, K: float, T: float, r: float, sigma: float, option_type: str):
    """
    Returns (delta, theta) for a European option.
    S      – spot price
    K      – strike
    T      – time to expiry in years  (> 0)
    r      – risk-free rate
    sigma  – implied volatility (decimal)
    option_type – 'call' or 'put'
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
        ) / TRADING_DAYS_PER_YEAR          # per calendar day
    else:
        delta = norm.cdf(d1) - 1
        theta = (
            -(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
            + r * K * math.exp(-r * T) * norm.cdf(-d2)
        ) / TRADING_DAYS_PER_YEAR

    return (round(delta, 6), round(theta, 6))


# ─────────────────────────────────────────────
# FETCH OPTIONS DATA
# ─────────────────────────────────────────────
def fetch_options(ticker_symbol: str) -> pd.DataFrame:
    ticker = yf.Ticker(ticker_symbol)

    # Current spot price
    info = ticker.fast_info
    spot = info.last_price
    if spot is None or spot <= 0:
        raise ValueError(f"Could not retrieve spot price for {ticker_symbol}")

    print(f"Spot price for {ticker_symbol}: {spot:.2f}")

    # Filter expiry dates within 1 year
    today = datetime.date.today()
    one_year_out = today + datetime.timedelta(days=365)
    expirations = [
        d for d in ticker.options
        if datetime.date.fromisoformat(d) <= one_year_out
    ]

    print(f"Found {len(expirations)} expiry dates within 1 year.")

    rows = []
    for exp_str in expirations:
        exp_date = datetime.date.fromisoformat(exp_str)
        T = (exp_date - today).days / 365.0

        chain = ticker.option_chain(exp_str)

        for opt_type, df in [("call", chain.calls), ("put", chain.puts)]:
            for _, row in df.iterrows():
                strike        = row.get("strike")
                volume        = row.get("volume")
                open_interest = row.get("openInterest")
                iv            = row.get("impliedVolatility")

                delta, theta = bs_greeks(
                    S=spot, K=strike, T=T,
                    r=RISK_FREE_RATE,
                    sigma=iv if (iv and not math.isnan(iv)) else 0.0,
                    option_type=opt_type,
                )

                rows.append({
                    "ticker":          ticker_symbol.upper(),
                    "expiry_date":     exp_date,
                    "option_type":     opt_type,
                    "strike_price":    strike,
                    "volume":          int(volume)        if (volume        and not math.isnan(volume))        else None,
                    "open_interest":   int(open_interest) if (open_interest and not math.isnan(open_interest)) else None,
                    "implied_vol":     round(iv, 6)       if (iv            and not math.isnan(iv))            else None,
                    "delta":           delta,
                    "theta":           theta,
                    "spot_price":      round(spot, 4),
                    "as_of_date":      today,
                })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# SQL SERVER – SCHEMA DDL
# ─────────────────────────────────────────────
SCHEMA_DDL = """
-- ================================================================
-- Database: OptionsDB
-- ================================================================

-- 1. Staging / history table (required by temporal)
IF OBJECT_ID('dbo.OptionsDataHistory', 'U') IS NOT NULL
    DROP TABLE dbo.OptionsDataHistory;

-- 2. Temporal (system-versioned) table
IF OBJECT_ID('dbo.OptionsData', 'U') IS NOT NULL
BEGIN
    ALTER TABLE dbo.OptionsData SET (SYSTEM_VERSIONING = OFF);
    DROP TABLE dbo.OptionsData;
END

CREATE TABLE dbo.OptionsData
(
    -- Surrogate PK
    OptionId        BIGINT          IDENTITY(1,1)   NOT NULL,

    -- Business key
    Ticker          NVARCHAR(10)    NOT NULL,
    AsOfDate        DATE            NOT NULL,
    ExpiryDate      DATE            NOT NULL,
    OptionType      NCHAR(4)        NOT NULL,   -- 'call' | 'put'
    StrikePrice     DECIMAL(18, 4)  NOT NULL,

    -- Market data
    Volume          INT             NULL,
    OpenInterest    INT             NULL,
    ImpliedVol      DECIMAL(10, 6)  NULL,
    SpotPrice       DECIMAL(18, 4)  NOT NULL,

    -- Greeks
    Delta           DECIMAL(10, 6)  NULL,
    Theta           DECIMAL(10, 6)  NULL,

    -- Temporal columns (SQL Server manages these automatically)
    SysStartTime    DATETIME2       GENERATED ALWAYS AS ROW START  NOT NULL,
    SysEndTime      DATETIME2       GENERATED ALWAYS AS ROW END    NOT NULL,

    CONSTRAINT PK_OptionsData PRIMARY KEY (OptionId),
    CONSTRAINT UQ_OptionsData_Key UNIQUE (Ticker, AsOfDate, ExpiryDate, OptionType, StrikePrice),

    PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)
)
WITH
(
    SYSTEM_VERSIONING = ON
    (
        HISTORY_TABLE = dbo.OptionsDataHistory,
        DATA_CONSISTENCY_CHECK = ON
    )
);

-- Useful indexes
CREATE INDEX IX_OptionsData_Ticker_AsOf
    ON dbo.OptionsData (Ticker, AsOfDate);

CREATE INDEX IX_OptionsData_Expiry
    ON dbo.OptionsData (ExpiryDate);
"""

UPSERT_SQL = """
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
# SQL SERVER – INSERT
# ─────────────────────────────────────────────
def create_schema(conn):
    cursor = conn.cursor()
    # Execute each statement individually (DDL blocks can't use ; separation in pyodbc)
    for statement in SCHEMA_DDL.split(";"):
        stmt = statement.strip()
        if stmt:
            cursor.execute(stmt)
    conn.commit()
    print("Schema created / verified.")


def insert_data(conn, df: pd.DataFrame):
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
    print(f"Upserted {len(records)} rows into dbo.OptionsData.")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    if len(sys.argv) < 2:
        print("Usage: python options_pipeline.py <TICKER>")
        sys.exit(1)

    ticker_symbol = sys.argv[1].upper()

    # 1. Fetch data
    print(f"\n=== Fetching options for {ticker_symbol} ===")
    df = fetch_options(ticker_symbol)
    print(df.head(10000).to_string(index=False))
    print(f"\nTotal rows fetched: {len(df)}")

    # 2. Connect to SQL Server
    print("\n=== Connecting to SQL Server ===")
    #conn = pyodbc.connect(SQL_SERVER_CONN)

    # 3. Create schema (idempotent)
    #create_schema(conn)

    # 4. Insert / upsert data
    #insert_data(conn, df)

    #conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
