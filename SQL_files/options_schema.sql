-- ================================================================
-- OptionsDB Schema  (SQL Server 2016+)
-- Run once to create all tables.
-- ================================================================

USE OptionsDB;
GO

-- ================================================================
-- TABLE 1: dbo.TickerList
-- ================================================================
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
    );

    INSERT INTO dbo.TickerList (Ticker, IsActive, Notes) VALUES
        ('AAPL', 1, 'Apple Inc.'),
        ('MSFT', 1, 'Microsoft Corp.'),
        ('TSLA', 1, 'Tesla Inc.'),
        ('SPY',  1, 'S&P 500 ETF'),
        ('NVDA', 1, 'NVIDIA Corp.');

    PRINT 'dbo.TickerList created and seeded.';
END
ELSE
    PRINT 'dbo.TickerList already exists – skipped.';
GO

-- ================================================================
-- TABLE 2: dbo.OptionsData  (system-versioned / temporal)
--
-- Column sizing rationale
-- -----------------------
-- StrikePrice / SpotPrice  DECIMAL(18,4)  up to 99,999,999,999,999.9999
-- ImpliedVol               DECIMAL(10,6)  up to 9999.999999  (9999 = 999900%)
-- Delta                    DECIMAL(10,6)  range [-1, 1] with plenty of headroom
-- Theta                    DECIMAL(10,6)  per-day decay; typically small but
--                                         can spike for near-expiry deep ITM;
--                                         Python clamps extreme values to NULL
-- ================================================================
IF OBJECT_ID('dbo.OptionsData', 'U') IS NOT NULL
BEGIN
    ALTER TABLE dbo.OptionsData SET (SYSTEM_VERSIONING = OFF);
    DROP TABLE IF EXISTS dbo.OptionsDataHistory;
    DROP TABLE dbo.OptionsData;
    PRINT 'Dropped existing dbo.OptionsData + history.';
END
GO

CREATE TABLE dbo.OptionsData
(
    -- Surrogate PK
    OptionId        BIGINT          IDENTITY(1,1)   NOT NULL,

    -- Natural / business key
    Ticker          NVARCHAR(10)    NOT NULL,
    AsOfDate        DATE            NOT NULL,        -- date data was pulled
    ExpiryDate      DATE            NOT NULL,        -- option expiry
    OptionType      NCHAR(4)        NOT NULL,        -- 'call' or 'put '
    StrikePrice     DECIMAL(18, 4)  NOT NULL,        -- max ~99 trillion

    -- Market data
    Volume          INT             NULL,
    OpenInterest    INT             NULL,
    ImpliedVol      DECIMAL(10, 6)  NULL,            -- 0.25 = 25%; NULL if invalid
    SpotPrice       DECIMAL(18, 4)  NOT NULL,

    -- Greeks  (Python sets to NULL if value exceeds ±9999.999999)
    Delta           DECIMAL(10, 6)  NULL,
    Theta           DECIMAL(10, 6)  NULL,

    -- Temporal columns – SQL Server maintains these automatically
    SysStartTime    DATETIME2(7)    GENERATED ALWAYS AS ROW START  NOT NULL,
    SysEndTime      DATETIME2(7)    GENERATED ALWAYS AS ROW END    NOT NULL,

    CONSTRAINT PK_OptionsData
        PRIMARY KEY CLUSTERED (OptionId),

    CONSTRAINT UQ_OptionsData_BizKey
        UNIQUE (Ticker, AsOfDate, ExpiryDate, OptionType, StrikePrice),

    CONSTRAINT FK_OptionsData_Ticker
        FOREIGN KEY (Ticker) REFERENCES dbo.TickerList (Ticker),

    PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime)
)
WITH
(
    SYSTEM_VERSIONING = ON
    (
        HISTORY_TABLE          = dbo.OptionsDataHistory,
        DATA_CONSISTENCY_CHECK = ON
    )
);
GO

CREATE NONCLUSTERED INDEX IX_OptionsData_Ticker_AsOf
    ON dbo.OptionsData (Ticker, AsOfDate)
    INCLUDE (ExpiryDate, OptionType, StrikePrice, Delta, Theta);

CREATE NONCLUSTERED INDEX IX_OptionsData_Expiry
    ON dbo.OptionsData (ExpiryDate)
    INCLUDE (Ticker, OptionType, StrikePrice);
GO

PRINT 'dbo.OptionsData (temporal) created successfully.';
GO

-- ================================================================
-- MANAGING TICKERS
-- ================================================================
-- Add    : INSERT INTO dbo.TickerList (Ticker, IsActive) VALUES ('AMZN', 1);
-- Pause  : UPDATE dbo.TickerList SET IsActive = 0 WHERE Ticker = 'TSLA';
-- Resume : UPDATE dbo.TickerList SET IsActive = 1 WHERE Ticker = 'TSLA';
-- List   : SELECT * FROM dbo.TickerList ORDER BY Ticker;

-- ================================================================
-- EXAMPLE TEMPORAL QUERIES
-- ================================================================
-- Current data:
--   SELECT * FROM dbo.OptionsData WHERE AsOfDate = CAST(GETDATE() AS DATE);
--
-- Point-in-time snapshot:
--   SELECT * FROM dbo.OptionsData
--   FOR SYSTEM_TIME AS OF '2024-06-01 00:00:00'
--   WHERE Ticker = 'AAPL';
--
-- Full history for one contract:
--   SELECT OptionId, StrikePrice, Delta, Theta, SysStartTime, SysEndTime
--   FROM dbo.OptionsData FOR SYSTEM_TIME ALL
--   WHERE Ticker = 'AAPL' AND ExpiryDate = '2025-01-17' AND OptionType = 'call'
--   ORDER BY SysStartTime;
