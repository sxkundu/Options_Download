USE [Options];
GO

IF OBJECT_ID('dbo.OpenInterestSpikes', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.OpenInterestSpikes
    (
        SpikeRunId              BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_OpenInterestSpikes PRIMARY KEY,
        RunAsOfDate             DATE NOT NULL,
        InsertedAtUtc           DATETIME2(0) NOT NULL CONSTRAINT DF_OpenInterestSpikes_InsertedAtUtc DEFAULT (SYSUTCDATETIME()),

        Ticker                  NVARCHAR(10) NOT NULL,
        OptionType              NCHAR(4) NOT NULL,
        StrikePrice             DECIMAL(18,4) NOT NULL,
        ExpiryDate              DATE NOT NULL,
        DaysToExpiry            INT NULL,

        AsOfDate_Prior          DATE NULL,
        AsOfDate_Today          DATE NOT NULL,
        DaysBetweenRecordings   INT NULL,

        OI_Prior                INT NULL,
        OI_Today                INT NULL,
        OI_Change               INT NULL,
        OI_Pct_Change           DECIMAL(10,2) NULL,
        OI_SpikeCategory        NVARCHAR(30) NULL,

        Volume_Prior            INT NULL,
        Volume_Today            INT NULL,
        Volume_Change           INT NULL,
        Volume_Pct_Change       DECIMAL(10,2) NULL,
        VolumeSurged            NVARCHAR(20) NULL,
        OI_Volume_Ratio         DECIMAL(10,2) NULL,

        IV_Prior_Pct            DECIMAL(10,4) NULL,
        IV_Today_Pct            DECIMAL(10,4) NULL,
        IV_Change_Pct_Points    DECIMAL(10,4) NULL,

        Delta_Today             DECIMAL(10,6) NULL,
        Theta_Today             DECIMAL(10,6) NULL,
        SpotPrice_Today         DECIMAL(18,4) NULL
    );

    -- Optional: prevent duplicates for the same run date + contract
    CREATE UNIQUE INDEX UX_OpenInterestSpikes_Run_Contract
    ON dbo.OpenInterestSpikes
    (
        RunAsOfDate, Ticker, ExpiryDate, OptionType, StrikePrice
    );
END
GO




USE [Options];
GO

DECLARE @RunDate DATE = (SELECT MAX(AsOfDate) FROM dbo.OptionsData);

-- 1) Temp table matches the PROC output EXACTLY (same columns, same order)
CREATE TABLE #Spikes
(
    Ticker                NVARCHAR(10) NOT NULL,
    OptionType            NCHAR(4)      NOT NULL,
    StrikePrice           DECIMAL(18,4) NOT NULL,
    ExpiryDate            DATE         NOT NULL,
    DaysToExpiry          INT          NULL,

    AsOfDate_Prior        DATE         NULL,
    AsOfDate_Today        DATE         NOT NULL,
    DaysBetweenRecordings INT          NULL,

    OI_Prior              INT          NULL,
    OI_Today              INT          NULL,
    OI_Change             INT          NULL,
    OI_Pct_Change         DECIMAL(10,2) NULL,
    OI_SpikeCategory      NVARCHAR(30) NULL,

    Volume_Prior          INT          NULL,
    Volume_Today          INT          NULL,
    Volume_Change         INT          NULL,
    Volume_Pct_Change     DECIMAL(10,2) NULL,
    VolumeSurged          NVARCHAR(20) NULL,
    OI_Volume_Ratio       DECIMAL(10,2) NULL,

    IV_Prior_Pct          DECIMAL(10,4) NULL,
    IV_Today_Pct          DECIMAL(10,4) NULL,
    IV_Change_Pct_Points  DECIMAL(10,4) NULL,

    Delta_Today           DECIMAL(10,6) NULL,
    Theta_Today           DECIMAL(10,6) NULL,
    SpotPrice_Today       DECIMAL(18,4) NULL
);

-- 2) Capture proc output
INSERT INTO #Spikes
EXEC dbo.usp_DetectOpenInterestSpikes
     @AsOfDate = @RunDate,
     @MinAbsoluteChange = 500,
     @MinRelativeChange = 0.25,
     @MinBaseOI = 100;

-- 3) Insert into your permanent table (exclude IDENTITY + DEFAULT columns)
INSERT INTO dbo.OpenInterestSpikes
(
    RunAsOfDate,
    Ticker, OptionType, StrikePrice, ExpiryDate, DaysToExpiry,
    AsOfDate_Prior, AsOfDate_Today, DaysBetweenRecordings,
    OI_Prior, OI_Today, OI_Change, OI_Pct_Change, OI_SpikeCategory,
    Volume_Prior, Volume_Today, Volume_Change, Volume_Pct_Change, VolumeSurged, OI_Volume_Ratio,
    IV_Prior_Pct, IV_Today_Pct, IV_Change_Pct_Points,
    Delta_Today, Theta_Today, SpotPrice_Today
)
SELECT
    @RunDate AS RunAsOfDate,
    Ticker, OptionType, StrikePrice, ExpiryDate, DaysToExpiry,
    AsOfDate_Prior, AsOfDate_Today, DaysBetweenRecordings,
    OI_Prior, OI_Today, OI_Change, OI_Pct_Change, OI_SpikeCategory,
    Volume_Prior, Volume_Today, Volume_Change, Volume_Pct_Change, VolumeSurged, OI_Volume_Ratio,
    IV_Prior_Pct, IV_Today_Pct, IV_Change_Pct_Points,
    Delta_Today, Theta_Today, SpotPrice_Today
FROM #Spikes;




select * from OpenInterestSpikes

DELETE dbo.OpenInterestSpikes
WHERE RunAsOfDate = @RunDate;

