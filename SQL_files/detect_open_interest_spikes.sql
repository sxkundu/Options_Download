-- ================================================================
-- detect_open_interest_spikes.sql
--
-- Detects spikes in Open Interest by comparing today's AsOfDate
-- vs the most recent prior AsOfDate for the same contract
-- (Ticker + ExpiryDate + OptionType + StrikePrice).
--
-- Source table : [Options].[dbo].[OptionsDataHistory]
--
-- Spike thresholds (adjust as needed):
--   @MinAbsoluteChange : minimum raw OI increase in contracts
--   @MinRelativeChange : minimum percentage increase (0.25 = 25%)
--   @MinBaseOI         : ignore contracts with tiny prior OI
--                        (prevents 1 -> 10 = 900% false signals)
-- ================================================================

USE [Options];
GO

DECLARE @Today          DATE = CAST(GETDATE() AS DATE);
DECLARE @MinAbsoluteChange  INT   = 500;
DECLARE @MinRelativeChange  FLOAT = 0.25;   -- 25%
DECLARE @MinBaseOI          INT   = 100;

-- ── Step 1: Get today's snapshot ─────────────────────────────────
-- Pull every contract row for the most recent AsOfDate in the table
WITH Today AS (
    SELECT
        Ticker,
        ExpiryDate,
        OptionType,
        StrikePrice,
        OpenInterest    AS OI_Today,
        Volume          AS Volume_Today,
        ImpliedVol      AS IV_Today,
        Delta           AS Delta_Today,
        Theta           AS Theta_Today,
        SpotPrice       AS SpotPrice_Today,
        AsOfDate        AS AsOfDate_Today
    FROM [Options].[dbo].[OptionsDataHistory]
    WHERE AsOfDate = @Today
),

-- ── Step 2: Get the most recent PRIOR AsOfDate per contract ───────
-- Using ROW_NUMBER to find the single closest prior date so the
-- comparison is always exactly one recording period back,
-- regardless of weekends or gaps in the data.
PriorRanked AS (
    SELECT
        Ticker,
        ExpiryDate,
        OptionType,
        StrikePrice,
        OpenInterest    AS OI_Prior,
        Volume          AS Volume_Prior,
        ImpliedVol      AS IV_Prior,
        AsOfDate        AS AsOfDate_Prior,
        ROW_NUMBER() OVER (
            PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
            ORDER BY AsOfDate DESC
        ) AS rn
    FROM [Options].[dbo].[OptionsDataHistory]
    WHERE AsOfDate < @Today       -- strictly before today
),

Prior AS (
    SELECT
        Ticker,
        ExpiryDate,
        OptionType,
        StrikePrice,
        OI_Prior,
        Volume_Prior,
        IV_Prior,
        AsOfDate_Prior
    FROM PriorRanked
    WHERE rn = 1                  -- keep only the most recent prior date
),

-- ── Step 3: Join today vs prior on the full contract key ──────────
Comparison AS (
    SELECT
        t.Ticker,
        t.ExpiryDate,
        t.OptionType,
        t.StrikePrice,

        -- AsOfDate context
        p.AsOfDate_Prior,
        t.AsOfDate_Today,
        DATEDIFF(DAY, p.AsOfDate_Prior, t.AsOfDate_Today) AS DaysBetweenRecordings,

        -- Open Interest comparison
        p.OI_Prior,
        t.OI_Today,
        t.OI_Today - p.OI_Prior                                         AS OI_Change,
        CASE
            WHEN p.OI_Prior > 0
            THEN CAST(t.OI_Today - p.OI_Prior AS FLOAT) / p.OI_Prior
            ELSE NULL
        END                                                             AS OI_Pct_Change,

        -- Volume comparison by AsOfDate
        p.Volume_Prior,
        t.Volume_Today,
        t.Volume_Today - p.Volume_Prior                                 AS Volume_Change,
        CASE
            WHEN p.Volume_Prior > 0
            THEN CAST(t.Volume_Today - p.Volume_Prior AS FLOAT) / p.Volume_Prior
            ELSE NULL
        END                                                             AS Volume_Pct_Change,

        -- IV context
        p.IV_Prior,
        t.IV_Today,
        t.IV_Today - p.IV_Prior                                         AS IV_Change,

        -- Greeks
        t.Delta_Today,
        t.Theta_Today,
        t.SpotPrice_Today,
        DATEDIFF(DAY, t.AsOfDate_Today, t.ExpiryDate)                  AS DaysToExpiry

    FROM Today t
    INNER JOIN Prior p
        ON  t.Ticker      = p.Ticker
        AND t.ExpiryDate  = p.ExpiryDate
        AND t.OptionType  = p.OptionType
        AND t.StrikePrice = p.StrikePrice
    WHERE p.OI_Prior >= @MinBaseOI
),

-- ── Step 4: Apply spike filters and classify ─────────────────────
Spikes AS (
    SELECT
        Ticker,
        OptionType,
        StrikePrice,
        ExpiryDate,
        DaysToExpiry,
        AsOfDate_Prior,
        AsOfDate_Today,
        DaysBetweenRecordings,
        OI_Prior,
        OI_Today,
        OI_Change,
        OI_Pct_Change,
        Volume_Prior,
        Volume_Today,
        Volume_Change,
        Volume_Pct_Change,
        IV_Prior,
        IV_Today,
        IV_Change,
        Delta_Today,
        Theta_Today,
        SpotPrice_Today,
        CASE
            WHEN OI_Pct_Change >= 2.00 THEN 'EXTREME   (>= 200%)'
            WHEN OI_Pct_Change >= 1.00 THEN 'VERY HIGH (>= 100%)'
            WHEN OI_Pct_Change >= 0.50 THEN 'HIGH      (>=  50%)'
            WHEN OI_Pct_Change >= 0.25 THEN 'MODERATE  (>=  25%)'
            ELSE 'NORMAL'
        END                                         AS OI_SpikeCategory,

        -- Volume also surged alongside OI?
        CASE
            WHEN Volume_Pct_Change >= 1.00 THEN 'YES (>= 100%)'
            WHEN Volume_Pct_Change >= 0.50 THEN 'YES (>=  50%)'
            WHEN Volume_Pct_Change >= 0.25 THEN 'YES (>=  25%)'
            ELSE 'NO'
        END                                         AS VolumeSurged,

        -- OI/Volume ratio: > 1 means more open contracts than traded today
        --   signals overnight accumulation rather than intraday activity
        CASE
            WHEN Volume_Today > 0
            THEN CAST(OI_Today AS FLOAT) / CAST(Volume_Today AS FLOAT)
            ELSE NULL
        END                                         AS OI_Volume_Ratio

    FROM Comparison
    WHERE OI_Change     >= @MinAbsoluteChange
      AND OI_Pct_Change >= @MinRelativeChange
)

-- ── Final output ─────────────────────────────────────────────────
SELECT
    -- Contract identity
    Ticker,
    OptionType,
    StrikePrice,
    ExpiryDate,
    DaysToExpiry,

    -- Date context
    AsOfDate_Prior,
    AsOfDate_Today,
    DaysBetweenRecordings,

    -- Open Interest spike
    OI_Prior,
    OI_Today,
    OI_Change,
    CAST(OI_Pct_Change * 100    AS DECIMAL(10, 2))  AS OI_Pct_Change,
    OI_SpikeCategory,

    -- Volume comparison (AsOfDate vs AsOfDate)
    Volume_Prior,
    Volume_Today,
    Volume_Change,
    CAST(Volume_Pct_Change * 100 AS DECIMAL(10, 2)) AS Volume_Pct_Change,
    VolumeSurged,
    CAST(OI_Volume_Ratio         AS DECIMAL(10, 2)) AS OI_Volume_Ratio,

    -- IV movement
    CAST(IV_Prior  * 100 AS DECIMAL(10, 4))         AS IV_Prior_Pct,
    CAST(IV_Today  * 100 AS DECIMAL(10, 4))         AS IV_Today_Pct,
    CAST(IV_Change * 100 AS DECIMAL(10, 4))         AS IV_Change_Pct_Points,

    -- Greeks for context
    Delta_Today,
    Theta_Today,
    SpotPrice_Today

FROM Spikes
ORDER BY
    OI_Pct_Change   DESC,
    OI_Change       DESC,
    Ticker          ASC,
    OptionType      ASC,
    ExpiryDate      ASC;
GO


-- ================================================================
-- SUMMARY VIEW: Spike counts rolled up by Ticker + OptionType
-- Uncomment to use as a quick dashboard or alert feed
-- ================================================================
/*
USE [Options];

DECLARE @Today DATE = CAST(GETDATE() AS DATE);

WITH Today AS (
    SELECT Ticker, ExpiryDate, OptionType, StrikePrice,
           OpenInterest AS OI_Today, Volume AS Volume_Today, AsOfDate AS AsOfDate_Today
    FROM [Options].[dbo].[OptionsDataHistory]
    WHERE AsOfDate = @Today
),
PriorRanked AS (
    SELECT Ticker, ExpiryDate, OptionType, StrikePrice,
           OpenInterest AS OI_Prior, Volume AS Volume_Prior, AsOfDate AS AsOfDate_Prior,
           ROW_NUMBER() OVER (
               PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
               ORDER BY AsOfDate DESC
           ) AS rn
    FROM [Options].[dbo].[OptionsDataHistory]
    WHERE AsOfDate < @Today
),
Prior AS (
    SELECT * FROM PriorRanked WHERE rn = 1
),
Spikes AS (
    SELECT t.Ticker, t.OptionType,
           t.OI_Today - p.OI_Prior                                    AS OI_Change,
           CAST(t.OI_Today - p.OI_Prior AS FLOAT) / NULLIF(p.OI_Prior, 0) AS OI_Pct,
           t.Volume_Today, p.Volume_Prior,
           DATEDIFF(DAY, @Today, t.ExpiryDate)                        AS DTE
    FROM Today t
    INNER JOIN Prior p
        ON t.Ticker = p.Ticker AND t.ExpiryDate = p.ExpiryDate
        AND t.OptionType = p.OptionType AND t.StrikePrice = p.StrikePrice
    WHERE p.OI_Prior        >= 100
      AND (t.OI_Today - p.OI_Prior) >= 500
      AND CAST(t.OI_Today - p.OI_Prior AS FLOAT) / NULLIF(p.OI_Prior,0) >= 0.25
)
SELECT
    Ticker,
    OptionType,
    COUNT(*)                            AS SpikeCount,
    SUM(OI_Change)                      AS Total_OI_Added,
    CAST(MAX(OI_Pct) * 100 AS DECIMAL(10,2)) AS Max_Pct_Change,
    MIN(DTE)                            AS Nearest_Expiry_DTE
FROM Spikes
GROUP BY Ticker, OptionType
ORDER BY Total_OI_Added DESC;
*/
