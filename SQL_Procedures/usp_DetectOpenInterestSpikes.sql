USE [Options];
GO

-- ================================================================
-- dbo.usp_DetectOpenInterestSpikes
--
-- Detects spikes in Open Interest by comparing a target AsOfDate
-- vs the most recent prior AsOfDate for the same contract
-- (Ticker + ExpiryDate + OptionType + StrikePrice).
--
-- Source table: dbo.OptionsData (current table)
--
-- Parameters:
--   @AsOfDate            : date to treat as "today" (defaults to MAX(AsOfDate))
--   @MinAbsoluteChange   : minimum raw OI increase
--   @MinRelativeChange   : minimum pct increase (0.25 = 25%)
--   @MinBaseOI           : minimum prior OI baseline
--
-- Notes:
--   - Uses last available AsOfDate in the table if @AsOfDate is NULL
--   - Uses most recent prior AsOfDate per contract (handles weekends/gaps)
-- Example usage:
-- EXEC dbo.usp_DetectOpenInterestSpikes; -- uses MAX(AsOfDate)
-- EXEC dbo.usp_DetectOpenInterestSpikes @AsOfDate = '2026-03-02';
-- EXEC dbo.usp_DetectOpenInterestSpikes @MinAbsoluteChange = 1000, @MinRelativeChange = 0.50, @MinBaseOI = 250;
-- ================================================================
CREATE OR ALTER PROCEDURE dbo.usp_DetectOpenInterestSpikes
(
      @AsOfDate           DATE  = NULL
    , @MinAbsoluteChange  INT   = 500
    , @MinRelativeChange  FLOAT = 0.25
    , @MinBaseOI          INT   = 100
)
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------
    -- Resolve AsOfDate to run for
    ----------------------------------------------------------------
    DECLARE @RunDate DATE;

    SELECT @RunDate =
        COALESCE(@AsOfDate, (SELECT MAX(AsOfDate) FROM dbo.OptionsData));

    IF @RunDate IS NULL
    BEGIN
        -- No rows in table
        SELECT
            CAST(NULL AS NVARCHAR(10))  AS Ticker,
            CAST(NULL AS NCHAR(4))      AS OptionType,
            CAST(NULL AS DECIMAL(18,4)) AS StrikePrice,
            CAST(NULL AS DATE)          AS ExpiryDate,
            CAST(NULL AS INT)           AS DaysToExpiry,
            CAST(NULL AS DATE)          AS AsOfDate_Prior,
            CAST(NULL AS DATE)          AS AsOfDate_Today,
            CAST(NULL AS INT)           AS DaysBetweenRecordings,
            CAST(NULL AS INT)           AS OI_Prior,
            CAST(NULL AS INT)           AS OI_Today,
            CAST(NULL AS INT)           AS OI_Change,
            CAST(NULL AS DECIMAL(10,2)) AS OI_Pct_Change,
            CAST(NULL AS NVARCHAR(30))  AS OI_SpikeCategory,
            CAST(NULL AS INT)           AS Volume_Prior,
            CAST(NULL AS INT)           AS Volume_Today,
            CAST(NULL AS INT)           AS Volume_Change,
            CAST(NULL AS DECIMAL(10,2)) AS Volume_Pct_Change,
            CAST(NULL AS NVARCHAR(20))  AS VolumeSurged,
            CAST(NULL AS DECIMAL(10,2)) AS OI_Volume_Ratio,
            CAST(NULL AS DECIMAL(10,4)) AS IV_Prior_Pct,
            CAST(NULL AS DECIMAL(10,4)) AS IV_Today_Pct,
            CAST(NULL AS DECIMAL(10,4)) AS IV_Change_Pct_Points,
            CAST(NULL AS DECIMAL(10,6)) AS Delta_Today,
            CAST(NULL AS DECIMAL(10,6)) AS Theta_Today,
            CAST(NULL AS DECIMAL(18,4)) AS SpotPrice_Today
        WHERE 1 = 0;

        RETURN;
    END

    ----------------------------------------------------------------
    -- Main query
    ----------------------------------------------------------------
    ;WITH Today AS
    (
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
        FROM dbo.OptionsData
        WHERE AsOfDate = @RunDate
    ),
    PriorRanked AS
    (
        SELECT
            Ticker,
            ExpiryDate,
            OptionType,
            StrikePrice,
            OpenInterest    AS OI_Prior,
            Volume          AS Volume_Prior,
            ImpliedVol      AS IV_Prior,
            AsOfDate        AS AsOfDate_Prior,
            ROW_NUMBER() OVER
            (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate DESC
            ) AS rn
        FROM dbo.OptionsData
        WHERE AsOfDate < @RunDate
    ),
    Prior AS
    (
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
        WHERE rn = 1
    ),
    Comparison AS
    (
        SELECT
            t.Ticker,
            t.ExpiryDate,
            t.OptionType,
            t.StrikePrice,

            p.AsOfDate_Prior,
            t.AsOfDate_Today,
            DATEDIFF(DAY, p.AsOfDate_Prior, t.AsOfDate_Today) AS DaysBetweenRecordings,

            p.OI_Prior,
            t.OI_Today,
            (t.OI_Today - p.OI_Prior) AS OI_Change,
            CASE WHEN p.OI_Prior > 0
                 THEN CAST(t.OI_Today - p.OI_Prior AS FLOAT) / p.OI_Prior
                 ELSE NULL
            END AS OI_Pct_Change,

            p.Volume_Prior,
            t.Volume_Today,
            (t.Volume_Today - p.Volume_Prior) AS Volume_Change,
            CASE WHEN p.Volume_Prior > 0
                 THEN CAST(t.Volume_Today - p.Volume_Prior AS FLOAT) / p.Volume_Prior
                 ELSE NULL
            END AS Volume_Pct_Change,

            p.IV_Prior,
            t.IV_Today,
            (t.IV_Today - p.IV_Prior) AS IV_Change,

            t.Delta_Today,
            t.Theta_Today,
            t.SpotPrice_Today,
            DATEDIFF(DAY, t.AsOfDate_Today, t.ExpiryDate) AS DaysToExpiry
        FROM Today t
        INNER JOIN Prior p
            ON  t.Ticker      = p.Ticker
            AND t.ExpiryDate  = p.ExpiryDate
            AND t.OptionType  = p.OptionType
            AND t.StrikePrice = p.StrikePrice
        WHERE p.OI_Prior >= @MinBaseOI
    ),
    Spikes AS
    (
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
            END AS OI_SpikeCategory,
            CASE
                WHEN Volume_Pct_Change >= 1.00 THEN 'YES (>= 100%)'
                WHEN Volume_Pct_Change >= 0.50 THEN 'YES (>=  50%)'
                WHEN Volume_Pct_Change >= 0.25 THEN 'YES (>=  25%)'
                ELSE 'NO'
            END AS VolumeSurged,
            CASE
                WHEN Volume_Today > 0
                THEN CAST(OI_Today AS FLOAT) / CAST(Volume_Today AS FLOAT)
                ELSE NULL
            END AS OI_Volume_Ratio
        FROM Comparison
        WHERE OI_Change     >= @MinAbsoluteChange
          AND OI_Pct_Change >= @MinRelativeChange
    )
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
        CAST(OI_Pct_Change * 100 AS DECIMAL(10, 2)) AS OI_Pct_Change,
        OI_SpikeCategory,

        -- Volume comparison
        Volume_Prior,
        Volume_Today,
        Volume_Change,
        CAST(Volume_Pct_Change * 100 AS DECIMAL(10, 2)) AS Volume_Pct_Change,
        VolumeSurged,
        CAST(OI_Volume_Ratio AS DECIMAL(10, 2)) AS OI_Volume_Ratio,

        -- IV movement
        CAST(IV_Prior  * 100 AS DECIMAL(10, 4)) AS IV_Prior_Pct,
        CAST(IV_Today  * 100 AS DECIMAL(10, 4)) AS IV_Today_Pct,
        CAST(IV_Change * 100 AS DECIMAL(10, 4)) AS IV_Change_Pct_Points,

        -- Greeks for context
        Delta_Today,
        Theta_Today,
        SpotPrice_Today
    FROM Spikes
    ORDER BY
        OI_Pct_Change DESC,
        OI_Change     DESC,
        Ticker        ASC,
        OptionType    ASC,
        ExpiryDate    ASC;
END
GO

-- Example usage:
-- EXEC dbo.usp_DetectOpenInterestSpikes; -- uses MAX(AsOfDate)
-- EXEC dbo.usp_DetectOpenInterestSpikes @AsOfDate = '2026-03-02';
-- EXEC dbo.usp_DetectOpenInterestSpikes @MinAbsoluteChange = 1000, @MinRelativeChange = 0.50, @MinBaseOI = 250;