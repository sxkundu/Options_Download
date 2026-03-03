USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetTop10UnusualOptions]    Script Date: 3/2/2026 6:17:29 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[GetTop10UnusualOptions]
    @TargetDate DATE = NULL -- Defaults to the latest available date
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Identify our dates
    DECLARE @CurrentDate DATE = @TargetDate;
    IF @CurrentDate IS NULL
        SELECT @CurrentDate = MAX(AsOfDate) FROM dbo.OptionsData;

    -- 2. Process all tickers to find anomalies
    WITH MarketStats AS
    (
        SELECT
            Ticker, AsOfDate, ExpiryDate, OptionType, StrikePrice, Volume, OpenInterest,
            -- 20-Day Stats for the Z-Score
            AVG(CAST(Volume AS FLOAT)) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS Vol_Avg_20,
            STDEVP(CAST(Volume AS FLOAT)) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS Vol_StdDev_20,
            -- Change in OI from previous record
            LAG(OpenInterest) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
            ) AS PrevOpenInterest
        FROM dbo.OptionsData
    ),
    Anomalies AS (
        SELECT *,
            (OpenInterest - ISNULL(PrevOpenInterest, 0)) AS OI_Change,
            CAST(CASE WHEN Vol_StdDev_20 = 0 THEN 0 
                 ELSE (Volume - Vol_Avg_20) / NULLIF(Vol_StdDev_20, 0) END AS DECIMAL(10,2)) AS Volume_ZScore
        FROM MarketStats
        WHERE AsOfDate = @CurrentDate
    )
    -- 3. Return the Top 10 by Z-Score
    SELECT TOP 10
        Ticker,
        OptionType,
        StrikePrice,
        ExpiryDate,
        Volume,
        Vol_Avg_20 AS Avg_Vol_20D,
        Volume_ZScore,
        OI_Change,
        CASE 
            WHEN Volume_ZScore > 5 THEN '🚨 EXTREME OUTLIER'
            WHEN Volume_ZScore > 3 THEN '🔥 HIGH ACTIVITY'
            ELSE 'SIGNIFICANT'
        END AS Alert_Level
    FROM Anomalies
    WHERE Volume_ZScore > 2.0 -- Only show things that are actually unusual
    ORDER BY Volume_ZScore DESC;
END;
GO


