USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetOptionsChangeHistoryPlusSMAZScore]    Script Date: 3/2/2026 6:15:37 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[GetOptionsChangeHistoryPlusSMAZScore]
    @Ticker    NVARCHAR(10) = NULL,
    @DaysBack  INT = NULL
AS
BEGIN
    --See the full history of changes for all tickers:
    --EXEC dbo.GetOptionsChangeHistoryPlusSMAZScore;

    --See only the changes from the last 7 days for Apple:
    --EXEC dbo.GetOptionsChangeHistoryPlusSMAZScore @Ticker = 'AAPL', @DaysBack = 7;

    --See the most recent changes for Tesla:
    --EXEC dbo.GetOptionsChangeHistoryPlusSMAZScore @Ticker = 'TSLA', @DaysBack = 1;

    -- Find the most statistically unusual volume spikes for Apple over the last month
    --EXEC dbo.GetOptionsChangeHistoryPlusSMAZScore @Ticker = 'AAPL', @DaysBack = 30;
    
    SET NOCOUNT ON;

    WITH OptionStats AS
    (
        SELECT
            Ticker,
            AsOfDate,
            ExpiryDate,
            OptionType,
            StrikePrice,
            Volume,
            OpenInterest,
            -- 1. Calculate 20-Day Moving Average and Standard Deviation for Volume
            AVG(CAST(Volume AS FLOAT)) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS Vol_Avg_20,
            STDEVP(CAST(Volume AS FLOAT)) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS Vol_StdDev_20,
            -- 2. Get Previous Day OI for the change calculation
            LAG(OpenInterest) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
            ) AS PrevOpenInterest
        FROM dbo.OptionsData
        WHERE (@Ticker IS NULL OR Ticker = @Ticker)
    )
    SELECT
        Ticker,
        AsOfDate,
        ExpiryDate,
        OptionType,
        StrikePrice,
        Volume,
        CAST(Vol_Avg_20 AS DECIMAL(10,2)) AS Vol_Avg_20,
        -- 3. Calculate Z-Score: (Current - Average) / StdDev
        CAST(
            CASE 
                WHEN Vol_StdDev_20 = 0 THEN 0 
                ELSE (Volume - Vol_Avg_20) / Vol_StdDev_20 
            END AS DECIMAL(10,2)
        ) AS Volume_ZScore,
        OpenInterest,
        (OpenInterest - ISNULL(PrevOpenInterest, 0)) AS OI_Change,
        -- 4. Final Signal Logic
        CASE 
            WHEN (Volume - Vol_Avg_20) / NULLIF(Vol_StdDev_20, 0) > 3 THEN '🔥 EXTREME'
            WHEN (Volume - Vol_Avg_20) / NULLIF(Vol_StdDev_20, 0) > 2 THEN '⭐ SIGNIFICANT'
            ELSE 'NORMAL'
        END AS Signal
    FROM OptionStats
    WHERE Vol_Avg_20 IS NOT NULL
      AND (@DaysBack IS NULL OR AsOfDate >= DATEADD(DAY, -@DaysBack, GETDATE()))
    ORDER BY Volume_ZScore DESC, Ticker, AsOfDate DESC;
END;
GO


