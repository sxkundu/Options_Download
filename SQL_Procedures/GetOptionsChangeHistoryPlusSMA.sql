USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetOptionsChangeHistoryPlusSMA]    Script Date: 3/2/2026 6:15:16 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[GetOptionsChangeHistoryPlusSMA]
    @Ticker    NVARCHAR(10) = NULL,
    @DaysBack  INT = NULL
AS
BEGIN
    
    --See the full history of changes for all tickers:
    --EXEC dbo.GetOptionsChangeHistoryPlusSMA;

    --See only the changes from the last 7 days for Apple:
    --EXEC dbo.GetOptionsChangeHistoryPlusSMA @Ticker = 'AAPL', @DaysBack = 7;

    --See the most recent changes for Tesla:
    --EXEC dbo.GetOptionsChangeHistoryPlusSMA @Ticker = 'TSLA', @DaysBack = 1;
    
    SET NOCOUNT ON;

    WITH OptionChanges AS
    (
        SELECT
            Ticker,
            AsOfDate,
            ExpiryDate,
            OptionType,
            StrikePrice,
            Volume,
            OpenInterest,

            -- 1. Get previous values using LAG
            LAG(Volume) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
            ) AS PrevVolume,

            LAG(OpenInterest) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
            ) AS PrevOpenInterest,

            -- 2. Calculate 5-Day Moving Averages
            AVG(CAST(Volume AS FLOAT)) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS Volume_SMA_5,

            AVG(CAST(OpenInterest AS FLOAT)) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS OI_SMA_5
        FROM dbo.OptionsData
        WHERE (@Ticker IS NULL OR Ticker = @Ticker)
    )
    SELECT
        Ticker,
        AsOfDate,
        ExpiryDate,
        OptionType,
        StrikePrice,

        -- Volume Metrics
        Volume,
        ISNULL(PrevVolume, 0) AS PrevVolume,
        (Volume - ISNULL(PrevVolume, 0)) AS VolumeChange,
        CAST(Volume_SMA_5 AS DECIMAL(10,2)) AS Vol_SMA_5,

        -- Open Interest Metrics
        OpenInterest,
        ISNULL(PrevOpenInterest, 0) AS PrevOpenInterest,
        (OpenInterest - ISNULL(PrevOpenInterest, 0)) AS OI_Change,
        CAST(OI_SMA_5 AS DECIMAL(10,2)) AS OI_SMA_5,

        -- Signal: Is today's volume significantly above average?
        CASE 
            WHEN Volume > (Volume_SMA_5 * 2) THEN 'UNUSUAL' 
            ELSE 'NORMAL' 
        END AS Volume_Signal

    FROM OptionChanges
    WHERE PrevVolume IS NOT NULL -- Ensures we only see rows with a prior day comparison
      AND (@DaysBack IS NULL OR AsOfDate >= DATEADD(DAY, -@DaysBack, GETDATE()))
    ORDER BY Ticker, ExpiryDate, StrikePrice, AsOfDate DESC;
END;
GO


