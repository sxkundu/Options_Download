USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetOptionsChangeHistory]    Script Date: 3/2/2026 6:14:24 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[GetOptionsChangeHistory]
    @Ticker    NVARCHAR(10) = NULL,
    @DaysBack  INT = NULL            -- Optional: Limit results to the last X days
AS
BEGIN
    --See the full history of changes for all tickers:
    --EXEC dbo.GetOptionsChangeHistory;

    --See only the changes from the last 7 days for Apple:
    --EXEC dbo.GetOptionsChangeHistory @Ticker = 'AAPL', @DaysBack = 7;

    --See the most recent changes for Tesla:
    --EXEC dbo.GetOptionsChangeHistory @Ticker = 'TSLA', @DaysBack = 1;
    
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

            -- Get previous date's data using LAG
            LAG(AsOfDate) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
            ) AS PrevAsOfDate,

            LAG(Volume) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
            ) AS PrevVolume,

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
        PrevAsOfDate,
        ExpiryDate,
        OptionType,
        StrikePrice,

        Volume,
        ISNULL(PrevVolume, 0) AS PrevVolume,
        (Volume - ISNULL(PrevVolume, 0)) AS VolumeChange,

        OpenInterest,
        ISNULL(PrevOpenInterest, 0) AS PrevOpenInterest,
        (OpenInterest - ISNULL(PrevOpenInterest, 0)) AS OpenInterestChange
    FROM OptionChanges
    WHERE PrevAsOfDate IS NOT NULL
      AND (@DaysBack IS NULL OR AsOfDate >= DATEADD(DAY, -@DaysBack, GETDATE()))
    ORDER BY Ticker, ExpiryDate, OptionType, StrikePrice, AsOfDate DESC;
END;
GO


