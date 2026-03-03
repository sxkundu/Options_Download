USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetOptionsVolumeChangeOnePeriodBack]    Script Date: 3/2/2026 6:16:46 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[GetOptionsVolumeChangeOnePeriodBack]
AS
BEGIN
    -- Settings to prevent extra result sets and improve performance
    SET NOCOUNT ON;

    DECLARE @CurrentAsOfDate DATE;
    DECLARE @PreviousAsOfDate DATE;

    -- 1. Get the most recent date
    SELECT @CurrentAsOfDate = MAX(AsOfDate)
    FROM dbo.OptionsData;

    -- 2. Get the date immediately preceding the most recent
    SELECT @PreviousAsOfDate = MAX(AsOfDate)
    FROM dbo.OptionsData
    WHERE AsOfDate < @CurrentAsOfDate;


    PRINT @CurrentAsOfDate;
    PRINT @PreviousAsOfDate;

    -- Optional: Debugging output for development
    -- PRINT CONCAT('Comparing Current: ', @CurrentAsOfDate, ' vs Previous: ', @PreviousAsOfDate);

    -- 3. Execute the Comparison Query
    SELECT
        c.Ticker,
        c.ExpiryDate,
        c.OptionType,
        c.StrikePrice,

        c.Volume,
        p.Volume AS PrevVolume,
        ISNULL(c.Volume, 0) - ISNULL(p.Volume, 0) AS VolumeChange,

        c.OpenInterest,
        p.OpenInterest AS PrevOpenInterest,
        ISNULL(c.OpenInterest, 0) - ISNULL(p.OpenInterest, 0) AS OpenInterestChange
    FROM dbo.OptionsData c
    LEFT JOIN dbo.OptionsData p
        ON  p.Ticker = c.Ticker
        AND p.ExpiryDate = c.ExpiryDate
        AND p.OptionType = c.OptionType
        AND p.StrikePrice = c.StrikePrice
        AND p.AsOfDate = @PreviousAsOfDate
    WHERE c.AsOfDate = @CurrentAsOfDate
    ORDER BY c.Ticker, c.ExpiryDate, c.OptionType, c.StrikePrice;

END;
GO


