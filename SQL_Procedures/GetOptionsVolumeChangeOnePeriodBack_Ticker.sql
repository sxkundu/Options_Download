USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetOptionsVolumeChangeOnePeriodBack_Ticker]    Script Date: 3/2/2026 6:17:09 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[GetOptionsVolumeChangeOnePeriodBack_Ticker]
    @Ticker NVARCHAR(10) = NULL,   -- Optional: Filter by Ticker
    @TargetDate DATE = NULL        -- Optional: Defaults to the latest available date
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentAsOfDate DATE = @TargetDate;
    DECLARE @PreviousAsOfDate DATE;

    -- 1. Determine the Current Date if not provided
    IF @CurrentAsOfDate IS NULL
    BEGIN
        SELECT @CurrentAsOfDate = MAX(AsOfDate) FROM dbo.OptionsData;
    END

    -- 2. Find the date immediately before the Current Date
    SELECT @PreviousAsOfDate = MAX(AsOfDate)
    FROM dbo.OptionsData
    WHERE AsOfDate < @CurrentAsOfDate;

    PRINT @CurrentAsOfDate;
    PRINT @PreviousAsOfDate;


    -- 3. Execute the Comparison Query
    SELECT
        c.Ticker,
        c.ExpiryDate,
        c.OptionType,
        c.StrikePrice,
        c.Volume,
        p.Volume AS PrevVolume,
        (ISNULL(c.Volume, 0) - ISNULL(p.Volume, 0)) AS VolumeChange,
        c.OpenInterest,
        p.OpenInterest AS PrevOpenInterest,
        (ISNULL(c.OpenInterest, 0) - ISNULL(p.OpenInterest, 0)) AS OpenInterestChange
    FROM dbo.OptionsData c
    LEFT JOIN dbo.OptionsData p
        ON  p.Ticker = c.Ticker
        AND p.ExpiryDate = c.ExpiryDate
        AND p.OptionType = c.OptionType
        AND p.StrikePrice = c.StrikePrice
        AND p.AsOfDate = @PreviousAsOfDate
    WHERE c.AsOfDate = @CurrentAsOfDate
      -- This logic handles the optional filter:
      AND (@Ticker IS NULL OR c.Ticker = @Ticker)
    ORDER BY c.Ticker, c.ExpiryDate, c.OptionType, c.StrikePrice;

END;
GO


