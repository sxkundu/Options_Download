USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetHighOIChangeOptions]    Script Date: 3/2/2026 6:12:10 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[GetHighOIChangeOptions]
    @TargetDate DATE = NULL,
    @MinOIChange INT = 0
AS
BEGIN
    --See all positive OI changes for the latest date:
    --EXEC dbo.GetHighOIChangeOptions;

    --See only significant moves (e.g., more than 1,000 new contracts):
    --EXEC dbo.GetHighOIChangeOptions @MinOIChange = 1000;

    --Check a specific date for a historical report:
    --EXEC dbo.GetHighOIChangeOptions @TargetDate = '2026-02-25';


    SET NOCOUNT ON;

    DECLARE @CurrentAsOfDate DATE = @TargetDate;
    DECLARE @PreviousAsOfDate DATE;

    -- 1. Default to latest date if none provided
    IF @CurrentAsOfDate IS NULL
        SELECT @CurrentAsOfDate = MAX(AsOfDate) FROM dbo.OptionsData;

    -- 2. Find the immediate previous trading date
    SELECT @PreviousAsOfDate = MAX(AsOfDate)
    FROM dbo.OptionsData
    WHERE AsOfDate < @CurrentAsOfDate;

    PRINT @CurrentAsOfDate;
    PRINT @PreviousAsOfDate;

    -- 3. Final Query focusing on raw change
    SELECT
        c.Ticker,
        c.ExpiryDate,
        c.OptionType,
        c.StrikePrice,
        p.OpenInterest AS PrevOpenInterest,
        c.OpenInterest AS CurrentOpenInterest,
        (ISNULL(c.OpenInterest, 0) - ISNULL(p.OpenInterest, 0)) AS OpenInterestChange
    FROM dbo.OptionsData c
    LEFT JOIN dbo.OptionsData p
        ON  p.Ticker = c.Ticker
        AND p.ExpiryDate = c.ExpiryDate
        AND p.OptionType = c.OptionType
        AND p.StrikePrice = c.StrikePrice
        AND p.AsOfDate = @PreviousAsOfDate
    WHERE c.AsOfDate = @CurrentAsOfDate
      AND (ISNULL(c.OpenInterest, 0) - ISNULL(p.OpenInterest, 0)) >= @MinOIChange
    ORDER BY OpenInterestChange DESC, c.Ticker ASC;

END;
GO


