USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetHighVolumeOptions]    Script Date: 3/2/2026 6:13:37 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[GetHighVolumeOptions]
    @TargetDate DATE = NULL,
    @MinVolumeChange INT = 0  -- Filter for changes greater than or equal to this value
AS
BEGIN
    --EXEC dbo.GetHighVolumeOptions;
    --See all data (sorted by highest volume change):
    --EXEC dbo.GetHighVolumeOptions;

    --See only contracts where Volume increased by at least 500:
    --EXEC dbo.GetHighVolumeOptions @MinVolumeChange = 500;

    --See contracts where Volume decreased (Negative values):
    --EXEC dbo.GetHighVolumeOptions @MinVolumeChange = -1000;

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

    -- 3. Use a CTE to calculate changes, then filter
    ;WITH OptionChanges AS (
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
    )
    SELECT * FROM OptionChanges
    WHERE VolumeChange >= @MinVolumeChange
    ORDER BY VolumeChange DESC, Ticker;

END;
GO


