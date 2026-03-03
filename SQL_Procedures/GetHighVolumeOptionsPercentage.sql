USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetHighVolumeOptionsPercentage]    Script Date: 3/2/2026 6:14:04 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[GetHighVolumeOptionsPercentage]
    @TargetDate DATE = NULL,
    @MinVolumeChange INT = 0
AS
BEGIN
     --EXEC dbo.GetHighVolumeOptionsPercentage;
    --See all data (sorted by highest volume change):
    --EXEC dbo.GetHighVolumeOptionsPercentage;

    --See only contracts where Volume increased by at least 500:
    --EXEC dbo.GetHighVolumeOptionsPercentage @MinVolumeChange = 500;

    --See contracts where Volume decreased (Negative values):
    --EXEC dbo.GetHighVolumeOptionsPercentage @MinVolumeChange = -1000;

    SET NOCOUNT ON;

    DECLARE @CurrentAsOfDate DATE = @TargetDate;
    DECLARE @PreviousAsOfDate DATE;

    IF @CurrentAsOfDate IS NULL
        SELECT @CurrentAsOfDate = MAX(AsOfDate) FROM dbo.OptionsData;

    SELECT @PreviousAsOfDate = MAX(AsOfDate)
    FROM dbo.OptionsData
    WHERE AsOfDate < @CurrentAsOfDate;

    PRINT @CurrentAsOfDate;
    PRINT @PreviousAsOfDate;

    ;WITH OptionChanges AS (
        SELECT
            c.Ticker,
            c.ExpiryDate,
            c.OptionType,
            c.StrikePrice,
            c.Volume,
            p.Volume AS PrevVolume,
            (ISNULL(c.Volume, 0) - ISNULL(p.Volume, 0)) AS VolumeChange,
            -- Calculate Percent Change, handling divide-by-zero
            CAST(
                CASE 
                    WHEN ISNULL(p.Volume, 0) = 0 THEN NULL 
                    ELSE ((CAST(c.Volume AS FLOAT) - p.Volume) / p.Volume) * 100 
                END AS DECIMAL(10, 2)
            ) AS VolumePercentChange,
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
    ORDER BY VolumePercentChange DESC, VolumeChange DESC;

END;
GO


