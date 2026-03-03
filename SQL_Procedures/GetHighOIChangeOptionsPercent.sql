USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetHighOIChangeOptionsPercent]    Script Date: 3/2/2026 6:13:08 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[GetHighOIChangeOptionsPercent]
    @TargetDate DATE = NULL,
    @MinOIChange INT = 0  -- Filter for Open Interest increase
AS
BEGIN
    
    --Find all contracts where at least 500 new positions were opened:
    --EXEC dbo.GetHighOIChangeOptionsPercent @MinOIChange = 500; 

    --Find the largest percentage spikes in OI for the latest date:
    --EXEC dbo.GetHighOIChangeOptionsPercent;
 
    --Check a specific historical date for OI spikes:
    --EXEC dbo.GetHighOIChangeOptionsPercent @TargetDate = '2025-12-01', @MinOIChange = 100;
    
    SET NOCOUNT ON;

    DECLARE @CurrentAsOfDate DATE = @TargetDate;
    DECLARE @PreviousAsOfDate DATE;

    -- 1. Default to latest date if NULL
    IF @CurrentAsOfDate IS NULL
        SELECT @CurrentAsOfDate = MAX(AsOfDate) FROM dbo.OptionsData;

    -- 2. Identify the prior trading day
    SELECT @PreviousAsOfDate = MAX(AsOfDate)
    FROM dbo.OptionsData
    WHERE AsOfDate < @CurrentAsOfDate;

    PRINT @CurrentAsOfDate;
    PRINT @PreviousAsOfDate;

    -- 3. Calculate OI Change and Percent Change
    ;WITH OIAnalysis AS (
        SELECT
            c.Ticker,
            c.ExpiryDate,
            c.OptionType,
            c.StrikePrice,
            p.OpenInterest AS PrevOpenInterest,
            c.OpenInterest AS CurrentOpenInterest,
            (ISNULL(c.OpenInterest, 0) - ISNULL(p.OpenInterest, 0)) AS OpenInterestChange,
            -- Calculate Percent Change with Float cast for precision
            CAST(
                CASE 
                    WHEN ISNULL(p.OpenInterest, 0) = 0 THEN NULL 
                    ELSE ((CAST(c.OpenInterest AS FLOAT) - p.OpenInterest) / p.OpenInterest) * 100 
                END AS DECIMAL(10, 2)
            ) AS OIPercentChange
        FROM dbo.OptionsData c
        LEFT JOIN dbo.OptionsData p
            ON  p.Ticker = c.Ticker
            AND p.ExpiryDate = c.ExpiryDate
            AND p.OptionType = c.OptionType
            AND p.StrikePrice = c.StrikePrice
            AND p.AsOfDate = @PreviousAsOfDate
        WHERE c.AsOfDate = @CurrentAsOfDate
    )
    SELECT * FROM OIAnalysis
    WHERE OpenInterestChange >= @MinOIChange
    ORDER BY OIPercentChange DESC, OpenInterestChange DESC;

END;
GO


