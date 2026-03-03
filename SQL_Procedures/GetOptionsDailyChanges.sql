USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetOptionsDailyChanges]    Script Date: 3/2/2026 6:15:56 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[GetOptionsDailyChanges]
    @SummaryView BIT = 0 -- 0 for Detailed, 1 for Ticker Summary
AS
BEGIN
    --For the full, detailed contract breakdown:
    --EXEC dbo.GetOptionsDailyChanges @SummaryView = 0;
    
    --For the high-level Ticker summary:
    --EXEC dbo.GetOptionsDailyChanges @SummaryView = 1;
    
    SET NOCOUNT ON;

    -- ── 1. Find the two most recent distinct AsOfDates ───
    DECLARE @CurrentDate DATE, @PriorDate DATE;

    SELECT @CurrentDate = MAX(AsOfDate) FROM dbo.OptionsData;
    
    SELECT @PriorDate = MAX(AsOfDate) 
    FROM dbo.OptionsData 
    WHERE AsOfDate < @CurrentDate;

    -- ── 2. Logic for Detailed View (Contract Level) ───
    IF @SummaryView = 0
    BEGIN
        WITH CurrentSnap AS (
            SELECT Ticker, ExpiryDate, OptionType, StrikePrice, Volume AS Volume_Current,
                   OpenInterest AS OI_Current, ImpliedVol AS IV_Current, 
                   Delta AS Delta_Current, SpotPrice AS SpotPrice_Current, AsOfDate AS AsOfDate_Current
            FROM dbo.OptionsData WHERE AsOfDate = @CurrentDate
        ),
        PriorSnap AS (
            SELECT Ticker, ExpiryDate, OptionType, StrikePrice, Volume AS Volume_Prior,
                   OpenInterest AS OI_Prior, ImpliedVol AS IV_Prior, AsOfDate AS AsOfDate_Prior
            FROM dbo.OptionsData WHERE AsOfDate = @PriorDate
        )
        SELECT
            c.Ticker, c.OptionType, c.StrikePrice, c.ExpiryDate,
            DATEDIFF(DAY, c.AsOfDate_Current, c.ExpiryDate) AS DaysToExpiry,
            p.AsOfDate_Prior, c.AsOfDate_Current,
            p.Volume_Prior, c.Volume_Current,
            (c.Volume_Current - ISNULL(p.Volume_Prior, 0)) AS Volume_Change,
            p.OI_Prior, c.OI_Current,
            (c.OI_Current - ISNULL(p.OI_Prior, 0)) AS OI_Change,
            CASE
                WHEN p.OI_Prior IS NULL THEN 'NEW'
                WHEN c.OI_Current > p.OI_Prior THEN 'INCREASING'
                WHEN c.OI_Current < p.OI_Prior THEN 'DECREASING'
                ELSE 'UNCHANGED'
            END AS OI_Direction,
            c.IV_Current, (c.IV_Current - p.IV_Prior) AS IV_Change,
            c.Delta_Current, c.SpotPrice_Current
        FROM CurrentSnap c
        LEFT JOIN PriorSnap p ON c.Ticker = p.Ticker AND c.ExpiryDate = p.ExpiryDate 
                             AND c.OptionType = p.OptionType AND c.StrikePrice = p.StrikePrice
        ORDER BY ABS(c.OI_Current - ISNULL(p.OI_Prior, 0)) DESC;
    END

    -- ── 3. Logic for Summary View (Ticker Level) ───
    ELSE
    BEGIN
        WITH CurrentSnap AS (
            SELECT Ticker, OptionType, Volume, OpenInterest 
            FROM dbo.OptionsData WHERE AsOfDate = @CurrentDate
        ),
        PriorSnap AS (
            SELECT Ticker, OptionType, Volume AS Volume_Prior, OpenInterest AS OI_Prior
            FROM dbo.OptionsData WHERE AsOfDate = @PriorDate
        )
        SELECT
            c.Ticker,
            c.OptionType,
            @PriorDate AS AsOfDate_Prior,
            @CurrentDate AS AsOfDate_Current,
            SUM(c.Volume) AS Total_Volume_Current,
            SUM(ISNULL(p.Volume_Prior, 0)) AS Total_Volume_Prior,
            SUM(c.Volume) - SUM(ISNULL(p.Volume_Prior, 0)) AS Total_Volume_Change,
            SUM(c.OpenInterest) AS Total_OI_Current,
            SUM(ISNULL(p.OI_Prior, 0)) AS Total_OI_Prior,
            SUM(c.OpenInterest) - SUM(ISNULL(p.OI_Prior, 0)) AS Total_OI_Change
        FROM CurrentSnap c
        LEFT JOIN PriorSnap p ON c.Ticker = p.Ticker AND c.OptionType = p.OptionType
        GROUP BY c.Ticker, c.OptionType
        ORDER BY ABS(SUM(c.OpenInterest) - SUM(ISNULL(p.OI_Prior, 0))) DESC;
    END
END;
GO


