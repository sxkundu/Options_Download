USE [Options]
GO

/****** Object:  StoredProcedure [dbo].[GetOptionsChangeHistory_PCR_Ratio]    Script Date: 3/2/2026 6:14:46 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[GetOptionsChangeHistory_PCR_Ratio]
    @Ticker    NVARCHAR(10) = NULL,
    @DaysBack  INT = NULL
AS
BEGIN
    
/*    
    Understanding the New Metrics
PCR_Ratio (Put/Call Ratio):

< 0.7: Often considered Bullish (more Calls than Puts).

> 1.0: Often considered Bearish (more Puts than Calls).

Net_Delta_Exposure: This multiplies each contract's volume by its Delta.

A large positive number suggests the volume is heavily weighted toward aggressive upside bets.

A large negative number suggests the volume is weighted toward downside protection or bets.

dbo.GetOptionsChangeHistory_PCR_Ratio @Ticker = 'NFLX'
dbo.GetOptionsChangeHistory_PCR_Ratio @Ticker = 'NVDA'

*/
    
    SET NOCOUNT ON;

    -- 1. Detailed Stats with Z-Score
    WITH OptionStats AS
    (
        SELECT
            Ticker, AsOfDate, ExpiryDate, OptionType, StrikePrice, Volume, OpenInterest, Delta,
            AVG(CAST(Volume AS FLOAT)) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS Vol_Avg_20,
            STDEVP(CAST(Volume AS FLOAT)) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS Vol_StdDev_20,
            LAG(OpenInterest) OVER (
                PARTITION BY Ticker, ExpiryDate, OptionType, StrikePrice
                ORDER BY AsOfDate
            ) AS PrevOpenInterest
        FROM dbo.OptionsData
        WHERE (@Ticker IS NULL OR Ticker = @Ticker)
    ),
    FinalDetails AS (
        SELECT *,
            CAST(CASE WHEN Vol_StdDev_20 = 0 THEN 0 
                 ELSE (Volume - Vol_Avg_20) / NULLIF(Vol_StdDev_20, 0) END AS DECIMAL(10,2)) AS Volume_ZScore
        FROM OptionStats
    )
    -- Output 1: Detailed Statistical Report
    SELECT 
        Ticker, AsOfDate, ExpiryDate, OptionType, StrikePrice, Volume, Volume_ZScore,
        OpenInterest, (OpenInterest - ISNULL(PrevOpenInterest, 0)) AS OI_Change,
        CASE 
            WHEN Volume_ZScore > 3 THEN '🔥 EXTREME'
            WHEN Volume_ZScore > 2 THEN '⭐ SIGNIFICANT'
            ELSE 'NORMAL'
        END AS Signal
    FROM FinalDetails
    WHERE (@DaysBack IS NULL OR AsOfDate >= DATEADD(DAY, -@DaysBack, GETDATE()))
    ORDER BY Volume_ZScore DESC, AsOfDate DESC;

    -- 2. Output 2: Sentiment Summary (Put/Call Ratio & Net Delta)
    SELECT 
        Ticker,
        AsOfDate,
        SUM(CASE WHEN OptionType = 'Call' THEN Volume ELSE 0 END) AS Total_Call_Vol,
        SUM(CASE WHEN OptionType = 'Put' THEN Volume ELSE 0 END) AS Total_Put_Vol,
        CAST(CAST(SUM(CASE WHEN OptionType = 'Put' THEN Volume ELSE 0 END) AS FLOAT) 
             / NULLIF(SUM(CASE WHEN OptionType = 'Call' THEN Volume ELSE 0 END), 0) AS DECIMAL(10,2)) AS PCR_Ratio,
        SUM(Volume * Delta) AS Net_Delta_Exposure -- Total "Directional" weight of the volume
    FROM dbo.OptionsData
    WHERE (@Ticker IS NULL OR Ticker = @Ticker)
      AND (@DaysBack IS NULL OR AsOfDate >= DATEADD(DAY, -@DaysBack, GETDATE()))
    GROUP BY Ticker, AsOfDate
    ORDER BY AsOfDate DESC, Ticker ASC;
END;
GO


