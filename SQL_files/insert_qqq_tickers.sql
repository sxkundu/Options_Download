-- ================================================================
-- insert_qqq_tickers.sql
-- Inserts all 104 QQQ (Invesco / Nasdaq-100) holdings into
-- dbo.TickerList.  Uses MERGE so it is safe to re-run:
--   - New tickers are inserted with IsActive = 1
--   - Existing tickers are left unchanged (no overwrite)
-- Holdings sourced from Invesco QQQ as of 2026-02-27.
-- ================================================================

USE OptionsDB;
GO

MERGE dbo.TickerList AS target
USING (VALUES
    -- #   Symbol   Company
    (  1,  'NVDA',  'Nvidia Corp'),
    (  2,  'AAPL',  'Apple Inc'),
    (  3,  'MSFT',  'Microsoft Corp'),
    (  4,  'AMZN',  'Amazon.com Inc'),
    (  5,  'TSLA',  'Tesla Inc'),
    (  6,  'META',  'Meta Platforms Inc'),
    (  7,  'GOOGL', 'Alphabet Inc Class A'),
    (  8,  'GOOG',  'Alphabet Inc Class C'),
    (  9,  'WMT',   'Walmart Inc'),
    ( 10,  'AVGO',  'Broadcom Inc'),
    ( 11,  'MU',    'Micron Technology Inc'),
    ( 12,  'COST',  'Costco Wholesale Corp'),
    ( 13,  'NFLX',  'Netflix Inc'),
    ( 14,  'AMD',   'Advanced Micro Devices Inc'),
    ( 15,  'CSCO',  'Cisco Systems Inc'),
    ( 16,  'PLTR',  'Palantir Technologies Inc'),
    ( 17,  'LRCX',  'Lam Research Corp'),
    ( 18,  'AMAT',  'Applied Materials Inc'),
    ( 19,  'TMUS',  'T-Mobile US Inc'),
    ( 20,  'LIN',   'Linde Plc'),
    ( 21,  'PEP',   'PepsiCo Inc'),
    ( 22,  'INTC',  'Intel Corp'),
    ( 23,  'AMGN',  'Amgen Inc'),
    ( 24,  'TXN',   'Texas Instruments Inc'),
    ( 25,  'KLAC',  'KLA Corp'),
    ( 26,  'GILD',  'Gilead Sciences Inc'),
    ( 27,  'ISRG',  'Intuitive Surgical Inc'),
    ( 28,  'ADI',   'Analog Devices Inc'),
    ( 29,  'HON',   'Honeywell International Inc'),
    ( 30,  'SHOP',  'Shopify Inc'),
    ( 31,  'QCOM',  'Qualcomm Inc'),
    ( 32,  'BKNG',  'Booking Holdings Inc'),
    ( 33,  'ASML',  'ASML Holding NV'),
    ( 34,  'APP',   'AppLovin Corp'),
    ( 35,  'VRTX',  'Vertex Pharmaceuticals Inc'),
    ( 36,  'CMCSA', 'Comcast Corp'),
    ( 37,  'SBUX',  'Starbucks Corp'),
    ( 38,  'ADBE',  'Adobe Inc'),
    ( 39,  'CEG',   'Constellation Energy Corp'),
    ( 40,  'INTU',  'Intuit Inc'),
    ( 41,  'PANW',  'Palo Alto Networks Inc'),
    ( 42,  'MELI',  'MercadoLibre Inc'),
    ( 43,  'WDC',   'Western Digital Corp'),
    ( 44,  'CRWD',  'CrowdStrike Holdings Inc'),
    ( 45,  'MAR',   'Marriott International Inc'),
    ( 46,  'STX',   'Seagate Technology Holdings Plc'),
    ( 47,  'ADP',   'Automatic Data Processing Inc'),
    ( 48,  'MNST',  'Monster Beverage Corp'),
    ( 49,  'SNPS',  'Synopsys Inc'),
    ( 50,  'CDNS',  'Cadence Design Systems Inc'),
    ( 51,  'REGN',  'Regeneron Pharmaceuticals Inc'),
    ( 52,  'CTAS',  'Cintas Corp'),
    ( 53,  'ORLY',  "O'Reilly Automotive Inc"),
    ( 54,  'CSX',   'CSX Corp'),
    ( 55,  'MDLZ',  'Mondelez International Inc'),
    ( 56,  'DASH',  'DoorDash Inc'),
    ( 57,  'WBD',   'Warner Bros. Discovery Inc'),
    ( 58,  'PDD',   'PDD Holdings Inc'),
    ( 59,  'AEP',   'American Electric Power Co Inc'),
    ( 60,  'MRVL',  'Marvell Technology Inc'),
    ( 61,  'PCAR',  'PACCAR Inc'),
    ( 62,  'ROST',  'Ross Stores Inc'),
    ( 63,  'BKR',   'Baker Hughes Co'),
    ( 64,  'FTNT',  'Fortinet Inc'),
    ( 65,  'NXPI',  'NXP Semiconductors NV'),
    ( 66,  'MPWR',  'Monolithic Power Systems Inc'),
    ( 67,  'ABNB',  'Airbnb Inc'),
    ( 68,  'FER',   'Ferrovial SE'),
    ( 69,  'FAST',  'Fastenal Co'),
    ( 70,  'IDXX',  'IDEXX Laboratories Inc'),
    ( 71,  'FANG',  'Diamondback Energy Inc'),
    ( 72,  'EA',    'Electronic Arts Inc'),
    ( 73,  'CCEP',  'Coca-Cola Europacific Partners Plc'),
    ( 74,  'EXC',   'Exelon Corp'),
    ( 75,  'XEL',   'Xcel Energy Inc'),
    ( 76,  'ADSK',  'Autodesk Inc'),
    ( 77,  'ALNY',  'Alnylam Pharmaceuticals Inc'),
    ( 78,  'ODFL',  'Old Dominion Freight Line Inc'),
    ( 79,  'MCHP',  'Microchip Technology Inc'),
    ( 80,  'KDP',   'Keurig Dr Pepper Inc'),
    ( 81,  'PYPL',  'PayPal Holdings Inc'),
    ( 82,  'DXCM',  'DexCom Inc'),
    ( 83,  'TEAM',  'Atlassian Corp'),
    ( 84,  'GEHC',  'GE HealthCare Technologies Inc'),
    ( 85,  'ON',    'ON Semiconductor Corp'),
    ( 86,  'VRSK',  'Verisk Analytics Inc'),
    ( 87,  'BIIB',  'Biogen Inc'),
    ( 88,  'GFS',   'GlobalFoundries Inc'),
    ( 89,  'TTWO',  'Take-Two Interactive Software Inc'),
    ( 90,  'ILMN',  'Illumina Inc'),
    ( 91,  'ANSS',  'ANSYS Inc'),
    ( 92,  'SIRI',  'Sirius XM Holdings Inc'),
    ( 93,  'CTSH',  'Cognizant Technology Solutions Corp'),
    ( 94,  'ZS',    'Zscaler Inc'),
    ( 95,  'LULU',  'Lululemon Athletica Inc'),
    ( 96,  'MDB',   'MongoDB Inc'),
    ( 97,  'FSLR',  'First Solar Inc'),
    ( 98,  'CSGP',  'CoStar Group Inc'),
    ( 99,  'CDW',   'CDW Corp'),
    (100,  'DLTR',  'Dollar Tree Inc'),
    (101,  'IBRX',  'ImmunityBio Inc'),
    (102,  'GDDY',  'GoDaddy Inc'),
    (103,  'CAVA',  'CAVA Group Inc'),
    (104,  'CCC',   'CCC Intelligent Solutions Holdings Inc')
) AS source (SortOrder, Ticker, Notes)
ON target.Ticker = source.Ticker
WHEN NOT MATCHED THEN
    INSERT (Ticker, IsActive, AddedDate, Notes)
    VALUES (source.Ticker, 1, CAST(GETDATE() AS DATE), source.Notes);

GO

-- Confirm results
SELECT COUNT(*) AS TotalTickers,
       SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END) AS ActiveTickers
FROM dbo.TickerList;
GO
