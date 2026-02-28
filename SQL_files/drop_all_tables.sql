-- ================================================================
-- drop_all_tables.sql
-- Drops all OptionsDB tables in the correct dependency order:
--   1. Disable system-versioning before dropping temporal tables
--   2. Drop history table
--   3. Drop temporal table
--   4. Drop TickerList (FK dependency removed first)
-- Safe to run multiple times (all checks are IF EXISTS).
-- ================================================================

USE OptionsDB;
GO

-- ── Step 1: Disable system-versioning on the temporal table ──────
IF OBJECT_ID('dbo.OptionsData', 'U') IS NOT NULL
BEGIN
    PRINT 'Disabling system-versioning on dbo.OptionsData...';
    ALTER TABLE dbo.OptionsData SET (SYSTEM_VERSIONING = OFF);
    PRINT 'System-versioning disabled.';
END
ELSE
    PRINT 'dbo.OptionsData does not exist – skipping system-versioning step.';
GO

-- ── Step 2: Drop the history table ───────────────────────────────
IF OBJECT_ID('dbo.OptionsDataHistory', 'U') IS NOT NULL
BEGIN
    PRINT 'Dropping dbo.OptionsDataHistory...';
    DROP TABLE dbo.OptionsDataHistory;
    PRINT 'dbo.OptionsDataHistory dropped.';
END
ELSE
    PRINT 'dbo.OptionsDataHistory does not exist – skipped.';
GO

-- ── Step 3: Drop the temporal (main) table ────────────────────────
IF OBJECT_ID('dbo.OptionsData', 'U') IS NOT NULL
BEGIN
    PRINT 'Dropping dbo.OptionsData...';
    DROP TABLE dbo.OptionsData;
    PRINT 'dbo.OptionsData dropped.';
END
ELSE
    PRINT 'dbo.OptionsData does not exist – skipped.';
GO

-- ── Step 4: Drop the TickerList table ────────────────────────────
-- Must come after OptionsData because of the FK constraint.
IF OBJECT_ID('dbo.TickerList', 'U') IS NOT NULL
BEGIN
    PRINT 'Dropping dbo.TickerList...';
    DROP TABLE dbo.TickerList;
    PRINT 'dbo.TickerList dropped.';
END
ELSE
    PRINT 'dbo.TickerList does not exist – skipped.';
GO

-- ── Final confirmation ────────────────────────────────────────────
PRINT '';
PRINT 'All OptionsDB tables dropped successfully.';
GO
