-- ================================================================
-- delete_options_history.sql
-- Clears all rows from the temporal history table.
-- ================================================================

USE [Options];
GO

-- Step 1: Turn off system-versioning (unlinks the history table)
ALTER TABLE [dbo].[OptionsData]
    SET (SYSTEM_VERSIONING = OFF);
GO

-- Step 2: Delete all rows from the history table
DELETE FROM [dbo].[OptionsDataHistory];
GO

-- Step 3: Re-enable system-versioning, re-linking the history table
ALTER TABLE [dbo].[OptionsData]
    SET (
        SYSTEM_VERSIONING = ON (
            HISTORY_TABLE = [dbo].[OptionsDataHistory],
            DATA_CONSISTENCY_CHECK = ON
        )
    );
GO

-- Confirm history is empty
SELECT COUNT(*) AS HistoryRowCount FROM [dbo].[OptionsDataHistory];
GO
