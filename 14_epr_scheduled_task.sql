/*
==============================================================================
EPR PACKAGING DATA - SCHEDULED TASK SETUP
==============================================================================
Purpose: Automate monthly execution of EPR data ingestion from Dropbox.

This task will:
- Run on the 1st of every month at 2 AM UTC
- Call the INGEST_EPR_FROM_DROPBOX stored procedure
- Load latest packaging data from Dropbox automatically

Prerequisites:
- 13_epr_dropbox_snowpark_procedure.sql must be executed first
- Dropbox API token must be configured
- Manual test of procedure should be successful

Author: Claude Code
Date: 2025-11-22
==============================================================================
*/

-- Use appropriate role and warehouse
USE ROLE ECONOMIC_ADMIN;
USE WAREHOUSE ECONOMIC_WH;
USE DATABASE ECONOMIC;

USE SCHEMA RAW;
/*
==============================================================================
STEP 1: CREATE SCHEDULED TASK
==============================================================================
Creates a task that runs monthly to ingest EPR data from Dropbox.
*/

CREATE OR REPLACE TASK TASK_INGEST_EPR_MONTHLY
WAREHOUSE = ECONOMIC_WH
SCHEDULE = 'USING CRON 0 2 1 * * UTC'  -- 1st of every month at 2:00 AM UTC
COMMENT = 'Monthly automated ingestion of EPR packaging data from Dropbox'
AS
CALL INGEST_EPR_FROM_DROPBOX(
    '/EPR/master_packaging_file.xlsx',  -- UPDATE THIS: Your Dropbox file path
    'Sheet1'                             -- UPDATE THIS: Your sheet name or '0'
);

/*
==============================================================================
STEP 2: ENABLE TASK EXECUTION
==============================================================================
Tasks are created in SUSPENDED state by default.
*/

-- Resume the task to enable scheduled execution
ALTER TASK TASK_INGEST_EPR_MONTHLY RESUME;

-- Verify task is running
SHOW TASKS LIKE 'TASK_INGEST_EPR_MONTHLY' IN SCHEMA

/*
==============================================================================
STEP 3: GRANT PERMISSIONS
==============================================================================
Allow ECONOMIC_WRITE role to monitor and manage the task.
*/

GRANT MONITOR ON TASK TASK_INGEST_EPR_MONTHLY TO ROLE ECONOMIC_WRITE;
GRANT OPERATE ON TASK TASK_INGEST_EPR_MONTHLY TO ROLE ECONOMIC_WRITE;

/*
==============================================================================
MONITORING AND MANAGEMENT COMMANDS
==============================================================================
Useful commands for managing and monitoring the scheduled task.
*/

-- ============================================
-- Check task status and schedule
-- ============================================
SHOW TASKS LIKE 'TASK_INGEST_EPR_MONTHLY' IN SCHEMA

-- ============================================
-- View task execution history
-- ============================================
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    RETURN_VALUE,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_INGEST_EPR_MONTHLY',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -30, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

-- ============================================
-- Manually trigger task (for testing)
-- ============================================
-- EXECUTE TASK TASK_INGEST_EPR_MONTHLY;

-- ============================================
-- Suspend task (pause scheduled execution)
-- ============================================
-- ALTER TASK TASK_INGEST_EPR_MONTHLY SUSPEND;

-- ============================================
-- Resume task (enable scheduled execution)
-- ============================================
-- ALTER TASK TASK_INGEST_EPR_MONTHLY RESUME;

-- ============================================
-- Modify task schedule
-- ============================================
-- Example: Change to run on 5th of every month at 3 AM
-- ALTER TASK TASK_INGEST_EPR_MONTHLY
-- SET SCHEDULE = 'USING CRON 0 3 5 * * UTC';

-- Example: Change to run every Sunday at 1 AM
-- ALTER TASK TASK_INGEST_EPR_MONTHLY
-- SET SCHEDULE = 'USING CRON 0 1 * * 0 UTC';

-- ============================================
-- Check latest data in table
-- ============================================
SELECT
    DATE_UPLOADED,
    SOURCE_FILE,
    COUNT(*) AS row_count,
    COUNT(DISTINCT SKU) AS unique_skus
FROM SKU_PACKAGING_DATA
GROUP BY DATE_UPLOADED, SOURCE_FILE
ORDER BY DATE_UPLOADED DESC;

-- ============================================
-- View latest ingested data sample
-- ============================================
SELECT
    SKU,
    PRODUCT_DESCRIPTION,
    CARTON_KG_PER_UNIT,
    PLASTIC_KG_PER_UNIT,
    FOAM_KG_PER_UNIT,
    DATE_UPLOADED,
    SOURCE_FILE
FROM SKU_PACKAGING_DATA
QUALIFY ROW_NUMBER() OVER (PARTITION BY SKU ORDER BY DATE_UPLOADED DESC) = 1
ORDER BY DATE_UPLOADED DESC
LIMIT 20;

/*
==============================================================================
ALTERNATIVE SCHEDULES (CRON EXAMPLES)
==============================================================================

Common scheduling patterns using CRON syntax:
Format: 'USING CRON <minute> <hour> <day-of-month> <month> <day-of-week> <timezone>'

Examples:
- Every day at 2 AM UTC:
  'USING CRON 0 2 * * * UTC'

- Every Monday at 9 AM UTC:
  'USING CRON 0 9 * * 1 UTC'

- First day of every month at 2 AM UTC:
  'USING CRON 0 2 1 * * UTC'  (current setting)

- 15th of every month at 3:30 AM UTC:
  'USING CRON 30 3 15 * * UTC'

- Every hour:
  'USING CRON 0 * * * * UTC'

- Every 6 hours:
  'USING CRON 0 */6 * * * UTC'

- First Monday of every month at 2 AM UTC:
  'USING CRON 0 2 1-7 * 1 UTC'

Convert UTC to your local timezone:
- Denmark (CET/CEST): UTC+1 (winter) / UTC+2 (summer)
- If you want 8 AM Copenhagen time in winter, use 7 AM UTC
- If you want 8 AM Copenhagen time in summer, use 6 AM UTC
*/

/*
==============================================================================
TASK DEPENDENCY EXAMPLE (Optional)
==============================================================================
If you want to chain tasks (e.g., refresh views after data load):
*/

-- Example: Task that runs after EPR ingestion completes
-- CREATE OR REPLACE TASK TASK_REFRESH_EPR_VIEWS
-- WAREHOUSE = ECONOMIC_WH
-- AFTER TASK_INGEST_EPR_MONTHLY
-- COMMENT = 'Refresh materialized views after EPR data ingestion'
-- AS
-- BEGIN
--     -- Refresh any materialized views or run data quality checks
--     CALL VALIDATE_EPR_DATA_QUALITY();
-- END;

-- Don't forget to RESUME dependent tasks:
-- ALTER TASK TASK_REFRESH_EPR_VIEWS RESUME;

/*
==============================================================================
NOTIFICATIONS SETUP (Optional but Recommended)
==============================================================================
Set up email notifications for task failures.
*/

-- Create notification integration (requires ACCOUNTADMIN)
-- USE ROLE ACCOUNTADMIN;
--
-- CREATE OR REPLACE NOTIFICATION INTEGRATION EPR_TASK_EMAIL_INTEGRATION
-- TYPE = EMAIL
-- ENABLED = TRUE
-- ALLOWED_RECIPIENTS = ('your-email@dogcopenhagen.com');
--
-- ALTER TASK TASK_INGEST_EPR_MONTHLY
-- SET ERROR_INTEGRATION = EPR_TASK_EMAIL_INTEGRATION;

/*
==============================================================================
DATA RETENTION POLICY (Optional)
==============================================================================
Keep only the latest version per SKU to save storage.
You might want to add a cleanup task.
*/

-- Example: Create a cleanup task that runs after ingestion
-- CREATE OR REPLACE TASK TASK_CLEANUP_OLD_EPR_DATA
-- WAREHOUSE = ECONOMIC_WH
-- AFTER TASK_INGEST_EPR_MONTHLY
-- COMMENT = 'Cleanup old EPR packaging data, keeping only latest per SKU'
-- AS
-- BEGIN
--     -- Keep only the most recent upload per SKU, delete older versions
--     DELETE FROM SKU_PACKAGING_DATA
--     WHERE (SKU, DATE_UPLOADED) NOT IN (
--         SELECT SKU, MAX(DATE_UPLOADED)
--         FROM SKU_PACKAGING_DATA
--         GROUP BY SKU
--     );
-- END;

-- ALTER TASK TASK_CLEANUP_OLD_EPR_DATA RESUME;

/*
==============================================================================
COST OPTIMIZATION
==============================================================================
Tasks consume compute credits when running. Optimize costs by:
*/

-- 1. Use smallest warehouse that meets performance needs
-- ALTER TASK TASK_INGEST_EPR_MONTHLY
-- SET WAREHOUSE = ECONOMIC_WH;  -- Use X-Small for this lightweight task

-- 2. Set task timeout to prevent runaway costs
-- ALTER TASK TASK_INGEST_EPR_MONTHLY
-- SET USER_TASK_TIMEOUT_MS = 300000;  -- 5 minutes max

-- 3. Monitor task costs
SELECT
    NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    WAREHOUSE_NAME,
    SCHEDULE,
    STATE,
    COMPLETED_TIME,
    RETURN_VALUE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'TASK_INGEST_EPR_MONTHLY'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

/*
==============================================================================
DEPLOYMENT COMPLETE
==============================================================================

✅ Objects Created:
   1. TASK: TASK_INGEST_EPR_MONTHLY

📋 Task Schedule:
   - Frequency: Monthly (1st of each month)
   - Time: 2:00 AM UTC
   - State: RESUMED (active)

🔍 Monitoring:
   - View task history: See queries above
   - Check data freshness: SELECT MAX(DATE_UPLOADED) FROM SKU_PACKAGING_DATA
   - Manual execution: EXECUTE TASK TASK_INGEST_EPR_MONTHLY

⚙️ Management:
   - Pause: ALTER TASK ... SUSPEND
   - Resume: ALTER TASK ... RESUME
   - Modify schedule: ALTER TASK ... SET SCHEDULE = '...'

📧 Recommended Next Steps:
   1. Set up email notifications for failures
   2. Test manual execution before first scheduled run
   3. Monitor first few scheduled runs
   4. Add data quality validation task (optional)
*/
