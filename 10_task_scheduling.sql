/*******************************************************************************
 * FILE 10: TASK SCHEDULING
 *
 * Purpose: Automates daily data refresh from Economic API
 *
 * Creates:
 * - ECONOMIC_DAILY_REFRESH task (suspended by default)
 *
 * Schedule:
 * - Runs daily at 2:00 AM Copenhagen time (Europe/Copenhagen timezone)
 * - Calls REST ingestion procedure
 * - Task is created in SUSPENDED state for safety
 *
 * Manual Control:
 * - Resume: ALTER TASK ECONOMIC_DAILY_REFRESH RESUME;
 * - Suspend: ALTER TASK ECONOMIC_DAILY_REFRESH SUSPEND;
 * - Execute manually: EXECUTE TASK ECONOMIC_DAILY_REFRESH;
 ******************************************************************************/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA UTIL;

/*******************************************************************************
 * CREATE DAILY REFRESH TASK
 *
 * This task runs the REST API ingestion procedure daily.
 * Adjust schedule as needed for your requirements.
 ******************************************************************************/

CREATE OR REPLACE TASK ECONOMIC_DAILY_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 2 * * * Europe/Copenhagen'  -- Daily at 2 AM Copenhagen time
  COMMENT = 'Daily refresh of Economic API data (REST endpoints)'
AS
  CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST_MONTHLY();

-- Task is created in SUSPENDED state by default (Snowflake behavior)
-- This is intentional for safety - you must explicitly resume it

/*******************************************************************************
 * OPTIONAL: CREATE OPENAPI REFRESH TASK
 *
 * If you activate OpenAPI endpoints, create a separate task for them.
 * Uncomment the code below to enable.
 ******************************************************************************/

-- CREATE OR REPLACE TASK ECONOMIC_OPENAPI_DAILY_REFRESH
--   WAREHOUSE = COMPUTE_WH
--   SCHEDULE = 'USING CRON 30 2 * * * Europe/Copenhagen'  -- Daily at 2:30 AM (30 min after REST)
--   COMMENT = 'Daily refresh of Economic API data (OpenAPI endpoints)'
-- AS
--   CALL UTIL.ECONOMIC_OPENAPI_DATAINGEST_MONTHLY();

/*******************************************************************************
 * GRANT TASK PRIVILEGES
 ******************************************************************************/

-- Grant ownership to ECONOMIC_ADMIN
GRANT OWNERSHIP ON TASK ECONOMIC_DAILY_REFRESH TO ROLE ECONOMIC_ADMIN COPY CURRENT GRANTS;

-- Grant monitoring privileges to ECONOMIC_WRITE
GRANT MONITOR ON TASK ECONOMIC_DAILY_REFRESH TO ROLE ECONOMIC_WRITE;

/*******************************************************************************
 * VERIFICATION
 ******************************************************************************/

-- Show created tasks
SHOW TASKS IN SCHEMA UTIL;

-- Check task definition
-- DESCRIBE TASK ECONOMIC_DAILY_REFRESH;

/*******************************************************************************
 * TASK MANAGEMENT COMMANDS
 ******************************************************************************/

-- Resume task (starts automatic execution)
-- ALTER TASK ECONOMIC_DAILY_REFRESH RESUME;

-- Suspend task (stops automatic execution)
-- ALTER TASK ECONOMIC_DAILY_REFRESH SUSPEND;

-- Execute task immediately (manual run)
-- EXECUTE TASK ECONOMIC_DAILY_REFRESH;

-- Check task history (last 100 runs)
-- SELECT
--     NAME,
--     STATE,
--     SCHEDULED_TIME,
--     QUERY_START_TIME,
--     COMPLETED_TIME,
--     RETURN_VALUE,
--     ERROR_CODE,
--     ERROR_MESSAGE
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
-- WHERE NAME = 'ECONOMIC_DAILY_REFRESH'
-- ORDER BY SCHEDULED_TIME DESC
-- LIMIT 100;

-- Check if task is currently running
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
-- ))
-- WHERE NAME = 'ECONOMIC_DAILY_REFRESH'
--   AND STATE = 'EXECUTING';

/*******************************************************************************
 * SCHEDULE EXAMPLES
 ******************************************************************************/

-- Daily at 2 AM Copenhagen time (current)
-- 'USING CRON 0 2 * * * Europe/Copenhagen'

-- Every 6 hours
-- 'USING CRON 0 */6 * * * Europe/Copenhagen'

-- Every hour during business hours (8 AM - 6 PM)
-- 'USING CRON 0 8-18 * * * Europe/Copenhagen'

-- Twice daily (6 AM and 6 PM)
-- 'USING CRON 0 6,18 * * * Europe/Copenhagen'

-- Weekly on Mondays at 3 AM
-- 'USING CRON 0 3 * * 1 Europe/Copenhagen'

-- First day of each month at 1 AM
-- 'USING CRON 0 1 1 * * Europe/Copenhagen'

/*******************************************************************************
 * MONITORING & ALERTING
 ******************************************************************************/

-- Create a view to monitor task success/failure
-- CREATE OR REPLACE VIEW UTIL.VW_TASK_MONITORING AS
-- SELECT
--     NAME AS task_name,
--     STATE AS task_state,
--     SCHEDULED_TIME,
--     QUERY_START_TIME,
--     COMPLETED_TIME,
--     DATEDIFF(SECOND, QUERY_START_TIME, COMPLETED_TIME) AS duration_seconds,
--     RETURN_VALUE,
--     ERROR_CODE,
--     ERROR_MESSAGE,
--     CASE
--         WHEN STATE = 'SUCCEEDED' THEN 'Success'
--         WHEN STATE = 'FAILED' THEN 'Failed'
--         WHEN STATE = 'CANCELLED' THEN 'Cancelled'
--         WHEN STATE = 'EXECUTING' THEN 'Running'
--         ELSE STATE
--     END AS status
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
-- ))
-- WHERE NAME LIKE 'ECONOMIC%'
-- ORDER BY SCHEDULED_TIME DESC;

-- Query monitoring view
-- SELECT * FROM UTIL.VW_TASK_MONITORING LIMIT 20;

-- Failed tasks in last 7 days
-- SELECT *
-- FROM UTIL.VW_TASK_MONITORING
-- WHERE status = 'Failed'
-- ORDER BY SCHEDULED_TIME DESC;

/*******************************************************************************
 * ERROR HANDLING
 ******************************************************************************/

-- If task fails repeatedly, check:
-- 1. Warehouse is available and running
-- 2. Economic API secrets are valid
-- 3. Network access integration is configured
-- 4. Procedure has proper permissions
-- 5. Economic API rate limits not exceeded

-- View recent errors
-- SELECT
--     SCHEDULED_TIME,
--     ERROR_CODE,
--     ERROR_MESSAGE
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
-- WHERE NAME = 'ECONOMIC_DAILY_REFRESH'
--   AND STATE = 'FAILED'
-- ORDER BY SCHEDULED_TIME DESC
-- LIMIT 10;

/*******************************************************************************
 * COST OPTIMIZATION
 ******************************************************************************/

-- Task execution costs:
-- - Warehouse compute time (billed per second, minimum 60 seconds)
-- - Data transfer (usually minimal for API calls)

-- To reduce costs:
-- 1. Use appropriate warehouse size (XS usually sufficient for this workload)
-- 2. Schedule during off-peak hours
-- 3. Consider less frequent refreshes if daily is not required
-- 4. Monitor warehouse utilization

-- Example: Use auto-suspend warehouse
-- ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;  -- Suspend after 1 minute idle
-- ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME = TRUE; -- Auto-resume when needed

/*******************************************************************************
 * POST-DEPLOYMENT ACTIONS
 ******************************************************************************/

-- After verifying the deployment works:
-- 1. Test manual execution: EXECUTE TASK ECONOMIC_DAILY_REFRESH;
-- 2. Verify data loads successfully
-- 3. Check Bronze and Silver views have data
-- 4. Resume task for automatic execution: ALTER TASK ECONOMIC_DAILY_REFRESH RESUME;
-- 5. Monitor first few automatic runs
-- 6. Set up alerting if task fails (via Snowflake notifications or external monitoring)

/*******************************************************************************
 * END OF FILE 10
 ******************************************************************************/
