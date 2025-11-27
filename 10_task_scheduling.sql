/*******************************************************************************
 * FILE 10: TASK SCHEDULING
 *
 * Purpose: Automates weekly/monthly data refresh from e-conomic, PrestaShop, and EPR
 *
 * Creates:
 * - ECONOMIC_RESTAPI_WEEKLY_REFRESH task (suspended by default)
 * - ECONOMIC_OPENAPI_WEEKLY_REFRESH task (suspended by default)
 * - PRESTASHOP_MONTHLY_REFRESH task (suspended by default)
 * - EPR_WEEKLY_REFRESH task (suspended by default)
 *
 * Schedule:
 * - e-conomic REST: Every Sunday at 12:00 PM Copenhagen time (incremental)
 * - e-conomic OpenAPI: Every Sunday at 12:30 PM Copenhagen time (incremental)
 * - PrestaShop: 1st of each month at 12:00 PM Copenhagen time (full refresh)
 * - EPR Packaging: Every Monday at 9:00 AM Copenhagen time (truncate and reload)
 * - Tasks are created in SUSPENDED state for safety
 *
 * Manual Control:
 * - Resume: ALTER TASK [task_name] RESUME;
 * - Suspend: ALTER TASK [task_name] SUSPEND;
 * - Execute manually: EXECUTE TASK [task_name];
 ******************************************************************************/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA UTIL;

/*******************************************************************************
 * TASK 1: E-CONOMIC REST WEEKLY REFRESH (INCREMENTAL)
 *
 * This task runs the e-conomic REST API ingestion procedure weekly.
 * Ingests: Customers, Products, Invoices (incremental using watermarks)
 * Runs FIRST every Sunday at 12:00 PM
 * Expected load time: 3-5 minutes (only loads new/updated data since last run)
 ******************************************************************************/

CREATE OR REPLACE TASK ECONOMIC_RESTAPI_WEEKLY_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 12 * * 0 Europe/Copenhagen'  -- Every Sunday at 12:00 PM
  COMMENT = 'Weekly incremental refresh of e-conomic REST API data (customers, products, invoices)'
AS
  CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST();

/*******************************************************************************
 * TASK 2: ECONOMIC OPENAPI WEEKLY REFRESH (INCREMENTAL)
 *
 * This task runs the e-conomic OpenAPI ingestion procedure weekly.
 * Ingests: Invoice lines (bulk) - incremental using watermarks
 * Runs 30 minutes AFTER REST task at 12:30 PM
 * Expected load time: 2-3 minutes (only loads lines for new invoices)
 ******************************************************************************/

CREATE OR REPLACE TASK ECONOMIC_OPENAPI_WEEKLY_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 30 12 * * 0 Europe/Copenhagen'  -- Every Sunday at 12:30 PM
  COMMENT = 'Weekly incremental refresh of e-conomic OpenAPI data (invoice lines)'
AS
  CALL UTIL.ECONOMIC_OPENAPI_DATAINGEST();

/*******************************************************************************
 * TASK 3: PRESTASHOP MONTHLY REFRESH (FULL REFRESH)
 *
 * This task runs the PrestaShop REST API ingestion procedure monthly.
 * Ingests: Products, Categories, Combinations (variants), Option Values
 * Scheduled on 1st of each month at 12:00 PM
 * Full refresh strategy (truncate and reload) since product hierarchy changes infrequently
 * Expected load time: 5-10 minutes
 ******************************************************************************/

CREATE OR REPLACE TASK PRESTASHOP_MONTHLY_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 12 1 * * Europe/Copenhagen'  -- 1st of month at 12:00 PM
  COMMENT = 'Monthly full refresh of PrestaShop API data (products, categories, variants)'
AS
  CALL UTIL.PRESTASHOP_RESTAPI_DATAINGEST();

/*******************************************************************************
 * TASK 4: EPR PACKAGING WEEKLY REFRESH (TRUNCATE AND RELOAD)
 *
 * This task runs the EPR packaging data ingestion procedure weekly.
 * Ingests: SKU packaging weights from Excel file in EPR_STAGE
 * Scheduled every Sunday at 1:00 PM (after e-conomic data refreshes)
 * Truncate and reload strategy (always reflects current packaging specs)
 * Expected load time: <1 minute (385 SKUs)
 *
 * Prerequisites:
 * - Excel file named 'DC ProductSpecs.xlsx' must be in EPR_STAGE
 * - Jan uploads/replaces file as needed
 * - Procedure automatically truncates table before loading
 ******************************************************************************/

CREATE OR REPLACE TASK EPR_WEEKLY_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 13 * * 0 Europe/Copenhagen'  -- Every Sunday at 1:00 PM
  COMMENT = 'Weekly refresh of EPR packaging data (truncate and reload from EPR_STAGE)'
AS
  CALL RAW.INGEST_EPR_FROM_STAGE('DC ProductSpecs.xlsx');

/*******************************************************************************
 * GRANT TASK PRIVILEGES
 ******************************************************************************/

GRANT OWNERSHIP ON TASK ECONOMIC_RESTAPI_WEEKLY_REFRESH TO ROLE ECONOMIC_ADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON TASK ECONOMIC_OPENAPI_WEEKLY_REFRESH TO ROLE ECONOMIC_ADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON TASK PRESTASHOP_MONTHLY_REFRESH TO ROLE ECONOMIC_ADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON TASK EPR_WEEKLY_REFRESH TO ROLE ECONOMIC_ADMIN COPY CURRENT GRANTS;

GRANT MONITOR ON TASK ECONOMIC_RESTAPI_WEEKLY_REFRESH TO ROLE ECONOMIC_WRITE;
GRANT MONITOR ON TASK ECONOMIC_OPENAPI_WEEKLY_REFRESH TO ROLE ECONOMIC_WRITE;
GRANT MONITOR ON TASK PRESTASHOP_MONTHLY_REFRESH TO ROLE ECONOMIC_WRITE;
GRANT MONITOR ON TASK EPR_WEEKLY_REFRESH TO ROLE ECONOMIC_WRITE;

/*******************************************************************************
 * TASK MANAGEMENT
 ******************************************************************************/

-- Resume tasks (enable automatic execution)
ALTER TASK ECONOMIC_RESTAPI_WEEKLY_REFRESH RESUME;
ALTER TASK ECONOMIC_OPENAPI_WEEKLY_REFRESH RESUME;
ALTER TASK PRESTASHOP_MONTHLY_REFRESH RESUME;
ALTER TASK EPR_WEEKLY_REFRESH RESUME;

-- Suspend tasks
-- ALTER TASK ECONOMIC_RESTAPI_WEEKLY_REFRESH SUSPEND;
-- ALTER TASK EPR_WEEKLY_REFRESH SUSPEND;

-- Execute manually
-- EXECUTE TASK ECONOMIC_RESTAPI_WEEKLY_REFRESH;
-- EXECUTE TASK EPR_WEEKLY_REFRESH;

-- Check task history
-- SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
-- WHERE NAME IN ('ECONOMIC_RESTAPI_WEEKLY_REFRESH', 'ECONOMIC_OPENAPI_WEEKLY_REFRESH', 'PRESTASHOP_MONTHLY_REFRESH', 'EPR_WEEKLY_REFRESH')
-- ORDER BY SCHEDULED_TIME DESC LIMIT 20;

/*******************************************************************************
 * END OF FILE 10
 ******************************************************************************/
