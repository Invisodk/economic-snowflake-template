/*******************************************************************************
 * ECONOMIC API TO SNOWFLAKE - MASTER EXECUTION SCRIPT
 *
 * Purpose: Executes all deployment files in the correct sequence.
 *          This script is called by 00_git_setup_and_deploy.sql
 *
 * Execution Order:
 *   01 - Network rules and secrets (API access)
 *   02 - Database, schemas, and RBAC (structure)
 *   03 - Configuration tables (endpoint definitions)
 *   04 - UDF (API caller function)
 *   05 - RAW tables (landing zone)
 *   06 - REST ingestion procedure
 *   07 - OpenAPI ingestion procedure
 *   08 - Bronze views (field extraction)
 *   09 - Silver views (business analytics)
 *   10 - Task scheduling (automation)
 *
 * Note: This script assumes it's being executed from a Git repository object.
 *       The variable $GIT_REPO_NAME should be set in the calling script.
 ******************************************************************************/

-- Ensure we're using the correct role for deployment
USE ROLE ACCOUNTADMIN;

/*******************************************************************************
 * FILE 01: NETWORK RULES & SECRETS SETUP
 *
 * Creates:
 * - Network rule for Economic API access
 * - Secrets for API authentication (AppSecret & AgreementGrant)
 * - External access integration
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/01_network_secrets_setup.sql;

/*******************************************************************************
 * FILE 02: DATABASE, SCHEMAS & RBAC SETUP
 *
 * Creates:
 * - ECONOMIC database
 * - Schemas: CONFIG, UTIL, RAW, BRONZE, SILVER
 * - Roles: ECONOMIC_ADMIN, ECONOMIC_WRITE, ECONOMIC_READ
 * - Grants and future grants
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/02_database_schema_roles_setup.sql;

/*******************************************************************************
 * FILE 03: CONFIGURATION TABLES
 *
 * Creates:
 * - CONFIG.ECONOMIC_ENDPOINTS table
 * - Populates with default Economic API endpoints
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/03_config_tables.sql;

/*******************************************************************************
 * FILE 04: ECONOMIC API UDF
 *
 * Creates:
 * - UTIL.ECONOMIC_API_V3 function
 * - Supports both REST and OpenAPI endpoints
 * - Enhanced error handling and demo mode
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/04_udf_economic_api_v3.sql;

/*******************************************************************************
 * FILE 05: RAW TABLES
 *
 * Creates:
 * - RAW.ECONOMIC_RESTAPI_JSON (REST API landing)
 * - RAW.ECONOMIC_OPENAPI_JSON (OpenAPI landing)
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/05_raw_tables.sql;

/*******************************************************************************
 * FILE 06: REST INGESTION PROCEDURE
 *
 * Creates:
 * - UTIL.ECONOMIC_RESTAPI_DATAINGEST_MONTHLY procedure
 * - Ingests all active REST endpoints from config table
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/06_usp_rest_ingestion.sql;

/*******************************************************************************
 * FILE 07: OPENAPI INGESTION PROCEDURE
 *
 * Creates:
 * - UTIL.ECONOMIC_OPENAPI_DATAINGEST_MONTHLY procedure
 * - Ingests all active OpenAPI endpoints from config table
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/07_usp_openapi_ingestion.sql;

/*******************************************************************************
 * FILE 08: BRONZE VIEWS
 *
 * Creates 8 Bronze layer views with field extraction:
 * - BRONZE.CUSTOMERS
 * - BRONZE.PRODUCTS
 * - BRONZE.INVOICES
 * - BRONZE.INVOICE_LINES
 * - BRONZE.ACCOUNTING_YEARS
 * - BRONZE.ACCOUNTING_ENTRIES
 * - BRONZE.ACCOUNTING_PERIODS
 * - BRONZE.ACCOUNTING_TOTALS
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/08_bronze_views.sql;

/*******************************************************************************
 * FILE 09: SILVER VIEWS
 *
 * Creates 3 Silver layer business-friendly views:
 * - SILVER.VW_SALES_DETAIL
 * - SILVER.VW_FINANCIAL_DETAIL
 * - SILVER.VW_CUSTOMER_MASTER
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/09_silver_views.sql;

/*******************************************************************************
 * FILE 10: TASK SCHEDULING
 *
 * Creates:
 * - ECONOMIC_DAILY_REFRESH task (suspended by default)
 * - Runs daily at 2 AM Copenhagen time
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/10_task_scheduling.sql;

/*******************************************************************************
 * DEPLOYMENT COMPLETE!
 ******************************************************************************/

-- Return success message
SELECT
    '✅ DEPLOYMENT COMPLETE!' AS STATUS,
    'Economic API integration has been deployed successfully.' AS MESSAGE,
    'Next steps: Update API secrets and run first ingestion.' AS ACTION;

/*******************************************************************************
 * POST-DEPLOYMENT INSTRUCTIONS
 ******************************************************************************/

-- Step 1: Update Economic API secrets with real credentials
-- USE ROLE ACCOUNTADMIN;
-- ALTER SECRET ECONOMIC_XAPIKEY_APPSECRET SET SECRET_STRING = 'your_actual_appsecret';
-- ALTER SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT SET SECRET_STRING = 'your_actual_agreementgrant';

-- Step 2: Test API connection
-- USE ROLE ECONOMIC_ADMIN;
-- USE DATABASE ECONOMIC;
-- SELECT UTIL.ECONOMIC_API_V3('customers', 'REST', 1000, 0);

-- Step 3: Run first ingestion
-- CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST_MONTHLY();

-- Step 4: Verify data in Bronze layer
-- SELECT * FROM BRONZE.CUSTOMERS LIMIT 10;
-- SELECT * FROM BRONZE.INVOICES LIMIT 10;

-- Step 5: Verify data in Silver layer
-- SELECT * FROM SILVER.VW_SALES_DETAIL LIMIT 10;

-- Step 6: Resume task for automated refresh (optional)
-- ALTER TASK ECONOMIC_DAILY_REFRESH RESUME;

/*******************************************************************************
 * END OF MASTER EXECUTION SCRIPT
 ******************************************************************************/
