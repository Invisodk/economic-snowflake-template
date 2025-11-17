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
USE DATABASE ECONOMIC;
USE SCHEMA CONFIG;

/*******************************************************************************
 * FILE 02: DATABASE, SCHEMAS & RBAC SETUP
 *
 * Creates:
 * - ECONOMIC database
 * - Schemas: CONFIG, UTIL, RAW, BRONZE, SILVER
 * - Roles: ECONOMIC_ADMIN, ECONOMIC_WRITE, ECONOMIC_READ
 * - Grants and future grants
 *
 * NOTE: Must run FIRST to create roles before secrets grant usage to them
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/02_database_schema_roles_setup.sql;
USE SCHEMA ECONOMIC.CONFIG;

/*******************************************************************************
 * FILE 01: NETWORK RULES & SECRETS SETUP
 *
 * Creates:
 * - Network rule for Economic API access
 * - Secrets for API authentication (AppSecret & AgreementGrant)
 * - External access integration
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/01_network_secrets_setup.sql;
USE SCHEMA ECONOMIC.CONFIG;

/*******************************************************************************
 * FILE 01B: PRESTASHOP NETWORK RULES & SECRETS SETUP
 *
 * Creates:
 * - Network rule for PrestaShop API access
 * - Secret for PrestaShop API authentication
 * - External access integration for PrestaShop
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/01b_network_secrets_setup_presta.sql;
USE SCHEMA ECONOMIC.CONFIG;

/*******************************************************************************
 * FILE 03: ECONOMIC CONFIGURATION TABLES
 *
 * Creates:
 * - CONFIG.ECONOMIC_ENDPOINTS table
 * - Populates with default Economic API endpoints
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/03_config_tables_economic.sql;
USE SCHEMA ECONOMIC.CONFIG;

/*******************************************************************************
 * FILE 03B: PRESTASHOP CONFIGURATION TABLES
 *
 * Creates:
 * - CONFIG.PRESTA_ENDPOINTS table
 * - Populates with default PrestaShop API endpoints
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/03b_config_tables_presta.sql;
USE SCHEMA ECONOMIC.CONFIG;

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/04_udf_economic_api_retriever.sql;
USE SCHEMA ECONOMIC.CONFIG;

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/04b_udf_presta_api_retriever.sql;
USE SCHEMA ECONOMIC.CONFIG;

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/05_raw_tables.sql;
USE SCHEMA ECONOMIC.CONFIG;

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/06_usp_rest_ingestion.sql;
USE SCHEMA ECONOMIC.CONFIG;

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/06b_presta_rest_ingestion.sql;
USE SCHEMA ECONOMIC.CONFIG;

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/07_usp_openapi_ingestion.sql;
USE SCHEMA ECONOMIC.CONFIG;

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/08_bronze_economic.sql;
USE SCHEMA ECONOMIC.CONFIG;

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/08b_expired_sku_logic.sql;
USE SCHEMA ECONOMIC.CONFIG;

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/08c_bronze_prestashop.sql;
USE SCHEMA ECONOMIC.CONFIG;

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/09_silver_views.sql;
USE SCHEMA ECONOMIC.CONFIG;

/*******************************************************************************
 * FILE 10: TASK SCHEDULING
 *
 * Creates:
 * - ECONOMIC_DAILY_REFRESH task (suspended by default)
 * - Runs daily at 2 AM Copenhagen time
 ******************************************************************************/

EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/10_task_scheduling.sql;
USE SCHEMA ECONOMIC.CONFIG;

/*******************************************************************************
 * FILE 11: CORTEX SEMANTIC MODEL SETUP (OPTIONAL)
 *
 * Creates:
 * - Snowflake Cortex semantic model for AI-powered analytics
 * - Enables natural language queries over your data
 * - Note: This is optional and requires Cortex features to be enabled
 ******************************************************************************/

-- Uncomment to deploy Cortex semantic model
-- EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/cortex_setup.sql;

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

-- Step 2: Update PrestaShop API secret with real credentials
-- ALTER SECRET PRESTASHOP_WS_KEY SET SECRET_STRING = 'your_actual_prestashop_ws_key';

-- Step 5: Run first data ingestion
-- CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST();
-- CALL UTIL.ECONOMIC_OPENAPI_DATAINGEST();
-- CALL UTIL.PRESTASHOP_RESTAPI_DATAINGEST();

-- Step 6: Verify data in Bronze layer (Economic)
-- SELECT * FROM BRONZE.CUSTOMERS LIMIT 10;
-- SELECT * FROM BRONZE.INVOICES LIMIT 10;
-- SELECT * FROM BRONZE.PRODUCTS LIMIT 10;

-- Step 7: Verify data in Bronze layer (PrestaShop)
-- SELECT * FROM BRONZE.PRESTA_PRODUCTS LIMIT 10;
-- SELECT * FROM BRONZE.PRESTA_COMBINATIONS LIMIT 10;

-- Step 8: Verify data in Silver layer
-- SELECT * FROM SILVER.VW_SALES_DETAIL LIMIT 10;

-- Step 9: Resume task for automated refresh (optional)
-- ALTER TASK ECONOMIC_DAILY_REFRESH RESUME;

/*******************************************************************************
 * END OF MASTER EXECUTION SCRIPT
 ******************************************************************************/
