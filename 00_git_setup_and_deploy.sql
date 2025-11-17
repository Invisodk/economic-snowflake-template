/*******************************************************************************
 * ECONOMIC API TO SNOWFLAKE - GIT INTEGRATION SETUP & DEPLOYMENT
 *
 * Purpose: Sets up Git integration with Snowflake and deploys the entire
 *          Economic API integration template from GitHub repository.
 *
 * Usage: Run this script ONCE in a Snowflake worksheet to:
 *        1. Create Git secret with your GitHub Personal Access Token (PAT)
 *        2. Create API integration for GitHub access
 *        3. Create Git repository object pointing to your template repo
 *        4. Execute all deployment files from the repository
 *        5. Clean up Git objects (security best practice)
 *
 * Prerequisites:
 *        - ACCOUNTADMIN role (or role with CREATE INTEGRATION privilege)
 *        - GitHub Personal Access Token with 'repo' scope
 *        - Repository must be accessible (public or PAT must have access)
 *
 * IMPORTANT: Update the values below before running!
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

/*******************************************************************************
 * STEP 0: CONFIGURATION
 *
 * UPDATE THESE VALUES FOR YOUR ENVIRONMENT
 ******************************************************************************/

-- Database and schema setup
SET DBNAME = 'ECONOMIC';
SET SCHEMANAME = 'CONFIG';
SET NAMESPACE = CONCAT($DBNAME, '.', $SCHEMANAME);

-- GitHub configuration (update lines 69-70 with your credentials!)
SET GITHUB_ORG = 'Invisodk';                          -- Your GitHub organization or username
SET REPO_NAME = 'economic-snowflake-template';           -- Repository name
SET BRANCH_NAME = 'main';                                -- Branch to deploy from

/*******************************************************************************
 * STEP 1: CREATE DATABASE AND SCHEMA
 ******************************************************************************/

CREATE DATABASE IF NOT EXISTS IDENTIFIER($DBNAME);
USE DATABASE IDENTIFIER($DBNAME);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($NAMESPACE);
USE SCHEMA IDENTIFIER($NAMESPACE);

/*******************************************************************************
 * STEP 2: CREATE GIT SECRET
 *
 * Stores your GitHub PAT token securely in Snowflake.
 * This secret is used to authenticate with GitHub.
 *
 * UPDATE USERNAME AND PASSWORD BELOW WITH YOUR GITHUB CREDENTIALS
 ******************************************************************************/

CREATE OR REPLACE SECRET GIT_ECONOMIC_TEMPLATE_SECRET
  TYPE = password
  USERNAME = 'your-username'           --  CHANGE THIS
  PASSWORD = 'your-pat-token'     -- CHANGE THIS
  COMMENT = 'GitHub PAT token for Economic Snowflake Template repository access';

-- Grant usage to SYSADMIN so they can use git integration
GRANT USAGE ON SECRET GIT_ECONOMIC_TEMPLATE_SECRET TO ROLE SYSADMIN;

/*******************************************************************************
 * STEP 3: CREATE API INTEGRATION FOR GITHUB
 *
 * Allows Snowflake to communicate with GitHub's API.
 * This integration is reusable for multiple repositories.
 ******************************************************************************/

SET API_PREFIX = CONCAT('https://github.com/', $GITHUB_ORG, '/');

CREATE OR REPLACE API INTEGRATION GIT_ECONOMIC_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ($API_PREFIX)
  ALLOWED_AUTHENTICATION_SECRETS = (GIT_ECONOMIC_TEMPLATE_SECRET)
  ENABLED = TRUE
  COMMENT = 'Git integration for Economic Snowflake Template deployment';

-- Grant usage to SYSADMIN
GRANT USAGE ON INTEGRATION GIT_ECONOMIC_INTEGRATION TO ROLE SYSADMIN;

-- Grant privilege to create git repositories
USE ROLE ACCOUNTADMIN;
GRANT CREATE GIT REPOSITORY ON SCHEMA IDENTIFIER($NAMESPACE) TO ROLE SYSADMIN;

/*******************************************************************************
 * STEP 4: CREATE GIT REPOSITORY OBJECT
 *
 * Creates a reference to your GitHub repository in Snowflake.
 * This allows you to execute SQL files directly from the repo.
 ******************************************************************************/

USE ROLE SYSADMIN;
USE SCHEMA IDENTIFIER($NAMESPACE);

SET GIT_ORIGIN = CONCAT('https://github.com/', $GITHUB_ORG, '/', $REPO_NAME);

CREATE OR REPLACE GIT REPOSITORY ECONOMIC_TEMPLATE_REPO
  API_INTEGRATION = GIT_ECONOMIC_INTEGRATION
  GIT_CREDENTIALS = GIT_ECONOMIC_TEMPLATE_SECRET
  ORIGIN = $GIT_ORIGIN
  COMMENT = 'Economic API integration template repository';

/*******************************************************************************
 * STEP 5: FETCH AND VERIFY REPOSITORY
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;
USE SCHEMA IDENTIFIER($NAMESPACE);

-- Fetch latest code from GitHub
ALTER GIT REPOSITORY ECONOMIC_TEMPLATE_REPO FETCH;

-- List files in repository (verification step)
-- Uncomment to verify files are accessible:
-- LS @ECONOMIC_TEMPLATE_REPO/branches/main;

/*******************************************************************************
 * STEP 6: EXECUTE DEPLOYMENT
 *
 * Runs the master execution script which will deploy all components:
 * - Network rules and secrets
 * - Database, schemas, and roles
 * - Configuration tables
 * - UDFs and stored procedures
 * - RAW tables
 * - Bronze and Silver views
 * - Task scheduling
 ******************************************************************************/

-- Execute the master deployment script
EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/00_execute_all_files_in_repo.sql;

/*******************************************************************************
 * STEP 7: POST-DEPLOYMENT TASKS
 *
 * After deployment completes, you MUST update the Economic API secrets
 * with your actual credentials!
 ******************************************************************************/

-- Update secrets with actual Economic API credentials
-- UNCOMMENT AND RUN THESE AFTER INITIAL DEPLOYMENT

-- USE ROLE ACCOUNTADMIN;
-- ALTER SECRET ECONOMIC_XAPIKEY_APPSECRET
--   SET SECRET_STRING = 'your_actual_appsecret_here';
--
-- ALTER SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT
--   SET SECRET_STRING = 'your_actual_agreementgrant_here';

-- Test the API connection (use demo=true for testing without real credentials)
-- USE ROLE ECONOMIC_ADMIN;
-- USE DATABASE ECONOMIC;
-- SELECT UTIL.ECONOMIC_API_V3('customers', 'REST', 1000, 0);

-- Run first data ingestion
-- CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST_MONTHLY();

/*******************************************************************************
 * STEP 8: CLEANUP (OPTIONAL - SECURITY BEST PRACTICE)
 *
 * After successful deployment, you can remove the Git objects.
 * This follows security best practices to minimize attack surface.
 *
 * Note: You'll need to recreate these if you want to re-deploy or update.
 ******************************************************************************/

-- UNCOMMENT TO CLEANUP AFTER SUCCESSFUL DEPLOYMENT

-- USE ROLE ACCOUNTADMIN;
-- DROP GIT REPOSITORY IF EXISTS ECONOMIC_TEMPLATE_REPO;
-- DROP INTEGRATION IF EXISTS GIT_ECONOMIC_INTEGRATION;
-- DROP SECRET IF EXISTS GIT_ECONOMIC_TEMPLATE_SECRET;

/*******************************************************************************
 * TROUBLESHOOTING
 ******************************************************************************/

-- Check if secret exists and who can use it
-- SHOW SECRETS LIKE 'GIT_ECONOMIC_TEMPLATE_SECRET';
-- SHOW GRANTS ON SECRET GIT_ECONOMIC_TEMPLATE_SECRET;

-- Check if integration exists
-- SHOW INTEGRATIONS LIKE 'GIT_ECONOMIC_INTEGRATION';

-- Check if repository exists and can be accessed
-- SHOW GIT REPOSITORIES LIKE 'ECONOMIC_TEMPLATE_REPO';

-- List files in repository
-- LS @ECONOMIC_TEMPLATE_REPO/branches/main;

-- Check git repository fetch status
-- SELECT SYSTEM$GIT_REPOSITORY_STATUS('ECONOMIC_TEMPLATE_REPO');

/*******************************************************************************
 * DEPLOYMENT VERIFICATION CHECKLIST
 ******************************************************************************/

-- [ ] Database ECONOMIC created
-- [ ] Schemas created: CONFIG, UTIL, RAW, BRONZE, SILVER
-- [ ] Roles created: ECONOMIC_ADMIN, ECONOMIC_WRITE, ECONOMIC_READ
-- [ ] Network rule and secrets created
-- [ ] External access integration created
-- [ ] UDF ECONOMIC_API_V3 created
-- [ ] Ingestion procedures created
-- [ ] Config table populated with endpoints
-- [ ] RAW tables created
-- [ ] Bronze views created (8 views)
-- [ ] Silver views created (3 views)
-- [ ] Tasks created but not started (resume manually after testing)

-- Verify database structure
-- SHOW SCHEMAS IN DATABASE ECONOMIC;
-- SHOW ROLES LIKE 'ECONOMIC%';
-- SHOW VIEWS IN SCHEMA ECONOMIC.BRONZE;
-- SHOW VIEWS IN SCHEMA ECONOMIC.SILVER;
-- SHOW PROCEDURES IN SCHEMA ECONOMIC.UTIL;
-- SHOW FUNCTIONS IN SCHEMA ECONOMIC.UTIL;

/*******************************************************************************
 * NEXT STEPS AFTER DEPLOYMENT
 ******************************************************************************/

-- 1. Update Economic API secrets with real credentials (see STEP 7 above)
-- 2. Test API connection with demo data
-- 3. Run initial data ingestion
-- 4. Verify data in Bronze views
-- 5. Verify data in Silver views
-- 6. Configure and resume scheduled tasks
-- 7. Grant appropriate roles to users:
--    - ECONOMIC_READ for analysts/BI tools
--    - ECONOMIC_WRITE for data engineers
--    - ECONOMIC_ADMIN for administrators
-- 8. Optional: Clean up Git objects (see STEP 8 above)

/*******************************************************************************
 * END OF SCRIPT
 ******************************************************************************/