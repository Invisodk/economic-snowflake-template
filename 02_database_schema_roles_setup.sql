/*******************************************************************************
 * FILE 02: DATABASE, SCHEMAS & RBAC SETUP
 *
 * Purpose: Creates database structure and role-based access control (RBAC)
 *
 * Creates:
 * - ECONOMIC database
 * - 5 schemas following medallion architecture
 * - 3 roles with hierarchical permissions
 * - Grants and future grants for proper access control
 *
 * Schema Architecture:
 * - CONFIG: Configuration tables (endpoint definitions, parameters)
 * - UTIL: Utilities (UDFs, stored procedures, functions)
 * - RAW: Landing zone for JSON data from API
 * - BRONZE: Field extraction views (parsed from JSON)
 * - SILVER: Business-friendly analytics views
 *
 * Role Hierarchy:
 * - ECONOMIC_ADMIN: Full control (deployment, maintenance, grants)
 * - ECONOMIC_WRITE: Can run ingestion, write to RAW, read all
 * - ECONOMIC_READ: Read-only access to BRONZE and SILVER
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

/*******************************************************************************
 * STEP 1: CREATE DATABASE
 ******************************************************************************/

CREATE DATABASE IF NOT EXISTS ECONOMIC
  COMMENT = 'Economic API integration - Medallion architecture (RAW → BRONZE → SILVER)';

USE DATABASE ECONOMIC;

/*******************************************************************************
 * STEP 2: CREATE SCHEMAS
 *
 * Following medallion architecture with configuration and utility schemas.
 ******************************************************************************/

-- CONFIG: Configuration tables
CREATE SCHEMA IF NOT EXISTS CONFIG
  COMMENT = 'Configuration tables for API endpoints and parameters';

-- UTIL: Utilities (UDFs, procedures)
CREATE SCHEMA IF NOT EXISTS UTIL
  COMMENT = 'UDFs and stored procedures for API calls and data processing';

-- RAW: Landing zone (JSON from API)
CREATE SCHEMA IF NOT EXISTS RAW
  COMMENT = 'Raw JSON data from Economic API (REST and OpenAPI endpoints)';

-- BRONZE: Field extraction (parsed views)
CREATE SCHEMA IF NOT EXISTS BRONZE
  COMMENT = 'Bronze layer - Field extraction views from raw JSON';

-- SILVER: Business-friendly views
CREATE SCHEMA IF NOT EXISTS SILVER
  COMMENT = 'Silver layer - Business-friendly analytics-ready views';

/*******************************************************************************
 * STEP 3: CREATE ROLES
 *
 * Three-tier role hierarchy for proper access control.
 ******************************************************************************/

-- ECONOMIC_ADMIN: Full administrative access
CREATE ROLE IF NOT EXISTS ECONOMIC_ADMIN
  COMMENT = 'Full administrative access to ECONOMIC database - for deployment and maintenance';

-- ECONOMIC_WRITE: Data engineering access
CREATE ROLE IF NOT EXISTS ECONOMIC_WRITE
  COMMENT = 'Can run data ingestion procedures and write to RAW schema';

-- ECONOMIC_READ: Analytics/BI access
CREATE ROLE IF NOT EXISTS ECONOMIC_READ
  COMMENT = 'Read-only access to BRONZE and SILVER layers for analysts and BI tools';

/*******************************************************************************
 * STEP 4: GRANT ROLE HIERARCHY
 *
 * ECONOMIC_ADMIN → ECONOMIC_WRITE → ECONOMIC_READ
 * Higher roles inherit permissions from lower roles.
 ******************************************************************************/

GRANT ROLE ECONOMIC_READ TO ROLE ECONOMIC_WRITE;
GRANT ROLE ECONOMIC_WRITE TO ROLE ECONOMIC_ADMIN;

-- Grant ECONOMIC_ADMIN to SYSADMIN for administrative access
GRANT ROLE ECONOMIC_ADMIN TO ROLE SYSADMIN;

/*******************************************************************************
 * STEP 5: DATABASE-LEVEL GRANTS
 ******************************************************************************/

-- ECONOMIC_ADMIN: Full database ownership
GRANT ALL PRIVILEGES ON DATABASE ECONOMIC TO ROLE ECONOMIC_ADMIN;

-- ECONOMIC_WRITE: Usage on database
GRANT USAGE ON DATABASE ECONOMIC TO ROLE ECONOMIC_WRITE;

-- ECONOMIC_READ: Usage on database
GRANT USAGE ON DATABASE ECONOMIC TO ROLE ECONOMIC_READ;

/*******************************************************************************
 * STEP 6: SCHEMA-LEVEL GRANTS
 ******************************************************************************/

-- === ECONOMIC_ADMIN: Full access to all schemas ===
GRANT ALL PRIVILEGES ON SCHEMA ECONOMIC.CONFIG TO ROLE ECONOMIC_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA ECONOMIC.UTIL TO ROLE ECONOMIC_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA ECONOMIC.RAW TO ROLE ECONOMIC_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA ECONOMIC.BRONZE TO ROLE ECONOMIC_ADMIN;
GRANT ALL PRIVILEGES ON SCHEMA ECONOMIC.SILVER TO ROLE ECONOMIC_ADMIN;

-- === ECONOMIC_WRITE: Usage and create on CONFIG, UTIL, RAW ===
GRANT USAGE ON SCHEMA ECONOMIC.CONFIG TO ROLE ECONOMIC_WRITE;
GRANT USAGE ON SCHEMA ECONOMIC.UTIL TO ROLE ECONOMIC_WRITE;
GRANT USAGE, CREATE TABLE ON SCHEMA ECONOMIC.RAW TO ROLE ECONOMIC_WRITE;

-- ECONOMIC_WRITE: Usage on BRONZE and SILVER for reading
GRANT USAGE ON SCHEMA ECONOMIC.BRONZE TO ROLE ECONOMIC_WRITE;
GRANT USAGE ON SCHEMA ECONOMIC.SILVER TO ROLE ECONOMIC_WRITE;

-- === ECONOMIC_READ: Usage on BRONZE and SILVER only ===
GRANT USAGE ON SCHEMA ECONOMIC.BRONZE TO ROLE ECONOMIC_READ;
GRANT USAGE ON SCHEMA ECONOMIC.SILVER TO ROLE ECONOMIC_READ;

/*******************************************************************************
 * STEP 7: OBJECT-LEVEL GRANTS (Existing Objects)
 ******************************************************************************/

-- === CONFIG schema: Read access for ECONOMIC_WRITE ===
GRANT SELECT ON ALL TABLES IN SCHEMA ECONOMIC.CONFIG TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON ALL VIEWS IN SCHEMA ECONOMIC.CONFIG TO ROLE ECONOMIC_WRITE;

-- === UTIL schema: Execute procedures and functions ===
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA ECONOMIC.UTIL TO ROLE ECONOMIC_WRITE;
GRANT USAGE ON ALL PROCEDURES IN SCHEMA ECONOMIC.UTIL TO ROLE ECONOMIC_WRITE;

-- === RAW schema: Full access for ECONOMIC_WRITE ===
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ECONOMIC.RAW TO ROLE ECONOMIC_WRITE;

-- === BRONZE schema: Read access ===
GRANT SELECT ON ALL VIEWS IN SCHEMA ECONOMIC.BRONZE TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON ALL VIEWS IN SCHEMA ECONOMIC.BRONZE TO ROLE ECONOMIC_READ;

-- === SILVER schema: Read access ===
GRANT SELECT ON ALL VIEWS IN SCHEMA ECONOMIC.SILVER TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON ALL VIEWS IN SCHEMA ECONOMIC.SILVER TO ROLE ECONOMIC_READ;

/*******************************************************************************
 * STEP 8: FUTURE GRANTS (For Objects Created After Deployment)
 *
 * Automatically grant permissions to new objects as they are created.
 * This is critical for maintenance and updates.
 ******************************************************************************/

-- === CONFIG schema future grants ===
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECONOMIC.CONFIG TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ECONOMIC.CONFIG TO ROLE ECONOMIC_WRITE;

-- === UTIL schema future grants ===
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA ECONOMIC.UTIL TO ROLE ECONOMIC_WRITE;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA ECONOMIC.UTIL TO ROLE ECONOMIC_WRITE;

-- === RAW schema future grants ===
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA ECONOMIC.RAW TO ROLE ECONOMIC_WRITE;

-- === BRONZE schema future grants ===
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ECONOMIC.BRONZE TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ECONOMIC.BRONZE TO ROLE ECONOMIC_READ;

-- === SILVER schema future grants ===
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ECONOMIC.SILVER TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ECONOMIC.SILVER TO ROLE ECONOMIC_READ;

/*******************************************************************************
 * STEP 9: WAREHOUSE GRANTS
 *
 * Grant access to COMPUTE_WH for query execution.
 * Adjust warehouse name if your environment uses a different warehouse.
 ******************************************************************************/

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ECONOMIC_ADMIN;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ECONOMIC_WRITE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ECONOMIC_READ;

/*******************************************************************************
 * VERIFICATION
 ******************************************************************************/

-- Show created database and schemas
SHOW DATABASES LIKE 'ECONOMIC';
SHOW SCHEMAS IN DATABASE ECONOMIC;

-- Show created roles
SHOW ROLES LIKE 'ECONOMIC%';

-- Show role hierarchy
-- SHOW GRANTS TO ROLE ECONOMIC_ADMIN;
-- SHOW GRANTS TO ROLE ECONOMIC_WRITE;
-- SHOW GRANTS TO ROLE ECONOMIC_READ;

-- Show grants on database
-- SHOW GRANTS ON DATABASE ECONOMIC;

-- Show grants on specific schema
-- SHOW GRANTS ON SCHEMA ECONOMIC.RAW;
-- SHOW GRANTS ON SCHEMA ECONOMIC.BRONZE;
-- SHOW GRANTS ON SCHEMA ECONOMIC.SILVER;

/*******************************************************************************
 * GRANTING ROLES TO USERS
 *
 * After deployment, grant roles to appropriate users:
 ******************************************************************************/

-- Grant to administrators
-- GRANT ROLE ECONOMIC_ADMIN TO USER admin_user;

-- Grant to data engineers
-- GRANT ROLE ECONOMIC_WRITE TO USER data_engineer_user;

-- Grant to analysts and BI tools
-- GRANT ROLE ECONOMIC_READ TO USER analyst_user;
-- GRANT ROLE ECONOMIC_READ TO USER tableau_service_account;

/*******************************************************************************
 * SETTING DEFAULT ROLES
 *
 * Users can have a default role that is automatically activated:
 ******************************************************************************/

-- Set default role for users
-- ALTER USER analyst_user SET DEFAULT_ROLE = ECONOMIC_READ;
-- ALTER USER data_engineer_user SET DEFAULT_ROLE = ECONOMIC_WRITE;

/*******************************************************************************
 * ROLE USAGE GUIDELINES
 ******************************************************************************/

-- ECONOMIC_ADMIN:
-- - Deploy and update the Economic integration
-- - Create/modify schemas, tables, views, procedures
-- - Grant roles to users
-- - Monitor and troubleshoot
-- - Access to all secrets and integrations

-- ECONOMIC_WRITE:
-- - Run data ingestion procedures
-- - Call API functions
-- - Write to RAW tables
-- - Read from all schemas (CONFIG, UTIL, RAW, BRONZE, SILVER)
-- - Troubleshoot data ingestion issues

-- ECONOMIC_READ:
-- - Query BRONZE and SILVER views
-- - Create reports and dashboards
-- - NO access to RAW data or ingestion procedures
-- - NO ability to modify data

/*******************************************************************************
 * END OF FILE 02
 ******************************************************************************/
