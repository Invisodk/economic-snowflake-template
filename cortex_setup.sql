/*---------------------------------------------------------------*/
/**                                                             **/
/*** SNOWFLAKE INTELLIGENCE SETUP FOR DOG COPENHAGEN           **/
/*** Cortex Analyst configuration for Economic & PrestaShop    **/
/*** API data integration                                      **/
/**                                                             **/
/*** Prerequisites: ECONOMIC database with SILVER views        **/
/*** Main data sources:                                        **/
/***   - SILVER.VW_SALES_DETAIL (enriched with PrestaShop)     **/
/***   - SILVER.VW_CUSTOMER_MASTER                             **/
/***   - SILVER.VW_PRODUCT_MASTER (PrestaShop catalog)         **/
/**                                                             **/
/*** ACCESS TO ACCOUNTADMIN ROLE REQUIRED FOR SETUP            **/
/**                                                             **/
/*---------------------------------------------------------------*/

-- Summary of objects created in this script:
--
-- Roles:
--   - snowflake_intelligence_admin (or extend ECONOMIC_ADMIN)
--
-- Warehouses:
--   - cortex_wh (for Cortex Analyst queries)
--
-- Databases:
--   - snowflake_intelligence (required for agents)
--
-- Schemas:
--   - snowflake_intelligence.agents
--   - economic.cortex (for semantic models stage)
--
-- Stages:
--   - economic.cortex.semantic_models (for YAML file upload)
--
-- Notification Integration:
--   - email_integration_dogcopenhagen
--
-- Stored Procedure:
--   - economic.cortex.send_email
--
-- Grants:
--   - Access to ECONOMIC.SILVER views for intelligence role
--   - Access to VW_SALES_DETAIL, VW_CUSTOMER_MASTER, VW_PRODUCT_MASTER


/*---------------------------------------------------------------*/
/*** STEP 1: CREATE ROLE AND WAREHOUSE                         ***/
/*---------------------------------------------------------------*/

USE ROLE ACCOUNTADMIN;

-- Create dedicated role for Snowflake Intelligence
-- (Or you can use your existing PLAYGROUND_ADMIN role)
CREATE OR REPLACE ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;

-- Grant role to current user
-- Replace 'YOUR_USERNAME' with your actual Snowflake username
-- Example: GRANT ROLE SNOWFLAKE_INTELLIGENCE_ADMIN TO USER "STORM.SORENSEN";
GRANT ROLE SNOWFLAKE_INTELLIGENCE_ADMIN TO USER "YOUR_USERNAME";

-- Create warehouse for Cortex Analyst queries
USE ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
CREATE OR REPLACE WAREHOUSE CORTEX_WH
  WITH WAREHOUSE_SIZE='X-Small'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for Cortex Analyst';


/*---------------------------------------------------------------*/
/*** STEP 2: CREATE SNOWFLAKE_INTELLIGENCE DATABASE            ***/
/*** (Required for agent storage)                              ***/
/*---------------------------------------------------------------*/

USE ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;


/*---------------------------------------------------------------*/
/*** STEP 3: CREATE CORTEX SCHEMA IN ECONOMIC                  ***/
/*** (For semantic models and procedures)                      ***/
/*---------------------------------------------------------------*/

USE ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE ECONOMIC TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
GRANT CREATE SCHEMA ON DATABASE ECONOMIC TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;

USE ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
USE DATABASE ECONOMIC;

CREATE SCHEMA IF NOT EXISTS CORTEX
  COMMENT = 'Schema for Cortex Analyst semantic models and supporting procedures';

USE SCHEMA CORTEX;


/*---------------------------------------------------------------*/
/*** STEP 4: GRANT ACCESS TO EXISTING DATA                     ***/
/*---------------------------------------------------------------*/

USE ROLE ACCOUNTADMIN;
USE DATABASE ECONOMIC;

-- Grant access to schemas
GRANT USAGE ON SCHEMA SILVER TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
GRANT USAGE ON SCHEMA BRONZE TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;

-- Grant SELECT on key Silver views (used in semantic model)
GRANT SELECT ON VIEW SILVER.VW_SALES_DETAIL TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
GRANT SELECT ON VIEW SILVER.VW_CUSTOMER_MASTER TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
GRANT SELECT ON VIEW SILVER.VW_PRODUCT_MASTER TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;

-- Grant SELECT on Bronze views (in case semantic model needs them)
GRANT SELECT ON ALL VIEWS IN SCHEMA BRONZE TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;


/*---------------------------------------------------------------*/
/*** STEP 5: CREATE STAGE FOR SEMANTIC MODELS                  ***/
/*---------------------------------------------------------------*/

USE ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA CORTEX;
USE WAREHOUSE CORTEX_WH;

CREATE OR REPLACE STAGE SEMANTIC_MODELS
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Stage for uploading Cortex Analyst semantic model YAML files';


/*---------------------------------------------------------------*/
/*** STEP 6: CREATE EMAIL NOTIFICATION INTEGRATION             ***/
/*---------------------------------------------------------------*/

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE NOTIFICATION INTEGRATION EMAIL_INTEGRATION_DOGCOPENHAGEN
  TYPE=EMAIL
  ENABLED=TRUE
  DEFAULT_SUBJECT = 'Dog Copenhagen - Snowflake Intelligence';

-- Grant usage to intelligence role
GRANT USAGE ON INTEGRATION EMAIL_INTEGRATION_DOGCOPENHAGEN TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;


/*---------------------------------------------------------------*/
/*** STEP 7: CREATE SEND_EMAIL STORED PROCEDURE                ***/
/*---------------------------------------------------------------*/

USE ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA CORTEX;

CREATE OR REPLACE PROCEDURE SEND_EMAIL(
    recipient_email VARCHAR,
    subject VARCHAR,
    body VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_email'
COMMENT = 'Send email notifications from Cortex Analyst agent'
AS
$$
def send_email(session, recipient_email, subject, body):
    try:
        # Escape single quotes in the body
        escaped_body = body.replace("'", "''")

        # Execute the system procedure call
        session.sql(f"""
            CALL SYSTEM$SEND_EMAIL(
                'EMAIL_INTEGRATION_DOGCOPENHAGEN',
                '{recipient_email}',
                '{subject}',
                '{escaped_body}',
                'text/html'
            )
        """).collect()

        return "Email sent successfully"
    except Exception as e:
        return f"Error sending email: {str(e)}"
$$;


/*---------------------------------------------------------------*/
/*** STEP 8: ENABLE CROSS-REGION CORTEX   (Already done!)                      ***/
/*---------------------------------------------------------------*/

-- USE ROLE ACCOUNTADMIN;

-- Enable cross-region Cortex (adjust region as needed)
-- For EU customers, use 'AWS_EU_CENTRAL_1' or appropriate region
-- ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US_WEST_2';


/*---------------------------------------------------------------*/
/*** STEP 9: VERIFICATION                                      ***/
/*---------------------------------------------------------------*/

USE ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
USE DATABASE ECONOMIC;
USE WAREHOUSE CORTEX_WH;

-- Verify access to data
SELECT 'Testing access to VW_SALES_DETAIL...' AS status;
SELECT COUNT(*) AS row_count FROM ECONOMIC.SILVER.VW_SALES_DETAIL;

SELECT 'Testing access to VW_CUSTOMER_MASTER...' AS status;
SELECT COUNT(*) AS row_count FROM ECONOMIC.SILVER.VW_CUSTOMER_MASTER;

SELECT 'Testing access to VW_PRODUCT_MASTER...' AS status;
SELECT COUNT(*) AS row_count FROM ECONOMIC.SILVER.VW_PRODUCT_MASTER;

-- Show created objects
SHOW WAREHOUSES LIKE 'CORTEX_WH';
SHOW STAGES IN SCHEMA ECONOMIC.CORTEX;
SHOW PROCEDURES IN SCHEMA ECONOMIC.CORTEX;

SELECT '
╔══════════════════════════════════════════════════════════════╗
║  Snowflake Intelligence Setup Complete!                      ║
╚══════════════════════════════════════════════════════════════╝

NEXT STEPS:
-----------
1. Upload semantic model YAML file:
   - In Snowsight: AI & ML → Cortex Analyst
   - Click "Create new model" → "Upload your YAML file"
   - Upload: semantic_model.yaml
   - Select: ECONOMIC.CORTEX.SEMANTIC_MODELS

2. Create Cortex Analyst Agent:
   - In Snowsight: AI & ML → Agents
   - Schema: SNOWFLAKE_INTELLIGENCE.AGENTS
   - Add Cortex Analyst tool with your semantic model
   - Warehouse: CORTEX_WH

3. Test queries (examples showcasing PrestaShop enrichment):
   - "What were our total sales by category?"
   - "Show me top 10 products by revenue with size and color"
   - "Which size and color combinations perform best?"
   - "Compare category performance between B2B vs B2C"
   - "What products have low inventory but high sales?"
   - "Show me sales by delivery country"

OBJECTS CREATED:
----------------
Role:       SNOWFLAKE_INTELLIGENCE_ADMIN
Warehouse:  CORTEX_WH
Database:   SNOWFLAKE_INTELLIGENCE (for agents)
Schema:     ECONOMIC.CORTEX (for semantic models)
Stage:      ECONOMIC.CORTEX.SEMANTIC_MODELS
Procedure:  ECONOMIC.CORTEX.SEND_EMAIL
Integration: EMAIL_INTEGRATION_DOGCOPENHAGEN

DATA ACCESS:
------------
✓ ECONOMIC.SILVER.VW_SALES_DETAIL (enriched with PrestaShop)
✓ ECONOMIC.SILVER.VW_CUSTOMER_MASTER
✓ ECONOMIC.SILVER.VW_PRODUCT_MASTER (PrestaShop catalog)
✓ ECONOMIC.BRONZE.* (all views)

PRESTASHOP ENRICHMENT:
---------------------
✓ Product categories (Harnesses, Leads, Collars, Bowls, Bags)
✓ Size variants (XS, S, M, L, XL, M/L, L/XL, One-Size)
✓ Color variants (Black, Mocca, Ocean Blue, Orange Sun, etc.)
✓ EAN13 barcodes
✓ Stock quantities
✓ 85%+ match rate for 2024-2025 data

' AS setup_complete;