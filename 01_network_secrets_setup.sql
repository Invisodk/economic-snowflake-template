/*******************************************************************************
 * FILE 01: NETWORK RULES & SECRETS SETUP
 *
 * Purpose: Configures network access and authentication for Economic API
 *
 * Creates:
 * - Network rule allowing egress to Economic API endpoints
 * - Secrets for API authentication (placeholder values)
 * - External access integration combining network rules and secrets
 *
 * Economic API Endpoints:
 * - restapi.e-conomic.com   (REST API)
 * - apis.e-conomic.com      (OpenAPI)
 *
 * Authentication:
 * Economic API uses two tokens:
 * 1. X-AppSecretToken       - Application secret
 * 2. X-AgreementGrantToken  - Agreement grant token
 *
 * IMPORTANT: After deployment, update secrets with actual credentials
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

/*******************************************************************************
 * STEP 1: CREATE NETWORK RULE FOR ECONOMIC API ACCESS
 *
 * Allows outbound HTTPS connections to Economic API domains.
 * Both REST and OpenAPI endpoints are included.
 ******************************************************************************/

CREATE OR REPLACE NETWORK RULE ECONOMIC_API_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'restapi.e-conomic.com',  -- REST API endpoint
    'apis.e-conomic.com'       -- OpenAPI endpoint
  )
  COMMENT = 'Network rule for Economic API access (REST and OpenAPI endpoints)';

/*******************************************************************************
 * STEP 2: CREATE SECRETS FOR API AUTHENTICATION
 *
 * PLACEHOLDER VALUES - UPDATE AFTER DEPLOYMENT
 *
 * To get your Economic API credentials:
 * 1. Log into your Economic account
 * 2. Go to Settings > API
 * 3. Create or view your API credentials
 * 4. Copy the AppSecretToken and AgreementGrantToken
 *
 * After deployment, update with:
 * ALTER SECRET ECONOMIC_XAPIKEY_APPSECRET
 *   SET SECRET_STRING = 'your_actual_appsecret_here';
 * ALTER SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT
 *   SET SECRET_STRING = 'your_actual_agreementgrant_here';
 *
 * For testing without credentials, use 'demo' for both secrets
 * and add &demo=true to API URLs (UDF handles this automatically).
 ******************************************************************************/

-- Secret 1: Application Secret Token
CREATE OR REPLACE SECRET ECONOMIC_XAPIKEY_APPSECRET
  TYPE = GENERIC_STRING
  SECRET_STRING = '{{ REPLACE_WITH_APPSECRET }}'
  COMMENT = 'Economic API X-AppSecretToken - UPDATE THIS AFTER DEPLOYMENT';

-- Secret 2: Agreement Grant Token
CREATE OR REPLACE SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT
  TYPE = GENERIC_STRING
  SECRET_STRING = '{{ REPLACE_WITH_AGREEMENTGRANT }}'
  COMMENT = 'Economic API X-AgreementGrantToken - UPDATE THIS AFTER DEPLOYMENT';

/*******************************************************************************
 * STEP 3: GRANT USAGE ON SECRETS
 *
 * Allows SYSADMIN and future ECONOMIC_ADMIN role to use these secrets.
 * UDFs will reference these secrets for API authentication.
 ******************************************************************************/

GRANT USAGE ON SECRET ECONOMIC_XAPIKEY_APPSECRET TO ROLE SYSADMIN;
GRANT USAGE ON SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT TO ROLE SYSADMIN;

-- Grant to ECONOMIC_ADMIN role (will be created in next file)
-- These grants will become effective once the role exists
GRANT USAGE ON SECRET ECONOMIC_XAPIKEY_APPSECRET TO ROLE ECONOMIC_ADMIN;
GRANT USAGE ON SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT TO ROLE ECONOMIC_ADMIN;

/*******************************************************************************
 * STEP 4: CREATE EXTERNAL ACCESS INTEGRATION
 *
 * Combines network rules and secrets into a single integration.
 * This integration is referenced by UDFs to access the Economic API.
 ******************************************************************************/

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ECONOMIC_API_INTEGRATION
  ALLOWED_NETWORK_RULES = (ECONOMIC_API_NETWORK_RULE)
  ALLOWED_AUTHENTICATION_SECRETS = (
    ECONOMIC_XAPIKEY_APPSECRET,
    ECONOMIC_XAPIKEY_AGREEMENTGRANT
  )
  ENABLED = TRUE
  COMMENT = 'External access integration for Economic API (REST and OpenAPI)';

/*******************************************************************************
 * STEP 5: GRANT USAGE ON INTEGRATION
 *
 * Allows roles to use this integration in UDFs.
 ******************************************************************************/

GRANT USAGE ON INTEGRATION ECONOMIC_API_INTEGRATION TO ROLE SYSADMIN;
GRANT USAGE ON INTEGRATION ECONOMIC_API_INTEGRATION TO ROLE ECONOMIC_ADMIN;

/*******************************************************************************
 * VERIFICATION
 ******************************************************************************/

-- Verify network rule created
SHOW NETWORK RULES LIKE 'ECONOMIC%';

-- Verify secrets created (SECRET_STRING will be hidden)
SHOW SECRETS LIKE 'ECONOMIC%';

-- Verify integration created
SHOW INTEGRATIONS LIKE 'ECONOMIC%';

-- Check grants on secrets
-- SHOW GRANTS ON SECRET ECONOMIC_XAPIKEY_APPSECRET;
-- SHOW GRANTS ON SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT;

/*******************************************************************************
 * POST-DEPLOYMENT: UPDATE SECRETS
 ******************************************************************************/

-- After deployment is complete, run these commands to update with real credentials:

-- Option 1: Production credentials
-- ALTER SECRET ECONOMIC_XAPIKEY_APPSECRET
--   SET SECRET_STRING = 'your_actual_appsecret_token_here';
-- ALTER SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT
--   SET SECRET_STRING = 'your_actual_agreementgrant_token_here';

-- Option 2: Demo mode (for testing without credentials)
-- ALTER SECRET ECONOMIC_XAPIKEY_APPSECRET
--   SET SECRET_STRING = 'demo';
-- ALTER SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT
--   SET SECRET_STRING = 'demo';

/*******************************************************************************
 * SECURITY NOTES
 ******************************************************************************/

-- NEVER commit actual credentials to Git repositories
-- Use Snowflake's secret management to store sensitive tokens
-- Rotate tokens regularly according to your security policy
-- Grant secret access only to roles that need it
-- Monitor secret usage via query history

/*******************************************************************************
 * END OF FILE 01
 ******************************************************************************/
