/*******************************************************************************
 * FILE 01: NETWORK RULES & SECRETS SETUP
 *
 * Purpose: Configures network access and authentication for Economic API
 *
 * Creates:
 * - Network rule allowing egress to PrestaShop API endpoints
 * - Secrets for API authentication (placeholder values)
 * - External access integration combining network rules and secrets
 *
 * Authentication:
 * PrestaShop API uses one token:
 * 1. X-AppSecretToken       - Application secret
 * 2. X-AgreementGrantToken  - Agreement grant token
 *
 * IMPORTANT: After deployment, update secrets with actual credentials!
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE NETWORK RULE PRESTASHOP_API_NETWORK_RULE
  MODE = EGRESS TYPE = HOST_PORT
  VALUE_LIST = ('{{ REPLACE_WITH_YOUR_PRESTASHOP_DOMAIN }}')
  COMMENT = 'Network rule for PrestaShop API - UPDATE VALUE_LIST with your domain';



-- Create API secret for PrestaShop WS Key
-- The PrestaShop API authenticates with a single ws_key parameter
CREATE OR REPLACE SECRET PRESTASHOP_WS_KEY
  TYPE = GENERIC_STRING
  SECRET_STRING = '{{ REPLACE_WITH_WSKEY }}'
  COMMENT = 'PrestaShop API AccessKey - UPDATE THIS AFTER DEPLOYMENT';

-- Create secret for PrestaShop domain
-- This allows the UDF to be portable across different PrestaShop installations
CREATE OR REPLACE SECRET PRESTASHOP_DOMAIN
  TYPE = GENERIC_STRING
  SECRET_STRING = '{{ REPLACE_WITH_YOUR_DOMAIN }}'
  COMMENT = 'PrestaShop domain (e.g., yourstore.com) - UPDATE THIS AFTER DEPLOYMENT';

 -- STEP 3: GRANT USAGE ON SECRETS

GRANT USAGE ON SECRET PRESTASHOP_WS_KEY TO ROLE SYSADMIN;
GRANT USAGE ON SECRET PRESTASHOP_DOMAIN TO ROLE SYSADMIN;

-- Grant to ECONOMIC_ADMIN role (will be created in next file)
-- These grants will become effective once the role exists
GRANT USAGE ON SECRET PRESTASHOP_WS_KEY TO ROLE ECONOMIC_ADMIN;
GRANT USAGE ON SECRET PRESTASHOP_DOMAIN TO ROLE ECONOMIC_ADMIN;



CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PRESTASHOP_API_INTEGRATION
  ALLOWED_NETWORK_RULES = (PRESTASHOP_API_NETWORK_RULE)
  ALLOWED_AUTHENTICATION_SECRETS = (PRESTASHOP_WS_KEY, PRESTASHOP_DOMAIN)
  ENABLED = TRUE
  COMMENT = 'External access integration for PrestaShop API';


GRANT USAGE ON INTEGRATION PRESTASHOP_API_INTEGRATION TO ROLE SYSADMIN;
GRANT USAGE ON INTEGRATION PRESTASHOP_API_INTEGRATION TO ROLE ECONOMIC_ADMIN;

/*******************************************************************************
 * VERIFICATION
 ******************************************************************************/

-- Verify network rule created
SHOW NETWORK RULES LIKE 'PRESTASHOP%';

-- Verify secrets created
SHOW SECRETS LIKE 'PRESTASHOP%';

-- Show integration details
SHOW INTEGRATIONS LIKE 'PRESTASHOP%';

/*******************************************************************************
 * END OF FILE 01b
 ******************************************************************************/