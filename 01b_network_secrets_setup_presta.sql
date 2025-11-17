/*******************************************************************************
 * FILE 01B: NETWORK RULES & SECRETS SETUP - PRESTASHOP
 *
 * Purpose: Configures network access and authentication for PrestaShop API
 *
 * Creates:
 * - Network rule allowing egress to PrestaShop API endpoints
 * - Secrets for API authentication (placeholder values)
 * - External access integration combining network rules and secrets
 *
 * Authentication:
 * PrestaShop API uses a single WS Key for authentication
 *
 * IMPORTANT: After deployment, update secrets with actual credentials!
 ******************************************************************************/

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE NETWORK RULE PRESTASHOP_API_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('dogcopenhagen.com')
  COMMENT = 'Network rule for PrestaShop API (dogcopenhagen.com)';

CREATE OR REPLACE SECRET PRESTASHOP_WS_KEY
  TYPE = GENERIC_STRING
  SECRET_STRING = 'YOUR_PRESTASHOP_WS_KEY_HERE'
  COMMENT = 'PrestaShop API WS Key - UPDATE THIS AFTER DEPLOYMENT';

GRANT USAGE ON SECRET PRESTASHOP_WS_KEY TO ROLE SYSADMIN;
GRANT USAGE ON SECRET PRESTASHOP_WS_KEY TO ROLE ECONOMIC_ADMIN;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PRESTASHOP_API_INTEGRATION
  ALLOWED_NETWORK_RULES = (PRESTASHOP_API_NETWORK_RULE)
  ALLOWED_AUTHENTICATION_SECRETS = (PRESTASHOP_WS_KEY)
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