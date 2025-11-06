/*******************************************************************************
 * FILE 04: ECONOMIC API UDF V2
 *
 * Purpose: Python UDF for calling Economic REST and OpenAPI endpoints
 *
 * Creates:
 * - UTIL.ECONOMIC_API_V2 function
 *
 * Features:
 * - Supports both REST (restapi.e-conomic.com) and OpenAPI (apis.e-conomic.com)
 * - Auto-detection of demo mode (when secrets = 'demo')
 * - HTTP compression for faster transfers (gzip, deflate)
 * - Enhanced error handling with detailed diagnostics
 * - Flexible pagination support
 *
 * Parameters:
 * - ENDPOINTPATH: API endpoint path (e.g., 'customers', 'invoices/booked')
 * - BASE: API type ('REST' or 'OPENAPI'), default 'REST'
 * - PAGESIZE: Number of records per request, default 1000
 * - STARTPAGE: Starting page number, default 0
 *
 * Returns:
 * - VARIANT: JSON response from Economic API
 *
 * Usage Examples:
 * - SELECT UTIL.ECONOMIC_API_V2('customers', 'REST', 1000, 0);
 * - SELECT UTIL.ECONOMIC_API_V2('invoices/booked', 'REST', 500, 2);
 * - SELECT UTIL.ECONOMIC_API_V2('journalsapi/v1.0.0/entries/booked', 'OPENAPI', 1000, 0);
 ******************************************************************************/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA UTIL;

/*******************************************************************************
 * CREATE ECONOMIC API UDF V3
 ******************************************************************************/

CREATE OR REPLACE FUNCTION ECONOMIC_API_V2(
  ENDPOINTPATH VARCHAR,
  BASE STRING DEFAULT 'REST',          -- 'REST' | 'OPENAPI'
  PAGESIZE INTEGER DEFAULT 1000,
  STARTPAGE INTEGER DEFAULT 0,
  CURSOR_TOKEN VARCHAR DEFAULT NULL    -- For cursor-based pagination (OpenAPI bulk endpoints)
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('requests')
HANDLER = 'EconomicApiRetriever'
EXTERNAL_ACCESS_INTEGRATIONS = (ECONOMIC_API_INTEGRATION)
SECRETS = (
  'appsecret'      = ECONOMIC_XAPIKEY_APPSECRET,
  'agreementgrant' = ECONOMIC_XAPIKEY_AGREEMENTGRANT
)
AS
$$
import _snowflake, requests

def EconomicApiRetriever(endpointpath: str, base: str='REST', pagesize: int=5, startpage: int=0, cursor_token: str=None):
    appsecret = _snowflake.get_generic_secret_string('appsecret').strip()
    agreementgrant = _snowflake.get_generic_secret_string('agreementgrant').strip()

    base_norm = (base or 'REST').upper()
    if base_norm == 'OPENAPI':
        host = 'https://apis.e-conomic.com/q2capi/v5.0.0/'
    else:
        host = 'https://restapi.e-conomic.com/'

    endpointpath = (endpointpath or '').lstrip('/')

    # Build URL based on pagination type
    if base_norm == 'OPENAPI' and cursor_token:
        # Cursor-based pagination
        url = f"{host}{endpointpath}?pageSize={pagesize}&cursor={cursor_token}"
    elif base_norm == 'OPENAPI':
        # First page of cursor-based pagination (no cursor)
        url = f"{host}{endpointpath}?pageSize={pagesize}"
    else:
        # REST API with skippages pagination
        url = f"{host}{endpointpath}?skippages={startpage}&pagesize={pagesize}"

    # auto-demo mode when both secrets are 'demo'
    if appsecret == 'demo' and agreementgrant == 'demo':
        sep = '&' if '?' in url else '?'
        url = f"{url}{sep}demo=true"

    headers = {
        "X-AppSecretToken": appsecret,
        "X-AgreementGrantToken": agreementgrant,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate",  # Enable HTTP compression for faster transfers
    }

    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        tail = lambda s: s[-4:] if isinstance(s,str) and len(s)>=4 else s
        raise Exception(f"API request failed {r.status_code}: {r.text} :: url={url} :: app_tail={tail(appsecret)} :: agr_tail={tail(agreementgrant)}")
    return r.json()
$$;

/*******************************************************************************
 * GRANT USAGE TO ROLES
 ******************************************************************************/

-- Grant to ECONOMIC_ADMIN
GRANT USAGE ON FUNCTION ECONOMIC_API_V2(VARCHAR, STRING, INTEGER, INTEGER, VARCHAR)
  TO ROLE ECONOMIC_ADMIN;

-- Grant to ECONOMIC_WRITE (for data engineers running ingestion)
GRANT USAGE ON FUNCTION ECONOMIC_API_V2(VARCHAR, STRING, INTEGER, INTEGER, VARCHAR)
  TO ROLE ECONOMIC_WRITE;

/*******************************************************************************
 * VERIFICATION & TESTING
 ******************************************************************************/

-- Test with demo mode (if secrets are set to 'demo')
-- SELECT UTIL.ECONOMIC_API_V2('customers', 'REST', 10, 0);

-- Test specific endpoint
-- SELECT UTIL.ECONOMIC_API_V2('products', 'REST', 100, 0);

-- Test OpenAPI endpoint
-- SELECT UTIL.ECONOMIC_API_V2('journalsapi/v1.0.0/entries/booked', 'OPENAPI', 50, 0);

-- Test pagination (page 2)
-- SELECT UTIL.ECONOMIC_API_V2('invoices/booked', 'REST', 1000, 1);

/*******************************************************************************
 * USAGE NOTES
 ******************************************************************************/

-- Demo Mode Testing:
-- If your secrets are set to 'demo', the UDF automatically appends &demo=true
-- to the URL. This allows testing without production credentials.

-- Pagination:
-- Economic API uses skip-based pagination:
-- - STARTPAGE=0, PAGESIZE=1000: Records 1-1000
-- - STARTPAGE=1, PAGESIZE=1000: Records 1001-2000
-- - STARTPAGE=2, PAGESIZE=1000: Records 2001-3000

-- Performance:
-- - Recommended PAGESIZE: 500-1000 (balance between API calls and memory)
-- - HTTP compression (gzip) reduces transfer time by ~70%
-- - 30-second timeout prevents hanging on slow responses

-- Error Handling:
-- If the UDF fails, check:
-- 1. Secrets are set correctly (last 4 chars shown in error)
-- 2. Network rule allows egress to Economic API
-- 3. External access integration is granted to your role
-- 4. Endpoint path is correct (check Economic API docs)

-- Common Error Codes:
-- - 401 Unauthorized: Invalid API tokens
-- - 403 Forbidden: Tokens valid but no access to resource
-- - 404 Not Found: Invalid endpoint path
-- - 429 Too Many Requests: Rate limit exceeded
-- - 500 Internal Server Error: Economic API issue

/*******************************************************************************
 * TROUBLESHOOTING
 ******************************************************************************/

-- Check if function exists
-- SHOW FUNCTIONS LIKE 'ECONOMIC_API_V2' IN SCHEMA UTIL;

-- Check grants
-- SHOW GRANTS ON FUNCTION ECONOMIC_API_V2(VARCHAR, STRING, INTEGER, INTEGER);

-- Test with minimal call
-- SELECT UTIL.ECONOMIC_API_V2('customers', 'REST', 1, 0);

-- Parse response to see structure
-- SELECT
--     f.value:customerNumber::NUMBER AS customer_number,
--     f.value:name::STRING AS customer_name
-- FROM TABLE(
--     FLATTEN(
--         input => (SELECT UTIL.ECONOMIC_API_V2('customers', 'REST', 10, 0):collection)
--     )
-- ) f
-- LIMIT 10;

/*******************************************************************************
 * FUTURE ENHANCEMENTS (V4 Ideas)
 ******************************************************************************/

-- Potential improvements for future versions:
-- - Automatic retry logic with exponential backoff
-- - Rate limit handling (429 responses)
-- - Multi-country support (COUNTRYCODE parameter)
-- - Caching layer to reduce API calls
-- - Request/response logging for auditing
-- - Support for POST/PUT/DELETE (currently GET only)

/*******************************************************************************
 * END OF FILE 04
 ******************************************************************************/
