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
 * CREATE ECONOMIC API UDF
 ******************************************************************************/

CREATE OR REPLACE FUNCTION ECONOMIC_API_V2(
  ENDPOINTPATH VARCHAR,
  BASE STRING DEFAULT 'REST',          -- 'REST' | 'OPENAPI'
  PAGESIZE INTEGER DEFAULT 1000,
  STARTPAGE INTEGER DEFAULT 0,
  CURSOR_TOKEN VARCHAR DEFAULT NULL,   -- For cursor-based pagination (OpenAPI bulk endpoints)
  FILTER_PARAM VARCHAR DEFAULT NULL    -- For incremental loading (e.g., 'lastUpdated$gte:2025-11-17T14:23:45Z')
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('requests')
HANDLER = 'EconomicApiRetriever'
EXTERNAL_ACCESS_INTEGRATIONS = (ECONOMIC_API_INTEGRATION)
SECRETS = (
  'appsecret'      = ECONOMIC.CONFIG.ECONOMIC_XAPIKEY_APPSECRET,
  'agreementgrant' = ECONOMIC.CONFIG.ECONOMIC_XAPIKEY_AGREEMENTGRANT
)
AS
$$
import _snowflake, requests

def EconomicApiRetriever(endpointpath: str, base: str='REST', pagesize: int=5, startpage: int=0, cursor_token: str=None, filter_param: str=None):
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

    # Add filter parameter for incremental loading (if provided)
    if filter_param:
        url = f"{url}&filter={filter_param}"

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

GRANT USAGE ON FUNCTION ECONOMIC_API_V2(VARCHAR, STRING, INTEGER, INTEGER, VARCHAR, VARCHAR)
  TO ROLE ECONOMIC_ADMIN;

GRANT USAGE ON FUNCTION ECONOMIC_API_V2(VARCHAR, STRING, INTEGER, INTEGER, VARCHAR, VARCHAR)
  TO ROLE ECONOMIC_WRITE;

/*******************************************************************************
 * END OF FILE 04
 ******************************************************************************/
