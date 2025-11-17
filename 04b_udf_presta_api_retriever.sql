/*******************************************************************************
 * FILE 04b: PrestaShop API UDF V2
 *
 * Purpose: Python UDF for calling PrestasShop REST and OpenAPI endpoints
 *
 * Creates:
 * - UTIL.PRESTASHOP_API function
 *
 * Features:
 * - Supports "https://dogcopenhagen.com
 * - Auto-detection of demo mode (when secrets = 'demo')
 * - HTTP compression for faster transfers (gzip, deflate)
 * - Enhanced error handling with detailed diagnostics
 * - Flexible pagination support
 *
 * Parameters:
 * - ENDPOINTPATH: API endpoint path (e.g., 'categries', 'products')
 * - BASE: API type default 'REST'
 * - PAGESIZE: Number of records per request, default 1000
 * - STARTPAGE: Starting page number, default 0
 *
 * Returns:
 * - VARIANT: JSON response from Presta API
 *
 * Usage Examples:
 * - SELECT UTIL.PRESTASHOP_API('products', 'REST', 1000, 0);
 ******************************************************************************/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA UTIL;

/*******************************************************************************
 * CREATE Presta API UDF V3
 ******************************************************************************/

CREATE OR REPLACE FUNCTION PRESTASHOP_API(
  ENDPOINT VARCHAR,
  PAGESIZE INTEGER DEFAULT 1000,
  STARTPAGE INTEGER DEFAULT 0
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('requests')
HANDLER = 'prestashop_api_retriever'
EXTERNAL_ACCESS_INTEGRATIONS = (PRESTASHOP_API_INTEGRATION)
SECRETS = (
  'ws_key' = ECONOMIC.CONFIG.PRESTASHOP_WS_KEY
)
AS
$$
import _snowflake, requests
from requests.auth import HTTPBasicAuth

def prestashop_api_retriever(endpoint: str, pagesize: int = 1000, startpage: int = 0):
    """
    Fetch data from PrestaShop REST API with pagination support.

    PrestaShop API pagination:
    - Uses limit/offset pattern: ?limit=[start,limit]
    - Example: ?limit=0,50 (start at record 0, get 50 records)
    - We calculate: offset = startpage * pagesize
    """
    ws_key = _snowflake.get_generic_secret_string('ws_key').strip()
    domain = "dogcopenhagen.com"  # Hardcoded PrestaShop domain

    # Clean endpoint path
    endpoint = (endpoint or "").lstrip("/")

    # Calculate offset based on page number
    offset = startpage * pagesize

    # Build URL with pagination
    # PrestaShop uses ?limit=offset,pagesize format
    url = f"https://{domain}/api/{endpoint}"

    # Define which fields to fetch per endpoint (only what we need!)
    endpoint_fields = {
        'products': '[id,name,id_category_default,active,date_upd,date_add]',
        'combinations': 'full',
        'categories': '[id,name,id_parent,level_depth,active]',
        'product_option_values': '[id,id_attribute_group,name]'
    }

    # Get display parameter for this endpoint (default to full if not specified)
    display = endpoint_fields.get(endpoint, 'full')

    # Add query parameters
    params = {
        'output_format': 'JSON',
        'display': display,  # Only fetch fields we need
        'language': 1,  # Danish language only
        'limit': f"{offset},{pagesize}"
    }

    # PrestaShop uses Basic Auth with ws_key as username and empty password
    auth = HTTPBasicAuth(ws_key, '')

    headers = {
        "Accept": "application/json"
    }

    try:
        r = requests.get(url, params=params, auth=auth, headers=headers, timeout=30)

        if r.status_code != 200:
            # Mask last 4 chars of ws_key for security in error messages
            tail = lambda s: s[-4:] if isinstance(s, str) and len(s) >= 4 else s
            raise Exception(
                f"API request failed {r.status_code}: {r.text[:200]} :: "
                f"url={url} :: ws_key_tail={tail(ws_key)}"
            )

        return r.json()

    except requests.exceptions.Timeout:
        raise Exception(f"API request timed out after 30 seconds for endpoint: {endpoint}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"API request failed: {str(e)}")
$$;

/*******************************************************************************
 * GRANT USAGE TO ROLES
 ******************************************************************************/

-- Grant to ECONOMIC_ADMIN
GRANT USAGE ON FUNCTION PRESTASHOP_API(VARCHAR, INTEGER, INTEGER)
  TO ROLE ECONOMIC_ADMIN;

-- Grant to ECONOMIC_WRITE (for data engineers running ingestion)
GRANT USAGE ON FUNCTION PRESTASHOP_API(VARCHAR, INTEGER, INTEGER)
  TO ROLE ECONOMIC_WRITE;


/*******************************************************************************
 * END OF FILE 04b
 ******************************************************************************/
