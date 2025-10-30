/*******************************************************************************
 * FILE 03: CONFIGURATION TABLES
 *
 * Purpose: Creates configuration tables for Economic API endpoints
 *
 * Creates:
 * - CONFIG.ECONOMIC_ENDPOINTS table
 * - Populates with default Economic API endpoints (REST and OpenAPI)
 *
 * The ECONOMIC_ENDPOINTS table controls which API endpoints are ingested:
 * - ENDPOINT_PATH: The API endpoint path (e.g., 'customers', 'invoices/booked')
 * - BASE: API type ('REST' or 'OPENAPI')
 * - DESCRIPTION: Human-readable description
 * - DEFAULT_PAGESIZE: Number of records per API call
 * - DEFAULT_STARTPAGE: Starting page number (usually 0)
 * - ACTIVE: Boolean flag to enable/disable endpoint ingestion
 * - UPDATED_AT: Timestamp of last configuration change
 ******************************************************************************/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA CONFIG;

/*******************************************************************************
 * STEP 1: CREATE ECONOMIC_ENDPOINTS TABLE
 ******************************************************************************/

CREATE OR REPLACE TABLE ECONOMIC_ENDPOINTS (
    ENDPOINT_PATH STRING NOT NULL,
    -- e.g., 'customers', 'products', 'invoices/booked', 'customersapi/v3.0.1/Contacts'

    BASE STRING DEFAULT 'REST',
    -- API type: 'REST' or 'OPENAPI'
    -- REST: Uses restapi.e-conomic.com
    -- OPENAPI: Uses apis.e-conomic.com

    DESCRIPTION STRING,
    -- Human-readable description of the endpoint

    DEFAULT_PAGESIZE NUMBER DEFAULT 1000,
    -- Number of records to fetch per API call (pagination)

    DEFAULT_STARTPAGE NUMBER DEFAULT 0,
    -- Starting page number (usually 0)

    ACTIVE BOOLEAN DEFAULT TRUE,
    -- Set to FALSE to skip this endpoint during ingestion

    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    -- Timestamp of last configuration change

    CONSTRAINT pk_economic_endpoints PRIMARY KEY (ENDPOINT_PATH, BASE)
)
COMMENT = 'Configuration table for Economic API endpoints (REST and OpenAPI)';

/*******************************************************************************
 * STEP 2: POPULATE WITH DEFAULT ENDPOINTS
 *
 * Common Economic API endpoints are pre-configured here.
 * Only REST endpoints for core data are active by default.
 *
 * To activate additional endpoints:
 * UPDATE CONFIG.ECONOMIC_ENDPOINTS SET ACTIVE = TRUE WHERE ENDPOINT_PATH = 'xxx';
 *
 * To add custom endpoints:
 * INSERT INTO CONFIG.ECONOMIC_ENDPOINTS (ENDPOINT_PATH, BASE, DESCRIPTION, ACTIVE)
 * VALUES ('your-endpoint', 'REST', 'Your description', TRUE);
 ******************************************************************************/

INSERT INTO ECONOMIC_ENDPOINTS (
    ENDPOINT_PATH,
    BASE,
    DESCRIPTION,
    DEFAULT_PAGESIZE,
    ACTIVE
)
VALUES
    -- === ACTIVE REST ENDPOINTS (Core data) ===
    (
        'customers',
        'REST',
        'Customer master data',
        1000,
        TRUE
    ),
    (
        'products',
        'REST',
        'Product catalog',
        1000,
        TRUE
    ),
    (
        'invoices/booked',
        'REST',
        'Booked invoices (finalized)',
        1000,
        TRUE
    ),

    -- === INACTIVE REST ENDPOINTS (Optional) ===
    (
        'invoices/drafts',
        'REST',
        'Draft invoices (not finalized)',
        1000,
        FALSE
    ),
    (
        'orders/sent',
        'REST',
        'Sent orders',
        1000,
        FALSE
    ),
    (
        'quotes/sent',
        'REST',
        'Sent quotes',
        1000,
        FALSE
    ),
    (
        'employees',
        'REST',
        'Employee master data',
        1000,
        FALSE
    ),
    (
        'product-groups',
        'REST',
        'Product groups/categories',
        1000,
        FALSE
    ),
    (
        'customer-groups',
        'REST',
        'Customer groups/segments',
        1000,
        FALSE
    ),
    (
        'units',
        'REST',
        'Units of measure',
        1000,
        FALSE
    ),
    (
        'payment-terms',
        'REST',
        'Payment terms',
        1000,
        FALSE
    ),
    (
        'vat-zones',
        'REST',
        'VAT zones',
        1000,
        FALSE
    ),

    -- === ACCOUNTING ENDPOINTS (Year-specific) ===
    (
        'accounting-years',
        'REST',
        'Accounting years info',
        1000,
        FALSE
    ),
    (
        'accounting-years/2024/entries',
        'REST',
        'Accounting entries for 2024',
        1000,
        FALSE
    ),
    (
        'accounting-years/2024/periods',
        'REST',
        'Accounting periods for 2024',
        1000,
        FALSE
    ),
    (
        'accounting-years/2024/totals',
        'REST',
        'Accounting totals for 2024',
        1000,
        FALSE
    ),
    (
        'accounting-years/2025/entries',
        'REST',
        'Accounting entries for 2025',
        1000,
        FALSE
    ),
    (
        'accounting-years/2025/periods',
        'REST',
        'Accounting periods for 2025',
        1000,
        FALSE
    ),
    (
        'accounting-years/2025/totals',
        'REST',
        'Accounting totals for 2025',
        1000,
        FALSE
    ),

    -- === OPENAPI ENDPOINTS (Advanced features) ===
    (
        'journalsapi/v1.0.0/entries/booked',
        'OPENAPI',
        'Booked journal entries via OpenAPI',
        1000,
        FALSE
    ),
    (
        'customersapi/v3.0.1/Contacts/paged',
        'OPENAPI',
        'Customer contacts (paged) via OpenAPI',
        1000,
        FALSE
    );

/*******************************************************************************
 * VERIFICATION
 ******************************************************************************/

-- Show all configured endpoints
SELECT
    ENDPOINT_PATH,
    BASE,
    DESCRIPTION,
    DEFAULT_PAGESIZE,
    ACTIVE,
    UPDATED_AT
FROM CONFIG.ECONOMIC_ENDPOINTS
ORDER BY ACTIVE DESC, BASE, ENDPOINT_PATH;

-- Count active vs inactive endpoints
SELECT
    BASE,
    ACTIVE,
    COUNT(*) AS ENDPOINT_COUNT
FROM CONFIG.ECONOMIC_ENDPOINTS
GROUP BY BASE, ACTIVE
ORDER BY BASE, ACTIVE DESC;

-- Show only active endpoints (these will be ingested)
SELECT
    ENDPOINT_PATH,
    BASE,
    DESCRIPTION
FROM CONFIG.ECONOMIC_ENDPOINTS
WHERE ACTIVE = TRUE
ORDER BY BASE, ENDPOINT_PATH;

/*******************************************************************************
 * COMMON CONFIGURATION TASKS
 ******************************************************************************/

-- Activate an endpoint
-- UPDATE CONFIG.ECONOMIC_ENDPOINTS
-- SET ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
-- WHERE ENDPOINT_PATH = 'employees';

-- Deactivate an endpoint
-- UPDATE CONFIG.ECONOMIC_ENDPOINTS
-- SET ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()
-- WHERE ENDPOINT_PATH = 'invoices/drafts';

-- Change page size for large datasets
-- UPDATE CONFIG.ECONOMIC_ENDPOINTS
-- SET DEFAULT_PAGESIZE = 500, UPDATED_AT = CURRENT_TIMESTAMP()
-- WHERE ENDPOINT_PATH = 'invoices/booked';

-- Add custom endpoint
-- INSERT INTO CONFIG.ECONOMIC_ENDPOINTS (
--     ENDPOINT_PATH,
--     BASE,
--     DESCRIPTION,
--     DEFAULT_PAGESIZE,
--     ACTIVE
-- )
-- VALUES (
--     'your-custom-endpoint',
--     'REST',
--     'Your custom endpoint description',
--     1000,
--     TRUE
-- );

-- Activate all accounting endpoints for current year
-- UPDATE CONFIG.ECONOMIC_ENDPOINTS
-- SET ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
-- WHERE ENDPOINT_PATH LIKE 'accounting-years/2025%';

/*******************************************************************************
 * NOTES ON ENDPOINT SELECTION
 ******************************************************************************/

-- Active by default:
-- - customers: Required for customer dimension
-- - products: Required for product dimension
-- - invoices/booked: Primary sales data source

-- Common additions:
-- - employees: If you need salesperson analysis
-- - product-groups: For product categorization
-- - customer-groups: For customer segmentation
-- - units: For unit of measure standardization

-- Accounting endpoints:
-- - Update year numbers annually
-- - Activate only if you need detailed GL data
-- - These can generate large data volumes

-- OpenAPI endpoints:
-- - More detailed data than REST equivalents
-- - Use when REST endpoints don't provide enough detail
-- - May have different rate limits

/*******************************************************************************
 * END OF FILE 03
 ******************************************************************************/
