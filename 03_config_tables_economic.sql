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
        ENDPOINT_PATH STRING,
        -- e.g. 'customers'  or 'customersapi/v3.0.1/Contacts'
        BASE STRING DEFAULT 'REST',
        -- 'REST' | 'OPENAPI'
        DESCRIPTION STRING,
        DEFAULT_PAGESIZE NUMBER DEFAULT 1000,
        DEFAULT_STARTPAGE NUMBER DEFAULT 0,
        ACTIVE BOOLEAN DEFAULT TRUE,
        UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
    );

COMMENT ON TABLE ECONOMIC_ENDPOINTS IS 'Configuration table for Economic API endpoints (REST and OPENAPI)';

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
VALUES (
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
        'Booked invoices',
        1000,
        TRUE
    ),
    (
        'invoices/drafts',
        'REST',
        'Draft invoices',
        1000,
        FALSE
    ),
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
        'invoices/booked/lines',
        'OPENAPI',
        'Booked invoice lines - bulk',
        1000,
        TRUE
    ),
    (
        'journalsapi/v1.0.0/entries/booked',
        'OPENAPI',
        'Booked journal entries',
        1000,
        FALSE
    ),
    (
        'customersapi/v3.0.1/Contacts/paged',
        'OPENAPI',
        'Customer contacts (paged)',
        1000,
        FALSE
    );

/*******************************************************************************
 * VERIFICATION
 ******************************************************************************/

-- Show all configured endpoints
-- SELECT
--     ENDPOINT_PATH,
--     BASE,
--     DESCRIPTION,
--     DEFAULT_PAGESIZE,
--     ACTIVE,
--     UPDATED_AT
-- FROM CONFIG.ECONOMIC_ENDPOINTS
-- ORDER BY ACTIVE DESC, BASE, ENDPOINT_PATH;

-- Count active vs inactive endpoints
SELECT
    BASE,
    ACTIVE,
    COUNT(*) AS ENDPOINT_COUNT
FROM CONFIG.ECONOMIC_ENDPOINTS
GROUP BY BASE, ACTIVE
ORDER BY BASE, ACTIVE DESC;

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

-- Change page size
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

/*******************************************************************************
 * END OF FILE 03
 ******************************************************************************/
