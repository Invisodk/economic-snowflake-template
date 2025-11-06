/*******************************************************************************
 * FILE 03: CONFIGURATION TABLES
 *
 * Purpose: Creates configuration tables for PrestaShop API endpoints
 *
 * Creates:
 * - CONFIG.PRESTASHOP_ENDPOINTS table
 * - Populates with default PrestaShop API endpoint
 *
 * The PRESTASHOP_ENDPOINTS table controls which API endpoints are ingested:
 * - ENDPOINT_PATH: The API endpoint path (e.g., 'products', 'combinations')
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
 * STEP 1: CREATE PRESTASHOP_ENDPOINTS TABLE
 ******************************************************************************/

CREATE OR REPLACE TABLE PRESTASHOP_ENDPOINTS (
        ENDPOINT_PATH STRING,
        BASE STRING DEFAULT 'REST',
        DESCRIPTION STRING,
        DEFAULT_PAGESIZE NUMBER DEFAULT 1000,
        DEFAULT_STARTPAGE NUMBER DEFAULT 0,
        ACTIVE BOOLEAN DEFAULT TRUE,
        UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
    );

COMMENT ON TABLE PRESTASHOP_ENDPOINTS IS 'Configuration table for PRESTASHOP API endpoints';

/*******************************************************************************
 * STEP 2: POPULATE WITH DEFAULT ENDPOINTS
 *
 * Common PrestaShop API endpoints are pre-configured here.
 * Only REST endpoints for core data are active by default.
 *
 * To activate additional endpoints:
 * UPDATE CONFIG.PRESTASHOP_ENDPOINTS SET ACTIVE = TRUE WHERE ENDPOINT_PATH = 'xxx';
 *
 * To add custom endpoints:
 * INSERT INTO CONFIG.PRESTASHOP_ENDPOINTS (ENDPOINT_PATH, BASE, DESCRIPTION, ACTIVE)
 * VALUES ('your-endpoint', 'REST', 'Your description', TRUE);
 ******************************************************************************/

INSERT INTO PRESTASHOP_ENDPOINTS (
        ENDPOINT_PATH,
        BASE,
        DESCRIPTION,
        DEFAULT_PAGESIZE,
        ACTIVE
    )
VALUES (
        'products',
        'REST',
        'Products master data',
        1000,
        TRUE
    ),
    (
        'combinations',
        'REST',
        'Product variant/SKU for joining invoice to products',
        1000,
        TRUE
    ),
    (
        'categories',
        'REST',
        'Product hierachy',
        1000,
        TRUE
    ),
    (
        'product_option_values',
        'REST',
        'Product details -> sizes and colors',
        1000,
        TRUE
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
-- FROM CONFIG.PRESTASHOP_ENDPOINTS
-- ORDER BY ACTIVE DESC, BASE, ENDPOINT_PATH;

-- Count active vs inactive endpoints
SELECT
    BASE,
    ACTIVE,
    COUNT(*) AS ENDPOINT_COUNT
FROM CONFIG.PRESTASHOP_ENDPOINTS
GROUP BY BASE, ACTIVE
ORDER BY BASE, ACTIVE DESC;

/*******************************************************************************
 * COMMON CONFIGURATION TASKS
 ******************************************************************************/

-- Activate an endpoint
-- UPDATE CONFIG.PRESTASHOP_ENDPOINTS
-- SET ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
-- WHERE ENDPOINT_PATH = 'categories';

-- Deactivate an endpoint
-- UPDATE CONFIG.PRESTASHOP_ENDPOINTS
-- SET ACTIVE = FALSE, UPDATED_AT = CURRENT_TIMESTAMP()
-- WHERE ENDPOINT_PATH = 'products';

-- Add custom endpoint
-- INSERT INTO CONFIG.PRESTASHOP_ENDPOINTS (
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
 * END OF FILE 03b
 ******************************************************************************/
