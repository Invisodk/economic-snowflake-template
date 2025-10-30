/*******************************************************************************
 * FILE 05: RAW TABLES
 *
 * Purpose: Creates landing zone tables for raw JSON data from Economic API
 *
 * Creates:
 * - RAW.ECONOMIC_RESTAPI_JSON (for REST API responses)
 * - RAW.ECONOMIC_OPENAPI_JSON (for OpenAPI responses)
 *
 * These tables serve as the landing zone in the medallion architecture.
 * No transformation occurs here - only storage of raw API responses.
 *
 * Table Structure:
 * - DATE_INSERTED: Timestamp when data was loaded
 * - API_ENDPOINT: Endpoint path that was called
 * - PAGE_NUMBER: Pagination page number
 * - RECORD_COUNT_PER_PAGE: Number of records in this page
 * - COLLECTION_JSON: Raw JSON response (VARIANT type)
 *
 * Data Flow:
 * API → UDF → Ingestion Procedure → RAW Tables → Bronze Views → Silver Views
 ******************************************************************************/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA RAW;

/*******************************************************************************
 * TABLE 1: ECONOMIC_RESTAPI_JSON
 *
 * Stores raw JSON responses from Economic REST API endpoints.
 * REST API responses typically have a 'collection' array containing records.
 ******************************************************************************/

CREATE OR REPLACE TABLE ECONOMIC_RESTAPI_JSON (
  DATE_INSERTED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  -- Timestamp when this record was inserted (useful for tracking data freshness)

  API_ENDPOINT STRING NOT NULL,
  -- API endpoint path (e.g., 'customers', 'products', 'invoices/booked')

  PAGE_NUMBER NUMBER NOT NULL,
  -- Page number from pagination (starts at 0)

  RECORD_COUNT_PER_PAGE NUMBER,
  -- Number of records in this page (used to detect last page)

  COLLECTION_JSON VARIANT NOT NULL
  -- Raw JSON response from API (stored as VARIANT for flexible querying)
)
COMMENT = 'Raw JSON data from Economic REST API endpoints (collection array structure)';

-- Add clustering key for better query performance on frequently filtered columns
ALTER TABLE ECONOMIC_RESTAPI_JSON CLUSTER BY (API_ENDPOINT, DATE_INSERTED);

/*******************************************************************************
 * TABLE 2: ECONOMIC_OPENAPI_JSON
 *
 * Stores raw JSON responses from Economic OpenAPI endpoints.
 * OpenAPI responses may have different structure than REST (often 'items' array).
 ******************************************************************************/

CREATE OR REPLACE TABLE ECONOMIC_OPENAPI_JSON (
  DATE_INSERTED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  -- Timestamp when this record was inserted

  API_ENDPOINT STRING NOT NULL,
  -- API endpoint path (e.g., 'journalsapi/v1.0.0/entries/booked')

  PAGE_NUMBER NUMBER NOT NULL,
  -- Page number from pagination (starts at 0)

  RECORD_COUNT_PER_PAGE NUMBER,
  -- Number of records in this page

  COLLECTION_JSON VARIANT NOT NULL
  -- Raw JSON response from API
)
COMMENT = 'Raw JSON data from Economic OpenAPI endpoints (items array structure)';

-- Add clustering key
ALTER TABLE ECONOMIC_OPENAPI_JSON CLUSTER BY (API_ENDPOINT, DATE_INSERTED);

/*******************************************************************************
 * GRANT PERMISSIONS
 *
 * Grants are already configured via future grants in file 02.
 * These explicit grants ensure permissions are applied immediately.
 ******************************************************************************/

-- ECONOMIC_ADMIN: Full access
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE ECONOMIC_RESTAPI_JSON TO ROLE ECONOMIC_ADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE ECONOMIC_OPENAPI_JSON TO ROLE ECONOMIC_ADMIN;

-- ECONOMIC_WRITE: Can write and truncate (for data engineers)
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE ECONOMIC_RESTAPI_JSON TO ROLE ECONOMIC_WRITE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE ECONOMIC_OPENAPI_JSON TO ROLE ECONOMIC_WRITE;

-- ECONOMIC_READ: No direct access to RAW (they use Bronze/Silver views)
-- Intentionally not granting access to RAW tables for read-only users

/*******************************************************************************
 * VERIFICATION
 ******************************************************************************/

-- Show created tables
SHOW TABLES IN SCHEMA ECONOMIC.RAW;

-- Check table structure
-- DESC TABLE ECONOMIC_RESTAPI_JSON;
-- DESC TABLE ECONOMIC_OPENAPI_JSON;

-- Check clustering
-- SHOW TABLES LIKE 'ECONOMIC%' IN SCHEMA RAW;

-- After first ingestion, check row counts
-- SELECT
--     'RESTAPI' AS TABLE_NAME,
--     COUNT(*) AS ROW_COUNT,
--     COUNT(DISTINCT API_ENDPOINT) AS UNIQUE_ENDPOINTS,
--     MIN(DATE_INSERTED) AS FIRST_LOAD,
--     MAX(DATE_INSERTED) AS LAST_LOAD
-- FROM ECONOMIC_RESTAPI_JSON
-- UNION ALL
-- SELECT
--     'OPENAPI' AS TABLE_NAME,
--     COUNT(*) AS ROW_COUNT,
--     COUNT(DISTINCT API_ENDPOINT) AS UNIQUE_ENDPOINTS,
--     MIN(DATE_INSERTED) AS FIRST_LOAD,
--     MAX(DATE_INSERTED) AS LAST_LOAD
-- FROM ECONOMIC_OPENAPI_JSON;

/*******************************************************************************
 * USAGE NOTES
 ******************************************************************************/

-- These tables are populated by:
-- - UTIL.ECONOMIC_RESTAPI_DATAINGEST_MONTHLY() procedure (file 06)
-- - UTIL.ECONOMIC_OPENAPI_DATAINGEST_MONTHLY() procedure (file 07)

-- Truncate Pattern:
-- Ingestion procedures truncate these tables before loading to ensure
-- clean full refresh each time. Modify procedures for incremental loads.

-- Data Retention:
-- Consider implementing Time Travel or archival strategy:
-- - Time Travel: Access historical data for up to 90 days
-- - Archival: Clone tables before truncate for historical analysis

-- Example: Create archive before loading
-- CREATE TABLE RAW.ECONOMIC_RESTAPI_JSON_ARCHIVE_20250129
--   CLONE RAW.ECONOMIC_RESTAPI_JSON;
-- CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST_MONTHLY();

-- Storage Optimization:
-- VARIANT columns are stored efficiently in Snowflake.
-- For very large datasets, consider:
-- - Partitioning by date or endpoint
-- - Periodic archival to separate tables
-- - Compression (automatic in Snowflake)

-- Clustering Benefits:
-- Clustering by (API_ENDPOINT, DATE_INSERTED) improves performance for:
-- - Filtering by specific endpoint
-- - Time-based queries (latest data)
-- - Bronze view queries (which filter by endpoint)

/*******************************************************************************
 * TROUBLESHOOTING
 ******************************************************************************/

-- Check table size and storage
-- SELECT
--     TABLE_NAME,
--     ROW_COUNT,
--     BYTES / (1024*1024*1024) AS SIZE_GB,
--     ACTIVE_BYTES / (1024*1024*1024) AS ACTIVE_SIZE_GB
-- FROM INFORMATION_SCHEMA.TABLES
-- WHERE TABLE_SCHEMA = 'RAW'
--   AND TABLE_NAME LIKE 'ECONOMIC%'
-- ORDER BY BYTES DESC;

-- Check clustering effectiveness
-- SELECT SYSTEM$CLUSTERING_INFORMATION('ECONOMIC_RESTAPI_JSON');

-- View sample data
-- SELECT
--     DATE_INSERTED,
--     API_ENDPOINT,
--     PAGE_NUMBER,
--     RECORD_COUNT_PER_PAGE
-- FROM ECONOMIC_RESTAPI_JSON
-- ORDER BY DATE_INSERTED DESC
-- LIMIT 10;

-- Inspect JSON structure for specific endpoint
-- SELECT COLLECTION_JSON
-- FROM ECONOMIC_RESTAPI_JSON
-- WHERE API_ENDPOINT = 'customers'
-- LIMIT 1;

-- Count records by endpoint
-- SELECT
--     API_ENDPOINT,
--     SUM(RECORD_COUNT_PER_PAGE) AS TOTAL_RECORDS,
--     COUNT(*) AS PAGE_COUNT,
--     MAX(PAGE_NUMBER) + 1 AS PAGES_LOADED
-- FROM ECONOMIC_RESTAPI_JSON
-- GROUP BY API_ENDPOINT
-- ORDER BY TOTAL_RECORDS DESC;

/*******************************************************************************
 * MAINTENANCE OPERATIONS
 ******************************************************************************/

-- Truncate tables (use with caution!)
-- TRUNCATE TABLE ECONOMIC_RESTAPI_JSON;
-- TRUNCATE TABLE ECONOMIC_OPENAPI_JSON;

-- Archive before truncate
-- CREATE TABLE RAW.ECONOMIC_RESTAPI_JSON_ARCHIVE_YYYYMMDD
--   CLONE RAW.ECONOMIC_RESTAPI_JSON;

-- Drop archived tables after retention period
-- DROP TABLE IF EXISTS RAW.ECONOMIC_RESTAPI_JSON_ARCHIVE_20241201;

-- Rebuild clustering (usually automatic, but can be manual)
-- ALTER TABLE ECONOMIC_RESTAPI_JSON RECLUSTER;

/*******************************************************************************
 * END OF FILE 05
 ******************************************************************************/
