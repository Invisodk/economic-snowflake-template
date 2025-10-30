/*******************************************************************************
 * FILE 07: OPENAPI INGESTION PROCEDURE
 *
 * Purpose: Ingests data from Economic OpenAPI endpoints into RAW table
 *
 * Creates:
 * - UTIL.ECONOMIC_OPENAPI_DATAINGEST_MONTHLY procedure
 *
 * Behavior:
 * - Truncates RAW.ECONOMIC_OPENAPI_JSON table (full refresh pattern)
 * - Queries CONFIG.ECONOMIC_ENDPOINTS for active OpenAPI endpoints
 * - Calls UTIL.ECONOMIC_API_V3 for each endpoint with pagination
 * - Handles 'items' array (OpenAPI structure) instead of 'collection'
 * - Inserts raw JSON responses into RAW table
 * - Returns success message with total record count
 *
 * Usage:
 * - CALL UTIL.ECONOMIC_OPENAPI_DATAINGEST_MONTHLY();
 ******************************************************************************/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE ECONOMIC_OPENAPI_DATAINGEST_MONTHLY()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
COMMENT = 'Ingests data from active OpenAPI endpoints in config table'
AS
$$
    try {
        // Step 1: Truncate table to ensure clean full refresh
        snowflake.execute({sqlText: "TRUNCATE TABLE RAW.ECONOMIC_OPENAPI_JSON"});

        // Step 2: Query for active OpenAPI endpoints
        const endpointQuerySql = `
            SELECT ENDPOINT_PATH, BASE, DEFAULT_PAGESIZE, DEFAULT_STARTPAGE
            FROM CONFIG.ECONOMIC_ENDPOINTS
            WHERE ACTIVE = TRUE AND UPPER(BASE) = 'OPENAPI'
            ORDER BY ENDPOINT_PATH
        `;

        const cursorResult = snowflake.createStatement({sqlText: endpointQuerySql}).execute();
        let totalRecords = 0;
        let endpointCount = 0;

        // Step 3: Process each active endpoint
        while (cursorResult.next()) {
            const ENDPOINT_PATH = cursorResult.getColumnValue(1);
            const BASE = cursorResult.getColumnValue(2);
            let pageSize = cursorResult.getColumnValue(3) || 1000;
            let startPage = cursorResult.getColumnValue(4) || 0;

            endpointCount++;
            let hasMorePages = true;
            let endpointRecords = 0;

            // Step 4: Fetch data page by page until API returns partial/empty page
            while (hasMorePages) {
                // Call API UDF
                const apiCallSql = `
                    SELECT UTIL.ECONOMIC_API_V3(?, ?, ?, ?) AS API_RESPONSE
                `;

                const stmt = snowflake.createStatement({
                    sqlText: apiCallSql,
                    binds: [ENDPOINT_PATH, BASE, pageSize, startPage]
                });
                const result = stmt.execute();

                // Step 5: Parse response and insert into RAW table
                if (result.next()) {
                    const jsonResult = result.getColumnValue(1);

                    // OpenAPI uses 'items' array instead of 'collection'
                    let recordCountPerPage = jsonResult.items ? jsonResult.items.length : 0;
                    totalRecords += recordCountPerPage;
                    endpointRecords += recordCountPerPage;

                    // Convert JSON object to string for PARSE_JSON
                    let jsonString = (typeof jsonResult === 'object') ? JSON.stringify(jsonResult) : jsonResult;

                    // Insert into RAW table
                    const insertSql = `
                        INSERT INTO RAW.ECONOMIC_OPENAPI_JSON
                        (DATE_INSERTED, API_ENDPOINT, PAGE_NUMBER, RECORD_COUNT_PER_PAGE, COLLECTION_JSON)
                        SELECT
                            CURRENT_TIMESTAMP(), ?, ?, ?, PARSE_JSON(?)
                    `;

                    const insertStmt = snowflake.createStatement({
                        sqlText: insertSql,
                        binds: [ENDPOINT_PATH, startPage, recordCountPerPage, jsonString]
                    });
                    insertStmt.execute();

                    // Step 6: Check if more pages exist
                    if (recordCountPerPage < pageSize) {
                        hasMorePages = false;
                    } else {
                        startPage++;
                    }
                } else {
                    hasMorePages = false;
                }
            }

            // Log progress for this endpoint
            // (In production, consider writing to a log table)
        }

        // Step 7: Return success message
        return `SUCCESS: Ingested data from ${endpointCount} OpenAPI endpoints. Total records: ${totalRecords}`;

    } catch (error) {
        // Return detailed error message
        return `ERROR: ${error.message} | Stack: ${error.stack}`;
    }
$$;

/*******************************************************************************
 * GRANT PERMISSIONS
 ******************************************************************************/

GRANT USAGE ON PROCEDURE ECONOMIC_OPENAPI_DATAINGEST_MONTHLY() TO ROLE ECONOMIC_ADMIN;
GRANT USAGE ON PROCEDURE ECONOMIC_OPENAPI_DATAINGEST_MONTHLY() TO ROLE ECONOMIC_WRITE;

/*******************************************************************************
 * USAGE EXAMPLES
 ******************************************************************************/

-- Execute the procedure
-- CALL UTIL.ECONOMIC_OPENAPI_DATAINGEST_MONTHLY();

-- Check results
-- SELECT
--     API_ENDPOINT,
--     COUNT(*) AS PAGE_COUNT,
--     SUM(RECORD_COUNT_PER_PAGE) AS TOTAL_RECORDS,
--     MAX(DATE_INSERTED) AS LAST_LOAD
-- FROM RAW.ECONOMIC_OPENAPI_JSON
-- GROUP BY API_ENDPOINT
-- ORDER BY TOTAL_RECORDS DESC;

/*******************************************************************************
 * NOTES ON OPENAPI VS REST
 ******************************************************************************/

-- OpenAPI endpoints typically:
-- - Use 'items' array instead of 'collection' array
-- - Provide more detailed data than REST equivalents
-- - May have different pagination behavior
-- - Are versioned (e.g., v1.0.0, v3.0.1)

-- Example OpenAPI endpoints:
-- - journalsapi/v1.0.0/entries/booked
-- - customersapi/v3.0.1/Contacts/paged
-- - invoicesapi/v2.0.0/invoices/booked

-- When to use OpenAPI vs REST:
-- - Use REST for basic data extraction (simpler, faster)
-- - Use OpenAPI when you need additional fields not in REST
-- - Check Economic API documentation for available fields

/*******************************************************************************
 * END OF FILE 07
 ******************************************************************************/
