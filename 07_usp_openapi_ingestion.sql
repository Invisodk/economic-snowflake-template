/*******************************************************************************
 * FILE 07: OPENAPI INGESTION PROCEDURE
 *
 * Purpose: Ingests data from Economic OpenAPI endpoints into RAW table
 *
 * Creates:
 * - UTIL.ECONOMIC_OPENAPI_DATAINGEST procedure
 *
 * Behavior:
 * - Truncates RAW.ECONOMIC_OPENAPI_JSON table (full refresh pattern)
 * - Queries CONFIG.ECONOMIC_ENDPOINTS for active OpenAPI endpoints
 * - Calls UTIL.ECONOMIC_API_V2 for each endpoint with pagination
 * - Handles 'items' array (OpenAPI structure) instead of 'collection'
 * - Inserts raw JSON responses into RAW table
 * - Returns success message with total record count
 *
 * Usage:
 * - CALL UTIL.ECONOMIC_OPENAPI_DATAINGEST();
 ******************************************************************************/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE ECONOMIC_OPENAPI_DATAINGEST()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {

        // NO LONGER TRUNCATE - Keep existing data for incremental loading
        // snowflake.execute({sqlText: "TRUNCATE TABLE RAW.ECONOMIC_OPENAPI_JSON"});

        // Query to fetch active OPENAPI endpoints from CONFIG.ECONOMIC_ENDPOINTS table
        const endpointQuerySql = `
            SELECT ENDPOINT_PATH, BASE, DEFAULT_PAGESIZE, DEFAULT_STARTPAGE
            FROM CONFIG.ECONOMIC_ENDPOINTS
            WHERE ACTIVE = TRUE AND UPPER(BASE)='OPENAPI'
        `;

        const cursorResult = snowflake.createStatement({sqlText: endpointQuerySql}).execute();
        let totalRecords = 0;

        // Process each active OPENAPI endpoint
        while (cursorResult.next()) {
            const ENDPOINT_PATH = cursorResult.getColumnValue(1);
            const BASE = cursorResult.getColumnValue(2); // OPENAPI
            let pageSize = cursorResult.getColumnValue(3) || 1000;
            let startPage = 0; // Not used for cursor pagination, but required by UDF

            // Get watermark for incremental loading
            const watermarkSql = `
                SELECT LAST_INVOICE_NUMBER
                FROM CONFIG.INGESTION_WATERMARKS
                WHERE API_ENDPOINT = ? AND BASE = ?
            `;
            const watermarkStmt = snowflake.createStatement({
                sqlText: watermarkSql,
                binds: [ENDPOINT_PATH, BASE]
            });
            const watermarkResult = watermarkStmt.execute();

            let filterParam = null;
            let maxInvoiceNumber = null;

            if (watermarkResult.next()) {
                const lastInvoiceNum = watermarkResult.getColumnValue(1);

                // Build filter for invoices/booked/lines using documentId
                if (ENDPOINT_PATH === 'invoices/booked/lines') {
                    if (lastInvoiceNum && lastInvoiceNum > 0) {
                        filterParam = 'documentId$gte:' + (lastInvoiceNum + 1);
                    }
                }
            }

            let hasMorePages = true;
            let cursorToken = null; // Start with no cursor (first page)
            let pageNumber = 0;
            let endpointRecordCount = 0;

            // Fetch data page by page using cursor-based pagination
            while (hasMorePages) {
                const apiCallSql = `
                    SELECT UTIL.ECONOMIC_API_V2(?, ?, ?, ?, ?, ?) AS ECONOMIC_COLLECTION_JSON
                `;

                // Prepare and execute the SQL command to fetch the JSON
                const stmt = snowflake.createStatement({
                    sqlText: apiCallSql,
                    binds: [ENDPOINT_PATH, BASE, pageSize, startPage, cursorToken, filterParam]
                });
                const result = stmt.execute();

                // Parse the JSON result and count the records in the "items" array
                if (result.next()) {
                    const jsonResult = result.getColumnValue(1);
                    let recordCountPerPage = jsonResult.items ? jsonResult.items.length : 0;
                    totalRecords += recordCountPerPage;
                    endpointRecordCount += recordCountPerPage;

                    // Track max documentId for watermark update
                    if (jsonResult.items) {
                        for (let i = 0; i < jsonResult.items.length; i++) {
                            const record = jsonResult.items[i];

                            // Track documentId for invoices/booked/lines
                            if (record.documentId) {
                                if (!maxInvoiceNumber || record.documentId > maxInvoiceNumber) {
                                    maxInvoiceNumber = record.documentId;
                                }
                            }
                        }
                    }

                    // Extract cursor for next page
                    let nextCursor = jsonResult.cursor || null;

                    // Convert the JSON object to a string if it is not already a string
                    let jsonString = (typeof jsonResult === 'object') ? JSON.stringify(jsonResult) : jsonResult;

                    // Insert the result along with the record count into the table
                    const insertSql = `
                        INSERT INTO RAW.ECONOMIC_OPENAPI_JSON
                        (DATE_INSERTED, API_ENDPOINT, PAGE_NUMBER, RECORD_COUNT_PER_PAGE, COLLECTION_JSON)
                        SELECT
                            CURRENT_TIMESTAMP(), ?, ?, ?, PARSE_JSON(?)
                    `;

                    const insertStmt = snowflake.createStatement({
                        sqlText: insertSql,
                        binds: [ENDPOINT_PATH, pageNumber, recordCountPerPage, jsonString]
                    });
                    insertStmt.execute();

                    // Determine if there are more pages
                    // Stop if: no cursor returned, empty items array, or items less than pageSize
                    if (!nextCursor || recordCountPerPage === 0 || recordCountPerPage < pageSize) {
                        hasMorePages = false;
                    } else {
                        cursorToken = nextCursor;
                        pageNumber++;
                    }
                } else {
                    hasMorePages = false;
                }
            }

            // Update watermark after processing endpoint
            if (endpointRecordCount > 0) {
                const updateWatermarkSql = `
                    UPDATE CONFIG.INGESTION_WATERMARKS
                    SET LAST_INVOICE_NUMBER = COALESCE(?, LAST_INVOICE_NUMBER),
                        LAST_INGESTION_DATE = CURRENT_TIMESTAMP(),
                        TOTAL_RECORDS_LOADED = TOTAL_RECORDS_LOADED + ?,
                        LAST_RUN_RECORDS = ?,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE API_ENDPOINT = ? AND BASE = ?
                `;
                const updateStmt = snowflake.createStatement({
                    sqlText: updateWatermarkSql,
                    binds: [maxInvoiceNumber, endpointRecordCount, endpointRecordCount, ENDPOINT_PATH, BASE]
                });
                updateStmt.execute();
            }
        }

        return 'UDF invoked and data inserted successfully. Total records processed: ' + totalRecords;
    } catch (error) {
        // Log the error message along with the error object to understand the structure
        return 'Error: ' + error.message + '; Error object: ' + JSON.stringify(error);
    }
$$
;

COMMENT ON PROCEDURE ECONOMIC_OPENAPI_DATAINGEST() IS 'Ingests data from ECONOMIC OPENAPI (invoices/booked/lines) into RAW.ECONOMIC_OPENAPI_JSON table. Truncates table before loading to ensure fresh data.';

/*******************************************************************************
 * GRANT PERMISSIONS
 ******************************************************************************/

GRANT USAGE ON PROCEDURE ECONOMIC_OPENAPI_DATAINGEST() TO ROLE ECONOMIC_ADMIN;
GRANT USAGE ON PROCEDURE ECONOMIC_OPENAPI_DATAINGEST() TO ROLE ECONOMIC_WRITE;

/*******************************************************************************
 * USAGE EXAMPLES
 ******************************************************************************/

-- Execute the procedure
-- CALL UTIL.ECONOMIC_OPENAPI_DATAINGEST();

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
