/*******************************************************************************
 * FILE 06: REST INGESTION PROCEDURE
 *
 * Purpose: Ingests data from Economic REST API endpoints into RAW table
 *
 * Creates:
 * - UTIL.ECONOMIC_RESTAPI_DATAINGEST procedure
 *
 * Behavior:
 * - Truncates RAW.ECONOMIC_RESTAPI_JSON table (full refresh pattern)
 * - Queries CONFIG.ECONOMIC_ENDPOINTS for active REST endpoints
 * - Calls UTIL.ECONOMIC_API_V2 for each endpoint with pagination
 * - Inserts raw JSON responses into RAW table
 * - Returns success message with total record count
 *
 * Usage:
 * - CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST();
 ******************************************************************************/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE ECONOMIC_RESTAPI_DATAINGEST()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {

        // Truncate the table to ensure it only holds the newest available data
        snowflake.execute({sqlText: "TRUNCATE TABLE RAW.ECONOMIC_RESTAPI_JSON"});

        // Query to fetch active REST endpoints from CONFIG.ECONOMIC_ENDPOINTS table
        const endpointQuerySql = `
            SELECT ENDPOINT_PATH, BASE, DEFAULT_PAGESIZE, DEFAULT_STARTPAGE
            FROM CONFIG.ECONOMIC_ENDPOINTS
            WHERE ACTIVE = TRUE AND UPPER(BASE)='REST'
        `;

        const cursorResult = snowflake.createStatement({sqlText: endpointQuerySql}).execute();
        let totalRecords = 0;

        // Process each active endpoint
        while (cursorResult.next()) {
            const ENDPOINT_PATH = cursorResult.getColumnValue(1);
            const BASE = cursorResult.getColumnValue(2); // REST
            let pageSize = cursorResult.getColumnValue(3) || 1000;
            let startPage = cursorResult.getColumnValue(4) || 0;

            let hasMorePages = true;

            // Fetch data page by page until API returns partial/empty page
            while (hasMorePages) {
                const apiCallSql = `
                    SELECT UTIL.ECONOMIC_API_V2(?, ?, ?, ?) AS ECONOMIC_COLLECTION_JSON
                `;

                // Prepare and execute the SQL command to fetch the JSON
                const stmt = snowflake.createStatement({
                    sqlText: apiCallSql,
                    binds: [ENDPOINT_PATH, BASE, pageSize, startPage]
                });
                const result = stmt.execute();

                // Parse the JSON result and count the records in the "collection" array
                if (result.next()) {
                    const jsonResult = result.getColumnValue(1);
                    let recordCountPerPage = jsonResult.collection ? jsonResult.collection.length : 0;
                    totalRecords += recordCountPerPage;

                    // Convert the JSON object to a string if it is not already a string
                    let jsonString = (typeof jsonResult === 'object') ? JSON.stringify(jsonResult) : jsonResult;

                    // Insert the result along with the record count into the table using a subquery
                    const insertSql = `
                        INSERT INTO RAW.ECONOMIC_RESTAPI_JSON
                        (DATE_INSERTED, API_ENDPOINT, PAGE_NUMBER, RECORD_COUNT_PER_PAGE, COLLECTION_JSON)
                        SELECT
                            CURRENT_TIMESTAMP(), ?, ?, ?, PARSE_JSON(?)
                    `;

                    const insertStmt = snowflake.createStatement({
                        sqlText: insertSql,
                        binds: [ENDPOINT_PATH, startPage, recordCountPerPage, jsonString]
                    });
                    insertStmt.execute();

                    // Determine if there are more pages
                    if (recordCountPerPage < pageSize) {
                        hasMorePages = false;
                    } else {
                        startPage++;
                    }
                } else {
                    hasMorePages = false;
                }
            }
        }

        return 'UDF invoked and data inserted successfully. Total records processed: ' + totalRecords;
    } catch (error) {
        // Log the error message along with the error object to understand the structure
        return 'Error: ' + error.message + '; Error object: ' + JSON.stringify(error);
    }
$$
;

COMMENT ON PROCEDURE ECONOMIC_RESTAPI_DATAINGEST() IS 'Ingests data from ECONOMIC RESTAPI into RAW.ECONOMIC_RESTAPI_JSON table. Truncates table before loading to ensure fresh data.';

/*******************************************************************************
 * GRANT PERMISSIONS
 ******************************************************************************/

GRANT USAGE ON PROCEDURE ECONOMIC_RESTAPI_DATAINGEST() TO ROLE ECONOMIC_ADMIN;
GRANT USAGE ON PROCEDURE ECONOMIC_RESTAPI_DATAINGEST() TO ROLE ECONOMIC_WRITE;

/*******************************************************************************
 * USAGE EXAMPLES
 ******************************************************************************/

-- Execute the procedure
-- CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST();

-- Check results
-- SELECT
--     API_ENDPOINT,
--     COUNT(*) AS PAGE_COUNT,
--     SUM(RECORD_COUNT_PER_PAGE) AS TOTAL_RECORDS,
--     MAX(DATE_INSERTED) AS LAST_LOAD
-- FROM RAW.ECONOMIC_RESTAPI_JSON
-- GROUP BY API_ENDPOINT
-- ORDER BY TOTAL_RECORDS DESC;

/*******************************************************************************
 * END OF FILE 06
 ******************************************************************************/
