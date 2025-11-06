/*******************************************************************************
 * FILE 06b: PRESTASHOP REST INGESTION PROCEDURE
 *
 * Purpose: Ingests data from PrestaShop REST API endpoints into RAW table
 *
 * Creates:
 * - UTIL.PRESTASHOP_RESTAPI_DATAINGEST procedure
 *
 * Behavior:
 * - Truncates RAW.PRESTA_RESTAPI_JSON table (full refresh pattern)
 * - Queries CONFIG.PRESTASHOP_ENDPOINTS for active REST endpoints
 * - Calls UTIL.PRESTASHOP_API for each endpoint with pagination
 * - Inserts raw JSON responses into RAW table
 * - Returns success message with total record count
 *
 * Usage:
 * - CALL UTIL.PRESTASHOP_RESTAPI_DATAINGEST();
 ******************************************************************************/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE PRESTASHOP_RESTAPI_DATAINGEST()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        // Truncate the table to ensure it only holds the newest available data
        snowflake.execute({sqlText: "TRUNCATE TABLE RAW.PRESTA_RESTAPI_JSON"});

        // Query to fetch active PrestaShop endpoints from CONFIG.PRESTASHOP_ENDPOINTS table
        const endpointQuerySql = `
            SELECT ENDPOINT_PATH, BASE, DEFAULT_PAGESIZE, DEFAULT_STARTPAGE
            FROM CONFIG.PRESTASHOP_ENDPOINTS
            WHERE ACTIVE = TRUE
        `;

        const cursorResult = snowflake.createStatement({sqlText: endpointQuerySql}).execute();
        let totalRecords = 0;
        let endpointSummary = [];

        // Process each active endpoint
        while (cursorResult.next()) {
            const ENDPOINT_PATH = cursorResult.getColumnValue(1);
            const BASE = cursorResult.getColumnValue(2); // REST
            let pageSize = cursorResult.getColumnValue(3) || 1000;
            let startPage = cursorResult.getColumnValue(4) || 0;

            let hasMorePages = true;
            let endpointRecords = 0;
            let pageCount = 0;

            // Fetch data page by page until API returns partial/empty page
            while (hasMorePages) {
                const apiCallSql = `
                    SELECT UTIL.PRESTASHOP_API(?, ?, ?) AS PRESTASHOP_COLLECTION_JSON
                `;

                // Prepare and execute the SQL command to fetch the JSON
                const stmt = snowflake.createStatement({
                    sqlText: apiCallSql,
                    binds: [ENDPOINT_PATH, pageSize, startPage]
                });
                const result = stmt.execute();

                // Parse the JSON result and count the records
                if (result.next()) {
                    const jsonResult = result.getColumnValue(1);

                    // PrestaShop API structure varies by endpoint
                    // - products: returns {products: [{product: {...}}]}
                    // - combinations: returns {combinations: [{combination: {...}}]}
                    // - categories: returns {categories: [{category: {...}}]}
                    // - product_option_values: returns {product_option_values: [{product_option_value: {...}}]}

                    let recordCountPerPage = 0;
                    let collection = null;

                    // Detect which array to use based on endpoint
                    if (jsonResult.products) {
                        collection = jsonResult.products;
                        recordCountPerPage = collection.length;
                    } else if (jsonResult.combinations) {
                        collection = jsonResult.combinations;
                        recordCountPerPage = collection.length;
                    } else if (jsonResult.categories) {
                        collection = jsonResult.categories;
                        recordCountPerPage = collection.length;
                    } else if (jsonResult.product_option_values) {
                        collection = jsonResult.product_option_values;
                        recordCountPerPage = collection.length;
                    } else {
                        // Unknown structure, log and continue
                        recordCountPerPage = 0;
                    }

                    totalRecords += recordCountPerPage;
                    endpointRecords += recordCountPerPage;
                    pageCount++;

                    // Convert the JSON object to a string if it is not already a string
                    let jsonString = (typeof jsonResult === 'object') ? JSON.stringify(jsonResult) : jsonResult;

                    // Insert the result along with the record count into the table
                    const insertSql = `
                        INSERT INTO RAW.PRESTA_RESTAPI_JSON
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
                    // PrestaShop returns empty array or partial results when no more data
                    if (recordCountPerPage < pageSize || recordCountPerPage === 0) {
                        hasMorePages = false;
                    } else {
                        startPage++;
                    }
                } else {
                    hasMorePages = false;
                }
            }

            // Store summary for this endpoint
            endpointSummary.push({
                endpoint: ENDPOINT_PATH,
                records: endpointRecords,
                pages: pageCount
            });
        }

        // Build result message
        let message = 'PrestaShop data ingestion completed successfully.\n\n';
        message += 'Total records processed: ' + totalRecords + '\n\n';
        message += 'Endpoint Summary:\n';
        endpointSummary.forEach(function(item) {
            message += '  - ' + item.endpoint + ': ' + item.records + ' records (' + item.pages + ' pages)\n';
        });

        return message;

    } catch (error) {
        // Log the error message along with the error object to understand the structure
        return 'Error: ' + error.message + '; Error object: ' + JSON.stringify(error);
    }
$$
;

COMMENT ON PROCEDURE PRESTASHOP_RESTAPI_DATAINGEST() IS
'Ingests product master data from PrestaShop API (products, combinations, categories, product_option_values) into RAW.PRESTA_RESTAPI_JSON table. Truncates table before loading to ensure fresh data.';

/*******************************************************************************
 * GRANT PERMISSIONS
 ******************************************************************************/

GRANT USAGE ON PROCEDURE PRESTASHOP_RESTAPI_DATAINGEST() TO ROLE ECONOMIC_ADMIN;
GRANT USAGE ON PROCEDURE PRESTASHOP_RESTAPI_DATAINGEST() TO ROLE ECONOMIC_WRITE;

/*******************************************************************************
 * USAGE EXAMPLES
 ******************************************************************************/

-- Execute the procedure
-- CALL UTIL.PRESTASHOP_RESTAPI_DATAINGEST();

-- Check results
-- SELECT
--     API_ENDPOINT,
--     COUNT(*) AS PAGE_COUNT,
--     SUM(RECORD_COUNT_PER_PAGE) AS TOTAL_RECORDS,
--     MAX(DATE_INSERTED) AS LAST_LOAD
-- FROM RAW.PRESTA_RESTAPI_JSON
-- GROUP BY API_ENDPOINT
-- ORDER BY TOTAL_RECORDS DESC;

/*******************************************************************************
 * END OF FILE 06b
 ******************************************************************************/
