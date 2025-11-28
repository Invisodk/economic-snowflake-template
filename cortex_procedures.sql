/*---------------------------------------------------------------*/
/**                                                             **/
/*** CORTEX ANALYST UTILITY PROCEDURES FOR DOG COPENHAGEN      **/
/*** Additional tools for reporting, alerts, and exports       **/
/**                                                             **/
/*** Prerequisites: cortex_setup.sql must be run first         **/
/**                                                             **/
/*---------------------------------------------------------------*/

-- Summary of procedures created in this script:
--
-- 1. EXPORT_TO_STAGE
--    - Export query results to CSV with download link
--    - Useful for: "Export last month's sales to Excel"
--
-- 2. SEND_WEEKLY_SALES_REPORT
--    - Automated weekly sales summary via email
--    - Useful for: Scheduled reporting
--
-- 3. CHECK_SALES_ALERT
--    - Alert when revenue drops below threshold
--    - Useful for: Performance monitoring

USE ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA CORTEX;
USE WAREHOUSE CORTEX_WH;


/*---------------------------------------------------------------*/
/*** PROCEDURE 1: EXPORT TO CSV/EXCEL                          ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE PROCEDURE EXPORT_TO_STAGE(
    query_result_table VARCHAR,
    export_filename VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'export_data'
COMMENT = 'Export query results to CSV file with presigned download URL. Valid for 1 hour.'
AS
$$
def export_data(session, query_result_table, export_filename):
    try:
        # Export query results to stage as CSV
        session.sql(f"""
            COPY INTO @ECONOMIC.CORTEX.SEMANTIC_MODELS/{export_filename}.csv
            FROM {query_result_table}
            FILE_FORMAT = (TYPE = CSV HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            OVERWRITE = TRUE
        """).collect()

        # Generate presigned URL for download
        result = session.sql(f"""
            SELECT GET_PRESIGNED_URL(@ECONOMIC.CORTEX.SEMANTIC_MODELS, '{export_filename}.csv', 3600) AS download_url
        """).collect()

        download_url = result[0]['DOWNLOAD_URL']

        return f"Export successful! Download link (valid for 1 hour): {download_url}"
    except Exception as e:
        return f"Export failed: {str(e)}"
$$;

-- Example usage:
-- First create temp table with query results, then export
-- CREATE TEMP TABLE my_results AS SELECT * FROM vw_sales_detail WHERE sale_date >= '2025-01-01';
-- CALL ECONOMIC.CORTEX.EXPORT_TO_STAGE('my_results', 'sales_january_2025');


/*---------------------------------------------------------------*/
/*** PROCEDURE 2: WEEKLY SALES REPORT                          ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE PROCEDURE SEND_WEEKLY_SALES_REPORT(
    recipient_email VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Send automated weekly sales summary email with key metrics'
AS
$$
BEGIN
    -- Get last week's sales metrics
    LET last_week_revenue NUMBER;
    LET last_week_orders NUMBER;
    LET top_category VARCHAR;
    LET top_category_revenue NUMBER;
    LET b2b_revenue NUMBER;
    LET b2c_revenue NUMBER;

    SELECT
        SUM(line_revenue_dkk),
        COUNT(DISTINCT invoice_id)
    INTO :last_week_revenue, :last_week_orders
    FROM ECONOMIC.SILVER.VW_SALES_DETAIL
    WHERE sale_date >= DATEADD(DAY, -7, CURRENT_DATE())
        AND sale_date < CURRENT_DATE();

    -- Get top category
    SELECT
        category,
        SUM(line_revenue_dkk)
    INTO :top_category, :top_category_revenue
    FROM ECONOMIC.SILVER.VW_SALES_DETAIL
    WHERE sale_date >= DATEADD(DAY, -7, CURRENT_DATE())
        AND prestashop_match_status = 'Matched'
    GROUP BY category
    ORDER BY SUM(line_revenue_dkk) DESC
    LIMIT 1;

    -- Get B2B vs B2C split
    SELECT
        SUM(CASE WHEN customer_segment = 'B2B' THEN line_revenue_dkk ELSE 0 END),
        SUM(CASE WHEN customer_segment = 'B2C' THEN line_revenue_dkk ELSE 0 END)
    INTO :b2b_revenue, :b2c_revenue
    FROM ECONOMIC.SILVER.VW_SALES_DETAIL
    WHERE sale_date >= DATEADD(DAY, -7, CURRENT_DATE());

    -- Build HTML email body
    LET email_body VARCHAR :=
        '<html><body style="font-family: Arial, sans-serif;">' ||
        '<h2>Ugentlig Salgsrapport - Dog Copenhagen</h2>' ||
        '<p><strong>Periode:</strong> Sidste 7 dage</p>' ||
        '<hr>' ||
        '<h3>Nøgletal:</h3>' ||
        '<ul>' ||
        '<li><strong>Total omsætning:</strong> ' || TO_CHAR(:last_week_revenue, '999,999.00') || ' kr</li>' ||
        '<li><strong>Antal ordrer:</strong> ' || :last_week_orders || '</li>' ||
        '<li><strong>Gennemsnitlig ordreværdi:</strong> ' || TO_CHAR(:last_week_revenue / NULLIF(:last_week_orders, 0), '999,999.00') || ' kr</li>' ||
        '</ul>' ||
        '<h3>Top Kategori:</h3>' ||
        '<p>' || :top_category || ' - ' || TO_CHAR(:top_category_revenue, '999,999.00') || ' kr</p>' ||
        '<h3>Kundesegmenter:</h3>' ||
        '<ul>' ||
        '<li><strong>B2B:</strong> ' || TO_CHAR(:b2b_revenue, '999,999.00') || ' kr</li>' ||
        '<li><strong>B2C:</strong> ' || TO_CHAR(:b2c_revenue, '999,999.00') || ' kr</li>' ||
        '</ul>' ||
        '<hr>' ||
        '<p style="color: #666; font-size: 12px;">Automatisk genereret af Snowflake Cortex Analyst</p>' ||
        '</body></html>';

    -- Send email
    CALL ECONOMIC.CORTEX.SEND_EMAIL(
        :recipient_email,
        'Ugentlig salgsrapport - Dog Copenhagen',
        :email_body
    );

    RETURN 'Weekly report sent to ' || :recipient_email || ' - Revenue: ' || TO_CHAR(:last_week_revenue, '999,999.00') || ' kr';
END;
$$;

-- Example usage:
-- CALL ECONOMIC.CORTEX.SEND_WEEKLY_SALES_REPORT('user@dogcopenhagen.dk');


/*---------------------------------------------------------------*/
/*** PROCEDURE 3: SALES PERFORMANCE ALERT                      ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE PROCEDURE CHECK_SALES_ALERT(
    threshold_amount NUMBER,
    recipient_email VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Send alert email when weekly revenue drops below threshold'
AS
$$
BEGIN
    LET this_week_revenue NUMBER;
    LET last_week_revenue NUMBER;
    LET this_week_orders NUMBER;

    -- Get this week's revenue (last 7 days)
    SELECT
        SUM(line_revenue_dkk),
        COUNT(DISTINCT invoice_id)
    INTO :this_week_revenue, :this_week_orders
    FROM ECONOMIC.SILVER.VW_SALES_DETAIL
    WHERE sale_date >= DATEADD(DAY, -7, CURRENT_DATE());

    -- Get previous week's revenue for comparison
    SELECT SUM(line_revenue_dkk)
    INTO :last_week_revenue
    FROM ECONOMIC.SILVER.VW_SALES_DETAIL
    WHERE sale_date >= DATEADD(DAY, -14, CURRENT_DATE())
        AND sale_date < DATEADD(DAY, -7, CURRENT_DATE());

    -- Check if below threshold
    IF (:this_week_revenue < :threshold_amount) THEN
        LET decline_pct NUMBER := ((:last_week_revenue - :this_week_revenue) / NULLIF(:last_week_revenue, 0)) * 100;

        LET alert_body VARCHAR :=
            '<html><body style="font-family: Arial, sans-serif;">' ||
            '<h2 style="color: #d9534f;">⚠️ SALGSADVARSEL</h2>' ||
            '<p>Denne uges omsætning er under det definerede threshold.</p>' ||
            '<hr>' ||
            '<h3>Detaljer:</h3>' ||
            '<ul>' ||
            '<li><strong>Denne uges omsætning:</strong> ' || TO_CHAR(:this_week_revenue, '999,999.00') || ' kr</li>' ||
            '<li><strong>Threshold:</strong> ' || TO_CHAR(:threshold_amount, '999,999.00') || ' kr</li>' ||
            '<li><strong>Forskel:</strong> ' || TO_CHAR(:threshold_amount - :this_week_revenue, '999,999.00') || ' kr under</li>' ||
            '<li><strong>Antal ordrer:</strong> ' || :this_week_orders || '</li>' ||
            '</ul>' ||
            '<h3>Sammenligning:</h3>' ||
            '<ul>' ||
            '<li><strong>Forrige uge:</strong> ' || TO_CHAR(:last_week_revenue, '999,999.00') || ' kr</li>' ||
            '<li><strong>Ændring:</strong> ' || TO_CHAR(:decline_pct, '999.0') || '%</li>' ||
            '</ul>' ||
            '<hr>' ||
            '<p style="color: #666; font-size: 12px;">Automatisk advarsel fra Snowflake Cortex Analyst</p>' ||
            '</body></html>';

        CALL ECONOMIC.CORTEX.SEND_EMAIL(
            :recipient_email,
            '⚠️ ADVARSEL: Lav omsætning denne uge',
            :alert_body
        );

        RETURN 'ALERT SENT - Revenue: ' || TO_CHAR(:this_week_revenue, '999,999.00') || ' kr (below threshold of ' || TO_CHAR(:threshold_amount, '999,999.00') || ' kr)';
    ELSE
        RETURN 'No alert needed - Revenue: ' || TO_CHAR(:this_week_revenue, '999,999.00') || ' kr (above threshold of ' || TO_CHAR(:threshold_amount, '999,999.00') || ' kr)';
    END IF;
END;
$$;

-- Example usage:
-- Check if this week's revenue is below 150,000 kr and send alert
-- CALL ECONOMIC.CORTEX.CHECK_SALES_ALERT(150000, 'manager@dogcopenhagen.dk');


/*---------------------------------------------------------------*/
/*** VERIFICATION & EXAMPLES                                   ***/
/*---------------------------------------------------------------*/

-- Show created procedures
SHOW PROCEDURES IN SCHEMA ECONOMIC.CORTEX;

SELECT '
╔══════════════════════════════════════════════════════════════╗
║  Cortex Utility Procedures Created Successfully!            ║
╚══════════════════════════════════════════════════════════════╝

PROCEDURES AVAILABLE:
--------------------

1. EXPORT_TO_STAGE(query_result_table, export_filename)
   Purpose: Export query results to CSV with download link
   Example:
     CREATE TEMP TABLE my_data AS
       SELECT * FROM vw_sales_detail WHERE sale_date >= ''2025-01-01'';
     CALL ECONOMIC.CORTEX.EXPORT_TO_STAGE(''my_data'', ''january_sales'');

2. SEND_WEEKLY_SALES_REPORT(recipient_email)
   Purpose: Send automated weekly sales summary
   Example:
     CALL ECONOMIC.CORTEX.SEND_WEEKLY_SALES_REPORT(''user@dogcopenhagen.dk'');

   Schedule with Task:
     CREATE TASK weekly_sales_report
       WAREHOUSE = CORTEX_WH
       SCHEDULE = ''USING CRON 0 8 * * MON Europe/Copenhagen''
     AS
       CALL ECONOMIC.CORTEX.SEND_WEEKLY_SALES_REPORT(''team@dogcopenhagen.dk'');

3. CHECK_SALES_ALERT(threshold_amount, recipient_email)
   Purpose: Alert when revenue drops below threshold
   Example:
     CALL ECONOMIC.CORTEX.CHECK_SALES_ALERT(150000, ''manager@dogcopenhagen.dk'');

   Schedule with Task:
     CREATE TASK daily_sales_check
       WAREHOUSE = CORTEX_WH
       SCHEDULE = ''USING CRON 0 9 * * * Europe/Copenhagen''
     AS
       CALL ECONOMIC.CORTEX.CHECK_SALES_ALERT(100000, ''alerts@dogcopenhagen.dk'');

AGENT INTEGRATION:
-----------------
Add these procedures as tools in your Cortex Analyst agent so users can:
- "Export last months sales to CSV"
- "Send weekly report to john@dogcopenhagen.dk"
- "Check if sales are below 150000 kr this week"

' AS setup_complete;
