/*
==============================================================================
EPR PACKAGING REPORTING VIEWS
==============================================================================
Purpose: Enable EPR (Extended Producer Responsibility) packaging reporting
         for Dog Copenhagen's compliance requirements.

Stakeholder Requirements:
- Report packaging consumption (Carton, Plastic, Foam) by period and country
- Line-level detail report showing all sales lines with packaging weights
- Aggregated "helicopter view" across all countries simultaneously

Data Flow:
1. Excel master file → Python upload → RAW.SKU_PACKAGING_DATA (table)
2. BRONZE.DIM_SKU_PACKAGING (view) - Deduplicated SKU packaging weights
3. SILVER.VW_EPR_DETAIL (view) - Invoice lines with packaging consumption
4. SILVER.VW_EPR_SUMMARY (view) - Aggregated by country and period

==============================================================================
*/

USE ROLE ECONOMIC_ADMIN;
USE WAREHOUSE ECONOMIC_WH;
USE DATABASE ECONOMIC;

/*
==============================================================================
STEP 1: CREATE RAW TABLE FOR SKU PACKAGING DATA
==============================================================================
This table stores the master packaging data uploaded from Excel.
It includes historical uploads (tracked by DATE_UPLOADED) so we can see
how packaging weights change over time.
*/

CREATE TABLE IF NOT EXISTS ECONOMIC.RAW.SKU_PACKAGING_DATA (
    SKU STRING NOT NULL COMMENT 'Product SKU - joins to INVOICE_LINES and PRODUCTS',
    PRODUCT_DESCRIPTION STRING COMMENT 'Product name from Excel master file',
    CARTON_KG_PER_UNIT NUMBER(10,4) COMMENT 'Carton/cardboard weight in kg per unit sold',
    PLASTIC_KG_PER_UNIT NUMBER(10,4) COMMENT 'Plastic weight in kg per unit sold',
    FOAM_KG_PER_UNIT NUMBER(10,4) COMMENT 'Foam/XPS weight in kg per unit sold',
    DATE_UPLOADED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'When this data was uploaded',
    SOURCE_FILE STRING COMMENT 'Excel filename for traceability',

    -- Composite primary key allows multiple versions over time
    PRIMARY KEY (SKU, DATE_UPLOADED)
)
COMMENT = 'Master packaging data for EPR reporting - uploaded from Excel master file';

-- Add clustering for better query performance
ALTER TABLE ECONOMIC.RAW.SKU_PACKAGING_DATA
CLUSTER BY (SKU, DATE_UPLOADED);

/*
==============================================================================
STEP 2: CREATE BRONZE VIEW - DIM_SKU_PACKAGING
==============================================================================
Deduplicates packaging data - latest upload wins per SKU.
This is the "current state" dimension for packaging weights.
*/

CREATE OR REPLACE VIEW ECONOMIC.BRONZE.DIM_SKU_PACKAGING
COMMENT = 'Deduplicated SKU packaging dimension - latest upload per SKU'
AS
SELECT
    SKU,
    PRODUCT_DESCRIPTION,
    CARTON_KG_PER_UNIT,
    PLASTIC_KG_PER_UNIT,
    FOAM_KG_PER_UNIT,
    DATE_UPLOADED AS LAST_UPDATED
FROM ECONOMIC.RAW.SKU_PACKAGING_DATA
-- Keep only the most recent upload per SKU
QUALIFY ROW_NUMBER() OVER (PARTITION BY SKU ORDER BY DATE_UPLOADED DESC) = 1;

/*
==============================================================================
STEP 3: CREATE SILVER VIEW - VW_EPR_DETAIL
==============================================================================
Line-level EPR detail view - every sales line with packaging consumption.

This view answers: "What packaging was used for each individual sale?"

Columns map to stakeholder requirements (from email):
- Salgsselskab = sales_company (customer_name)
- Leveringsland = epr_reporting_country (delivery_country)
- Varebeskrivelse = product_description (kolonne A masterark)
- Varenummer = sku (kolonne D masterark)
- Antal solgt = quantity_sold
- Vægt Pap = total_carton_kg (kolonne N masterark × quantity)
- Vægt plastik = total_plastic_kg (kolonne O masterark × quantity)
- Vægt XY = total_foam_kg (kolonne M masterark × quantity)
*/

CREATE OR REPLACE VIEW ECONOMIC.SILVER.VW_EPR_DETAIL
COMMENT = 'EPR line-level detail - shows packaging consumption per invoice line'
AS
SELECT
    -- Invoice & Line Identifiers
    sd.invoice_id,
    sd.line_id,
    sd.sale_date,

    -- Customer Info (Salgsselskab)
    sd.customer_id,
    sd.customer_name AS sales_company,
    sd.customer_segment,

    -- Geography (Leveringsland)
    sd.delivery_country,
    sd.customer_country,
    COALESCE(sd.delivery_country, sd.customer_country, 'Unknown') AS epr_reporting_country,
    sd.market,

    -- Product Info (Varebeskrivelse, Varenummer)
    sd.product_sku AS sku,
    COALESCE(pkg.PRODUCT_DESCRIPTION, sd.product_name) AS product_description,
    sd.category,
    sd.size,
    sd.color,
    sd.ean13,

    -- Sales Quantity (Antal solgt)
    sd.quantity_sold,

    -- Packaging Weights per Unit (from Excel master - kolonne M, N, O)
    pkg.CARTON_KG_PER_UNIT,
    pkg.PLASTIC_KG_PER_UNIT,
    pkg.FOAM_KG_PER_UNIT,

    -- Total Packaging Consumption (Vægt Pap, Vægt plastik, Vægt XY)
    -- Formula: Quantity Sold × Weight per Unit
    sd.quantity_sold * COALESCE(pkg.CARTON_KG_PER_UNIT, 0) AS total_carton_kg,
    sd.quantity_sold * COALESCE(pkg.PLASTIC_KG_PER_UNIT, 0) AS total_plastic_kg,
    sd.quantity_sold * COALESCE(pkg.FOAM_KG_PER_UNIT, 0) AS total_foam_kg,

    -- Total packaging across all materials
    sd.quantity_sold * COALESCE(
        pkg.CARTON_KG_PER_UNIT + pkg.PLASTIC_KG_PER_UNIT + pkg.FOAM_KG_PER_UNIT,
        0
    ) AS total_packaging_kg,

    -- Revenue Context (for internal analysis)
    sd.line_revenue_dkk,
    sd.line_profit_dkk,

    -- Data Quality Flags
    CASE
        WHEN pkg.SKU IS NOT NULL THEN 'Matched'
        WHEN sd.product_sku IN ('rabat', 'fragtum', 'fragtmm', 'diff') THEN 'Adjustment Item'
        WHEN sd.product_sku = 'Diverse' THEN 'B2B Wholesale'
        ELSE 'Missing Packaging Data'
    END AS packaging_data_status,

    -- Exclude non-product items from coverage calculations
    CASE
        WHEN sd.product_sku NOT IN ('rabat', 'fragtum', 'fragtmm', 'diff', 'Diverse') THEN 1
        ELSE 0
    END AS is_physical_product,

    -- Metadata
    pkg.LAST_UPDATED AS packaging_data_last_updated,
    sd.data_last_refreshed AS sales_data_last_refreshed

FROM ECONOMIC.SILVER.VW_SALES_DETAIL sd
LEFT JOIN ECONOMIC.BRONZE.DIM_SKU_PACKAGING pkg
    ON sd.product_sku = pkg.SKU
WHERE sd.sale_date IS NOT NULL
  AND sd.quantity_sold > 0; -- Only positive quantities for EPR reporting

/*
==============================================================================
STEP 4: CREATE SILVER VIEW - VW_EPR_SUMMARY
==============================================================================
Aggregated EPR summary - "helicopter view" by country and period.

This view answers: "What is our total packaging consumption by country and month?"

This is the primary view for the EPR dashboard showing accumulated consumption
across all countries simultaneously (stakeholder's main requirement).
*/

CREATE OR REPLACE VIEW ECONOMIC.SILVER.VW_EPR_SUMMARY
COMMENT = 'EPR aggregated summary - packaging consumption by country and period'
AS
SELECT
    -- Time Period Dimensions
    YEAR(sale_date) AS year,
    QUARTER(sale_date) AS quarter,
    MONTH(sale_date) AS month,
    DATE_TRUNC('MONTH', sale_date) AS month_start_date,
    TO_VARCHAR(sale_date, 'YYYY-MM') AS period_month,
    TO_VARCHAR(sale_date, 'YYYY-Q"Q"') AS period_quarter,
    TO_VARCHAR(sale_date, 'YYYY') AS period_year,

    -- Geography Dimensions
    epr_reporting_country AS country,
    market,

    -- Aggregated Packaging Consumption (kg)
    ROUND(SUM(total_carton_kg), 2) AS total_carton_kg,
    ROUND(SUM(total_plastic_kg), 2) AS total_plastic_kg,
    ROUND(SUM(total_foam_kg), 2) AS total_foam_kg,
    ROUND(SUM(total_packaging_kg), 2) AS total_packaging_kg,

    -- Supporting Metrics
    COUNT(DISTINCT invoice_id) AS invoice_count,
    COUNT(*) AS line_count,
    ROUND(SUM(quantity_sold), 2) AS total_units_sold,
    COUNT(DISTINCT sku) AS unique_skus,
    ROUND(SUM(line_revenue_dkk), 2) AS total_revenue_dkk,

    -- Data Quality Metrics
    SUM(CASE WHEN packaging_data_status = 'Matched' THEN 1 ELSE 0 END) AS lines_with_packaging_data,
    SUM(CASE WHEN is_physical_product = 1 THEN 1 ELSE 0 END) AS physical_product_lines,

    -- Coverage Percentage (only for physical products)
    ROUND(
        SUM(CASE WHEN packaging_data_status = 'Matched' AND is_physical_product = 1 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(is_physical_product), 0),
        2
    ) AS packaging_data_coverage_pct,

    -- Packaging Intensity Metrics (kg per unit sold)
    ROUND(SUM(total_carton_kg) / NULLIF(SUM(quantity_sold), 0), 4) AS avg_carton_kg_per_unit,
    ROUND(SUM(total_plastic_kg) / NULLIF(SUM(quantity_sold), 0), 4) AS avg_plastic_kg_per_unit,
    ROUND(SUM(total_foam_kg) / NULLIF(SUM(quantity_sold), 0), 4) AS avg_foam_kg_per_unit,
    ROUND(SUM(total_packaging_kg) / NULLIF(SUM(quantity_sold), 0), 4) AS avg_packaging_kg_per_unit,

    -- Packaging as % of revenue (cost perspective)
    ROUND(
        SUM(total_packaging_kg) / NULLIF(SUM(line_revenue_dkk), 0) * 1000,
        2
    ) AS packaging_kg_per_1000_dkk_revenue

FROM ECONOMIC.SILVER.VW_EPR_DETAIL
GROUP BY
    YEAR(sale_date),
    QUARTER(sale_date),
    MONTH(sale_date),
    DATE_TRUNC('MONTH', sale_date),
    TO_VARCHAR(sale_date, 'YYYY-MM'),
    TO_VARCHAR(sale_date, 'YYYY-Q"Q"'),
    TO_VARCHAR(sale_date, 'YYYY'),
    epr_reporting_country,
    market;

/*
==============================================================================
STEP 5: CREATE CONVENIENCE VIEW - VW_EPR_SUMMARY_YEARLY
==============================================================================
Yearly aggregation for annual EPR compliance reporting.
*/

CREATE OR REPLACE VIEW ECONOMIC.SILVER.VW_EPR_SUMMARY_YEARLY
COMMENT = 'EPR yearly summary - packaging consumption by country and year'
AS
SELECT
    -- Time Period
    year,
    period_year,

    -- Geography
    country,
    market,

    -- Aggregated Packaging Consumption (kg)
    ROUND(SUM(total_carton_kg), 2) AS total_carton_kg,
    ROUND(SUM(total_plastic_kg), 2) AS total_plastic_kg,
    ROUND(SUM(total_foam_kg), 2) AS total_foam_kg,
    ROUND(SUM(total_packaging_kg), 2) AS total_packaging_kg,

    -- Supporting Metrics
    SUM(invoice_count) AS invoice_count,
    SUM(line_count) AS line_count,
    ROUND(SUM(total_units_sold), 2) AS total_units_sold,
    SUM(unique_skus) AS unique_skus,
    ROUND(SUM(total_revenue_dkk), 2) AS total_revenue_dkk,

    -- Data Quality
    ROUND(
        SUM(lines_with_packaging_data) * 100.0 / NULLIF(SUM(physical_product_lines), 0),
        2
    ) AS packaging_data_coverage_pct,

    -- Intensity Metrics
    ROUND(SUM(total_carton_kg) / NULLIF(SUM(total_units_sold), 0), 4) AS avg_carton_kg_per_unit,
    ROUND(SUM(total_plastic_kg) / NULLIF(SUM(total_units_sold), 0), 4) AS avg_plastic_kg_per_unit,
    ROUND(SUM(total_foam_kg) / NULLIF(SUM(total_units_sold), 0), 4) AS avg_foam_kg_per_unit,
    ROUND(SUM(total_packaging_kg) / NULLIF(SUM(total_units_sold), 0), 4) AS avg_packaging_kg_per_unit

FROM ECONOMIC.SILVER.VW_EPR_SUMMARY
GROUP BY
    year,
    period_year,
    country,
    market;

/*
==============================================================================
STEP 6: GRANT PERMISSIONS
==============================================================================
Ensure read-only users can access EPR views.
*/

-- Grant SELECT on RAW table to WRITE role (for Python upload script)
GRANT SELECT, INSERT ON TABLE ECONOMIC.RAW.SKU_PACKAGING_DATA TO ROLE ECONOMIC_WRITE;

-- Grant SELECT on all new views to READ role
GRANT SELECT ON VIEW ECONOMIC.BRONZE.DIM_SKU_PACKAGING TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW ECONOMIC.SILVER.VW_EPR_DETAIL TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW ECONOMIC.SILVER.VW_EPR_SUMMARY TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW ECONOMIC.SILVER.VW_EPR_SUMMARY_YEARLY TO ROLE ECONOMIC_READ;

-- Grant to WRITE role as well
GRANT SELECT ON VIEW ECONOMIC.BRONZE.DIM_SKU_PACKAGING TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW ECONOMIC.SILVER.VW_EPR_DETAIL TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW ECONOMIC.SILVER.VW_EPR_SUMMARY TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW ECONOMIC.SILVER.VW_EPR_SUMMARY_YEARLY TO ROLE ECONOMIC_WRITE;

/*
==============================================================================
STEP 7: VALIDATION QUERIES
==============================================================================
Run these queries to verify the setup and data quality.
*/

-- Check if table exists and is empty (before upload)
SELECT 'RAW.SKU_PACKAGING_DATA' AS table_name, COUNT(*) AS row_count
FROM ECONOMIC.RAW.SKU_PACKAGING_DATA;

-- After upload: Check data quality in Bronze view
SELECT
    COUNT(*) AS total_skus,
    COUNT(CARTON_KG_PER_UNIT) AS skus_with_carton,
    COUNT(PLASTIC_KG_PER_UNIT) AS skus_with_plastic,
    COUNT(FOAM_KG_PER_UNIT) AS skus_with_foam,
    ROUND(AVG(CARTON_KG_PER_UNIT), 4) AS avg_carton_kg,
    ROUND(AVG(PLASTIC_KG_PER_UNIT), 4) AS avg_plastic_kg,
    ROUND(AVG(FOAM_KG_PER_UNIT), 4) AS avg_foam_kg
FROM ECONOMIC.BRONZE.DIM_SKU_PACKAGING;

-- Check EPR detail view - sample recent data
SELECT
    sale_date,
    sales_company,
    epr_reporting_country,
    sku,
    product_description,
    quantity_sold,
    ROUND(total_carton_kg, 2) AS carton_kg,
    ROUND(total_plastic_kg, 2) AS plastic_kg,
    ROUND(total_foam_kg, 2) AS foam_kg,
    packaging_data_status
FROM ECONOMIC.SILVER.VW_EPR_DETAIL
WHERE sale_date >= DATEADD('month', -1, CURRENT_DATE())
ORDER BY sale_date DESC
LIMIT 20;

-- Check EPR summary view - top countries by packaging consumption
SELECT
    period_month,
    country,
    ROUND(total_carton_kg, 2) AS carton_kg,
    ROUND(total_plastic_kg, 2) AS plastic_kg,
    ROUND(total_foam_kg, 2) AS foam_kg,
    ROUND(total_packaging_kg, 2) AS total_kg,
    packaging_data_coverage_pct
FROM ECONOMIC.SILVER.VW_EPR_SUMMARY
WHERE year = YEAR(CURRENT_DATE())
ORDER BY total_packaging_kg DESC
LIMIT 20;

-- Coverage analysis: How many SKUs have packaging data?
SELECT
    packaging_data_status,
    COUNT(DISTINCT sku) AS unique_skus,
    SUM(quantity_sold) AS units_sold,
    ROUND(SUM(line_revenue_dkk), 2) AS revenue_dkk,
    COUNT(*) AS line_count
FROM ECONOMIC.SILVER.VW_EPR_DETAIL
WHERE sale_date >= DATEADD('year', -1, CURRENT_DATE())
  AND is_physical_product = 1
GROUP BY packaging_data_status
ORDER BY units_sold DESC;

-- Find SKUs with highest packaging consumption (potential optimization targets)
SELECT
    sku,
    ANY_VALUE(product_description) AS product,
    SUM(quantity_sold) AS units_sold,
    ROUND(SUM(total_packaging_kg), 2) AS total_packaging_kg,
    ROUND(AVG(CARTON_KG_PER_UNIT + PLASTIC_KG_PER_UNIT + FOAM_KG_PER_UNIT), 4) AS avg_kg_per_unit
FROM ECONOMIC.SILVER.VW_EPR_DETAIL
WHERE sale_date >= DATEADD('year', -1, CURRENT_DATE())
  AND packaging_data_status = 'Matched'
GROUP BY sku
ORDER BY total_packaging_kg DESC
LIMIT 20;

/*
==============================================================================
DEPLOYMENT COMPLETE
==============================================================================

✅ Objects Created:
   1. RAW.SKU_PACKAGING_DATA (table)
   2. BRONZE.DIM_SKU_PACKAGING (view)
   3. SILVER.VW_EPR_DETAIL (view)
   4. SILVER.VW_EPR_SUMMARY (view)
   5. SILVER.VW_EPR_SUMMARY_YEARLY (view)

📋 Next Steps:
   1. Update Excel file path in upload_epr_packaging_data.py
   2. Set Snowflake credentials as environment variables:
      export SNOWFLAKE_ACCOUNT='your_account'
      export SNOWFLAKE_USER='your_username'
      export SNOWFLAKE_PASSWORD='your_password'

   3. Run Python upload script:
      python scripts/upload_epr_packaging_data.py

   4. Verify data with validation queries above

   5. Connect Tableau to these views:
      - VW_EPR_SUMMARY (for aggregated dashboard)
      - VW_EPR_DETAIL (for line-level drill-down)

📊 Tableau Dashboard Tips:
   - Use VW_EPR_SUMMARY for main "helicopter view"
   - Add filters for period_month and country (multi-select)
   - Create calculated field for packaging mix:
     [total_carton_kg] / [total_packaging_kg]
   - Use VW_EPR_DETAIL for detail drill-through report
*/
