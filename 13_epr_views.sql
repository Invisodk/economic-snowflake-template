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
1. Excel master file → Python SKU_PACKAGING_DATA (table)
2. BRONZE.DIM_SKU_PACKAGING (view) - Deduplicated SKU packaging weights
3. SILVER.VW_EPR_DETAIL (view) - Invoice lines with packaging consumption
4. SILVER.VW_EPR_SUMMARY (view) - Aggregated by country and period

==============================================================================
*/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA RAW;

/*
==============================================================================
STEP 1 TABLE FOR SKU PACKAGING DATA
==============================================================================
This table stores the master packaging data uploaded from Excel.
It includes historical uploads (tracked by DATE_UPLOADED) so we can see
how packaging weights change over time.
*/

CREATE TABLE IF NOT EXISTS SKU_PACKAGING_DATA (
    SKU STRING NOT NULL COMMENT 'Product SKU - joins to INVOICE_LINES and VW_PRODUCT_MASTER',
    CARTON_KG_PER_UNIT NUMBER(10,4) COMMENT 'Carton/cardboard weight in kg per unit sold',
    PLASTIC_KG_PER_UNIT NUMBER(10,4) COMMENT 'Plastic weight in kg per unit sold',
    FOAM_KG_PER_UNIT NUMBER(10,4) COMMENT 'Foam/XPS weight in kg per unit sold',
    DATE_UPLOADED TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'When this data was uploaded',
    SOURCE_FILE STRING COMMENT 'Excel filename for traceability',

    -- Composite primary key allows multiple versions over time
    PRIMARY KEY (SKU, DATE_UPLOADED)
)
COMMENT = 'Master packaging data for EPR reporting - only packaging weights (product names come from PrestaShop)';

-- Add clustering for better query performance
ALTER TABLE SKU_PACKAGING_DATA
CLUSTER BY (SKU, DATE_UPLOADED);

/*
==============================================================================
STEP 2: CREATE BRONZE VIEW - DIM_SKU_PACKAGING
==============================================================================
Deduplicates packaging data - latest upload wins per SKU.
This is the "current state" dimension for packaging weights.
*/

CREATE OR REPLACE VIEW BRONZE.DIM_SKU_PACKAGING
COMMENT = 'Deduplicated SKU packaging dimension with product attributes from PrestaShop'
AS
SELECT
    -- SKU and Packaging Data
    pkg.SKU,
    pkg.CARTON_KG_PER_UNIT,
    pkg.PLASTIC_KG_PER_UNIT,
    pkg.FOAM_KG_PER_UNIT,
    pkg.DATE_UPLOADED AS LAST_UPDATED,

    -- Product Attributes from PrestaShop
    pm.PRODUCT_NAME AS PRODUCT_DESCRIPTION,
    pm.CATEGORY_NAME,
    pm.SIZE,
    pm.COLOR,

    -- Data Quality Flag
    CASE
        WHEN pm.PRODUCT_SKU IS NOT NULL THEN 'Matched with PrestaShop'
        ELSE 'Not in PrestaShop'
    END AS PRESTASHOP_MATCH_STATUS

FROM (
    SELECT
        SKU,
        CARTON_KG_PER_UNIT,
        PLASTIC_KG_PER_UNIT,
        FOAM_KG_PER_UNIT,
        DATE_UPLOADED
    FROM SKU_PACKAGING_DATA
    -- Keep only the most recent upload per SKU
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SKU ORDER BY DATE_UPLOADED DESC) = 1
) pkg
LEFT JOIN SILVER.VW_PRODUCT_MASTER pm
    ON pkg.SKU = pm.PRODUCT_SKU;

/*
==============================================================================
STEP 3: CREATE SILVER VIEW - VW_EPR_DETAIL (SIMPLE!)
==============================================================================
Line-level EPR detail - every sales line with packaging data.
Tableau handles all dates, aggregations, and calculations!
*/

CREATE OR REPLACE VIEW SILVER.VW_EPR_DETAIL
COMMENT = 'EPR line-level detail - simple view for Tableau'
AS
SELECT
    -- Date (only one - Tableau handles the rest!)
    sd.sale_date,

    -- Customer
    sd.customer_name,
    sd.customer_segment,

    -- Geography
    COALESCE(sd.delivery_country, sd.customer_country, 'Unknown') AS country,

    -- Product (from PrestaShop!)
    sd.product_sku AS sku,
    COALESCE(pkg.PRODUCT_DESCRIPTION, sd.product_name) AS product_name,
    COALESCE(pkg.CATEGORY_NAME, sd.category) AS category,
    COALESCE(pkg.SIZE, sd.size) AS size,
    COALESCE(pkg.COLOR, sd.color) AS color,

    -- Sales Quantity
    sd.quantity_sold,

    -- Packaging per Unit (kg)
    pkg.CARTON_KG_PER_UNIT,
    pkg.PLASTIC_KG_PER_UNIT,
    pkg.FOAM_KG_PER_UNIT,

    -- Revenue (optional for analysis)
    sd.line_revenue_dkk,

    -- Data Quality
    pkg.PRESTASHOP_MATCH_STATUS AS packaging_match_status

FROM SILVER.VW_SALES_DETAIL sd
LEFT JOIN BRONZE.DIM_SKU_PACKAGING pkg
    ON sd.product_sku = pkg.SKU
WHERE sd.sale_date IS NOT NULL
  AND sd.quantity_sold > 0
  AND sd.product_sku NOT IN ('rabat', 'fragtum', 'fragtmm', 'diff', 'Diverse'); -- Only physical products

/*
==============================================================================
STEP 4: GRANT PERMISSIONS
==============================================================================
Ensure users can access EPR views.
*/

-- Grant SELECT on table to WRITE role (for procedure)
GRANT SELECT, INSERT ON TABLE SKU_PACKAGING_DATA TO ROLE ECONOMIC_WRITE;

-- Grant SELECT on views to READ role
GRANT SELECT ON VIEW BRONZE.DIM_SKU_PACKAGING TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW SILVER.VW_EPR_DETAIL TO ROLE ECONOMIC_READ;

-- Grant to WRITE role as well
GRANT SELECT ON VIEW BRONZE.DIM_SKU_PACKAGING TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW SILVER.VW_EPR_DETAIL TO ROLE ECONOMIC_WRITE;

/*
==============================================================================
STEP 5: VALIDATION QUERIES
==============================================================================
Run these to verify everything works.
*/

-- Check packaging data table
SELECT COUNT(*) as total_rows FROM SKU_PACKAGING_DATA;

-- Check Bronze view (with PrestaShop data)
SELECT * FROM BRONZE.DIM_SKU_PACKAGING LIMIT 10;

-- Check EPR detail view
SELECT * FROM SILVER.VW_EPR_DETAIL
WHERE sale_date >= DATEADD('month', -1, CURRENT_DATE())
LIMIT 20;

-- Check data quality
SELECT
    packaging_match_status,
    COUNT(*) as lines,
    COUNT(DISTINCT sku) as unique_skus
FROM SILVER.VW_EPR_DETAIL
WHERE sale_date >= DATEADD('year', -1, CURRENT_DATE())
GROUP BY packaging_match_status;

/*
==============================================================================
DEPLOYMENT COMPLETE!
==============================================================================

✅ Objects Created:
   1. RAW.SKU_PACKAGING_DATA (table)
   2. BRONZE.DIM_SKU_PACKAGING (view - with PrestaShop data)
   3. SILVER.VW_EPR_DETAIL (view - simple for Tableau!)

📊 For Tableau:
   - Connect to: SILVER.VW_EPR_DETAIL
   - This has everything you need!
   - Tableau handles all dates, aggregations, calculations

🎯 The view has:
   - sale_date (single date column)
   - customer_name, customer_segment
   - country
   - sku, product_name, category, size, color (from PrestaShop!)
   - quantity_sold
   - carton_kg_per_unit, plastic_kg_per_unit, foam_kg_per_unit
   - line_revenue_dkk
   - packaging_match_status

💡 In Tableau you can:
   - Calculate total packaging: [quantity_sold] * ([carton_kg] + [plastic_kg] + [foam_kg])
   - Aggregate by month: MONTH([sale_date])
   - Group by country, category, etc.
   - Create your own summaries!
*/
