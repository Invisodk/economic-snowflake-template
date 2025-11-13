/*---------------------------------------------------------------*/
/**                                                             **/
/*** SILVER LAYER - BULK ENDPOINTS WITH PRESTASHOP             ***/
/*** Uses new simplified Bronze views (bulk endpoints).        ***/
/*** BACKWARD COMPATIBLE: Keeps all column names from old      ***/
/*** VW_SALES_DETAIL to avoid breaking existing dashboards.    ***/
/***                                                           ***/
/*** Data Sources:                                             ***/
/***   - INVOICE_LINES: OpenAPI bulk endpoint (line-level)     ***/
/***   - INVOICES: REST API (header-level: dates, customer)    ***/
/***   - CUSTOMERS: REST API (customer master)                 ***/
/***   - PRODUCTS: REST API (product catalog from e-conomic)   ***/
/***   - DIM_PRODUCT_SKU: PrestaShop (category, size, color)   ***/
/***                                                           ***/
/*** Key Change: INVOICE_LINES now joins with INVOICES to get  ***/
/*** invoice_date, customer_number, delivery_country, etc.     ***/
/**                                                             **/
/*---------------------------------------------------------------*/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA SILVER;


-- ============================================================
-- VIEW 1: SALES DETAIL (ENHANCED WITH PRESTASHOP)
-- One row per invoice line - NO AGGREGATION
-- BACKWARD COMPATIBLE: All original column names preserved
-- ============================================================
CREATE OR REPLACE VIEW VW_SALES_DETAIL AS
SELECT
  -- ==== INVOICE IDENTIFIERS ====
  il.invoice_number                                   AS invoice_id,
  il.line_number                                      AS line_id,
  inv.invoice_date                                    AS sale_date,

  -- ==== CUSTOMER INFO ====
  inv.customer_number                                 AS customer_id,
  COALESCE(c.customer_name, 'Unknown')               AS customer_name,
  COALESCE(c.city, 'Unknown')                        AS customer_city,
  COALESCE(c.country, 'Unknown')                     AS customer_country,
  CASE
    WHEN c.customer_group_number = 1 THEN 'B2C'
    WHEN c.customer_group_number >= 2 THEN 'B2B'
    ELSE 'Unknown'
  END                                                 AS customer_segment,

  -- ==== PRODUCT INFO (FROM E-CONOMIC) ====
  il.sku                                              AS product_sku,

  -- ==== UNIFIED PRODUCT NAME ====
  -- ps.product_name already includes fallback logic via DIM_PRODUCT_SKU_ENRICHED:
  -- PrestaShop name → Economic invoice description → 'Unknown Product'
  COALESCE(ps.product_name, 'Unknown Product')       AS product_name,

  -- Core product name (stripped of size/color for grouping)
  REGEXP_REPLACE(
    COALESCE(p.product_name, il.line_description, 'Unknown Product'),
    ' : Color - .*$| - Color : .*$| - Farve : .*$| - Färg : .*$| - Colour : .*$',
    ''
  )                                                   AS core_product_economic,

  COALESCE(p.product_group_name, 'Other')            AS product_category_economic,

  -- ==== PRODUCT MASTER DATA (FROM PRESTASHOP) ====
  -- Category with special handling for special items
  CASE
    WHEN il.sku = 'Diverse' THEN 'B2B Wholesale'  -- Bulk mixed-product shipments to distributors
    WHEN il.sku IN ('rabat', 'fragtum', 'fragtmm', 'diff') THEN 'Adjustments'
    WHEN ps.category_name IS NOT NULL THEN ps.category_name
    ELSE 'Uncategorized'
  END                                                 AS category,

  ps.parent_category_id                               AS parent_category_id,
  ps.category_level                                   AS category_level,
  ps.size                                             AS size,
  ps.color                                            AS color,
  ps.ean13                                            AS ean13,
  ps.stock_quantity                                   AS prestashop_stock_qty,

  -- ==== PRODUCT MATCHING STATUS ====
  CASE
    WHEN il.sku = 'Diverse' THEN 'B2B Wholesale'
    WHEN il.sku IN ('rabat', 'fragtum', 'fragtmm', 'diff') THEN 'Adjustment Item'
    WHEN ps.sku IS NOT NULL THEN 'Matched'
    ELSE 'Not in PrestaShop'
  END                                                 AS prestashop_match_status,

  -- ==== GEOGRAPHY ====
  -- Standardize country names to merge variations
  CASE
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('United Kingdom', 'England', 'GB', 'Great Britain', 'UK')
      THEN 'United Kingdom'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('JP', 'Japan')
      THEN 'Japan'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('Danmark', 'Denmark', 'DK')
      THEN 'Denmark'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('SE', 'Sweden', 'Sverige')
      THEN 'Sweden'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('NO', 'Norway', 'Norge')
      THEN 'Norway'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('FR', 'France')
      THEN 'France'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('BE', 'Belgium', 'Belgique', 'België')
      THEN 'Belgium'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('FI', 'Finland', 'Suomi')
      THEN 'Finland'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('DE', 'Germany', 'Deutschland')
      THEN 'Germany'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('NL', 'Netherlands', 'Nederland', 'Holland')
      THEN 'Netherlands'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('US', 'USA', 'United States', 'United States of America')
      THEN 'United States'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('CH', 'Switzerland', 'Schweiz', 'Suisse')
      THEN 'Switzerland'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('AT', 'Austria', 'Österreich')
      THEN 'Austria'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('IT', 'Italy', 'Italia')
      THEN 'Italy'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('ES', 'Spain', 'España')
      THEN 'Spain'
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('CN', 'China')
      THEN 'China'
    ELSE COALESCE(inv.delivery_country, inv.recipient_country, 'Unknown')
  END                                                 AS delivery_country,
  CASE
    WHEN COALESCE(inv.delivery_country, inv.recipient_country) IN ('Danmark', 'Denmark', 'DK')
    THEN 'National'
    ELSE 'International'
  END                                                 AS market,

  -- ==== CURRENCY INFO ====
  inv.invoice_currency                                AS invoice_currency,
  inv.exchange_rate                                   AS exchange_rate,

  -- ==== SALES METRICS (BASE CURRENCY - DKK) ====
  il.quantity                                         AS quantity_sold,
  il.unit_net_price_base_currency                     AS unit_price_dkk,
  il.line_net_amount_base_currency                    AS line_revenue_dkk,

  -- ==== REVENUE BREAKDOWN ====
  -- Product revenue excludes adjustment lines (rabat, freight, etc.)
  CASE
    WHEN il.sku IN ('rabat', 'fragtmm', 'fragtum') OR il.sku LIKE 'fragt%' THEN 0
    WHEN il.line_net_amount_base_currency < 0 THEN 0  -- Negative adjustments
    ELSE il.line_net_amount_base_currency
  END                                                 AS product_revenue_dkk,

  -- ==== COST & PROFIT (BASE CURRENCY - DKK) ====
  COALESCE(il.unit_cost_price_base_currency, 0)      AS unit_cost_dkk,
  COALESCE(il.unit_cost_price_base_currency, 0) * il.quantity AS line_cost_dkk,
  il.line_net_amount_base_currency -
    (COALESCE(il.unit_cost_price_base_currency, 0) * il.quantity) AS line_profit_dkk,

  -- ==== DATA FRESHNESS METADATA ====
  il.api_timestamp                                    AS data_last_refreshed

FROM BRONZE.INVOICE_LINES il
-- NEW: Join with INVOICES to get header-level fields (date, customer, delivery, currency)
LEFT JOIN BRONZE.INVOICES inv
  ON il.invoice_number = inv.invoice_number
LEFT JOIN BRONZE.CUSTOMERS c
  ON inv.customer_number = c.customer_number
LEFT JOIN BRONZE.PRODUCTS p
  ON il.sku = p.sku
-- Join with PrestaShop SKU dimension (enriched with fallback for pre-2024 data)
LEFT JOIN BRONZE.DIM_PRODUCT_SKU_ENRICHED ps
  ON il.sku = ps.sku
WHERE inv.invoice_date IS NOT NULL;  -- Only include lines with valid invoice dates

COMMENT ON VIEW VW_SALES_DETAIL IS
'Sales detail - one row per invoice line. NO aggregation.
ENHANCED: Includes PrestaShop product master data (category, size, color, EAN13) via direct SKU join.
PRODUCT NAME: Uses PrestaShop name for matched SKUs, falls back to Economic for unmatched items.
CATEGORY LOGIC:
  - Matched SKUs → PrestaShop category
  - Adjustment items (rabat, fragtum, fragtmm, diff, diverse) → "Adjustments"
  - Others → "Uncategorized"
Uses bulk endpoints for performance (~55 API calls vs 100,000+).
Data sources: INVOICE_LINES (bulk) + INVOICES (headers) + CUSTOMERS + PRODUCTS + PrestaShop.
CURRENCY: All monetary amounts in DKK (base currency).';


-- ============================================================
-- VIEW 2: CUSTOMER MASTER (UNCHANGED)
-- One row per customer
-- ============================================================
CREATE OR REPLACE VIEW VW_CUSTOMER_MASTER AS
SELECT
  -- ==== CUSTOMER ID ====
  customer_number                                     AS customer_id,

  -- ==== BASIC INFO ====
  customer_name                                       AS customer_name,
  COALESCE(city, 'Unknown')                          AS customer_city,
  COALESCE(country, 'Unknown')                       AS customer_country,

  -- ==== SEGMENTATION ====
  customer_group_number                               AS customer_group_id,
  CASE
    WHEN customer_group_number = 1 THEN 'B2C'
    WHEN customer_group_number >= 2 THEN 'B2B'
    ELSE 'Unknown'
  END                                                 AS customer_segment,

  -- ==== CONTACT INFO ====
  email                                               AS email_address,
  telephone                                           AS phone_number,
  mobile_phone                                        AS mobile_number,

  -- ==== BUSINESS INFO ====
  currency                                            AS preferred_currency,
  payment_terms_number                                AS payment_terms_id,
  vatzone_number                                      AS vat_zone_id,
  salesperson_employee_number                         AS sales_rep_id,

  -- ==== METADATA ====
  last_updated                                        AS last_updated_date

FROM BRONZE.CUSTOMERS;

COMMENT ON VIEW VW_CUSTOMER_MASTER IS
'Customer master data - one row per customer.
Use for: Customer lookups, segmentation, contact info.';


-- ============================================================
-- VIEW 3: PRODUCT MASTER (FROM PRESTASHOP)
-- One row per SKU with complete product hierarchy
-- ============================================================
CREATE OR REPLACE VIEW VW_PRODUCT_MASTER AS
SELECT
  -- ==== SKU IDENTIFIER ====
  sku                                                 AS product_sku,
  combination_id                                      AS variant_id,
  product_id                                          AS parent_product_id,

  -- ==== PRODUCT INFO ====
  product_name                                        AS product_name,
  product_active                                      AS is_active,

  -- ==== CATEGORY HIERARCHY ====
  category_id                                         AS category_id,
  category_name                                       AS category_name,
  parent_category_id                                  AS parent_category_id,
  category_level                                      AS category_level,

  -- ==== VARIANT ATTRIBUTES ====
  size                                                AS size,
  color                                               AS color,
  all_attributes                                      AS all_attributes_text,

  -- ==== IDENTIFIERS ====
  ean13                                               AS ean13,

  -- ==== PRICING & STOCK ====
  price_impact                                        AS price_impact,
  stock_quantity                                      AS stock_quantity,

  -- ==== METADATA ====
  last_updated                                        AS last_updated_date,
  dim_created_at                                      AS dim_created_timestamp

FROM BRONZE.DIM_PRODUCT_SKU_ENRICHED;

COMMENT ON VIEW VW_PRODUCT_MASTER IS
'Product master data from PrestaShop (enriched with pre-2024 Economic data) - one row per SKU/variant.
Includes complete product hierarchy: category, size, color, EAN13.
Includes ALL SKUs from invoice lines with fallback to Economic product names for pre-2024 data.
Use for: Product lookups, category analysis, inventory views.';


-- ============================================================
-- GRANTS
-- ============================================================
-- ECONOMIC_ADMIN: Full access to all Silver views
GRANT SELECT ON VIEW VW_SALES_DETAIL TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW VW_CUSTOMER_MASTER TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW VW_PRODUCT_MASTER TO ROLE ECONOMIC_ADMIN;

-- ECONOMIC_WRITE: Read access for data engineers
GRANT SELECT ON VIEW VW_SALES_DETAIL TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW VW_CUSTOMER_MASTER TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW VW_PRODUCT_MASTER TO ROLE ECONOMIC_WRITE;

-- ECONOMIC_READ: Read access for analysts and BI tools
GRANT SELECT ON VIEW VW_SALES_DETAIL TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW VW_CUSTOMER_MASTER TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW VW_PRODUCT_MASTER TO ROLE ECONOMIC_READ;


-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================

-- Row counts
-- SELECT 'VW_SALES_DETAIL' AS view_name, COUNT(*) AS row_count FROM VW_SALES_DETAIL
-- UNION ALL
-- SELECT 'VW_CUSTOMER_MASTER', COUNT(*) FROM VW_CUSTOMER_MASTER
-- UNION ALL
-- SELECT 'VW_PRODUCT_MASTER', COUNT(*) FROM VW_PRODUCT_MASTER;

-- Check all columns
-- SELECT
--   invoice_id,
--   line_id,
--   sale_date,
--   customer_id,
--   customer_name,
--   customer_city,
--   customer_country,
--   customer_segment,
--   product_sku,
--   product_name,  -- Unified name (PrestaShop → Economic fallback)
--   core_product_economic,
--   product_category_economic,
--   -- PrestaShop columns
--   category,
--   size,
--   color,
--   ean13,
--   prestashop_match_status,
--   -- Geography
--   delivery_country,
--   market,
--   -- Currency
--   invoice_currency,
--   exchange_rate,
--   -- Sales metrics
--   quantity_sold,
--   unit_price_dkk,
--   line_revenue_dkk,
--   product_revenue_dkk,
--   -- Cost & profit
--   unit_cost_dkk,
--   line_cost_dkk,
--   line_profit_dkk
-- FROM VW_SALES_DETAIL
-- LIMIT 10;

-- Check PrestaShop match rate
-- SELECT
--   prestashop_match_status,
--   COUNT(*) AS line_count,
--   SUM(line_revenue_dkk) AS revenue_dkk,
--   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_lines,
--   ROUND(SUM(line_revenue_dkk) * 100.0 / SUM(SUM(line_revenue_dkk)) OVER (), 2) AS pct_of_revenue
-- FROM VW_SALES_DETAIL
-- GROUP BY prestashop_match_status;

-- Revenue by category (new capability!)
-- SELECT
--   category,
--   COUNT(DISTINCT product_sku) AS sku_count,
--   SUM(quantity_sold) AS units_sold,
--   SUM(line_revenue_dkk) AS revenue_dkk,
--   SUM(line_profit_dkk) AS profit_dkk,
--   ROUND(SUM(line_profit_dkk) / NULLIF(SUM(line_revenue_dkk), 0) * 100, 2) AS margin_pct
-- FROM VW_SALES_DETAIL
-- WHERE prestashop_match_status = 'Matched'
--   AND product_revenue_dkk > 0  -- Exclude discounts/freight
-- GROUP BY category
-- ORDER BY revenue_dkk DESC;

-- Revenue by size and color (new capability!)
-- SELECT
--   size,
--   color,
--   COUNT(*) AS line_count,
--   SUM(quantity_sold) AS units_sold,
--   SUM(line_revenue_dkk) AS revenue_dkk
-- FROM VW_SALES_DETAIL
-- WHERE prestashop_match_status = 'Matched'
--   AND product_revenue_dkk > 0
-- GROUP BY size, color
-- ORDER BY revenue_dkk DESC
-- LIMIT 20;

-- Check for any NULL invoice_dates (data quality)
-- SELECT COUNT(*) AS lines_without_date
-- FROM BRONZE.INVOICE_LINES il
-- LEFT JOIN BRONZE.INVOICES inv ON il.invoice_number = inv.invoice_number
-- WHERE inv.invoice_date IS NULL;

/*---------------------------------------------------------------*/
/*** END OF FILE 09 - Silver Views                            ***/
/*---------------------------------------------------------------*/





