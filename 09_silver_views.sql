/*---------------------------------------------------------------*/
/**                                                             **/
/*** SILVER LAYER - CLEAN & INTUITIVE                          **/
/*** Detail-level views with business-friendly names           **/
/*** NO AGGREGATION - Let BI tools handle that                 **/
/**                                                             **/
/*** 3 Core Views:                                             **/
/*** 1. VW_SALES_DETAIL - Product sales (51k+ rows)            **/
/*** 2. VW_FINANCIAL_DETAIL - Accounting entries               **/
/*** 3. VW_CUSTOMER_MASTER - Customer dimension                **/
/**                                                             **/
/*---------------------------------------------------------------*/
USE ROLE PLAYGROUND_ADMIN;
USE DATABASE PLAYGROUND;
USE SCHEMA SILVER;


-- ============================================================
-- VIEW 1: SALES DETAIL
-- One row per invoice line - NO AGGREGATION
-- ============================================================
CREATE OR REPLACE VIEW VW_SALES_DETAIL AS
SELECT
  -- ==== INVOICE IDENTIFIERS ====
  il.invoice_number                                   AS invoice_id,
  il.line_number                                      AS line_id,
  il.invoice_date                                     AS sale_date,

  -- ==== CUSTOMER INFO ====
  il.customer_number                                  AS customer_id,
  COALESCE(c.customer_name, 'Unknown')               AS customer_name,
  COALESCE(c.city, 'Unknown')                        AS customer_city,
  COALESCE(c.country, 'Unknown')                     AS customer_country,
  CASE
    WHEN c.customer_group_number = 1 THEN 'B2C'
    WHEN c.customer_group_number >= 2 THEN 'B2B'
    ELSE 'Unknown'
  END                                                 AS customer_segment,

  -- ==== PRODUCT INFO ====
  il.sku                                              AS product_sku,
  COALESCE(p.product_name, il.line_description, 'Unknown Product') AS product_name,
  REGEXP_REPLACE(
    COALESCE(p.product_name, il.line_description, 'Unknown Product'),
    ' - (Size|Størrelse|Storlek) : .*$| - [A-Za-z0-9/\-]+ - .*$',
    ''
  )                                                   AS core_product,
  COALESCE(p.product_group_name, 'Other')            AS product_category,
  COALESCE(p.unit_name, 'pcs')                       AS unit_of_measure,

  -- ==== LINE TYPE CLASSIFICATION ====
  CASE
    WHEN il.sku = 'rabat' THEN 'RABAT'
    WHEN il.sku = 'fragtmm' OR il.sku LIKE 'fragt%' THEN 'FRAGT'
    WHEN LOWER(COALESCE(p.product_group_name, il.line_description)) LIKE '%ydelse%' THEN 'YDELSE'
    WHEN LOWER(COALESCE(p.product_group_name, il.line_description)) LIKE '%service%' THEN 'YDELSE'
    WHEN LOWER(COALESCE(p.product_group_name, il.line_description)) LIKE '%vare%' THEN 'VARE'
    WHEN il.line_net_amount < 0 THEN 'RABAT'
    ELSE 'VARE'
  END                                                 AS line_type,

  -- ==== GEOGRAPHY ====
  COALESCE(
    il.delivery_country,
    il.recipient_country,
    'Unknown'
  )                                                   AS delivery_country,
  CASE
    WHEN COALESCE(il.delivery_country, il.recipient_country) IN ('Danmark', 'Denmark')
    THEN 'National'
    ELSE 'International'
  END                                                 AS market,

  -- ==== CURRENCY INFO ====
  il.invoice_currency                                 AS invoice_currency,
  il.exchange_rate                                    AS exchange_rate,

  -- ==== SALES METRICS (BASE CURRENCY - DKK) ====
  il.quantity                                         AS quantity_sold,
  il.unit_net_price_base_currency                    AS unit_price_dkk,
  il.line_net_amount_base_currency                   AS line_revenue_dkk,  -- Total line revenue (includes everything)

  -- ==== REVENUE BREAKDOWN (for detailed sales analysis) ====
  -- These sum up to line_revenue_dkk
  CASE
    WHEN line_type = 'VARE' THEN il.line_net_amount_base_currency
    ELSE 0
  END                                                 AS product_revenue_dkk,

  CASE
    WHEN line_type = 'FRAGT' THEN il.line_net_amount_base_currency
    ELSE 0
  END                                                 AS freight_revenue_dkk,

  CASE
    WHEN line_type = 'RABAT' THEN il.line_net_amount_base_currency
    ELSE 0
  END                                                 AS discount_amount_dkk,

  CASE
    WHEN line_type = 'YDELSE' THEN il.line_net_amount_base_currency
    ELSE 0
  END                                                 AS service_revenue_dkk,

  -- ==== COST & PROFIT (BASE CURRENCY - DKK) ====
  COALESCE(il.unit_cost_price_base_currency, 0)      AS unit_cost_dkk,
  COALESCE(il.unit_cost_price_base_currency, 0) * il.quantity AS line_cost_dkk,
  il.line_net_amount_base_currency -
    (COALESCE(il.unit_cost_price_base_currency, 0) * il.quantity) AS line_profit_dkk,
  ROUND(
    CASE
      WHEN il.line_net_amount_base_currency <> 0
      THEN ((il.line_net_amount_base_currency - (COALESCE(il.unit_cost_price_base_currency, 0) * il.quantity))
            / il.line_net_amount_base_currency) * 100
      ELSE 0
    END, 2
  )                                                   AS profit_margin_percent

FROM BRONZE.INVOICE_LINES il
LEFT JOIN BRONZE.CUSTOMERS c
  ON il.customer_number = c.customer_number
LEFT JOIN BRONZE.PRODUCTS p
  ON il.sku = p.sku
WHERE il.invoice_date IS NOT NULL;

COMMENT ON VIEW VW_SALES_DETAIL IS
'Sales detail - one row per invoice line (51k+ rows). NO aggregation.
Use for: Product analysis, customer behavior, country performance, margin analysis.
CURRENCY: All monetary amounts in DKK (base currency) - multi-currency invoices converted using exchange_rate.
REVENUE BREAKDOWN: line_revenue_dkk = product_revenue_dkk + freight_revenue_dkk + discount_amount_dkk + service_revenue_dkk
NOTE: Only sale_date included - Tableau automatically creates Year/Quarter/Month hierarchies.';


-- ============================================================
-- VIEW 2: CUSTOMER MASTER
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
-- GRANTS
-- ============================================================
GRANT SELECT ON VIEW VW_SALES_DETAIL TO ROLE PLAYGROUND_ADMIN;
GRANT SELECT ON VIEW VW_FINANCIAL_DETAIL TO ROLE PLAYGROUND_ADMIN;
GRANT SELECT ON VIEW VW_CUSTOMER_MASTER TO ROLE PLAYGROUND_ADMIN;


-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================

-- Row counts
SELECT 'VW_SALES_DETAIL' AS view_name, COUNT(*) AS row_count FROM VW_SALES_DETAIL
UNION ALL
SELECT 'VW_FINANCIAL_DETAIL', COUNT(*) FROM VW_FINANCIAL_DETAIL
UNION ALL
SELECT 'VW_CUSTOMER_MASTER', COUNT(*) FROM VW_CUSTOMER_MASTER;

-- Sample data
SELECT * FROM VW_SALES_DETAIL LIMIT 10;
SELECT * FROM VW_FINANCIAL_DETAIL LIMIT 10;
SELECT * FROM VW_CUSTOMER_MASTER LIMIT 10;


/*---------------------------------------------------------------*/
/*** USAGE EXAMPLES                                            ***/
/*---------------------------------------------------------------*/

/*

EXAMPLE ANALYSES (for BI tools):
---------------------------------

NOTE: All monetary values are in DKK (base currency) for accurate analysis

1. Revenue by Country (DKK):
   SELECT delivery_country, SUM(line_revenue_dkk) AS revenue_dkk
   FROM VW_SALES_DETAIL
   GROUP BY delivery_country
   ORDER BY revenue_dkk DESC;

2. Top Products by Revenue (DKK):
   SELECT product_name,
          SUM(quantity_sold) AS units_sold,
          SUM(line_revenue_dkk) AS revenue_dkk
   FROM VW_SALES_DETAIL
   GROUP BY product_name
   ORDER BY revenue_dkk DESC
   LIMIT 10;

3. Monthly Revenue Trend (DKK):
   SELECT DATE_TRUNC('month', sale_date) AS month,
          SUM(line_revenue_dkk) AS revenue_dkk
   FROM VW_SALES_DETAIL
   GROUP BY DATE_TRUNC('month', sale_date)
   ORDER BY month;

4. B2B vs B2C Revenue & Profit (DKK):
   SELECT customer_segment,
          COUNT(DISTINCT customer_id) AS customer_count,
          SUM(line_revenue_dkk) AS revenue_dkk,
          SUM(line_profit_dkk) AS profit_dkk,
          AVG(profit_margin_percent) AS avg_margin_pct
   FROM VW_SALES_DETAIL
   GROUP BY customer_segment;

5. Profit Margin by Product Category (DKK):
   SELECT product_category,
          SUM(line_revenue_dkk) AS revenue_dkk,
          SUM(line_profit_dkk) AS profit_dkk,
          (SUM(line_profit_dkk) / NULLIF(SUM(line_revenue_dkk), 0)) * 100 AS margin_pct
   FROM VW_SALES_DETAIL
   GROUP BY product_category
   ORDER BY revenue_dkk DESC;

6. National vs International Sales (DKK):
   SELECT market,
          COUNT(DISTINCT invoice_id) AS invoice_count,
          SUM(line_revenue_dkk) AS revenue_dkk,
          SUM(line_profit_dkk) AS profit_dkk
   FROM VW_SALES_DETAIL
   GROUP BY market;

7. Multi-Currency Invoice Analysis:
   SELECT invoice_currency,
          COUNT(DISTINCT invoice_id) AS invoice_count,
          AVG(exchange_rate) AS avg_exchange_rate,
          SUM(line_revenue_dkk) AS total_revenue_dkk
   FROM VW_SALES_DETAIL
   GROUP BY invoice_currency
   ORDER BY total_revenue_dkk DESC;

8. Line Type Distribution (DKK):
   SELECT line_type,
          COUNT(*) AS line_count,
          SUM(line_revenue_dkk) AS revenue_dkk,
          AVG(profit_margin_percent) AS avg_margin_pct
   FROM VW_SALES_DETAIL
   GROUP BY line_type
   ORDER BY revenue_dkk DESC;

9. Pure Product Sales vs Total Invoice Value:
   SELECT
          SUM(product_revenue_dkk) AS product_sales_dkk,      -- Only VARE
          SUM(freight_revenue_dkk) AS freight_charges_dkk,     -- Only FRAGT
          SUM(discount_amount_dkk) AS total_discounts_dkk,     -- Only RABAT (negative)
          SUM(service_revenue_dkk) AS service_revenue_dkk,     -- Only YDELSE
          SUM(line_revenue_dkk) AS total_invoice_value_dkk,    -- Everything
          -- Verification: should equal total_invoice_value_dkk
          SUM(product_revenue_dkk + freight_revenue_dkk + discount_amount_dkk + service_revenue_dkk) AS breakdown_sum_dkk
   FROM VW_SALES_DETAIL
   WHERE sale_date >= '2024-01-01';

10. Product Sales by Country (excluding freight/discounts):
    SELECT delivery_country,
           SUM(product_revenue_dkk) AS product_sales_dkk,     -- Pure product sales
           SUM(line_revenue_dkk) AS total_revenue_dkk,         -- Includes freight/discounts
           SUM(freight_revenue_dkk) AS freight_dkk,
           SUM(discount_amount_dkk) AS discounts_dkk
    FROM VW_SALES_DETAIL
    GROUP BY delivery_country
    ORDER BY product_sales_dkk DESC;

11. Average Discount % by Customer Segment:
    SELECT customer_segment,
           SUM(product_revenue_dkk) AS gross_product_sales_dkk,
           SUM(discount_amount_dkk) AS total_discounts_dkk,
           (SUM(discount_amount_dkk) / NULLIF(SUM(product_revenue_dkk), 0)) * 100 AS avg_discount_percent
    FROM VW_SALES_DETAIL
    GROUP BY customer_segment;

*/
