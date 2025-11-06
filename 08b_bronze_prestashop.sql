/*---------------------------------------------------------------*/
/**                                                             **/
/*** BRONZE LAYER VIEWS - PRESTASHOP API DATA                  ***/
/*** Normalized views for PrestaShop product master data.      ***/
/*** These views flatten the JSON structure from RAW layer     ***/
/*** into business-friendly columnar format.                   ***/
/***                                                           ***/
/*** Data Model:                                               ***/
/***   - PRESTA_PRODUCTS: Parent product info                  ***/
/***   - PRESTA_CATEGORIES: Category hierarchy                 ***/
/***   - PRESTA_COMBINATIONS: Variants with SKU/reference      ***/
/***   - PRESTA_OPTION_VALUES: Size/color attributes           ***/
/***   - DIM_PRODUCT_SKU: Complete SKU dimension (enriched)    ***/
/***                                                           ***/
/*** Purpose: Bridge between e-conomic sales data and          ***/
/*** PrestaShop product hierarchy for BI reporting.            ***/
/**                                                             **/
/*---------------------------------------------------------------*/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA BRONZE;


/*---------------------------------------------------------------*/
/*** 1. PRESTA_PRODUCTS - Parent Product Level                 ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW PRESTA_PRODUCTS AS
SELECT
  r.DATE_INSERTED                                     AS api_timestamp,
  -- Core product info
  p.value:"id"::INT                                   AS product_id,
  p.value:"name"::STRING                              AS product_name,

  -- Category reference
  p.value:"id_category_default"::INT                  AS id_category_default,

  -- Product attributes
  p.value:"active"::BOOLEAN                           AS active,

  -- Timestamps
  p.value:"date_add"::TIMESTAMP_LTZ                   AS date_added,
  p.value:"date_upd"::TIMESTAMP_LTZ                   AS date_updated

FROM RAW.PRESTA_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"products") p
WHERE r.API_ENDPOINT = 'products';

COMMENT ON VIEW PRESTA_PRODUCTS IS 'PrestaShop products (parent level) - contains base product info and default category reference';


/*---------------------------------------------------------------*/
/*** 2. PRESTA_CATEGORIES - Category Hierarchy                 ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW PRESTA_CATEGORIES AS
SELECT
  r.DATE_INSERTED                                     AS api_timestamp,
  -- Core category info
  c.value:"id"::INT                                   AS category_id,
  c.value:"name"::STRING                              AS category_name_da,
  c.value:"id_parent"::INT                            AS parent_category_id,

  -- Additional attributes
  c.value:"active"::BOOLEAN                           AS active,
  c.value:"level_depth"::INT                          AS level_depth

FROM RAW.PRESTA_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"categories") c
WHERE r.API_ENDPOINT = 'categories';

COMMENT ON VIEW PRESTA_CATEGORIES IS 'PrestaShop categories - hierarchical structure with parent_category_id for building product taxonomy';


/*---------------------------------------------------------------*/
/*** 3. PRESTA_COMBINATIONS - Variant Level (SKU/Reference)    ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW PRESTA_COMBINATIONS AS
SELECT
  r.DATE_INSERTED                                     AS api_timestamp,
  -- Core combination info
  c.value:"id"::INT                                   AS combination_id,
  c.value:"id_product"::INT                           AS product_id,

  -- *** KEY FIELD: SKU/Reference (joins to e-conomic) ***
  c.value:"reference"::STRING                         AS sku,

  -- Identifiers
  c.value:"ean13"::STRING                             AS ean13,

  -- Pricing and stock
  c.value:"price"::NUMBER                             AS price_impact,
  c.value:"quantity"::INT                             AS quantity_in_stock

FROM RAW.PRESTA_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"combinations") c
WHERE r.API_ENDPOINT = 'combinations';

COMMENT ON VIEW PRESTA_COMBINATIONS IS 'PrestaShop combinations (variant level) - contains SKU/reference field that joins to e-conomic invoice lines. This is the key linking table.';


/*---------------------------------------------------------------*/
/*** 4. PRESTA_COMBINATION_OPTIONS - Link combinations to      ***/
/***    option values (size, color, etc.)                      ***/
/*---------------------------------------------------------------*/

-- This view flattens the associations array in combinations
-- Each combination can have multiple option values (e.g., Size=M, Color=Blue)
CREATE OR REPLACE VIEW PRESTA_COMBINATION_OPTIONS AS
SELECT
  r.DATE_INSERTED                                     AS api_timestamp,
  c.value:"id"::INT                                   AS combination_id,
  opt.value:"id"::INT                                 AS option_value_id
FROM RAW.PRESTA_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"combinations") c,
     LATERAL FLATTEN(input => c.value:"associations"."product_option_values", OUTER => TRUE) opt
WHERE r.API_ENDPOINT = 'combinations';

COMMENT ON VIEW PRESTA_COMBINATION_OPTIONS IS 'Link table between combinations and option values - many-to-many relationship (e.g., combination 949 has option_value 9 (Size) and 27 (Color))';


/*---------------------------------------------------------------*/
/*** 5. PRESTA_OPTION_VALUES - Attribute Values (Size, Color)  ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW PRESTA_OPTION_VALUES AS
SELECT
  r.DATE_INSERTED                                     AS api_timestamp,
  -- Core option value info
  ov.value:"id"::INT                                  AS option_value_id,
  ov.value:"id_attribute_group"::INT                  AS option_group_id,
  ov.value:"name"::STRING                             AS option_value_name

  -- Note: color and position not included in display parameter
  -- Only fetching: [id,id_attribute_group,name]

FROM RAW.PRESTA_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"product_option_values") ov
WHERE r.API_ENDPOINT = 'product_option_values';

COMMENT ON VIEW PRESTA_OPTION_VALUES IS 'PrestaShop option values - contains size/color names (e.g., "M", "Ocean Blue") linked by option_value_id';


/*---------------------------------------------------------------*/
/*** 6. DIM_PRODUCT_SKU - Complete SKU Dimension (ENRICHED)    ***/
/*---------------------------------------------------------------*/

-- This is the "golden" dimension that joins everything together
-- Starting from combinations (variant level with SKU), we enrich with:
--   - Product name and category from products
--   - Category hierarchy from categories
--   - Size and color from option_values via combination_options

CREATE OR REPLACE VIEW DIM_PRODUCT_SKU AS
WITH
-- Get size values (option_group_id = 1 typically for Size)
sizes AS (
  SELECT
    co.combination_id,
    ov.option_value_name AS size_name,
    ov.option_group_id
  FROM PRESTA_COMBINATION_OPTIONS co
  JOIN PRESTA_OPTION_VALUES ov ON co.option_value_id = ov.option_value_id
  WHERE ov.option_group_id = 1  -- Adjust this if needed based on your PrestaShop setup
),
-- Get color values (option_group_id = 2 typically for Color)
colors AS (
  SELECT
    co.combination_id,
    ov.option_value_name AS color_name,
    ov.option_group_id
  FROM PRESTA_COMBINATION_OPTIONS co
  JOIN PRESTA_OPTION_VALUES ov ON co.option_value_id = ov.option_value_id
  WHERE ov.option_group_id = 2  -- Adjust this if needed based on your PrestaShop setup
),
-- Get all option values (for cases where option_group_id is not 1 or 2)
all_options AS (
  SELECT
    co.combination_id,
    LISTAGG(ov.option_value_name, ' / ') WITHIN GROUP (ORDER BY ov.option_group_id) AS all_options_text
  FROM PRESTA_COMBINATION_OPTIONS co
  JOIN PRESTA_OPTION_VALUES ov ON co.option_value_id = ov.option_value_id
  GROUP BY co.combination_id
)
SELECT
  -- SKU identifier (joins to e-conomic)
  c.sku                                               AS sku,

  -- Product info
  c.product_id                                        AS product_id,
  COALESCE(p.product_name, 'Unknown Product')         AS product_name,
  -- Note: description not fetched in display parameter
  p.active                                            AS product_active,

  -- Category info
  cat.category_id                                     AS category_id,
  COALESCE(cat.category_name_da, 'Uncategorized')     AS category_name,
  cat.parent_category_id                              AS parent_category_id,
  cat.level_depth                                     AS category_level,

  -- Variant attributes
  sz.size_name                                        AS size,
  col.color_name                                      AS color,
  -- Note: color_code not fetched in display parameter
  opt.all_options_text                                AS all_attributes,

  -- Additional variant info
  c.combination_id                                    AS combination_id,
  c.ean13                                             AS ean13,
  c.price_impact                                      AS price_impact,
  c.quantity_in_stock                                 AS stock_quantity,
  -- Note: is_default not fetched in display parameter

  -- Timestamps
  p.date_updated                                      AS last_updated,

  -- Metadata
  CURRENT_TIMESTAMP()                                 AS dim_created_at

FROM PRESTA_COMBINATIONS c
LEFT JOIN PRESTA_PRODUCTS p ON c.product_id = p.product_id
LEFT JOIN PRESTA_CATEGORIES cat
  ON p.id_category_default = cat.category_id
  AND (cat.category_name_da != 'Shop' OR cat.category_name_da IS NULL)  -- Exclude "Shop" category, allow NULL
LEFT JOIN sizes sz ON c.combination_id = sz.combination_id
LEFT JOIN colors col ON c.combination_id = col.combination_id
LEFT JOIN all_options opt ON c.combination_id = opt.combination_id
WHERE c.sku IS NOT NULL  -- Only include combinations with SKU
;

COMMENT ON VIEW DIM_PRODUCT_SKU IS
'Complete SKU dimension - enriches variant-level data from combinations with product names, categories, size, and color.
This is the key dimension for joining e-conomic sales data with PrestaShop product master data.

USAGE: This dimension is designed to be LEFT JOINed from INVOICE_LINES in the Silver layer:
  SELECT il.*, dim.product_name, dim.category_name, dim.size, dim.color
  FROM BRONZE.INVOICE_LINES il
  LEFT JOIN BRONZE.DIM_PRODUCT_SKU dim ON il.sku = dim.sku

NOTE: For SKUs not found in PrestaShop (pre-2024 data), use DIM_PRODUCT_SKU_ENRICHED which includes
invoice line descriptions as fallback.';


/*---------------------------------------------------------------*/
/*** 7. DIM_PRODUCT_SKU_ENRICHED - With Invoice Line Fallback ***/
/*---------------------------------------------------------------*/

-- This view extends DIM_PRODUCT_SKU by including ALL SKUs from invoice lines,
-- even if they don't exist in PrestaShop (e.g., pre-2024 Economic data).
-- For unmatched SKUs, it uses the invoice line description as product name.

CREATE OR REPLACE VIEW DIM_PRODUCT_SKU_ENRICHED AS
WITH all_invoice_skus AS (
  -- Get distinct SKUs from invoice lines with their most common description
  SELECT
    sku,
    -- Use the most frequent line_description for this SKU as fallback
    MODE(line_description) AS economic_product_name
  FROM INVOICE_LINES
  WHERE sku IS NOT NULL
  GROUP BY sku
)
SELECT
  -- Use invoice SKUs as source of truth
  inv.sku,

  -- Product info: PrestaShop first, then Economic fallback
  COALESCE(dim.product_id, -1)                        AS product_id,  -- -1 indicates no PrestaShop match
  COALESCE(dim.product_name, inv.economic_product_name, 'Unknown Product') AS product_name,
  dim.product_active                                  AS product_active,

  -- Category info (NULL if no PrestaShop match)
  dim.category_id,
  dim.category_name,
  dim.parent_category_id,
  dim.category_level,

  -- Variant attributes (NULL if no PrestaShop match)
  dim.size,
  dim.color,
  dim.all_attributes,

  -- Additional variant info
  dim.combination_id,
  dim.ean13,
  dim.price_impact,
  dim.stock_quantity,

  -- Flags for data quality
  CASE WHEN dim.sku IS NULL THEN TRUE ELSE FALSE END AS is_economic_only,  -- SKU not in PrestaShop
  CASE WHEN dim.sku IS NOT NULL THEN TRUE ELSE FALSE END AS has_prestashop_data,

  -- Timestamps
  COALESCE(dim.last_updated, CURRENT_TIMESTAMP())     AS last_updated,
  CURRENT_TIMESTAMP()                                 AS dim_created_at

FROM all_invoice_skus inv
LEFT JOIN DIM_PRODUCT_SKU dim ON inv.sku = dim.sku;

COMMENT ON VIEW DIM_PRODUCT_SKU_ENRICHED IS
'Enriched SKU dimension with invoice line fallback - includes ALL SKUs from invoice lines.
For SKUs not in PrestaShop (e.g., pre-2024 data with old SKU format), uses invoice line description as product name.
Use flags: is_economic_only=TRUE for unmatched SKUs, has_prestashop_data=TRUE for matched SKUs.

DATA INSIGHT: Match rate is 85-95% for 2024+, but drops significantly for pre-2024 data due to old SKU format.

USAGE: This is the recommended dimension for Silver layer joins:
  SELECT il.*, dim.product_name, dim.category_name, dim.size, dim.color, dim.is_economic_only
  FROM BRONZE.INVOICE_LINES il
  LEFT JOIN BRONZE.DIM_PRODUCT_SKU_ENRICHED dim ON il.sku = dim.sku';


/*---------------------------------------------------------------*/
/*** GRANT PERMISSIONS                                         ***/
/*---------------------------------------------------------------*/

-- ECONOMIC_ADMIN: Full access to all PrestaShop Bronze views
GRANT SELECT ON VIEW PRESTA_PRODUCTS TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW PRESTA_CATEGORIES TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW PRESTA_COMBINATIONS TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW PRESTA_COMBINATION_OPTIONS TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW PRESTA_OPTION_VALUES TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW DIM_PRODUCT_SKU TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW DIM_PRODUCT_SKU_ENRICHED TO ROLE ECONOMIC_ADMIN;

-- ECONOMIC_WRITE: Read access for data engineers
GRANT SELECT ON VIEW PRESTA_PRODUCTS TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW PRESTA_CATEGORIES TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW PRESTA_COMBINATIONS TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW PRESTA_COMBINATION_OPTIONS TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW PRESTA_OPTION_VALUES TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW DIM_PRODUCT_SKU TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW DIM_PRODUCT_SKU_ENRICHED TO ROLE ECONOMIC_WRITE;

-- ECONOMIC_READ: Read access for analysts and BI tools
GRANT SELECT ON VIEW PRESTA_PRODUCTS TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW PRESTA_CATEGORIES TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW PRESTA_COMBINATIONS TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW PRESTA_COMBINATION_OPTIONS TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW PRESTA_OPTION_VALUES TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW DIM_PRODUCT_SKU TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW DIM_PRODUCT_SKU_ENRICHED TO ROLE ECONOMIC_READ;


/*---------------------------------------------------------------*/
/*** VERIFICATION QUERIES                                      ***/
/*---------------------------------------------------------------*/

-- View all PrestaShop Bronze views
-- SHOW VIEWS IN SCHEMA BRONZE LIKE 'PRESTA%';
-- SHOW VIEWS IN SCHEMA BRONZE LIKE 'DIM_%';

-- Test key views
-- SELECT * FROM BRONZE.PRESTA_PRODUCTS LIMIT 10;
-- SELECT * FROM BRONZE.PRESTA_CATEGORIES LIMIT 10;
-- SELECT * FROM BRONZE.PRESTA_COMBINATIONS LIMIT 10;
-- SELECT * FROM BRONZE.PRESTA_OPTION_VALUES LIMIT 10;
-- SELECT * FROM BRONZE.DIM_PRODUCT_SKU LIMIT 10;

-- Check SKU distribution
-- SELECT COUNT(*) AS total_skus, COUNT(DISTINCT sku) AS unique_skus
-- FROM BRONZE.DIM_PRODUCT_SKU;

-- Check category distribution
-- SELECT category_name, COUNT(*) AS sku_count
-- FROM BRONZE.DIM_PRODUCT_SKU
-- GROUP BY category_name
-- ORDER BY sku_count DESC;

-- Check size/color distribution
-- SELECT size, color, COUNT(*) AS sku_count
-- FROM BRONZE.DIM_PRODUCT_SKU
-- GROUP BY size, color
-- ORDER BY sku_count DESC;

-- Test enriched dimension with fallback
-- SELECT * FROM BRONZE.DIM_PRODUCT_SKU_ENRICHED LIMIT 10;

-- Check SKU match rate (PrestaShop vs Economic only)
-- SELECT
--   has_prestashop_data,
--   is_economic_only,
--   COUNT(*) AS sku_count,
--   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
-- FROM BRONZE.DIM_PRODUCT_SKU_ENRICHED
-- GROUP BY has_prestashop_data, is_economic_only;

-- Compare product names: PrestaShop vs Economic fallback
-- SELECT
--   sku,
--   product_name,
--   is_economic_only,
--   category_name
-- FROM BRONZE.DIM_PRODUCT_SKU_ENRICHED
-- WHERE is_economic_only = TRUE
-- LIMIT 20;

/*---------------------------------------------------------------*/
/*** END OF FILE 08b - PrestaShop Bronze Views                ***/
/*---------------------------------------------------------------*/
