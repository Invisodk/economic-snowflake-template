/*---------------------------------------------------------------*/
/**                                                             **/
/*** EXPIRED SKU LOGIC - Historical Product Data Integration   ***/
/*** Purpose: Integrate 360 expired SKUs (pre-2024) that are   ***/
/*** no longer in PrestaShop but exist in Economic invoices.   ***/
/***                                                           ***/
/*** Strategy: Create synthetic PrestaShop views that mimic    ***/
/*** the structure of active PrestaShop data. These views      ***/
/*** are UNION'ed into the main bronze layer views, allowing   ***/
/*** expired SKUs to flow naturally through the product        ***/
/*** hierarchy (DIM_PRODUCT_SKU → DIM_PRODUCT_SKU_ENRICHED).   ***/
/***                                                           ***/
/*** Data Source: Excel file with 360 records containing:      ***/
/*** - PRODUCT (name), COLOR, SIZE, SKU, EAN                   ***/
/*** - CATEGORY_NO, CATEGORY_NAME                              ***/
/***                                                           ***/
/*** Architecture:                                             ***/
/*** 1. EXPIRED_SKU_LIST table (this file) - holds Excel data  ***/
/*** 2. Synthetic views (below) - transform into PrestaShop    ***/
/***    structure with negative IDs to avoid collisions        ***/
/*** 3. Bronze views (08b) - UNION synthetic + active data     ***/
/**                                                             **/
/*---------------------------------------------------------------*/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA BRONZE;

/*---------------------------------------------------------------*/
/*** 1. EXPIRED SKU TABLE - Raw Excel Data                     ***/
/*---------------------------------------------------------------*/

create or replace TABLE RAW.EXPIRED_SKU_LIST (
	PRODUCT VARCHAR(16777216),
	COLOR VARCHAR(16777216),
	SIZE VARCHAR(16777216),
	SKU VARCHAR(16777216),
	EAN VARCHAR(16777216),
	CATEGORY_NO VARCHAR(16777216),
	CATEGORY_NAME VARCHAR(16777216)
);

COMMENT ON TABLE RAW.EXPIRED_SKU_LIST IS
'Expired SKUs from Excel - 360 historical products (pre-2024) not in PrestaShop but present in Economic invoices.
Loaded manually from stakeholder spreadsheet. These SKUs are transformed into synthetic PrestaShop views
and merged into the bronze layer to enable complete historical BI reporting.';


/*---------------------------------------------------------------*/
/*** 2. SYNTHETIC PRESTA_COMBINATIONS - Expired SKUs as        ***/
/***    variant-level combinations                             ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW PRESTA_COMBINATIONS_EXPIRED AS
  SELECT
    TO_TIMESTAMP_NTZ('2024-01-01 00:00:00') AS api_timestamp,  -- Fixed timestamp for expired SKUs

    -- Synthetic IDs (negative to avoid collision with real PrestaShop IDs)
    -1 * ROW_NUMBER() OVER (ORDER BY SKU) AS combination_id, -- order by sku (one per sku)

    -- Link to correct product_id based on product name (matches PRESTA_PRODUCTS_EXPIRED logic)
    -1 * DENSE_RANK() OVER (ORDER BY NULLIF(TRIM(PRODUCT), '')) AS product_id,

    -- SKU from Excel
    UPPER(TRIM(SKU)) AS sku,

    -- EAN from Excel
    NULLIF(TRIM(EAN), '') AS ean13,

    -- No price/stock data for expired items
    0 AS price_impact,
    0 AS quantity_in_stock

  FROM RAW.EXPIRED_SKU_LIST
  WHERE SKU IS NOT NULL AND TRIM(SKU) != '';

COMMENT ON VIEW PRESTA_COMBINATIONS_EXPIRED IS
'Synthetic combinations view for expired SKUs. Uses negative combination_ids to avoid collision with active PrestaShop data.
Each combination is linked to its correct product_id based on the PRODUCT name from the Excel file.
Provides SKU-level data that flows through DIM_PRODUCT_SKU.';


/*---------------------------------------------------------------*/
/*** 3. SYNTHETIC PRESTA_PRODUCTS - Parent product records     ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW PRESTA_PRODUCTS_EXPIRED AS
  SELECT DISTINCT
    TO_TIMESTAMP_NTZ('2024-01-01 00:00:00') AS api_timestamp,  -- Fixed timestamp for expired SKUs
    -- Create unique product_id for each unique product name (-1, -2, -3, etc.)
    -1 * DENSE_RANK() OVER (ORDER BY NULLIF(TRIM(PRODUCT), '')) AS product_id,
    NULLIF(TRIM(PRODUCT), '') AS product_name,
    TRY_TO_NUMBER(CATEGORY_NO) AS id_category_default,
    FALSE AS active,  -- Expired = inactive
    TO_TIMESTAMP_NTZ('2024-01-01 00:00:00') AS date_added,
    TO_TIMESTAMP_NTZ('2024-01-01 00:00:00') AS date_updated
  FROM RAW.EXPIRED_SKU_LIST
  WHERE PRODUCT IS NOT NULL;

COMMENT ON VIEW PRESTA_PRODUCTS_EXPIRED IS
'Synthetic products view for expired SKUs. Creates one product record per unique product name with negative product_ids (-1, -2, -3, etc.).';


/*---------------------------------------------------------------*/
/*** 4. SYNTHETIC PRESTA_CATEGORIES - Category mappings        ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW PRESTA_CATEGORIES_EXPIRED AS
  SELECT DISTINCT
    TO_TIMESTAMP_NTZ('2024-01-01 00:00:00') AS api_timestamp,  -- Fixed timestamp for expired SKUs
    TRY_TO_NUMBER(CATEGORY_NO) AS category_id,
    NULLIF(TRIM(CATEGORY_NAME), '') AS category_name_da,
    2 AS parent_category_id,  -- Assume root parent
    TRUE AS active,
    1 AS level_depth
  FROM RAW.EXPIRED_SKU_LIST
  WHERE CATEGORY_NO IS NOT NULL;

COMMENT ON VIEW PRESTA_CATEGORIES_EXPIRED IS
'Synthetic categories view for expired SKUs. Maps CATEGORY_NO and CATEGORY_NAME from Excel to PrestaShop category structure.';


/*---------------------------------------------------------------*/
/*** 5. SYNTHETIC PRESTA_OPTION_VALUES - Size & Color values   ***/
/*---------------------------------------------------------------*/

-- Create synthetic option values for Size and Color from expired SKUs
CREATE OR REPLACE VIEW PRESTA_OPTION_VALUES_EXPIRED AS
-- Sizes from expired SKUs
SELECT DISTINCT
  TO_TIMESTAMP_NTZ('2024-01-01 00:00:00') AS api_timestamp,  -- Fixed timestamp for expired SKUs
  -- Use negative IDs starting from -10000 to avoid collision
  -10000 - ROW_NUMBER() OVER (ORDER BY NULLIF(TRIM(SIZE), '')) AS option_value_id,
  1 AS option_group_id,  -- Size group
  NULLIF(TRIM(SIZE), '') AS option_value_name
FROM RAW.EXPIRED_SKU_LIST
WHERE NULLIF(TRIM(SIZE), '') IS NOT NULL

UNION ALL

-- Colors from expired SKUs
SELECT DISTINCT
  TO_TIMESTAMP_NTZ('2024-01-01 00:00:00') AS api_timestamp,  -- Fixed timestamp for expired SKUs
  -- Use negative IDs starting from -20000 to avoid collision with sizes
  -20000 - ROW_NUMBER() OVER (ORDER BY NULLIF(TRIM(COLOR), '')) AS option_value_id,
  2 AS option_group_id,  -- Color group
  NULLIF(TRIM(COLOR), '') AS option_value_name
FROM RAW.EXPIRED_SKU_LIST
WHERE NULLIF(TRIM(COLOR), '') IS NOT NULL;

COMMENT ON VIEW PRESTA_OPTION_VALUES_EXPIRED IS
'Synthetic option values for expired SKUs. Creates size (option_group_id=1) and color (option_group_id=2) values.
Uses negative IDs: -10000 range for sizes, -20000 range for colors to avoid collision with active PrestaShop data.';


/*---------------------------------------------------------------*/
/*** 6. SYNTHETIC PRESTA_COMBINATION_OPTIONS - Link combos to  ***/
/***    their size/color attributes                            ***/
/*---------------------------------------------------------------*/

-- Create combination options linking expired combinations to their size/color
CREATE OR REPLACE VIEW PRESTA_COMBINATION_OPTIONS_EXPIRED AS
WITH expired_with_ids AS (
  SELECT
    -1 * ROW_NUMBER() OVER (ORDER BY SKU) AS combination_id,
    NULLIF(TRIM(SIZE), '') AS size_value,
    NULLIF(TRIM(COLOR), '') AS color_value
  FROM RAW.EXPIRED_SKU_LIST
  WHERE SKU IS NOT NULL AND TRIM(SKU) != ''
),
size_ids AS (
  SELECT DISTINCT
    -10000 - ROW_NUMBER() OVER (ORDER BY NULLIF(TRIM(SIZE), '')) AS option_value_id,
    NULLIF(TRIM(SIZE), '') AS size_value
  FROM RAW.EXPIRED_SKU_LIST
  WHERE NULLIF(TRIM(SIZE), '') IS NOT NULL
),
color_ids AS (
  SELECT DISTINCT
    -20000 - ROW_NUMBER() OVER (ORDER BY NULLIF(TRIM(COLOR), '')) AS option_value_id,
    NULLIF(TRIM(COLOR), '') AS color_value
  FROM RAW.EXPIRED_SKU_LIST
  WHERE NULLIF(TRIM(COLOR), '') IS NOT NULL
)
-- Link combinations to size option values
SELECT
  TO_TIMESTAMP_NTZ('2024-01-01 00:00:00') AS api_timestamp,  -- Fixed timestamp for expired SKUs
  e.combination_id,
  s.option_value_id
FROM expired_with_ids e
JOIN size_ids s ON e.size_value = s.size_value
WHERE e.size_value IS NOT NULL

UNION ALL

-- Link combinations to color option values
SELECT
  TO_TIMESTAMP_NTZ('2024-01-01 00:00:00') AS api_timestamp,  -- Fixed timestamp for expired SKUs
  e.combination_id,
  c.option_value_id
FROM expired_with_ids e
JOIN color_ids c ON e.color_value = c.color_value
WHERE e.color_value IS NOT NULL;

COMMENT ON VIEW PRESTA_COMBINATION_OPTIONS_EXPIRED IS
'Synthetic combination options for expired SKUs. Maps each expired combination (by synthetic combination_id)
to its size and color option values. This enables the DIM_PRODUCT_SKU view to naturally extract size/color
attributes for expired SKUs using the same CTEs as active PrestaShop products.';


/*---------------------------------------------------------------*/
/*** GRANT PERMISSIONS                                         ***/
/*---------------------------------------------------------------*/

GRANT SELECT ON TABLE RAW.EXPIRED_SKU_LIST TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON TABLE RAW.EXPIRED_SKU_LIST TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON TABLE RAW.EXPIRED_SKU_LIST TO ROLE ECONOMIC_READ;

GRANT SELECT ON VIEW PRESTA_COMBINATIONS_EXPIRED TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW PRESTA_COMBINATIONS_EXPIRED TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW PRESTA_COMBINATIONS_EXPIRED TO ROLE ECONOMIC_READ;

GRANT SELECT ON VIEW PRESTA_PRODUCTS_EXPIRED TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW PRESTA_PRODUCTS_EXPIRED TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW PRESTA_PRODUCTS_EXPIRED TO ROLE ECONOMIC_READ;

GRANT SELECT ON VIEW PRESTA_CATEGORIES_EXPIRED TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW PRESTA_CATEGORIES_EXPIRED TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW PRESTA_CATEGORIES_EXPIRED TO ROLE ECONOMIC_READ;

GRANT SELECT ON VIEW PRESTA_OPTION_VALUES_EXPIRED TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW PRESTA_OPTION_VALUES_EXPIRED TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW PRESTA_OPTION_VALUES_EXPIRED TO ROLE ECONOMIC_READ;

GRANT SELECT ON VIEW PRESTA_COMBINATION_OPTIONS_EXPIRED TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW PRESTA_COMBINATION_OPTIONS_EXPIRED TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW PRESTA_COMBINATION_OPTIONS_EXPIRED TO ROLE ECONOMIC_READ;


/*---------------------------------------------------------------*/
/*** VERIFICATION QUERIES                                      ***/
/*---------------------------------------------------------------*/

-- Test expired SKU views
-- SELECT * FROM PRESTA_COMBINATIONS_EXPIRED LIMIT 10;
-- SELECT * FROM PRESTA_PRODUCTS_EXPIRED LIMIT 10;
-- SELECT * FROM PRESTA_CATEGORIES_EXPIRED LIMIT 10;
-- SELECT * FROM PRESTA_OPTION_VALUES_EXPIRED LIMIT 20;
-- SELECT * FROM PRESTA_COMBINATION_OPTIONS_EXPIRED LIMIT 20;

-- Count expired SKUs
-- SELECT COUNT(*) AS expired_sku_count FROM RAW.EXPIRED_SKU_LIST;
-- SELECT COUNT(*) AS expired_combo_count FROM PRESTA_COMBINATIONS_EXPIRED;

-- Verify ID ranges (should all be negative)
-- SELECT MIN(combination_id), MAX(combination_id) FROM PRESTA_COMBINATIONS_EXPIRED;
-- SELECT MIN(option_value_id), MAX(option_value_id) FROM PRESTA_OPTION_VALUES_EXPIRED;

/*---------------------------------------------------------------*/
/*** END OF FILE 11 - Expired SKU Logic                       ***/
/*---------------------------------------------------------------*/