/*---------------------------------------------------------------*/
/**                                                             **/
/*** BRONZE LAYER VIEWS - ECONOMIC API DATA (ENHANCED)         ***/
/*** Comprehensive field extraction to support all business    ***/
/*** questions including country-based sales analysis.         ***/
/***                                                           ***/
/***                                                           ***/
/*** Approach:                                                 ***/
/*** - Extract ALL relevant fields (comprehensive)             ***/
/*** - Use snake_case naming convention                        ***/
/*** - Include _raw column for full JSON access                ***/
/*** - Separate REST and OpenAPI sources                       ***/
/**                                                             **/
/*---------------------------------------------------------------*/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA BRONZE;

/*---------------------------------------------------------------*/
/*** REST API VIEWS (from RAW.ECONOMIC_RESTAPI_JSON & ECONOMIC_OPENAPI_JSON)          ***/
/*---------------------------------------------------------------*/

  -- 5 views:
  -- 1. CUSTOMERS - Customer master data (balance, currency, VAT zone)
  -- 2. PRODUCTS - Product catalog (SKU, pricing, groups, units)
  -- 3. INVOICES - Invoice headers (amounts, dates, customer)
  -- 4. INVOICE_LINES - Line items with products, quantities, pricing
  -- 5. INGESTION_METADATA - Showcases the data freshness and quality

/*---------------------------------------------------------------*/
/*** 1. CUSTOMERS - Customer Master Data                      ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW CUSTOMERS AS
WITH deduplicated AS (
  SELECT
    r.DATE_INSERTED                                    AS api_timestamp,
    -- Core customer info
    c.value:"customerNumber"::INT                      AS customer_number,
    c.value:"name"::STRING                             AS customer_name,
    c.value:"customerGroup"."customerGroupNumber"::INT AS customer_group_number,
    c.value:"currency"::STRING                         AS currency,
    c.value:"vatZone"."vatZoneNumber"::INT             AS vatzone_number,
    c.value:"balance"::NUMBER                          AS balance,
    c.value:"dueAmount"::NUMBER                        AS due_amount,

    -- Geographic information (for country-based analysis)
    c.value:"country"::STRING                          AS country,
    c.value:"address"::STRING                          AS address,
    c.value:"city"::STRING                             AS city,
    c.value:"zip"::STRING                              AS zip,

    -- Contact information
    c.value:"email"::STRING                            AS email,
    c.value:"mobilePhone"::STRING                      AS mobile_phone,
    c.value:"telephoneAndFaxNumber"::STRING            AS telephone,

    -- Sales person
    c.value:"salesPerson"."employeeNumber"::INT        AS salesperson_employee_number,

    -- Payment terms
    c.value:"paymentTerms"."paymentTermsNumber"::INT   AS payment_terms_number,

    -- Contact person
    c.value:"attention"."customerContactNumber"::INT   AS attention_contact_number,
    c.value:"customerContact"."customerContactNumber"::INT AS customer_contact_number,

    -- Timestamps
    c.value:"lastUpdated"::TIMESTAMP_LTZ               AS last_updated,

    -- Deduplication: keep only latest version per customer
    ROW_NUMBER() OVER (PARTITION BY c.value:"customerNumber"::INT ORDER BY r.DATE_INSERTED DESC) AS rn

  FROM RAW.ECONOMIC_RESTAPI_JSON r,
       LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") c
  WHERE r.API_ENDPOINT = 'customers'
)
SELECT
  api_timestamp,
  customer_number,
  customer_name,
  customer_group_number,
  currency,
  vatzone_number,
  balance,
  due_amount,
  country,
  address,
  city,
  zip,
  email,
  mobile_phone,
  telephone,
  salesperson_employee_number,
  payment_terms_number,
  attention_contact_number,
  customer_contact_number,
  last_updated
FROM deduplicated
WHERE rn = 1;

COMMENT ON VIEW CUSTOMERS IS 'Customer master data - includes country, contact info, and customer segmentation (B2B/B2C via customer_group_number)';





/*---------------------------------------------------------------*/
/*** 2. PRODUCTS - Product Catalog                            ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW PRODUCTS AS
WITH deduplicated AS (
  SELECT
    r.DATE_INSERTED                                AS api_timestamp,

    -- Core product info
    p.value:"productNumber"::STRING                AS sku,
    p.value:"name"::STRING                         AS product_name,
    p.value:"description"::STRING                  AS description,

    -- Pricing (for margin analysis)
    p.value:"salesPrice"::NUMBER                   AS sales_price,
    p.value:"recommendedPrice"::NUMBER             AS recommended_price,
    p.value:"costPrice"::NUMBER                    AS cost_price,

    -- Product attributes
    p.value:"barred"::BOOLEAN                      AS barred,

    -- Product group
    p.value:"productGroup"."productGroupNumber"::INT AS product_group_number,
    p.value:"productGroup"."name"::STRING          AS product_group_name,
    p.value:"productGroup"."inventoryEnabled"::BOOLEAN AS inventory_enabled,

    -- Unit
    p.value:"unit"."name"::STRING                  AS unit_name,
    p.value:"unit"."unitNumber"::INT               AS unit_number,

    -- Timestamps
    p.value:"lastUpdated"::TIMESTAMP_LTZ           AS last_updated,

    -- Deduplication: keep only latest version per product
    ROW_NUMBER() OVER (PARTITION BY p.value:"productNumber"::STRING ORDER BY r.DATE_INSERTED DESC) AS rn

  FROM RAW.ECONOMIC_RESTAPI_JSON r,
       LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") p
  WHERE r.API_ENDPOINT = 'products'
)
SELECT
  api_timestamp,
  sku,
  product_name,
  description,
  sales_price,
  recommended_price,
  cost_price,
  barred,
  product_group_number,
  product_group_name,
  inventory_enabled,
  unit_name,
  unit_number,
  last_updated
FROM deduplicated
WHERE rn = 1;

COMMENT ON VIEW PRODUCTS IS 'Product catalog - includes SKU, pricing, cost price for margin analysis, and product groups';





/*---------------------------------------------------------------*/
/*** 3. INVOICES - Invoice Headers (from REST API)            ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW INVOICES AS
WITH deduplicated AS (
  SELECT
    r.DATE_INSERTED                                       AS api_timestamp,

    -- Core invoice info
    i.value:"bookedInvoiceNumber"::INT                    AS invoice_number,
    i.value:"date"::DATE                                  AS invoice_date,
    i.value:"dueDate"::DATE                               AS due_date,

    -- Customer reference
    i.value:"customer"."customerNumber"::INT              AS customer_number,

    -- Currency and exchange
    i.value:"currency"::STRING                            AS invoice_currency,
    i.value:"exchangeRate"::FLOAT                         AS exchange_rate,

    -- Amounts (in base currency - DKK)
    i.value:"netAmount"::FLOAT                            AS net_amount,
    i.value:"netAmountInBaseCurrency"::FLOAT              AS net_amount_base_currency,
    i.value:"grossAmount"::FLOAT                          AS gross_amount,
    i.value:"grossAmountInBaseCurrency"::FLOAT            AS gross_amount_base_currency,
    i.value:"vatAmount"::FLOAT                            AS vat_amount,
    i.value:"roundingAmount"::FLOAT                       AS rounding_amount,

    -- Payment status
    i.value:"remainder"::FLOAT                            AS remainder,
    i.value:"remainderInBaseCurrency"::FLOAT              AS remainder_base_currency,

    -- Delivery location
    i.value:"delivery"."address"::STRING                  AS delivery_address,
    i.value:"delivery"."city"::STRING                     AS delivery_city,
    i.value:"delivery"."country"::STRING                  AS delivery_country,
    i.value:"delivery"."zip"::STRING                      AS delivery_zip,
    i.value:"deliveryLocation"."deliveryLocationNumber"::INT AS delivery_location_number,

    -- Recipient information (billing address)
    i.value:"recipient"."name"::STRING                    AS recipient_name,
    i.value:"recipient"."address"::STRING                 AS recipient_address,
    i.value:"recipient"."city"::STRING                    AS recipient_city,
    i.value:"recipient"."country"::STRING                 AS recipient_country,
    i.value:"recipient"."zip"::STRING                     AS recipient_zip,
    i.value:"recipient"."vatZone"."vatZoneNumber"::INT    AS recipient_vatzone_number,

    -- References
    i.value:"references"."salesPerson"."employeeNumber"::INT AS sales_person_employee_number,
    i.value:"references"."customerContact"."customerContactNumber"::INT AS customer_contact_number,

    -- Payment terms
    i.value:"paymentTerms"."paymentTermsNumber"::INT      AS payment_terms_number,
    i.value:"paymentTerms"."daysOfCredit"::INT            AS payment_days_of_credit,

    -- Layout
    i.value:"layout"."layoutNumber"::INT                  AS layout_number,

    -- Order reference
    i.value:"orderNumber"::INT                            AS order_number,

    -- Deduplication: keep only latest version per invoice (handles remainder updates)
    ROW_NUMBER() OVER (PARTITION BY i.value:"bookedInvoiceNumber"::INT ORDER BY r.DATE_INSERTED DESC) AS rn

  FROM RAW.ECONOMIC_RESTAPI_JSON r,
       LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") i
  WHERE r.API_ENDPOINT = 'invoices/booked'
)
SELECT
  api_timestamp,
  invoice_number,
  invoice_date,
  due_date,
  customer_number,
  invoice_currency,
  exchange_rate,
  net_amount,
  net_amount_base_currency,
  gross_amount,
  gross_amount_base_currency,
  vat_amount,
  rounding_amount,
  remainder,
  remainder_base_currency,
  delivery_address,
  delivery_city,
  delivery_country,
  delivery_zip,
  delivery_location_number,
  recipient_name,
  recipient_address,
  recipient_city,
  recipient_country,
  recipient_zip,
  recipient_vatzone_number,
  sales_person_employee_number,
  customer_contact_number,
  payment_terms_number,
  payment_days_of_credit,
  layout_number,
  order_number
FROM deduplicated
WHERE rn = 1;

COMMENT ON VIEW INVOICES IS 'Invoice headers - includes customer, amounts, currency, delivery location, and recipient info. Join with INVOICE_LINES on invoice_number.';




/*---------------------------------------------------------------*/
/*** 4. INVOICE_LINES - Invoice Line Items (from OpenAPI Bulk)***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW INVOICE_LINES AS
WITH invoice_exchange_rates AS (
  -- First get all invoice exchange rates (with deduplication)
  SELECT
    i.value:"bookedInvoiceNumber"::INT AS invoice_number,
    i.value:"exchangeRate"::FLOAT AS exchange_rate,
    i.value:"currency"::STRING AS invoice_currency,
    ROW_NUMBER() OVER (PARTITION BY i.value:"bookedInvoiceNumber"::INT ORDER BY ir.DATE_INSERTED DESC) AS rn
  FROM RAW.ECONOMIC_RESTAPI_JSON ir,
       LATERAL FLATTEN(input => ir.COLLECTION_JSON:"collection") i
  WHERE ir.API_ENDPOINT = 'invoices/booked'
),
invoice_exchange_rates_dedup AS (
  SELECT invoice_number, exchange_rate, invoice_currency
  FROM invoice_exchange_rates
  WHERE rn = 1
),
lines_raw AS (
  -- Then get all invoice lines
  SELECT
    r.DATE_INSERTED                           AS api_timestamp,
    line.value:"documentId"::INT              AS invoice_number,
    line.value:"number"::INT                  AS line_number,
    line.value:"userInterfaceNumber"::INT     AS line_number_ui,
    line.value:"productNumber"::STRING        AS sku,
    line.value:"description"::STRING          AS line_description,
    line.value:"quantity"::FLOAT              AS quantity,
    line.value:"unitNetPrice"::FLOAT          AS unit_net_price,
    line.value:"unitCostPrice"::FLOAT         AS unit_cost_price,
    line.value:"totalNetAmount"::FLOAT        AS line_net_amount,
    line.value:"discountPercentage"::FLOAT    AS discount_percentage,
    line.value:"vatRate"::FLOAT               AS vat_rate,
    line.value:"vatAmount"::FLOAT             AS vat_amount,
    -- Deduplication: keep only latest version per line
    ROW_NUMBER() OVER (PARTITION BY line.value:"documentId"::INT, line.value:"number"::INT ORDER BY r.DATE_INSERTED DESC) AS rn
  FROM RAW.ECONOMIC_OPENAPI_JSON r,
       LATERAL FLATTEN(input => r.COLLECTION_JSON:items) line
  WHERE r.API_ENDPOINT = 'invoices/booked/lines'
),
lines_dedup AS (
  SELECT
    api_timestamp,
    invoice_number,
    line_number,
    line_number_ui,
    sku,
    line_description,
    quantity,
    unit_net_price,
    unit_cost_price,
    line_net_amount,
    discount_percentage,
    vat_rate,
    vat_amount
  FROM lines_raw
  WHERE rn = 1
)
SELECT
  lr.api_timestamp,
  lr.invoice_number,
  lr.line_number,
  lr.line_number_ui,
  lr.sku,
  lr.line_description,
  lr.quantity,

  -- Currency context
  inv.invoice_currency,
  inv.exchange_rate,

  -- Amounts in original invoice currency
  lr.unit_net_price,
  lr.unit_cost_price,
  lr.line_net_amount,

  -- Convert to BASE CURRENCY (DKK) using exchange rate
  -- Exchange rate format: 746.469659 means 1 EUR = 7.46469659 DKK (divide by 100)
  ROUND(lr.unit_net_price * (COALESCE(inv.exchange_rate, 100) / 100), 2)     AS unit_net_price_base_currency,
  ROUND(lr.unit_cost_price * (COALESCE(inv.exchange_rate, 100) / 100), 2)    AS unit_cost_price_base_currency,
  ROUND(lr.line_net_amount * (COALESCE(inv.exchange_rate, 100) / 100), 2)    AS line_net_amount_base_currency,

  -- Discounts and VAT (for sales analysis)
  lr.discount_percentage,
  lr.vat_rate,
  lr.vat_amount

FROM lines_dedup lr
LEFT JOIN invoice_exchange_rates_dedup inv ON lr.invoice_number = inv.invoice_number;

COMMENT ON VIEW INVOICE_LINES IS 'Invoice line items from bulk endpoint - includes SKU, quantities, pricing, costs, and VAT. Amounts converted to base currency (DKK) using exchange rate from INVOICES table. Join with INVOICES on invoice_number and PRODUCTS/PrestaShop on sku.';








/*---------------------------------------------------------------*/
/*** 5. INGESTION_METADATA - API Ingestion Monitoring         ***/
/*---------------------------------------------------------------*/

CREATE OR REPLACE VIEW INGESTION_METADATA AS
SELECT
  API_ENDPOINT,
  MIN(DATE_INSERTED) AS first_ingestion,
  MAX(DATE_INSERTED) AS last_ingestion,
  COUNT(*) AS page_count,
  SUM(RECORD_COUNT_PER_PAGE) AS total_records,
  MAX(PAGE_NUMBER) AS max_page_number
FROM (
  SELECT API_ENDPOINT, DATE_INSERTED, PAGE_NUMBER, RECORD_COUNT_PER_PAGE
  FROM RAW.ECONOMIC_RESTAPI_JSON
  UNION ALL
  SELECT API_ENDPOINT, DATE_INSERTED, PAGE_NUMBER, RECORD_COUNT_PER_PAGE
  FROM RAW.ECONOMIC_OPENAPI_JSON
  UNION ALL
  SELECT API_ENDPOINT, DATE_INSERTED, PAGE_NUMBER, RECORD_COUNT_PER_PAGE
  FROM RAW.PRESTA_RESTAPI_JSON
) combined
GROUP BY API_ENDPOINT
ORDER BY last_ingestion DESC;

COMMENT ON VIEW INGESTION_METADATA IS 'Monitoring view - tracks ingestion stats across all API sources (Economic REST, Economic OpenAPI, PrestaShop)';

/*---------------------------------------------------------------*/
/*** GRANT PERMISSIONS                                         ***/
/*---------------------------------------------------------------*/

-- ECONOMIC_ADMIN: Full access to all Bronze views
GRANT SELECT ON VIEW CUSTOMERS TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW PRODUCTS TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW INVOICES TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW INVOICE_LINES TO ROLE ECONOMIC_ADMIN;
GRANT SELECT ON VIEW INGESTION_METADATA TO ROLE ECONOMIC_ADMIN;

-- ECONOMIC_WRITE: Read access for data engineers
GRANT SELECT ON VIEW CUSTOMERS TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW PRODUCTS TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW INVOICES TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW INVOICE_LINES TO ROLE ECONOMIC_WRITE;
GRANT SELECT ON VIEW INGESTION_METADATA TO ROLE ECONOMIC_WRITE;

-- ECONOMIC_READ: Read access for analysts and BI tools
GRANT SELECT ON VIEW CUSTOMERS TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW PRODUCTS TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW INVOICES TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW INVOICE_LINES TO ROLE ECONOMIC_READ;
GRANT SELECT ON VIEW INGESTION_METADATA TO ROLE ECONOMIC_READ;

/*---------------------------------------------------------------*/
/*** VERIFICATION QUERIES                                      ***/
/*---------------------------------------------------------------*/

-- Show all Bronze views created
-- SHOW VIEWS IN SCHEMA ECONOMIC.BRONZE;

-- Test each view
-- SELECT * FROM BRONZE.CUSTOMERS LIMIT 10;
-- SELECT * FROM BRONZE.PRODUCTS LIMIT 10;
-- SELECT * FROM BRONZE.INVOICES LIMIT 10;
-- SELECT * FROM BRONZE.INVOICE_LINES LIMIT 10;

-- Check data freshness
-- SELECT * FROM BRONZE.INGESTION_METADATA ORDER BY last_ingestion DESC;

-- Test join between INVOICES and INVOICE_LINES
-- SELECT
--   i.invoice_number,
--   i.invoice_date,
--   i.customer_number,
--   i.delivery_country,
--   COUNT(*) AS line_count,
--   SUM(il.line_net_amount_base_currency) AS total_amount
-- FROM BRONZE.INVOICES i
-- JOIN BRONZE.INVOICE_LINES il ON i.invoice_number = il.invoice_number
-- GROUP BY i.invoice_number, i.invoice_date, i.customer_number, i.delivery_country
-- ORDER BY i.invoice_date DESC
-- LIMIT 10;

/*---------------------------------------------------------------*/
/*** END OF FILE 08 - Economic Bronze Views                   ***/
/*---------------------------------------------------------------*/
