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

USE ROLE PLAYGROUND_ADMIN;
USE DATABASE PLAYGROUND;
USE SCHEMA BRONZE;

/*---------------------------------------------------------------*/
/*** REST API VIEWS (from RAW.ECONOMIC_RESTAPI_JSON)          ***/
/*---------------------------------------------------------------*/

  -- 8 views:
  -- 1. CUSTOMERS - Customer master data (balance, currency, VAT zone)
  -- 2. PRODUCTS - Product catalog (SKU, pricing, groups, units)
  -- 3. INVOICES - Invoice headers (amounts, dates, customer)
  -- 4. INVOICE_LINES - Line items with products, quantities, pricing
  -- 5. ACCOUNTING_YEARS - Year definitions (dates, open/closed status)
  -- 6. ACCOUNTING_ENTRIES - Journal entries (amounts, accounts, vouchers)
  -- 7. ACCOUNTING_PERIODS - Period definitions (dates, status)
  -- 8. ACCOUNTING_TOTALS - Account balances (debit/credit/balance)

-- ============================================================
-- CUSTOMERS (ENHANCED)
-- ============================================================
CREATE OR REPLACE VIEW CUSTOMERS AS
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
  -- Geographic information (CRITICAL for country-based analysis)
  c.value:"country"::STRING                          AS country,
  c.value:"address"::STRING                          AS address,
  c.value:"city"::STRING                             AS city,
  c.value:"zip"::STRING                              AS zip,
  -- Contact information
  c.value:"email"::STRING                            AS email,
  c.value:"mobilePhone"::STRING                      AS mobile_phone,
  c.value:"telephoneAndFaxNumber"::STRING            AS telephone,
  -- Sales person (may help identify B2B vs B2C)
  c.value:"salesPerson"."employeeNumber"::INT        AS salesperson_employee_number,
  -- Payment terms
  c.value:"paymentTerms"."paymentTermsNumber"::INT   AS payment_terms_number,
  -- Contact person
  c.value:"attention"."customerContactNumber"::INT   AS attention_contact_number,
  c.value:"customerContact"."customerContactNumber"::INT AS customer_contact_number,
  -- Timestamps
  c.value:"lastUpdated"::TIMESTAMP_LTZ               AS last_updated,
  c.value                                             AS _raw
FROM RAW.ECONOMIC_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") c
WHERE r.API_ENDPOINT = 'customers';

COMMENT ON VIEW CUSTOMERS IS 'Customer master data - includes country, contact info, and sales person for B2B/B2C classification';


-- ============================================================
-- PRODUCTS (ENHANCED)
-- ============================================================
CREATE OR REPLACE VIEW PRODUCTS AS
SELECT
  r.DATE_INSERTED                                AS api_timestamp,
  -- Core product info
  p.value:"productNumber"::STRING                AS sku,
  p.value:"name"::STRING                         AS product_name,
  p.value:"description"::STRING                  AS description,
  -- Pricing (CRITICAL for margin analysis)
  p.value:"salesPrice"::NUMBER                   AS sales_price,
  p.value:"recommendedPrice"::NUMBER             AS recommended_price,
  p.value:"costPrice"::NUMBER                    AS cost_price,  -- NEW: For margin calculation
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
  p.value                                         AS _raw
FROM RAW.ECONOMIC_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") p
WHERE r.API_ENDPOINT = 'products';

COMMENT ON VIEW PRODUCTS IS 'Product catalog - includes cost price for margin analysis';


-- ============================================================
-- INVOICES (ENHANCED - Headers)
-- ============================================================
CREATE OR REPLACE VIEW INVOICES AS
SELECT
  r.DATE_INSERTED                                AS api_timestamp,
  -- Core invoice info
  h.value:"bookedInvoiceNumber"::INT             AS invoice_number,
  h.value:"date"::DATE                           AS invoice_date,
  h.value:"dueDate"::DATE                        AS due_date,
  h.value:"currency"::STRING                     AS currency,
  h.value:"exchangeRate"::NUMBER                 AS exchange_rate,
  -- Customer reference
  h.value:"customer"."customerNumber"::INT       AS customer_number,
  -- Amounts
  h.value:"grossAmount"::NUMBER                  AS gross_amount,
  h.value:"netAmount"::NUMBER                    AS net_amount,
  h.value:"vatAmount"::NUMBER                    AS vat_amount,
  h.value:"roundingAmount"::NUMBER               AS rounding_amount,
  h.value:"grossAmountInBaseCurrency"::NUMBER    AS gross_amount_base_currency,
  h.value:"netAmountInBaseCurrency"::NUMBER      AS net_amount_base_currency,
  -- Delivery information (CRITICAL for country-based sales)
  h.value:"delivery"."country"::STRING           AS delivery_country,      -- KEY FIELD!
  h.value:"delivery"."address"::STRING           AS delivery_address,
  h.value:"delivery"."city"::STRING              AS delivery_city,
  h.value:"delivery"."zip"::STRING               AS delivery_zip,
  h.value:"deliveryLocation"."deliveryLocationNumber"::INT AS delivery_location_number,
  -- Recipient information (CRITICAL for country-based sales)
  h.value:"recipient"."country"::STRING          AS recipient_country,     -- KEY FIELD!
  h.value:"recipient"."name"::STRING             AS recipient_name,
  h.value:"recipient"."address"::STRING          AS recipient_address,
  h.value:"recipient"."city"::STRING             AS recipient_city,
  h.value:"recipient"."zip"::STRING              AS recipient_zip,
  h.value:"recipient"."vatZone"."vatZoneNumber"::INT AS recipient_vatzone_number,
  h.value:"recipient"."attention"."customerContactNumber"::INT AS recipient_contact_number,
  -- Sales person (for B2B/B2C classification)
  h.value:"references"."salesPerson"."employeeNumber"::INT AS salesperson_employee_number,
  h.value:"references"."customerContact"."customerContactNumber"::INT AS reference_contact_number,
  -- Payment
  h.value:"paymentTerms"."paymentTermsNumber"::INT AS payment_terms_number,
  h.value:"paymentTerms"."daysOfCredit"::INT     AS payment_days_of_credit,
  h.value:"paymentTerms"."name"::STRING          AS payment_terms_name,
  h.value:"remainder"::NUMBER                    AS remainder,
  h.value:"remainderInBaseCurrency"::NUMBER      AS remainder_base_currency,
  -- Order reference
  h.value:"orderNumber"::INT                     AS order_number,          -- NEW: Order reference
  -- Layout
  h.value:"layout"."layoutNumber"::INT           AS layout_number,
  h.value                                         AS _raw_header
FROM RAW.ECONOMIC_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") h
WHERE r.API_ENDPOINT = 'invoices/booked';

COMMENT ON VIEW INVOICES IS 'Booked invoice headers - includes delivery_country and recipient_country for geographic sales analysis';


-- ============================================================
-- INVOICE_LINES (ENHANCED WITH BASE CURRENCY)
-- ============================================================
-- NOTE: This view uses data from individual invoice detail calls
-- Run UTIL.ECONOMIC_INVOICE_DETAILS_INGEST() after the main ingestion
-- to populate invoice line data
--
-- CURRENCY HANDLING:
-- - Economic API provides line amounts in invoice currency (DKK, EUR, SEK, etc.)
-- - Base currency amounts are calculated using exchangeRate from invoice header
-- - Exchange rate is stored as basis points (divide by 100)
-- - Example: exchangeRate=100 means 1:1 (DKK), exchangeRate=745 means 7.45 DKK per 1 EUR
CREATE OR REPLACE VIEW INVOICE_LINES AS
WITH hdr AS (
  SELECT
    r.DATE_INSERTED,
    h.value AS header
  FROM RAW.ECONOMIC_RESTAPI_JSON r,
       LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") h
  WHERE r.API_ENDPOINT = 'invoices/booked/detail'  -- ← Changed from 'invoices/booked'
)
SELECT
  hdr.DATE_INSERTED                                   AS api_timestamp,
  -- Header reference
  header:"bookedInvoiceNumber"::INT                   AS invoice_number,
  header:"date"::DATE                                 AS invoice_date,
  header:"customer"."customerNumber"::INT             AS customer_number,
  -- Currency information (CRITICAL for multi-currency analysis)
  header:"currency"::STRING                           AS invoice_currency,
  header:"exchangeRate"::NUMBER                       AS exchange_rate,
  -- Geographic info from header (for joining)
  header:"delivery"."country"::STRING                 AS delivery_country,
  header:"recipient"."country"::STRING                AS recipient_country,
  -- Line details
  l.value:"lineNumber"::INT                           AS line_number,
  l.value:"product"."productNumber"::STRING           AS sku,
  l.value:"description"::STRING                       AS line_description,
  -- Quantities
  l.value:"quantity"::NUMBER                          AS quantity,

  -- === PRICING IN INVOICE CURRENCY (original) ===
  l.value:"unitNetPrice"::NUMBER                      AS unit_net_price,
  l.value:"unitCostPrice"::NUMBER                     AS unit_cost_price,
  l.value:"totalNetAmount"::NUMBER                    AS line_net_amount,

  -- === PRICING IN BASE CURRENCY (DKK) - CALCULATED ===
  -- Convert using exchange rate: amount * (exchangeRate / 100)
  -- Rounded to 2 decimals for currency precision
  ROUND(
    l.value:"unitNetPrice"::NUMBER *
    (header:"exchangeRate"::NUMBER / 100), 2
  )                                                   AS unit_net_price_base_currency,

  ROUND(
    l.value:"unitCostPrice"::NUMBER *
    (header:"exchangeRate"::NUMBER / 100), 2
  )                                                   AS unit_cost_price_base_currency,

  ROUND(
    l.value:"totalNetAmount"::NUMBER *
    (header:"exchangeRate"::NUMBER / 100), 2
  )                                                   AS line_net_amount_base_currency,

  -- Margin (Economic provides this in base currency already)
  l.value:"marginInBaseCurrency"::NUMBER              AS margin_base_currency,
  l.value:"marginPercentage"::NUMBER                  AS margin_percentage,

  -- Unit
  l.value:"unit"."name"::STRING                       AS unit_name,
  l.value:"unit"."unitNumber"::INT                    AS unit_number,
  -- Sorting
  l.value:"sortKey"::INT                              AS sort_key,
  l.value                                              AS _raw_line
FROM hdr,
     LATERAL FLATTEN(input => header:"lines") l;

COMMENT ON VIEW INVOICE_LINES IS 'Invoice line items from individual invoice detail calls - includes currency conversion to base currency (DKK) for accurate multi-currency analysis. Run ECONOMIC_INVOICE_DETAILS_INGEST() to populate.';


-- ============================================================
-- ACCOUNTING_YEARS
-- ============================================================
CREATE OR REPLACE VIEW ACCOUNTING_YEARS AS
SELECT
  r.DATE_INSERTED                AS api_timestamp,
  y.value:"year"::STRING         AS year,
  y.value:"fromDate"::DATE       AS from_date,
  y.value:"toDate"::DATE         AS to_date,
  y.value:"closed"::BOOLEAN      AS closed,
  y.value                         AS _raw
FROM RAW.ECONOMIC_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") y
WHERE r.API_ENDPOINT = 'accounting-years';

COMMENT ON VIEW ACCOUNTING_YEARS IS 'Accounting year definitions from Economic REST API';


-- ============================================================
-- ACCOUNTING_ENTRIES
-- ============================================================
CREATE OR REPLACE VIEW ACCOUNTING_ENTRIES AS
SELECT
  r.DATE_INSERTED                                 AS api_timestamp,
  e.value:"entryNumber"::INT                      AS entry_number,
  e.value:"date"::DATE                            AS entry_date,
  e.value:"entryType"::STRING                     AS entry_type,
  e.value:"currency"::STRING                      AS currency,
  e.value:"amount"::NUMBER                        AS amount,
  e.value:"amountInBaseCurrency"::NUMBER          AS amount_base_currency,
  e.value:"voucherNumber"::INT                    AS voucher_number,
  e.value:"account"."accountNumber"::INT          AS account_number,
  e.value:"departmentalDistribution"."departmentNumber"::INT AS department_number,
  e.value:"text"::STRING                          AS entry_text,
  e.value                                          AS _raw
FROM RAW.ECONOMIC_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") e
WHERE r.API_ENDPOINT LIKE 'accounting-years/%/entries';

COMMENT ON VIEW ACCOUNTING_ENTRIES IS 'Accounting entries (journal entries) from Economic REST API';


-- ============================================================
-- ACCOUNTING_PERIODS
-- ============================================================
CREATE OR REPLACE VIEW ACCOUNTING_PERIODS AS
SELECT
  r.DATE_INSERTED                    AS api_timestamp,
  p.value:"periodNumber"::INT        AS period_number,
  p.value:"name"::STRING             AS period_name,
  p.value:"fromDate"::DATE           AS from_date,
  p.value:"toDate"::DATE             AS to_date,
  p.value:"closed"::BOOLEAN          AS closed,
  p.value                             AS _raw
FROM RAW.ECONOMIC_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") p
WHERE r.API_ENDPOINT LIKE 'accounting-years/%/periods';

COMMENT ON VIEW ACCOUNTING_PERIODS IS 'Accounting periods from Economic REST API';


-- ============================================================
-- ACCOUNTING_TOTALS
-- ============================================================
CREATE OR REPLACE VIEW ACCOUNTING_TOTALS AS
SELECT
  r.DATE_INSERTED                       AS api_timestamp,
  t.value:"account"."accountNumber"::INT AS account_number,
  t.value:"totalDebit"::NUMBER          AS total_debit,
  t.value:"totalCredit"::NUMBER         AS total_credit,
  t.value:"balance"::NUMBER             AS balance,
  t.value                                AS _raw
FROM RAW.ECONOMIC_RESTAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"collection") t
WHERE r.API_ENDPOINT LIKE 'accounting-years/%/totals';

COMMENT ON VIEW ACCOUNTING_TOTALS IS 'Account totals by year from Economic REST API';


/*---------------------------------------------------------------*/
/*** OPENAPI VIEWS (from RAW.ECONOMIC_OPENAPI_JSON)           ***/
/*---------------------------------------------------------------*/


  -- 2 views:
  -- 9. ✅ JOURNAL_ENTRIES - Journal entries from OpenAPI (uses items)
  -- 10. ✅ CUSTOMER_CONTACTS - Customer contact details (email, phone)

-- ============================================================
-- JOURNAL_ENTRIES (OpenAPI)
-- ============================================================
CREATE OR REPLACE VIEW JOURNAL_ENTRIES AS
SELECT
  r.DATE_INSERTED                                   AS api_timestamp,
  j.value:"journalEntryNumber"::INT                 AS journal_entry_number,
  j.value:"entryDate"::DATE                         AS entry_date,
  j.value:"entryType"::STRING                       AS entry_type,
  j.value:"voucherNumber"::INT                      AS voucher_number,
  j.value:"text"::STRING                            AS entry_text,
  j.value:"accountNumber"::INT                      AS account_number,
  j.value:"amount"::NUMBER                          AS amount,
  j.value:"amountBaseCurrency"::NUMBER              AS amount_base_currency,
  j.value:"currency"::STRING                        AS currency,
  j.value:"departmentNumber"::INT                   AS department_number,
  j.value                                            AS _raw
FROM RAW.ECONOMIC_OPENAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"items") j
WHERE r.API_ENDPOINT LIKE 'journalsapi/%/entries/booked';

COMMENT ON VIEW JOURNAL_ENTRIES IS 'Journal entries from Economic OpenAPI';


-- ============================================================
-- CUSTOMER_CONTACTS (OpenAPI)
-- ============================================================
CREATE OR REPLACE VIEW CUSTOMER_CONTACTS AS
SELECT
  r.DATE_INSERTED                           AS api_timestamp,
  c.value:"contactId"::INT                  AS contact_id,
  c.value:"customerNumber"::INT             AS customer_number,
  c.value:"name"::STRING                    AS contact_name,
  c.value:"email"::STRING                   AS email,
  c.value:"phone"::STRING                   AS phone,
  c.value:"isPrimary"::BOOLEAN              AS is_primary,
  c.value                                    AS _raw
FROM RAW.ECONOMIC_OPENAPI_JSON r,
     LATERAL FLATTEN(input => r.COLLECTION_JSON:"items") c
WHERE r.API_ENDPOINT LIKE 'customersapi/%/Contacts/paged';

COMMENT ON VIEW CUSTOMER_CONTACTS IS 'Customer contacts from Economic OpenAPI';


/*---------------------------------------------------------------*/
/*** METADATA & MONITORING VIEWS                              ***/
/*---------------------------------------------------------------*/

-- ============================================================
-- INGESTION_METADATA
-- ============================================================

  -- Monitoring (1 view):
 -- 11. ✅ INGESTION_METADATA - Tracks data freshness for all endpoints
CREATE OR REPLACE VIEW INGESTION_METADATA AS
SELECT
  'REST' AS api_type,
  API_ENDPOINT,
  MAX(DATE_INSERTED) as last_load_timestamp,
  COUNT(*) as page_count,
  SUM(RECORD_COUNT_PER_PAGE) as total_records_ingested
FROM RAW.ECONOMIC_RESTAPI_JSON
GROUP BY API_ENDPOINT

UNION ALL

SELECT
  'OPENAPI' AS api_type,
  API_ENDPOINT,
  MAX(DATE_INSERTED) as last_load_timestamp,
  COUNT(*) as page_count,
  SUM(RECORD_COUNT_PER_PAGE) as total_records_ingested
FROM RAW.ECONOMIC_OPENAPI_JSON
GROUP BY API_ENDPOINT

ORDER BY api_type, API_ENDPOINT;

COMMENT ON VIEW INGESTION_METADATA IS 'Tracks data freshness and volume across all Economic API endpoints';


/*---------------------------------------------------------------*/
/*** VERIFICATION QUERIES                                      ***/
/*---------------------------------------------------------------*/

-- View all Bronze views
-- SHOW VIEWS IN SCHEMA BRONZE;

-- Test key views for business questions
-- SELECT * FROM BRONZE.CUSTOMERS WHERE country IN ('France', 'Netherlands') LIMIT 10;
-- SELECT * FROM BRONZE.INVOICES WHERE delivery_country IN ('France', 'Netherlands') LIMIT 10;
-- SELECT * FROM BRONZE.INVOICE_LINES WHERE sku BETWEEN '1' AND '10' LIMIT 10;

-- Test country distribution
-- SELECT delivery_country, COUNT(*) as invoice_count
-- FROM BRONZE.INVOICES
-- GROUP BY delivery_country;

-- Test SKU sales by customer
-- SELECT customer_number, sku, SUM(quantity) as total_quantity
-- FROM BRONZE.INVOICE_LINES
-- GROUP BY customer_number, sku
-- ORDER BY total_quantity DESC;

-- Check data freshness
-- SELECT * FROM BRONZE.INGESTION_METADATA;
