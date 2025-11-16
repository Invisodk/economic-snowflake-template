# Economic + PrestaShop → Snowflake Integration

Production-ready data pipeline integrating e-conomic ERP and PrestaShop e-commerce into Snowflake.

## What This Does

- **Ingests data** from e-conomic (invoices, customers, products, accounting) and PrestaShop (product catalog, variants)
- **Transforms data** through medallion architecture (RAW → BRONZE → SILVER)
- **Enriches sales** with product details (category, size, color) from PrestaShop
- **Segments geography** into 7 regional markets (32+ countries supported)
- **Handles expired SKUs** with intelligent fallback logic
- **Automates daily** refreshes via scheduled tasks
- **Converts currencies** to base currency (DKK) with exchange rates

## Quick Start

### 1. Deploy to Snowflake

Open `00_git_setup_and_deploy.sql` in Snowflake, update these lines:
```sql
SET GITHUB_ORG = 'Invisodk';                    -- Your GitHub org
SET REPO_NAME = 'economic-snowflake-template';  -- Your repo name
```

Update lines 61-62 with your GitHub PAT:
```sql
USERNAME = 'your-github-username'
PASSWORD = 'your-pat-token'
```

Run the entire script. Deployment takes ~3 minutes.

### 2. Configure API Credentials

```sql
USE ROLE ACCOUNTADMIN;

-- e-conomic API
ALTER SECRET ECONOMIC_XAPIKEY_APPSECRET SET SECRET_STRING = 'your_appsecret';
ALTER SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT SET SECRET_STRING = 'your_agreementgrant';

-- PrestaShop API
ALTER SECRET PRESTA_API_KEY SET SECRET_STRING = 'your_ws_key';
ALTER SECRET PRESTA_DOMAIN SET SECRET_STRING = 'yourstore.com';
ALTER NETWORK RULE PRESTA_APIS_NETWORK_RULE SET VALUE_LIST = ('yourstore.com');
```

### 3. Run First Data Load

```sql
USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;

CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST_MONTHLY();
CALL UTIL.PRESTA_RESTAPI_DATAINGEST();
```

### 4. Query Your Data

```sql
-- Sales by region with product enrichment
SELECT
    market,
    product_category,
    COUNT(DISTINCT invoice_id) AS orders,
    SUM(line_revenue) AS revenue,
    AVG(profit_margin_percent) AS avg_margin
FROM SILVER.VW_SALES_DETAIL
WHERE sale_date >= '2024-01-01'
GROUP BY market, product_category
ORDER BY revenue DESC;
```

## File Structure

```
00_git_setup_and_deploy.sql          # Start here
00_execute_all_files_in_repo.sql     # Master script (auto-executed)

01_network_secrets_setup.sql         # e-conomic API setup
01b_network_secrets_setup_presta.sql # PrestaShop API setup
02_database_schema_roles_setup.sql   # Database & roles
03_config_tables_economic.sql        # e-conomic endpoints
03b_config_tables_presta.sql         # PrestaShop endpoints
04_udf_economic_api_retriever.sql    # e-conomic API function
04b_udf_presta_api_retriever.sql     # PrestaShop API function
05_raw_tables.sql                    # Landing tables
06_usp_rest_ingestion.sql            # e-conomic REST ingestion
06b_presta_rest_ingestion.sql        # PrestaShop ingestion
07_usp_openapi_ingestion.sql         # e-conomic OpenAPI ingestion
08_bronze_economic.sql               # e-conomic views (8 views)
08b_bronze_prestashop.sql            # PrestaShop views (3 views)
08c_expired_sku_logic.sql            # SKU matching & fallback
09_silver_views.sql                  # Analytics view (VW_SALES_DETAIL)
10_task_scheduling.sql               # Daily automation
cortex_setup.sql                     # Optional: AI semantic model
```

## Key Features

- **32 countries** mapped to 7 regional markets (Nordics, Western Europe, Southern Europe, etc.)
- **Currency conversion** built into INVOICE_LINES (converts all to DKK base currency)
- **Product enrichment** via SKU matching between e-conomic and PrestaShop
- **Expired SKU handling** for products no longer in catalog
- **RBAC** with 3 roles: ECONOMIC_ADMIN, ECONOMIC_WRITE, ECONOMIC_READ

## Troubleshooting

**API returns 401**: Check your secrets are set correctly
**No data in views**: Run ingestion procedures manually first
**PrestaShop connection fails**: Verify Web Services are enabled in PrestaShop admin and network rule has correct domain

## Maintenance

Enable automated daily refresh:
```sql
ALTER TASK ECONOMIC_DAILY_REFRESH RESUME;
ALTER TASK PRESTA_DAILY_REFRESH RESUME;
```

Monitor task execution:
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE '%ECONOMIC%' OR NAME LIKE '%PRESTA%'
ORDER BY SCHEDULED_TIME DESC LIMIT 10;
```
