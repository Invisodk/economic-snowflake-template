# Economic API to Snowflake Integration Template

**Production-ready template for ingesting Economic API data into Snowflake using medallion architecture (RAW → BRONZE → SILVER).**

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Deployment](#deployment)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Model](#data-model)
- [Troubleshooting](#troubleshooting)
- [Maintenance](#maintenance)
- [Contributing](#contributing)

---

## 🎯 Overview

This template provides a complete, enterprise-grade solution for integrating Economic (e-conomic.com) API data into Snowflake. It handles:

- **Data Ingestion**: Automated API calls with pagination
- **Data Storage**: Medallion architecture (RAW → BRONZE → SILVER)
- **Data Transformation**: Field extraction and business logic
- **Automation**: Scheduled daily refreshes
- **Security**: Role-based access control (RBAC)
- **Deployment**: Git-integrated single-command setup

**Time to deploy**: ~15 minutes
**Primary use case**: Sales analytics, customer insights, financial reporting

---

## ✨ Features

### Core Capabilities
- ✅ **Dual API Support**: REST and OpenAPI endpoints
- ✅ **Medallion Architecture**: RAW → BRONZE → SILVER layers
- ✅ **Git Integration**: Deploy from GitHub repository
- ✅ **Auto-Pagination**: Handles large datasets automatically
- ✅ **Demo Mode**: Test without production credentials
- ✅ **Error Handling**: Comprehensive error reporting
- ✅ **RBAC**: Three-tier role hierarchy (ADMIN/WRITE/READ)
- ✅ **Scheduling**: Automated daily refresh
- ✅ **Business Views**: Pre-built analytics views

### Data Coverage
- **Customers** (21 fields) - Master data with geography
- **Products** (17 fields) - Catalog with pricing
- **Invoices** (21 fields) - Headers with delivery info
- **Invoice Lines** (18 fields) - Line items with margins
- **Accounting** (4 views) - Years, entries, periods, totals

### Business Questions Answered
1. **B2C sales to France** - `WHERE customer_segment = 'B2C' AND delivery_country = 'France'`
2. **Customer purchases of specific SKU** - `WHERE customer_id = 123 AND product_sku = 'XX-XX'`
3. **Profit margins by product** - Pre-calculated in `VW_SALES_DETAIL`
4. **Geographic sales analysis** - Delivery and recipient countries tracked

---

## 🏗️ Architecture

### File Structure

```
economic-snowflake-template/
├── 00_git_setup_and_deploy.sql          # Git integration setup (run first)
├── 00_execute_all_files_in_repo.sql     # Master deployment script
├── 01_network_secrets_setup.sql         # API access configuration
├── 02_database_schema_roles_setup.sql   # Database & RBAC
├── 03_config_tables.sql                 # Endpoint configuration
├── 04_udf_economic_api_v3.sql           # API caller function
├── 05_raw_tables.sql                    # Landing tables
├── 06_usp_rest_ingestion.sql            # REST ingestion procedure
├── 07_usp_openapi_ingestion.sql         # OpenAPI ingestion procedure
├── 08_bronze_views.sql                  # Field extraction (8 views)
├── 09_silver_views.sql                  # Business analytics (3 views)
├── 10_task_scheduling.sql               # Automated refresh
├── README.md                            # This file
└── samples/                             # Sample JSON responses
    ├── CUSTOMERS.json
    ├── INVOICES.json
    ├── INVOICE_LINES.json
    └── PRODUCTS.json
```

### Database Structure

```
ECONOMIC (database)
  ├── CONFIG (schema)
  │   └── ECONOMIC_ENDPOINTS         # Configuration table
  │
  ├── UTIL (schema)
  │   ├── ECONOMIC_API_V3           # API UDF
  │   ├── ECONOMIC_RESTAPI_DATAINGEST_MONTHLY  # REST procedure
  │   └── ECONOMIC_OPENAPI_DATAINGEST_MONTHLY  # OpenAPI procedure
  │
  ├── RAW (schema)
  │   ├── ECONOMIC_RESTAPI_JSON     # REST landing table
  │   └── ECONOMIC_OPENAPI_JSON     # OpenAPI landing table
  │
  ├── BRONZE (schema)
  │   ├── CUSTOMERS                 # Field extraction views
  │   ├── PRODUCTS
  │   ├── INVOICES
  │   ├── INVOICE_LINES
  │   ├── ACCOUNTING_YEARS
  │   ├── ACCOUNTING_ENTRIES
  │   ├── ACCOUNTING_PERIODS
  │   └── ACCOUNTING_TOTALS
  │
  └── SILVER (schema)
      ├── VW_SALES_DETAIL          # Sales with profit margins
      ├── VW_FINANCIAL_DETAIL      # Accounting entries
      └── VW_CUSTOMER_MASTER       # Customer dimension
```

### Role Hierarchy

```
ECONOMIC_ADMIN (Full control)
    │
    ├─ Create/modify all objects
    ├─ Grant roles to users
    └─ Access to secrets
    │
    └── ECONOMIC_WRITE (Data engineering)
        │
        ├─ Run ingestion procedures
        ├─ Call API functions
        ├─ Write to RAW tables
        └─ Read from all schemas
        │
        └── ECONOMIC_READ (Analytics/BI)
            │
            ├─ Query BRONZE views
            ├─ Query SILVER views
            └─ Create reports/dashboards
```

---

## 📋 Prerequisites

### Snowflake Requirements
- Snowflake account (any edition)
- `ACCOUNTADMIN` role access (for deployment)
- Warehouse available (e.g., `COMPUTE_WH`)
- Snowflake version: Any recent version

### Economic API Requirements
- Economic account (e-conomic.com)
- API credentials:
  - **X-AppSecretToken**
  - **X-AgreementGrantToken**
- Get these from: Settings → API in Economic dashboard

### GitHub Requirements (for Git deployment)
- GitHub account
- Personal Access Token (PAT) with `repo` scope
- Repository to host the template (can be private)

### Local Requirements (for testing)
- Git installed
- Text editor (VS Code, Sublime, etc.)
- Snowflake CLI or web interface access

---

## 🚀 Quick Start

### 1. Clone or Download Template

```bash
git clone https://github.com/your-org/economic-snowflake-template.git
cd economic-snowflake-template
```

### 2. Push to Your GitHub Repository

```bash
# Create new repo on GitHub: your-org/economic-client-name
git remote add origin https://github.com/your-org/economic-client-name.git
git push -u origin main
```

### 3. Deploy to Snowflake

#### Option A: Git-Based Deployment (Recommended)

1. Open Snowflake web interface
2. Create new worksheet
3. Copy contents of `00_git_setup_and_deploy.sql`
4. Update variables:
   ```sql
   SET GITHUB_USERNAME = 'your-username';
   SET GITHUB_PAT_TOKEN = 'github_pat_XXXXXXXXXXXXX';
   SET GITHUB_ORG = 'your-org';
   SET REPO_NAME = 'economic-client-name';
   ```
5. Run the entire script
6. Wait ~2-3 minutes for deployment

#### Option B: Manual Deployment

1. Run each file sequentially (01, 02, 03, ... 10)
2. Wait for each to complete before running next
3. Check for errors after each file

### 4. Configure API Secrets

```sql
USE ROLE ACCOUNTADMIN;

-- Update with actual Economic API credentials
ALTER SECRET ECONOMIC_XAPIKEY_APPSECRET
  SET SECRET_STRING = 'your_actual_appsecret_here';

ALTER SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT
  SET SECRET_STRING = 'your_actual_agreementgrant_here';
```

### 5. Test API Connection

```sql
USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;

-- Test with small dataset
SELECT UTIL.ECONOMIC_API_V3('customers', 'REST', 10, 0);
```

### 6. Run First Ingestion

```sql
CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST_MONTHLY();
```

### 7. Verify Data

```sql
-- Check raw data loaded
SELECT COUNT(*) FROM RAW.ECONOMIC_RESTAPI_JSON;

-- Check Bronze views
SELECT * FROM BRONZE.CUSTOMERS LIMIT 10;

-- Check Silver views
SELECT * FROM SILVER.VW_SALES_DETAIL LIMIT 10;
```

### 8. Resume Automated Task (Optional)

```sql
ALTER TASK ECONOMIC_DAILY_REFRESH RESUME;
```

---

## 🔧 Configuration

### Activating Additional Endpoints

By default, only core endpoints are active:
- `customers`
- `products`
- `invoices/booked`

To activate more endpoints:

```sql
-- Activate employees endpoint
UPDATE CONFIG.ECONOMIC_ENDPOINTS
SET ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE ENDPOINT_PATH = 'employees';

-- Activate all accounting endpoints for 2025
UPDATE CONFIG.ECONOMIC_ENDPOINTS
SET ACTIVE = TRUE, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE ENDPOINT_PATH LIKE 'accounting-years/2025%';

-- View all available endpoints
SELECT ENDPOINT_PATH, BASE, DESCRIPTION, ACTIVE
FROM CONFIG.ECONOMIC_ENDPOINTS
ORDER BY ACTIVE DESC, BASE, ENDPOINT_PATH;
```

### Adding Custom Endpoints

```sql
INSERT INTO CONFIG.ECONOMIC_ENDPOINTS (
    ENDPOINT_PATH,
    BASE,
    DESCRIPTION,
    DEFAULT_PAGESIZE,
    ACTIVE
)
VALUES (
    'your-custom-endpoint',
    'REST',
    'Your custom endpoint description',
    1000,
    TRUE
);
```

### Changing Refresh Schedule

```sql
-- Edit task schedule
ALTER TASK ECONOMIC_DAILY_REFRESH SET SCHEDULE = 'USING CRON 0 6 * * * Europe/Copenhagen';

-- Common schedules:
-- Every 6 hours:  0 */6 * * * Europe/Copenhagen
-- Twice daily:    0 6,18 * * * Europe/Copenhagen
-- Weekly Monday:  0 3 * * 1 Europe/Copenhagen
```

---

## 📊 Usage

### For Data Analysts (ECONOMIC_READ role)

```sql
USE ROLE ECONOMIC_READ;
USE DATABASE ECONOMIC;

-- Sales by country
SELECT
    delivery_country,
    COUNT(DISTINCT invoice_id) AS invoice_count,
    SUM(line_revenue) AS total_revenue,
    AVG(profit_margin_percent) AS avg_margin_pct
FROM SILVER.VW_SALES_DETAIL
WHERE sale_date >= '2024-01-01'
GROUP BY delivery_country
ORDER BY total_revenue DESC;

-- Customer segmentation
SELECT
    customer_segment,
    COUNT(DISTINCT customer_id) AS customer_count,
    SUM(line_revenue) AS total_revenue
FROM SILVER.VW_SALES_DETAIL
WHERE sale_date >= DATEADD(MONTH, -12, CURRENT_DATE())
GROUP BY customer_segment;

-- Product performance
SELECT
    product_sku,
    product_name,
    SUM(quantity_sold) AS total_units,
    SUM(line_revenue) AS total_revenue,
    AVG(profit_margin_percent) AS avg_margin_pct
FROM SILVER.VW_SALES_DETAIL
WHERE sale_date >= DATEADD(MONTH, -3, CURRENT_DATE())
GROUP BY product_sku, product_name
ORDER BY total_revenue DESC
LIMIT 20;
```

### For Data Engineers (ECONOMIC_WRITE role)

```sql
USE ROLE ECONOMIC_WRITE;
USE DATABASE ECONOMIC;

-- Manual ingestion
CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST_MONTHLY();

-- Check data freshness
SELECT
    API_ENDPOINT,
    MAX(DATE_INSERTED) AS last_load,
    SUM(RECORD_COUNT_PER_PAGE) AS total_records
FROM RAW.ECONOMIC_RESTAPI_JSON
GROUP BY API_ENDPOINT
ORDER BY last_load DESC;

-- Monitor task history
SELECT
    SCHEDULED_TIME,
    STATE,
    RETURN_VALUE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'ECONOMIC_DAILY_REFRESH'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
```

### For Administrators (ECONOMIC_ADMIN role)

```sql
USE ROLE ECONOMIC_ADMIN;

-- Grant roles to users
GRANT ROLE ECONOMIC_READ TO USER analyst_user;
GRANT ROLE ECONOMIC_WRITE TO USER data_engineer;

-- Check object counts
SELECT 'Tables' AS object_type, COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'RAW'
UNION ALL
SELECT 'Bronze Views', COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'BRONZE'
UNION ALL
SELECT 'Silver Views', COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'SILVER';

-- Update API secrets (when rotating)
ALTER SECRET ECONOMIC_XAPIKEY_APPSECRET
  SET SECRET_STRING = 'new_secret_value';
```

---

## 🔍 Troubleshooting

### Common Issues

#### 1. API Request Fails with 401 Unauthorized

**Cause**: Invalid API credentials

**Solution**:
```sql
-- Verify secrets are set
SHOW SECRETS LIKE 'ECONOMIC%';

-- Update with correct credentials
ALTER SECRET ECONOMIC_XAPIKEY_APPSECRET SET SECRET_STRING = 'correct_value';
ALTER SECRET ECONOMIC_XAPIKEY_AGREEMENTGRANT SET SECRET_STRING = 'correct_value';

-- Test connection
SELECT UTIL.ECONOMIC_API_V3('customers', 'REST', 1, 0);
```

#### 2. Task Fails to Execute

**Cause**: Warehouse not available or permissions issue

**Solution**:
```sql
-- Check warehouse status
SHOW WAREHOUSES LIKE 'COMPUTE_WH';

-- Verify task state
SHOW TASKS IN SCHEMA UTIL;

-- Check task history for errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'ECONOMIC_DAILY_REFRESH'
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;
```

#### 3. Bronze Views Return No Data

**Cause**: RAW tables empty or endpoint not active

**Solution**:
```sql
-- Check if RAW tables have data
SELECT COUNT(*) FROM RAW.ECONOMIC_RESTAPI_JSON;

-- Check active endpoints
SELECT * FROM CONFIG.ECONOMIC_ENDPOINTS WHERE ACTIVE = TRUE;

-- Run manual ingestion
CALL UTIL.ECONOMIC_RESTAPI_DATAINGEST_MONTHLY();
```

#### 4. Git Integration Fails

**Cause**: Invalid PAT token or repository permissions

**Solution**:
```sql
-- Verify Git repository object
SHOW GIT REPOSITORIES;

-- List files in repository
LIST @ECONOMIC_TEMPLATE_REPO/branches/main;

-- Recreate with correct credentials
DROP GIT REPOSITORY ECONOMIC_TEMPLATE_REPO;
-- Then re-run 00_git_setup_and_deploy.sql with correct PAT
```

---

## 🛠️ Maintenance

### Regular Tasks

#### Weekly
- Monitor task success rate
- Check data freshness
- Review error logs

#### Monthly
- Rotate API credentials (if required by security policy)
- Review and optimize warehouse usage
- Archive old RAW data if needed

#### Quarterly
- Review active endpoints (add/remove as needed)
- Update Bronze/Silver views if new fields needed
- Audit user access and role grants

### Updating the Template

To deploy updates from the template repository:

```sql
USE ROLE ACCOUNTADMIN;

-- Fetch latest changes
ALTER GIT REPOSITORY ECONOMIC_TEMPLATE_REPO FETCH;

-- Re-execute deployment
EXECUTE IMMEDIATE FROM @ECONOMIC_TEMPLATE_REPO/branches/main/00_execute_all_files_in_repo.sql;
```

### Backup Strategy

```sql
-- Clone RAW tables before major updates
CREATE TABLE RAW.ECONOMIC_RESTAPI_JSON_BACKUP_20250129
  CLONE RAW.ECONOMIC_RESTAPI_JSON;

-- Time Travel for recent recovery (up to 90 days)
SELECT * FROM RAW.ECONOMIC_RESTAPI_JSON
AT(OFFSET => -3600);  -- 1 hour ago
```

---

## 🤝 Contributing

Improvements and contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

---

## 📝 License

[Your License Here]

---

## 🆘 Support

For issues or questions:

1. Check this README and troubleshooting section
2. Review Economic API documentation: https://restdocs.e-conomic.com/
3. Check Snowflake documentation: https://docs.snowflake.com/
4. Open an issue in the GitHub repository

---

## 📚 Additional Resources

- [Economic REST API Documentation](https://restdocs.e-conomic.com/)
- [Economic OpenAPI Specification](https://apis.e-conomic.com/)
- [Snowflake External Functions](https://docs.snowflake.com/en/sql-reference/external-functions-introduction)
- [Snowflake Git Integration](https://docs.snowflake.com/en/developer-guide/git/git-overview)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

---

**Built with ❤️ for efficient Economic data integration**
