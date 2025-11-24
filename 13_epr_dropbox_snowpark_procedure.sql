/*
==============================================================================
EPR PACKAGING DATA - SNOWPARK DROPBOX INTEGRATION
==============================================================================
Purpose: Automated monthly data ingestion from Dropbox Excel file directly
         into Snowflake using Snowpark Python stored procedure.

Benefits:
- Fully automated - no manual file downloads
- Scheduled execution (monthly or on-demand)
- Direct Dropbox API integration
- Column selection and data transformation in Python
- All code runs inside Snowflake

Prerequisites:
1. Snowflake account with Snowpark enabled
2. Dropbox App with API access token (instructions below)
3. Anaconda packages accepted in Snowflake

Author: Claude Code
Date: 2025-11-22
==============================================================================
*/

-- Use appropriate role and warehouse
USE ROLE ACCOUNTADMIN; -- Need ACCOUNTADMIN to create external access integration
USE WAREHOUSE ECONOMIC_WH;
USE DATABASE ECONOMIC;
USE SCHEMA RAW;

/*
==============================================================================
STEP 1: ENABLE ANACONDA PACKAGES (ONE-TIME SETUP)
==============================================================================
Snowflake partners with Anaconda to provide Python packages.
You must accept terms once per account.
*/

-- Check if Anaconda is enabled (run this first)
-- If not enabled, go to: Admin > Billing & Terms > Enable Anaconda packages
-- Or run via SnowSQL/Worksheet:
-- NOTE: This requires ORGADMIN role
-- USE ROLE ORGADMIN;
-- SELECT SYSTEM$ACKNOWLEDGE_ANACONDA_TERMS();

/*
==============================================================================
STEP 2: CREATE SECRET FOR DROPBOX API TOKEN
==============================================================================
Store your Dropbox API token securely in Snowflake.
*/

-- Create secret to store Dropbox API token
CREATE OR REPLACE SECRET ECONOMIC.RAW.DROPBOX_API_TOKEN
TYPE = GENERIC_STRING
SECRET_STRING = 'YOUR_DROPBOX_API_TOKEN_HERE'  -- Replace with actual token
COMMENT = 'Dropbox API access token for EPR packaging data file access';

-- Grant usage to ECONOMIC_WRITE role
GRANT USAGE ON SECRET ECONOMIC.RAW.DROPBOX_API_TOKEN TO ROLE ECONOMIC_WRITE;

/*
==============================================================================
STEP 3: CREATE NETWORK RULE FOR DROPBOX API
==============================================================================
Allow Snowflake to make outbound connections to Dropbox API.
*/

CREATE OR REPLACE NETWORK RULE ECONOMIC.RAW.DROPBOX_API_NETWORK_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = (
    'api.dropboxapi.com',
    'content.dropboxapi.com'
)
COMMENT = 'Allow outbound HTTPS connections to Dropbox API';

/*
==============================================================================
STEP 4: CREATE EXTERNAL ACCESS INTEGRATION
==============================================================================
Combines network rules and secrets for external API access.
*/

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION DROPBOX_EXTERNAL_ACCESS_INTEGRATION
ALLOWED_NETWORK_RULES = (ECONOMIC.RAW.DROPBOX_API_NETWORK_RULE)
ALLOWED_AUTHENTICATION_SECRETS = (ECONOMIC.RAW.DROPBOX_API_TOKEN)
ENABLED = TRUE
COMMENT = 'External access integration for Dropbox API';

-- Grant usage to ECONOMIC_WRITE role
GRANT USAGE ON INTEGRATION DROPBOX_EXTERNAL_ACCESS_INTEGRATION TO ROLE ECONOMIC_WRITE;

/*
==============================================================================
STEP 5: CREATE SNOWPARK STORED PROCEDURE
==============================================================================
Python procedure that:
1. Connects to Dropbox API
2. Downloads the Excel file
3. Extracts specified columns
4. Loads data into SKU_PACKAGING_DATA table
*/

-- Switch to ECONOMIC_WRITE role for procedure creation
USE ROLE ECONOMIC_ADMIN;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE INGEST_EPR_FROM_DROPBOX(
    DROPBOX_FILE_PATH VARCHAR,  -- e.g., '/EPR/master_packaging_file.xlsx'
    SHEET_NAME VARCHAR          -- e.g., 'Sheet1' or '0' for first sheet
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'openpyxl',
    'requests'
)
EXTERNAL_ACCESS_INTEGRATIONS = (DROPBOX_EXTERNAL_ACCESS_INTEGRATION)
SECRETS = ('dropbox_token' = ECONOMIC.RAW.DROPBOX_API_TOKEN)
HANDLER = 'run'
EXECUTE AS CALLER
COMMENT = 'Downloads EPR packaging Excel file from Dropbox and ingests into Snowflake'
AS
$$
import requests
import pandas as pd
import io
from datetime import datetime
from snowflake.snowpark import Session

def run(session: Session, dropbox_file_path: str, sheet_name: str) -> str:
    """
    Main handler function that downloads Excel from Dropbox and loads to Snowflake.

    Args:
        session: Snowflake session object
        dropbox_file_path: Path to file in Dropbox (e.g., '/EPR/file.xlsx')
        sheet_name: Excel sheet name or index (e.g., 'Sheet1' or '0')

    Returns:
        Status message with row counts
    """

    try:
        # Get Dropbox API token from secret
        dropbox_token = session.get_secret_string('dropbox_token')

        # Step 1: Download file from Dropbox
        session.sql("SELECT SYSTEM$LOG_INFO('Starting EPR data ingestion from Dropbox...')").collect()

        headers = {
            'Authorization': f'Bearer {dropbox_token}',
            'Dropbox-API-Arg': f'{{"path": "{dropbox_file_path}"}}'
        }

        response = requests.post(
            'https://content.dropboxapi.com/2/files/download',
            headers=headers,
            timeout=60
        )

        if response.status_code != 200:
            error_msg = f"Failed to download file from Dropbox: {response.status_code} - {response.text}"
            session.sql(f"SELECT SYSTEM$LOG_ERROR('{error_msg}')").collect()
            return f"ERROR: {error_msg}"

        session.sql("SELECT SYSTEM$LOG_INFO('File downloaded successfully from Dropbox')").collect()

        # Step 2: Read Excel file from bytes
        excel_data = io.BytesIO(response.content)

        # Convert sheet_name to integer if it's numeric
        if sheet_name.isdigit():
            sheet_name_param = int(sheet_name)
        else:
            sheet_name_param = sheet_name

        df = pd.read_excel(excel_data, sheet_name=sheet_name_param)
        session.sql(f"SELECT SYSTEM$LOG_INFO('Excel file read successfully. Total rows: {len(df)}')").collect()

        # Step 3: Extract packaging columns
        def find_column(df, possible_names):
            """Find column by checking multiple possible names."""
            for name in possible_names:
                if name in df.columns:
                    return name
            # Try case-insensitive partial match
            for col in df.columns:
                for name in possible_names:
                    if name.lower() in str(col).lower():
                        return col
            return None

        # Find the correct column names
        product_col = find_column(df, ['Product', 'product', 'COLLARS', 'Product Description'])
        sku_col = find_column(df, ['SKU', 'sku', 'Varenummer'])
        carton_col = find_column(df, ['Packaging - carton (g)', 'carton', 'Packaging - carton', 'Pap'])
        plastic_col = find_column(df, ['Packaging - plastic (g)', 'plastic', 'Packaging - plastic', 'Plastik'])
        foam_col = find_column(df, ['Packaging - foam (g)', 'foam', 'Packaging - foam', 'XPS', 'XY'])

        # Verify all required columns were found
        missing = []
        if not product_col: missing.append('Product')
        if not sku_col: missing.append('SKU')
        if not carton_col: missing.append('Packaging - carton (g)')
        if not plastic_col: missing.append('Packaging - plastic (g)')
        if not foam_col: missing.append('Packaging - foam (g)')

        if missing:
            error_msg = f"Missing required columns: {missing}. Available: {list(df.columns)}"
            session.sql(f"SELECT SYSTEM$LOG_ERROR('{error_msg}')").collect()
            return f"ERROR: {error_msg}"

        session.sql(f"SELECT SYSTEM$LOG_INFO('Found all required columns: Product={product_col}, SKU={sku_col}, Carton={carton_col}, Plastic={plastic_col}, Foam={foam_col}')").collect()

        # Step 4: Transform data
        epr_df = pd.DataFrame({
            'SKU': df[sku_col],
            'PRODUCT_DESCRIPTION': df[product_col],
            'CARTON_G': pd.to_numeric(df[carton_col], errors='coerce'),
            'PLASTIC_G': pd.to_numeric(df[plastic_col], errors='coerce'),
            'FOAM_G': pd.to_numeric(df[foam_col], errors='coerce')
        })

        # Convert grams to kilograms
        epr_df['CARTON_KG_PER_UNIT'] = epr_df['CARTON_G'] / 1000
        epr_df['PLASTIC_KG_PER_UNIT'] = epr_df['PLASTIC_G'] / 1000
        epr_df['FOAM_KG_PER_UNIT'] = epr_df['FOAM_G'] / 1000

        # Drop gram columns (only need kg)
        epr_df = epr_df.drop(columns=['CARTON_G', 'PLASTIC_G', 'FOAM_G'])

        # Remove rows where SKU is missing
        epr_df = epr_df.dropna(subset=['SKU'])

        # Add metadata
        epr_df['DATE_UPLOADED'] = datetime.now()
        epr_df['SOURCE_FILE'] = dropbox_file_path.split('/')[-1]  # Extract filename

        # Ensure SKU is string type
        epr_df['SKU'] = epr_df['SKU'].astype(str)

        session.sql(f"SELECT SYSTEM$LOG_INFO('Transformed {len(epr_df)} SKUs for upload')").collect()

        # Step 5: Load to Snowflake
        # Create Snowpark DataFrame
        snow_df = session.create_dataframe(epr_df)

        # Write to table
        snow_df.write.mode('append').save_as_table('ECONOMIC.RAW.SKU_PACKAGING_DATA')

        session.sql(f"SELECT SYSTEM$LOG_INFO('Successfully loaded {len(epr_df)} rows into SKU_PACKAGING_DATA')").collect()

        # Step 6: Return summary
        summary = {
            'status': 'SUCCESS',
            'rows_loaded': len(epr_df),
            'file_path': dropbox_file_path,
            'timestamp': datetime.now().isoformat(),
            'skus_with_carton': int(epr_df['CARTON_KG_PER_UNIT'].notna().sum()),
            'skus_with_plastic': int(epr_df['PLASTIC_KG_PER_UNIT'].notna().sum()),
            'skus_with_foam': int(epr_df['FOAM_KG_PER_UNIT'].notna().sum())
        }

        return f"SUCCESS: Loaded {summary['rows_loaded']} SKUs from Dropbox. Carton: {summary['skus_with_carton']}, Plastic: {summary['skus_with_plastic']}, Foam: {summary['skus_with_foam']}"

    except Exception as e:
        error_msg = f"ERROR in EPR ingestion: {str(e)}"
        session.sql(f"SELECT SYSTEM$LOG_ERROR('{error_msg}')").collect()
        return error_msg
$$;

-- Grant execute permission
GRANT USAGE ON PROCEDURE ECONOMIC.RAW.INGEST_EPR_FROM_DROPBOX(VARCHAR, VARCHAR) TO ROLE ECONOMIC_WRITE;

/*
==============================================================================
STEP 6: TEST THE PROCEDURE (MANUAL EXECUTION)
==============================================================================
Test the procedure manually before setting up scheduled task.
*/

-- Test execution (UPDATE THE FILE PATH!)
-- CALL ECONOMIC.RAW.INGEST_EPR_FROM_DROPBOX(
--     '/EPR/master_packaging_file.xlsx',  -- Your Dropbox file path
--     'Sheet1'                             -- Your sheet name or '0' for first sheet
-- );

-- Verify the data loaded
-- SELECT * FROM ECONOMIC.RAW.SKU_PACKAGING_DATA ORDER BY DATE_UPLOADED DESC LIMIT 10;

-- Check the Bronze view
-- SELECT * FROM ECONOMIC.BRONZE.DIM_SKU_PACKAGING LIMIT 10;

/*
==============================================================================
DEPLOYMENT NOTES
==============================================================================

✅ Objects Created:
   1. SECRET: ECONOMIC.RAW.DROPBOX_API_TOKEN
   2. NETWORK RULE: ECONOMIC.RAW.DROPBOX_API_NETWORK_RULE
   3. EXTERNAL ACCESS INTEGRATION: DROPBOX_EXTERNAL_ACCESS_INTEGRATION
   4. STORED PROCEDURE: ECONOMIC.RAW.INGEST_EPR_FROM_DROPBOX

📋 Next Steps:
   1. Get Dropbox API token (see setup guide)
   2. Update the SECRET with your actual Dropbox token
   3. Test the procedure manually
   4. Set up scheduled task (see 14_epr_scheduled_task.sql)

🔐 Security Notes:
   - API token is stored as a Snowflake secret (encrypted)
   - Network rules restrict access to Dropbox API only
   - Procedure executes as caller (ECONOMIC_WRITE role)
*/
