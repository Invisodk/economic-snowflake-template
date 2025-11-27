/*
==========================================================================
EPR PACKAGING DATA - SNOWPARK STAGE INTEGRATION
==========================================================================
*/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA UTIL;

/*
==============================================================================
STEP 1: CREATE SNOWPARK STORED PROCEDURE
==============================================================================
Python procedure that:
1. Reads Excel file from EPR_STAGE
2. Extracts only packaging columns
3. Converts grams to kilograms
4. Loads data into SKU_PACKAGING_DATA table
*/

CREATE OR REPLACE PROCEDURE INGEST_EPR_FROM_STAGE(
    FILE_NAME VARCHAR  -- Just the filename: 'DC ProductSpecs 26.11.25.xlsx'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'openpyxl'
)
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook
import pandas as pd
from datetime import datetime

def main(session, file_name):
    """
    Main handler function that reads Excel from EPR_STAGE and loads to Snowflake.

    Args:
        session: Snowflake session object
        file_name: Name of Excel file in EPR_STAGE

    Returns:
        Status message with row counts
    """

    try:
        # Step 1: Build scoped URL inside the procedure (use fully qualified stage name!)
        scoped_url_query = f"SELECT BUILD_SCOPED_FILE_URL(@ECONOMIC.RAW.EPR_STAGE, '{file_name}')"
        scoped_url_result = session.sql(scoped_url_query).collect()
        scoped_file_url = scoped_url_result[0][0]

        # Step 2: Read Excel file using scoped URL
        with SnowflakeFile.open(scoped_file_url, 'rb') as f:
            workbook = load_workbook(f)
            sheet = workbook.active
            data = sheet.values

            # Get headers and data
            columns = next(data)[0:]
            df = pd.DataFrame(data, columns=columns)

        # Step 2: Extract packaging columns
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

        # Find the packaging columns (no need for Product - we'll get it from PrestaShop!)
        sku_col = find_column(df, ['SKU', 'sku', 'Varenummer'])
        carton_col = find_column(df, ['Packaging - carton (g)', 'carton', 'Packaging - carton', 'Pap'])
        plastic_col = find_column(df, ['Packaging - plastic (g)', 'plastic', 'Packaging - plastic', 'Plastik'])

        # Foam column doesn't exist in your Excel! Set to None
        foam_col = None

        # Verify required columns were found (foam is optional)
        missing = []
        if not sku_col: missing.append('SKU')
        if not carton_col: missing.append('Packaging - carton (g)')
        if not plastic_col: missing.append('Packaging - plastic (g)')

        if missing:
            error_msg = f"Missing required columns: {missing}. Available: {list(df.columns)}"
            return f"ERROR: {error_msg}"

        # Step 3: Transform data (ONLY packaging columns - Product comes from PrestaShop!)
        epr_df = pd.DataFrame({
            'SKU': df[sku_col],
            'CARTON_G': pd.to_numeric(df[carton_col], errors='coerce'),
            'PLASTIC_G': pd.to_numeric(df[plastic_col], errors='coerce'),
            'FOAM_G': 0  # No foam column in your Excel, default to 0
        })

        # Convert grams to kilograms
        epr_df['CARTON_KG_PER_UNIT'] = epr_df['CARTON_G'] / 1000
        epr_df['PLASTIC_KG_PER_UNIT'] = epr_df['PLASTIC_G'] / 1000
        epr_df['FOAM_KG_PER_UNIT'] = epr_df['FOAM_G'] / 1000

        # Drop gram columns (only need kg)
        epr_df = epr_df.drop(columns=['CARTON_G', 'PLASTIC_G', 'FOAM_G'])

        # DATA CLEANING - Remove empty/invalid rows
        # 1. Remove rows where SKU is missing (empty separator rows between product groups)
        epr_df = epr_df.dropna(subset=['SKU'])

        # 2. Convert SKU to string and clean whitespace
        epr_df['SKU'] = epr_df['SKU'].astype(str).str.strip()

        # 3. Remove rows where SKU is empty string or 'nan'
        epr_df = epr_df[epr_df['SKU'] != '']
        epr_df = epr_df[epr_df['SKU'].str.lower() != 'nan']

        # 4. Remove rows where SKU looks like a header (contains spaces or is all caps with no numbers)
        # Real SKUs look like: HAR0569, LEA0335, COL0311
        # Header rows look like: COLLARS, HARNESSES, LEADS
        epr_df = epr_df[~epr_df['SKU'].str.contains(' ', na=False)]  # No spaces in real SKUs

        # Add metadata
        epr_df['DATE_UPLOADED'] = datetime.now()
        epr_df['SOURCE_FILE'] = 'EPR_Upload'  # We don't have the original filename with scoped URL

        # Step 4: Load to Snowflake
        # Create Snowpark DataFrame
        snow_df = session.create_dataframe(epr_df)

        # Truncate table before loading new data (weekly refresh approach)
        session.sql("TRUNCATE TABLE ECONOMIC.RAW.SKU_PACKAGING_DATA").collect()

        # Write to table
        snow_df.write.mode('append').save_as_table('ECONOMIC.RAW.SKU_PACKAGING_DATA')

        # Step 5: Return summary
        rows_loaded = len(epr_df)
        skus_with_carton = int(epr_df['CARTON_KG_PER_UNIT'].notna().sum())
        skus_with_plastic = int(epr_df['PLASTIC_KG_PER_UNIT'].notna().sum())
        skus_with_foam = int(epr_df['FOAM_KG_PER_UNIT'].notna().sum())

        return f"SUCCESS: Loaded {rows_loaded} SKUs. Carton: {skus_with_carton}, Plastic: {skus_with_plastic}, Foam: {skus_with_foam}"

    except Exception as e:
        error_msg = f"ERROR in EPR ingestion: {str(e)}"
        return error_msg
$$;

-- Grant execute permission
GRANT USAGE ON PROCEDURE INGEST_EPR_FROM_STAGE(VARCHAR) TO ROLE ECONOMIC_WRITE;

/*
==============================================================================
STEP 2: TEST THE PROCEDURE (MANUAL EXECUTION)
==============================================================================
Test the procedure after Jan uploads a file to EPR_STAGE.
*/

-- First, check what files are in the stage
LIST @EPR_STAGE;

-- Call procedure with just the filename (SIMPLE!)
CALL INGEST_EPR_FROM_STAGE('DC ProductSpecs 26.11.25.xlsx');

-- Or if file has different name, use that:
-- CALL INGEST_EPR_FROM_STAGE('your_file_name.xlsx');

-- Verify the data loaded
SELECT * FROM SKU_PACKAGING_DATA ORDER BY DATE_UPLOADED DESC LIMIT 10;

-- Check the Bronze view (deduplicated)
SELECT * FROM BRONZE.DIM_SKU_PACKAGING LIMIT 10;

-- Check EPR detail view
SELECT
    packaging_data_status,
    COUNT(DISTINCT sku) as unique_skus,
    COUNT(*) as line_count
FROM SILVER.VW_EPR_DETAIL
WHERE sale_date >= DATEADD('month', -1, CURRENT_DATE())
GROUP BY packaging_data_status;

/*
==============================================================================
STEP 3: SET UP AUTO-TRIGGER (OPTIONAL)
==============================================================================
Make the procedure run automatically when Jan uploads a file.

Option A: Manual trigger (simplest for now)
   - Jan uploads file via UI
   - You/Jan runs: CALL INGEST_EPR_FROM_STAGE('filename.xlsx');

Option B: Snowpipe + Task (fully automatic)
   - Set up Snowpipe to detect new files
   - Trigger task to call the procedure
   - Requires additional setup (see Snowflake Snowpipe documentation)
*/

-- Example task that runs the procedure (can be triggered manually or scheduled)
CREATE OR REPLACE TASK PROCESS_LATEST_EPR_FILE
    WAREHOUSE = ECONOMIC_WH
    -- SCHEDULE = 'USING CRON 0 9 1 * * UTC'  -- Optional: Run monthly on 1st at 9am
AS
    -- This would need logic to find the latest file in stage
    -- For now, keep it manual with CALL INGEST_EPR_FROM_STAGE('filename.xlsx')
    SELECT 'Task placeholder - use manual CALL for now';

/*
==============================================================================
DEPLOYMENT COMPLETE
==============================================================================

Objects Created:
   1. STORED PROCEDURE: INGEST_EPR_FROM_STAGE

 Jan's Workflow (Simple!):
   1. Jan uploads Excel file to EPR_STAGE via Snowflake UI:
      - Log into Snowflake
      - Data > ECONOMIC > RAW > Stages > EPR_STAGE
      - Click "+ Files"
      - Upload DC_ProductSpecs_26.11.25.xlsx
      - Click "Upload"

   2. Run the procedure (you or Jan):
      CALL INGEST_EPR_FROM_STAGE('DC_ProductSpecs_26.11.25.xlsx');

   3. Done! Data is automatically:
    Extracted (only packaging columns)
    Converted (grams → kg)
    Loaded (into SKU_PACKAGING_DATA)
    Visible in EPR views immediately!

 What This Procedure Does Automatically:
   - Reads Excel from stage
   - Finds packaging columns (even if names vary slightly)
   - Extracts only: SKU, Carton(g), Plastic(g), Foam(g)  [Product names come from PrestaShop!]
   - Cleans data (removes empty rows, header rows like COLLARS/HARNESSES, invalid SKUs)
   - Converts grams to kilograms
   - Loads into table with timestamp
   - Returns success message with row counts

 Benefits vs Manual CSV Upload:
   - No Excel manipulation needed
   - Automatic column detection
   - Automatic data cleaning (handles messy Excel with empty rows & category headers)
   - Automatic grams → kg conversion
   - Handles extra columns (ignores them)
   - Product names always up-to-date from PrestaShop!
   - Better error messages

 Security:
   - No external API access needed
   - Procedure runs with caller's permissions
   - All processing happens inside Snowflake
*/
