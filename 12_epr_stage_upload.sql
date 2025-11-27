/*
==========================================================================
EPR PACKAGING DATA
==========================================================================
*/

USE ROLE ECONOMIC_ADMIN;
USE DATABASE ECONOMIC;
USE SCHEMA RAW;

/*
==============================================================================
STEP 1: CREATE INTERNAL STAGE FOR EPR FILES
==============================================================================
An internal stage is like a folder in Snowflake for storing files.
*/

CREATE STAGE IF NOT EXISTS EPR_STAGE
    COMMENT = 'Stage for EPR packaging Excel/CSV files - Jan uploads here';

-- Grant permissions (need USAGE for BUILD_SCOPED_FILE_URL)
GRANT USAGE ON STAGE EPR_STAGE TO ROLE ECONOMIC_ADMIN;
GRANT USAGE ON STAGE EPR_STAGE TO ROLE ECONOMIC_WRITE;
GRANT READ, WRITE ON STAGE EPR_STAGE TO ROLE ECONOMIC_WRITE;
GRANT READ ON STAGE EPR_STAGE TO ROLE ECONOMIC_READ;

-- View files in stage (will be empty initially)
LIST @EPR_STAGE;

/*
==============================================================================
STEP 2: CREATE FILE FORMAT FOR CSV/EXCEL
=========================================================
*/

CREATE OR REPLACE FILE FORMAT .EPR_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1          -- Skip the header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMMENT = 'CSV format for EPR packaging data with headers';

/*
Upload via Snowflake Web UI
------------------------------------------------------
1. Log into Snowflake web interface
2. Navigate to: Data > Databases > EC > Stages
3. Click on EPR_STAGE"
4. Click "+ Files" button (top right)
5. Select your Excel/CSV file
6. Click "Upload"
7. Done! File is now in the stage
*/