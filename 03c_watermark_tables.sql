USE DATABASE ECONOMIC;
USE ROLE ECONOMIC_ADMIN;
USE SCHEMA CONFIG;

CREATE OR REPLACE TABLE INGESTION_WATERMARKS (

    API_ENDPOINT VARCHAR(255) NOT NULL,
    BASE VARCHAR(10) NOT NULL,

    -- Watermark values
    LAST_UPDATED_TIMESTAMP TIMESTAMP_LTZ, -- customer/products
    LAST_INVOICE_NUMBER NUMBER(38,0), -- invoices/booked & lines

    -- Metadata
    LAST_INGESTION_DATE TIMESTAMP_LTZ NOT NULL, -- last data ingestion
    TOTAL_RECORDS_LOADED NUMBER(38,0) DEFAULT 0, -- running total records loaded
    LAST_RUN_RECORDS NUMBER(38,0) DEFAULT 0, -- Records loaded in the last run

    -- Tracking
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),

    -- constraints
    PRIMARY KEY (API_ENDPOINT, BASE),
    CONSTRAINT CHK_BASE_TYPE CHECK (BASE IN ('REST', 'OPENAPI'))

);
    COMMENT ON TABLE CONFIG.INGESTION_WATERMARKS IS
    'Watermark table for incremental data loading. Tracks last loaded timestamp or invoice number per + endpoint to enable delta loads instead of full refresh. Note: bookedInvoiceNumber (REST) = + documentId (OpenAPI) - verified 2025-11-24.';



    INSERT INTO INGESTION_WATERMARKS (
        API_ENDPOINT, BASE, LAST_UPDATED_TIMESTAMP, LAST_INVOICE_NUMBER, LAST_INGESTION_DATE, TOTAL_RECORDS_LOADED, LAST_RUN_RECORDS
        )
    VALUES
    -- REST endpoints with lastUpdated field support
    ('customers', 'REST', NULL, NULL, CURRENT_TIMESTAMP(), 0, 0),
    ('products', 'REST', NULL, NULL, CURRENT_TIMESTAMP(), 0, 0),
    
    -- Invoice endpoints use invoice number (bookedInvoiceNumber = documentId)
    ('invoices/booked', 'REST', NULL, 0, CURRENT_TIMESTAMP(), 0, 0),
    ('invoices/booked/lines', 'OPENAPI', NULL, 0, CURRENT_TIMESTAMP(), 0, 0);
