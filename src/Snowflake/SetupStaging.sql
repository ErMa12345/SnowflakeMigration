-- Test Script for usp_BulkImportBudgetData
-- Prerequisites: Run Deploy.sql first, then upload MockBudgetData.csv to the stage

USE WAREHOUSE SNOWFLAKETAKEHOME;
USE DATABASE FINANCIAL_PLANNING;
USE SCHEMA Planning;

-- Step 1: Create File Format
CREATE OR REPLACE FILE FORMAT Planning.CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '')
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- Step 2: Create Internal Stage
CREATE STAGE IF NOT EXISTS Planning.budget_import_stage;

-- Step 3: Upload file instruction
-- Upload the file in webUI


