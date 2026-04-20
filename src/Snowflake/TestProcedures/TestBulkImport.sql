USE WAREHOUSE SNOWFLAKETAKEHOME;
USE DATABASE FINANCIAL_PLANNING;
USE SCHEMA Planning;

SET budget_id = (SELECT MAX(BudgetHeaderID) FROM Planning.BudgetHeader);
SELECT $budget_id AS BudgetHeaderID;

CALL Planning.usp_BulkImportBudgetData(
  FILE_PATH => '@Planning.budget_import_stage/MockBudgetData.csv',
  TARGET_BUDGET_HEADER_ID => $budget_id,
  VALIDATION_MODE => 'STRICT',
  DUPLICATE_HANDLING => 'REJECT',
  BATCH_SIZE => 10000
);
SELECT * FROM Planning.BudgetLineItem WHERE BudgetHeaderID = $budget_id;

SELECT
  COUNT(*) AS TotalRecords,
  SUM(OriginalAmount) AS TotalOriginal,
  SUM(AdjustedAmount) AS TotalAdjusted
FROM Planning.BudgetLineItem
WHERE BudgetHeaderID = $budget_id;

