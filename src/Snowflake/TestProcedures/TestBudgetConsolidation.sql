USE WAREHOUSE SNOWFLAKETAKEHOME;
USE DATABASE FINANCIAL_PLANNING;
USE SCHEMA Planning;

-- ============================================================================
-- Test 1: Basic Consolidation - Annual Operating Budget (BudgetHeaderID = 1)
-- ============================================================================

-- First, approve the source budget (required for consolidation)
UPDATE Planning.BudgetHeader
SET StatusCode = 'APPROVED'
WHERE BudgetHeaderID = 1;

-- Check budget status
SELECT *
FROM Planning.BudgetHeader
WHERE BudgetHeaderID = 1;

-- Call the consolidation procedure
CALL Planning.usp_ProcessBudgetConsolidation(
    SourceBudgetHeaderID => 1,
    ConsolidationType => 'FULL',
    IncludeEliminations => TRUE,
    RecalculateAllocations => TRUE,
    ProcessingOptions => NULL,
    UserID => 1,
    DebugMode => TRUE
);

-- View the created consolidated budget header
SELECT BudgetHeaderID, BudgetCode, BudgetName, BudgetType, StatusCode,
       BaseBudgetHeaderID, ExtendedProperties
FROM Planning.BudgetHeader
WHERE BudgetType = 'CONSOLIDATED'
  AND BaseBudgetHeaderID = 1
ORDER BY BudgetHeaderID DESC
LIMIT 1;

-- View consolidated budget line items 
SELECT
    bli.BudgetLineItemID,
    bh.BudgetCode,
    gla.AccountNumber,
    gla.AccountName,
    cc.CostCenterCode,
    cc.CostCenterName,
    fp.PeriodName,
    bli.OriginalAmount,
    bli.SpreadMethodCode,
    bli.SourceReference
FROM Planning.BudgetLineItem bli
INNER JOIN Planning.BudgetHeader bh ON bli.BudgetHeaderID = bh.BudgetHeaderID
INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
INNER JOIN Planning.CostCenter cc ON bli.CostCenterID = cc.CostCenterID
INNER JOIN Planning.FiscalPeriod fp ON bli.FiscalPeriodID = fp.FiscalPeriodID
WHERE bh.BudgetType = 'CONSOLIDATED'
  AND bh.BaseBudgetHeaderID = 1
ORDER BY fp.FiscalMonth, cc.CostCenterCode, gla.AccountNumber;

-- Summary by Cost Center
SELECT
    cc.CostCenterCode,
    cc.CostCenterName,
    COUNT(*) AS LineItemCount,
    SUM(bli.OriginalAmount) AS TotalAmount
FROM Planning.BudgetLineItem bli
INNER JOIN Planning.BudgetHeader bh ON bli.BudgetHeaderID = bh.BudgetHeaderID
INNER JOIN Planning.CostCenter cc ON bli.CostCenterID = cc.CostCenterID
WHERE bh.BudgetType = 'CONSOLIDATED'
  AND bh.BaseBudgetHeaderID = 1
GROUP BY cc.CostCenterCode, cc.CostCenterName
ORDER BY cc.CostCenterCode;

-- ============================================================================
-- Test 2: Consolidation with Processing Options - Q1 Forecast (BudgetHeaderID = 2)
-- ============================================================================
SELECT '=== Test 2: Consolidation with Processing Options ===' AS TestName;

-- Approve the source budget
UPDATE Planning.BudgetHeader
SET StatusCode = 'APPROVED'
WHERE BudgetHeaderID = 2;

-- Call with JSON processing options
CALL Planning.usp_ProcessBudgetConsolidation(
    SourceBudgetHeaderID => 2,
    ConsolidationType => 'FULL',
    IncludeEliminations => FALSE,
    RecalculateAllocations => TRUE,
    ProcessingOptions => PARSE_JSON('{
        "IncludeZeroBalances": false,
        "RoundingPrecision": 2
    }'),
    UserID => 1,
    DebugMode => TRUE
);

-- View results
SELECT
    bh.BudgetCode,
    COUNT(*) AS TotalLines,
    SUM(bli.OriginalAmount) AS TotalAmount,
    MIN(bli.OriginalAmount) AS MinAmount,
    MAX(bli.OriginalAmount) AS MaxAmount
FROM Planning.BudgetLineItem bli
INNER JOIN Planning.BudgetHeader bh ON bli.BudgetHeaderID = bh.BudgetHeaderID
WHERE bh.BudgetType = 'CONSOLIDATED'
  AND bh.BaseBudgetHeaderID = 2
GROUP BY bh.BudgetCode;

-- ============================================================================
-- Test 3: Optimistic Scenario Consolidation (BudgetHeaderID = 3)
-- ============================================================================
SELECT '=== Test 3: Optimistic Scenario Consolidation ===' AS TestName;

UPDATE Planning.BudgetHeader
SET StatusCode = 'APPROVED'
WHERE BudgetHeaderID = 3;

CALL Planning.usp_ProcessBudgetConsolidation(
    SourceBudgetHeaderID => 3,
    ConsolidationType => 'FULL',
    IncludeEliminations => TRUE,
    RecalculateAllocations => TRUE,
    ProcessingOptions => NULL,
    UserID => 1,
    DebugMode => FALSE
);

-- ============================================================================
-- Test 4: Hierarchy
-- ============================================================================

-- Show hierarchy with consolidated amounts
SELECT
    cc.HierarchyLevel,
    cc.HierarchyPath,
    cc.CostCenterCode,
    cc.CostCenterName,
    gla.AccountNumber,
    gla.AccountName,
    SUM(bli.OriginalAmount) AS ConsolidatedAmount
FROM Planning.BudgetLineItem bli
INNER JOIN Planning.BudgetHeader bh ON bli.BudgetHeaderID = bh.BudgetHeaderID
INNER JOIN Planning.CostCenter cc ON bli.CostCenterID = cc.CostCenterID
INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
WHERE bh.BudgetType = 'CONSOLIDATED'
  AND bh.BaseBudgetHeaderID = 1
GROUP BY cc.HierarchyLevel, cc.HierarchyPath, cc.CostCenterCode, cc.CostCenterName,
         gla.AccountNumber, gla.AccountName
ORDER BY cc.HierarchyPath, gla.AccountNumber;

-- ============================================================================
-- Cleanup (Optional - uncomment to remove test consolidated budgets)
-- ============================================================================
/*
DELETE FROM Planning.BudgetLineItem
WHERE BudgetHeaderID IN (
    SELECT BudgetHeaderID
    FROM Planning.BudgetHeader
    WHERE BudgetType = 'CONSOLIDATED'
);

DELETE FROM Planning.BudgetHeader
WHERE BudgetType = 'CONSOLIDATED';

DELETE FROM Planning.BudgetHeader
WHERE BudgetCode = 'BUD-2024-TEST';
*/
