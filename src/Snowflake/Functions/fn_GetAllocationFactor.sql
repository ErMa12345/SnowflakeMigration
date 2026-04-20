/*
    fn_GetAllocationFactor - Calculates allocation factor based on various drivers
    Dependencies: CostCenter, BudgetLineItem, GLAccount

    Migration Notes:
    - Converted from T-SQL to Snowflake SQL UDF
    - Removed SCHEMABINDING (not applicable)
    - RETURNS NULL ON NULL INPUT → Snowflake handles this automatically
    - BIT → BOOLEAN
    - DECIMAL(18,10) kept (Snowflake supports high precision)
    - Note: FinalAmount assumed to be regular column (was computed in SQL Server)
*/

CREATE OR REPLACE FUNCTION Planning.fn_GetAllocationFactor(
    SourceCostCenterID      FLOAT,
    TargetCostCenterID      FLOAT,
    AllocationBasis         VARCHAR(30),
    FiscalPeriodID          FLOAT,
    BudgetHeaderID          FLOAT
)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    if (SOURCECOSTCENTERID == null || TARGETCOSTCENTERID == null ||
        ALLOCATIONBASIS == null || FISCALPERIODID == null) {
        return null;
    }

    var factor = 0;
    var sourceTotal = null;
    var targetValue = null;
    var childCount = 0;

    // Different allocation bases require different calculations
    if (ALLOCATIONBASIS == 'HEADCOUNT') {
        // Get headcount from cost center attributes (simplified)
        var stmt1 = snowflake.createStatement({
            sqlText: `SELECT SUM(cc.AllocationWeight) AS total
                      FROM Planning.CostCenter cc
                      WHERE cc.ParentCostCenterID = :1
                        AND cc.IsActive = TRUE`,
            binds: [SOURCECOSTCENTERID]
        });
        var result1 = stmt1.execute();
        if (result1.next()) {
            sourceTotal = result1.getColumnValue(1);
        }

        var stmt2 = snowflake.createStatement({
            sqlText: `SELECT cc.AllocationWeight
                      FROM Planning.CostCenter cc
                      WHERE cc.CostCenterID = :1
                        AND cc.IsActive = TRUE`,
            binds: [TARGETCOSTCENTERID]
        });
        var result2 = stmt2.execute();
        if (result2.next()) {
            targetValue = result2.getColumnValue(1);
        }

    } else if (ALLOCATIONBASIS == 'REVENUE') {
        // Revenue-based allocation from budget line items
        var stmt1 = snowflake.createStatement({
            sqlText: `SELECT SUM(bli.OriginalAmount + bli.AdjustedAmount) AS total
                      FROM Planning.BudgetLineItem bli
                      INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
                      INNER JOIN Planning.CostCenter cc ON bli.CostCenterID = cc.CostCenterID
                      WHERE (cc.ParentCostCenterID = :1 OR cc.CostCenterID = :1)
                        AND gla.AccountType = 'R'
                        AND bli.FiscalPeriodID = :2
                        AND (:3 IS NULL OR bli.BudgetHeaderID = :3)`,
            binds: [SOURCECOSTCENTERID, FISCALPERIODID, BUDGETHEADERID]
        });
        var result1 = stmt1.execute();
        if (result1.next()) {
            sourceTotal = result1.getColumnValue(1);
        }

        var stmt2 = snowflake.createStatement({
            sqlText: `SELECT SUM(bli.OriginalAmount + bli.AdjustedAmount) AS total
                      FROM Planning.BudgetLineItem bli
                      INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
                      WHERE bli.CostCenterID = :1
                        AND gla.AccountType = 'R'
                        AND bli.FiscalPeriodID = :2
                        AND (:3 IS NULL OR bli.BudgetHeaderID = :3)`,
            binds: [TARGETCOSTCENTERID, FISCALPERIODID, BUDGETHEADERID]
        });
        var result2 = stmt2.execute();
        if (result2.next()) {
            targetValue = result2.getColumnValue(1);
        }

    } else if (ALLOCATIONBASIS == 'EXPENSE') {
        var stmt1 = snowflake.createStatement({
            sqlText: `SELECT SUM(bli.OriginalAmount + bli.AdjustedAmount) AS total
                      FROM Planning.BudgetLineItem bli
                      INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
                      INNER JOIN Planning.CostCenter cc ON bli.CostCenterID = cc.CostCenterID
                      WHERE (cc.ParentCostCenterID = :1 OR cc.CostCenterID = :1)
                        AND gla.AccountType = 'X'
                        AND bli.FiscalPeriodID = :2
                        AND (:3 IS NULL OR bli.BudgetHeaderID = :3)`,
            binds: [SOURCECOSTCENTERID, FISCALPERIODID, BUDGETHEADERID]
        });
        var result1 = stmt1.execute();
        if (result1.next()) {
            sourceTotal = result1.getColumnValue(1);
        }

        var stmt2 = snowflake.createStatement({
            sqlText: `SELECT SUM(bli.OriginalAmount + bli.AdjustedAmount) AS total
                      FROM Planning.BudgetLineItem bli
                      INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
                      WHERE bli.CostCenterID = :1
                        AND gla.AccountType = 'X'
                        AND bli.FiscalPeriodID = :2
                        AND (:3 IS NULL OR bli.BudgetHeaderID = :3)`,
            binds: [TARGETCOSTCENTERID, FISCALPERIODID, BUDGETHEADERID]
        });
        var result2 = stmt2.execute();
        if (result2.next()) {
            targetValue = result2.getColumnValue(1);
        }

    } else if (ALLOCATIONBASIS == 'EQUAL') {
        // Equal distribution among all children
        var stmt = snowflake.createStatement({
            sqlText: `SELECT COUNT(*) AS cnt
                      FROM Planning.CostCenter cc
                      WHERE cc.ParentCostCenterID = :1
                        AND cc.IsActive = TRUE`,
            binds: [SOURCECOSTCENTERID]
        });
        var result = stmt.execute();
        if (result.next()) {
            childCount = result.getColumnValue(1);
        }

        if (childCount > 0) {
            factor = 1.0 / childCount;
        }

        return factor;
    }

    // Calculate factor with null protection
    if (sourceTotal != null && sourceTotal != 0 && targetValue != null) {
        factor = targetValue / sourceTotal;
    }

    return factor;
$$;
