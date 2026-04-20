/*
    vw_BudgetConsolidationSummary - Consolidated view of budget data with hierarchy rollups
    Dependencies: BudgetHeader, BudgetLineItem, GLAccount, CostCenter, FiscalPeriod

    Migration Notes:
    - SQL Server Indexed View → Snowflake Materialized View
    - SCHEMABINDING removed (not applicable in Snowflake)
    - COUNT_BIG() → COUNT() (Snowflake COUNT returns BIGINT by default)
    - ISNULL → NVL or COALESCE
    - Indexes removed (Snowflake manages materialized view optimization internally)
    - INCLUDE columns in indexes removed (not applicable)
    - Clustering key can be added for query performance

    Note: In Snowflake, materialized views automatically refresh but have different
    semantics than SQL Server indexed views. Consider trade-offs:
    - Automatic background refresh vs on-demand
    - Storage costs
    - Query performance benefits

    Alternative: Could remain as regular VIEW if materialization not needed
*/

-- Option 1: Regular VIEW (no storage overhead, computed on query)
CREATE OR REPLACE VIEW Planning.vw_BudgetConsolidationSummary
AS
SELECT
    bh.BudgetHeaderID,
    bh.BudgetCode,
    bh.BudgetName,
    bh.BudgetType,
    bh.ScenarioType,
    bh.FiscalYear,
    fp.FiscalPeriodID,
    fp.FiscalQuarter,
    fp.FiscalMonth,
    fp.PeriodName,
    gla.GLAccountID,
    gla.AccountNumber,
    gla.AccountName,
    gla.AccountType,
    cc.CostCenterID,
    cc.CostCenterCode,
    cc.CostCenterName,
    cc.ParentCostCenterID,
    -- Aggregations
    SUM(bli.OriginalAmount) AS TotalOriginalAmount,
    SUM(bli.AdjustedAmount) AS TotalAdjustedAmount,
    SUM(bli.OriginalAmount + bli.AdjustedAmount) AS TotalFinalAmount,
    SUM(NVL(bli.LocalCurrencyAmount, 0)) AS TotalLocalCurrency,
    SUM(NVL(bli.ReportingCurrencyAmount, 0)) AS TotalReportingCurrency,
    COUNT(*) AS LineItemCount  -- Returns BIGINT in Snowflake
FROM Planning.BudgetLineItem bli
INNER JOIN Planning.BudgetHeader bh ON bli.BudgetHeaderID = bh.BudgetHeaderID
INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
INNER JOIN Planning.CostCenter cc ON bli.CostCenterID = cc.CostCenterID
INNER JOIN Planning.FiscalPeriod fp ON bli.FiscalPeriodID = fp.FiscalPeriodID
GROUP BY
    bh.BudgetHeaderID,
    bh.BudgetCode,
    bh.BudgetName,
    bh.BudgetType,
    bh.ScenarioType,
    bh.FiscalYear,
    fp.FiscalPeriodID,
    fp.FiscalQuarter,
    fp.FiscalMonth,
    fp.PeriodName,
    gla.GLAccountID,
    gla.AccountNumber,
    gla.AccountName,
    gla.AccountType,
    cc.CostCenterID,
    cc.CostCenterCode,
    cc.CostCenterName,
    cc.ParentCostCenterID;


