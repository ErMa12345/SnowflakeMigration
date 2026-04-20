/*
    tvf_GetBudgetVariance - Table function for budget vs actual variance
    Dependencies: BudgetHeader, BudgetLineItem, GLAccount, CostCenter, FiscalPeriod

    Migration Notes:
    - SQL Server Inline TVF → Snowflake TABLE function (SECURE)
    - SCHEMABINDING removed (not applicable)
    - ISNULL → COALESCE or NVL
    - Note: In Snowflake, table functions can be used in FROM clause just like views
    - Alternative: Could be converted to a parameterized VIEW (but less flexible)
*/

CREATE OR REPLACE FUNCTION Planning.tvf_GetBudgetVariance(
    BaseBudgetHeaderID      INT,
    ComparisonBudgetHeaderID INT,
    FiscalYear_param        SMALLINT,
    CostCenterID_param      INT,
    AccountType_param       CHAR(1),
    VarianceThresholdPct    DECIMAL(5,2)
)
RETURNS TABLE (
    GLAccountID             INT,
    AccountNumber           VARCHAR(20),
    AccountName             VARCHAR(150),
    AccountType             CHAR(1),
    CostCenterID            INT,
    CostCenterCode          VARCHAR(20),
    CostCenterName          VARCHAR(100),
    FiscalPeriodID          INT,
    FiscalYear              SMALLINT,
    FiscalMonth             TINYINT,
    PeriodName              VARCHAR(50),
    BudgetAmount            DECIMAL(19,4),
    ComparisonAmount        DECIMAL(19,4),
    VarianceAmount          DECIMAL(19,4),
    VariancePercentage      DECIMAL(10,2),
    VarianceStatus          VARCHAR(20),
    ExceedsThreshold        BOOLEAN
)
AS
$$
    WITH BaseBudget AS (
        SELECT
            bli.GLAccountID,
            bli.CostCenterID,
            bli.FiscalPeriodID,
            SUM(bli.OriginalAmount + bli.AdjustedAmount) AS BudgetAmount  -- FinalAmount calculation
        FROM Planning.BudgetLineItem bli
        WHERE bli.BudgetHeaderID = BaseBudgetHeaderID
        GROUP BY bli.GLAccountID, bli.CostCenterID, bli.FiscalPeriodID
    ),
    ComparisonBudget AS (
        SELECT
            bli.GLAccountID,
            bli.CostCenterID,
            bli.FiscalPeriodID,
            SUM(bli.OriginalAmount + bli.AdjustedAmount) AS ComparisonAmount  -- FinalAmount calculation
        FROM Planning.BudgetLineItem bli
        WHERE bli.BudgetHeaderID = ComparisonBudgetHeaderID
        GROUP BY bli.GLAccountID, bli.CostCenterID, bli.FiscalPeriodID
    ),
    Combined AS (
        SELECT
            COALESCE(b.GLAccountID, c.GLAccountID) AS GLAccountID,
            COALESCE(b.CostCenterID, c.CostCenterID) AS CostCenterID,
            COALESCE(b.FiscalPeriodID, c.FiscalPeriodID) AS FiscalPeriodID,
            NVL(b.BudgetAmount, 0) AS BudgetAmount,
            NVL(c.ComparisonAmount, 0) AS ComparisonAmount,
            NVL(c.ComparisonAmount, 0) - NVL(b.BudgetAmount, 0) AS VarianceAmount,
            CASE
                WHEN NVL(b.BudgetAmount, 0) = 0 THEN NULL
                ELSE (NVL(c.ComparisonAmount, 0) - NVL(b.BudgetAmount, 0)) / b.BudgetAmount * 100
            END AS VariancePercentage
        FROM BaseBudget b
        FULL OUTER JOIN ComparisonBudget c
            ON b.GLAccountID = c.GLAccountID
            AND b.CostCenterID = c.CostCenterID
            AND b.FiscalPeriodID = c.FiscalPeriodID
    )
    SELECT
        comb.GLAccountID,
        gla.AccountNumber,
        gla.AccountName,
        gla.AccountType,
        comb.CostCenterID,
        cc.CostCenterCode,
        cc.CostCenterName,
        comb.FiscalPeriodID,
        fp.FiscalYear,
        fp.FiscalMonth,
        fp.PeriodName,
        comb.BudgetAmount,
        comb.ComparisonAmount,
        comb.VarianceAmount,
        comb.VariancePercentage,
        CASE
            WHEN comb.VarianceAmount > 0 THEN 'FAVORABLE'
            WHEN comb.VarianceAmount < 0 THEN 'UNFAVORABLE'
            ELSE 'ON_TARGET'
        END AS VarianceStatus,
        CASE
            WHEN ABS(comb.VariancePercentage) > NVL(VarianceThresholdPct, 100) THEN TRUE
            ELSE FALSE
        END AS ExceedsThreshold
    FROM Combined comb
    INNER JOIN Planning.GLAccount gla ON comb.GLAccountID = gla.GLAccountID
    INNER JOIN Planning.CostCenter cc ON comb.CostCenterID = cc.CostCenterID
    INNER JOIN Planning.FiscalPeriod fp ON comb.FiscalPeriodID = fp.FiscalPeriodID
    WHERE (FiscalYear_param IS NULL OR fp.FiscalYear = FiscalYear_param)
      AND (CostCenterID_param IS NULL OR comb.CostCenterID = CostCenterID_param)
      AND (AccountType_param IS NULL OR gla.AccountType = AccountType_param)
$$;


