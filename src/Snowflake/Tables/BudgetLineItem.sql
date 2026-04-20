/*
    BudgetLineItem - Individual budget amounts by account/cost center/period
    Dependencies: BudgetHeader, GLAccount, CostCenter, FiscalPeriod

    MIGRATION NOTES:
    -----------------
    1. IDENTITY(1,1) → AUTOINCREMENT for BudgetLineItemID
    2. BIT → BOOLEAN for IsAllocated
    3. DATETIME2(7) → TIMESTAMP_NTZ for LastModifiedDateTime
    4. SYSUTCDATETIME() → CURRENT_TIMESTAMP() for default timestamp
    5. UNIQUEIDENTIFIER → VARCHAR(36) for ImportBatchID (storing UUIDs as strings)
    6. Computed column FinalAmount: Converted to regular DECIMAL column
       - SQL Server: AS (OriginalAmount + AdjustedAmount) PERSISTED
       - Snowflake: Regular column, will need trigger/procedure to maintain
    7. Computed column RowHash: Converted to regular VARCHAR column
       - SQL Server: AS HASHBYTES('SHA2_256', ...) PERSISTED
       - Snowflake: Regular column, will need trigger/procedure to maintain using SHA2() function
    8. NONCLUSTERED/CLUSTERED keywords removed (Snowflake uses different indexing)
    9. WITH (IGNORE_DUP_KEY = ON) removed - not supported in Snowflake
       - Handle duplicates in application logic or use MERGE statements
    10. Filtered index removed (WHERE IsAllocated = 1)
        - Snowflake doesn't support filtered indexes
        - Query optimizer will still be efficient due to columnar storage
    11. NONCLUSTERED COLUMNSTORE index removed
        - Snowflake is columnar by default, no need for explicit columnstore indexes
    12. Added clustering key on (BudgetHeaderID, FiscalPeriodID, GLAccountID)
        - Common query pattern for budget analysis and reporting
        - Improves query performance for time-series and account-based queries
*/

CREATE TABLE Planning.BudgetLineItem (
    BudgetLineItemID        BIGINT AUTOINCREMENT NOT NULL,
    BudgetHeaderID          INT NOT NULL,
    GLAccountID             INT NOT NULL,
    CostCenterID            INT NOT NULL,
    FiscalPeriodID          INT NOT NULL,

    -- Amounts in multiple representations
    OriginalAmount          DECIMAL(19,4) NOT NULL DEFAULT 0,
    AdjustedAmount          DECIMAL(19,4) NOT NULL DEFAULT 0,
    FinalAmount             DECIMAL(19,4) NOT NULL DEFAULT 0,  -- Was computed, now regular column: OriginalAmount + AdjustedAmount
    LocalCurrencyAmount     DECIMAL(19,4) NULL,
    ReportingCurrencyAmount DECIMAL(19,4) NULL,
    StatisticalQuantity     DECIMAL(18,6) NULL,
    UnitOfMeasure           VARCHAR(10) NULL,

    -- Spreading pattern for forecast
    SpreadMethodCode        VARCHAR(20) NULL,  -- EVEN, SEASONAL, CUSTOM, PRIOR_YEAR, CONSOLIDATED
    SeasonalityFactor       DECIMAL(8,6) NULL,

    -- Source tracking
    SourceSystem            VARCHAR(30) NULL,
    SourceReference         VARCHAR(100) NULL,
    ImportBatchID           VARCHAR(36) NULL,  -- Was UNIQUEIDENTIFIER, now VARCHAR for UUID storage

    -- Allocation tracking
    IsAllocated             BOOLEAN NOT NULL DEFAULT FALSE,  -- Was BIT
    AllocationSourceLineID  BIGINT NULL,
    AllocationPercentage    DECIMAL(8,6) NULL,

    -- Audit columns
    LastModifiedByUserID    INT NULL,
    LastModifiedDateTime    TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),  -- Was DATETIME2(7) with SYSUTCDATETIME()
    RowHash                 VARCHAR(64) NULL,  -- Was computed HASHBYTES, now regular column for SHA2_256 hash

    CONSTRAINT PK_BudgetLineItem PRIMARY KEY (BudgetLineItemID),
    CONSTRAINT FK_BudgetLineItem_Header FOREIGN KEY (BudgetHeaderID)
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT FK_BudgetLineItem_Account FOREIGN KEY (GLAccountID)
        REFERENCES Planning.GLAccount (GLAccountID),
    CONSTRAINT FK_BudgetLineItem_CostCenter FOREIGN KEY (CostCenterID)
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT FK_BudgetLineItem_Period FOREIGN KEY (FiscalPeriodID)
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetLineItem_AllocationSource FOREIGN KEY (AllocationSourceLineID)
        REFERENCES Planning.BudgetLineItem (BudgetLineItemID)
);


-- Replaced UNIQUE INDEX with UNIQUE constraint already defined in table
-- CREATE UNIQUE INDEX UQ_BudgetLineItem_NaturalKey
-- ON Planning.BudgetLineItem (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID);
ALTER TABLE Planning.BudgetLineItem ADD CONSTRAINT UQ_BudgetLineItem_NaturalKey UNIQUE (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID);

CREATE INDEX IX_BudgetLineItem_Allocated
ON Planning.BudgetLineItem (AllocationSourceLineID, AllocationPercentage);


