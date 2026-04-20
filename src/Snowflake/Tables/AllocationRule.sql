/*
    AllocationRule - Rules for cost allocation across cost centers
    Dependencies: CostCenter, GLAccount

    MIGRATION NOTES:
    -----------------
    1. IDENTITY(1,1) → AUTOINCREMENT for AllocationRuleID
    2. NVARCHAR → VARCHAR for RuleName and RuleDescription
    3. BIT → BOOLEAN for IsActive
    4. DATETIME2(7) → TIMESTAMP_NTZ for CreatedDateTime and ModifiedDateTime
    5. SYSUTCDATETIME() → CURRENT_TIMESTAMP() for default timestamps
    6. XML → VARIANT for TargetSpecification
       - SQL Server uses XML type for complex hierarchical data
       - Snowflake uses VARIANT with JSON format for semi-structured data
       - Will need to convert XML data to JSON during migration
       - Use PARSE_JSON() for inserting JSON data
    7. PRIMARY XML INDEX removed
       - Snowflake doesn't have XML indexes
       - VARIANT columns are automatically optimized for nested data access
       - Can query JSON using dot notation: TargetSpecification:targets[0].costCenter
    8. NONCLUSTERED/CLUSTERED keywords removed
    9. Added clustering key on (EffectiveFromDate, ExecutionSequence)
       - Common query pattern for active rules ordered by execution sequence
       - Improves performance for allocation processing queries
*/

CREATE TABLE Planning.AllocationRule (
    AllocationRuleID        INT AUTOINCREMENT NOT NULL,
    RuleCode                VARCHAR(30) NOT NULL,
    RuleName                VARCHAR(100) NOT NULL,  -- Was NVARCHAR
    RuleDescription         VARCHAR(500) NULL,       -- Was NVARCHAR
    RuleType                VARCHAR(20) NOT NULL,    -- DIRECT, STEP_DOWN, RECIPROCAL, ACTIVITY_BASED
    AllocationMethod        VARCHAR(20) NOT NULL,    -- FIXED_PCT, HEADCOUNT, SQUARE_FOOTAGE, REVENUE, CUSTOM

    -- Source specification
    SourceCostCenterID      INT NULL,                -- NULL means all cost centers matching pattern
    SourceCostCenterPattern VARCHAR(50) NULL,        -- Regex pattern for cost center matching
    SourceAccountPattern    VARCHAR(50) NULL,        -- Regex pattern for account matching

    -- Target specification using VARIANT for flexibility
    -- Was XML in SQL Server, now VARIANT (JSON) in Snowflake
    -- Example JSON structure:
    -- {
    --   "targets": [
    --     {"costCenterID": 101, "percentage": 30.5},
    --     {"costCenterID": 102, "percentage": 69.5}
    --   ],
    --   "constraints": {"minAmount": 1000.00}
    -- }
    TargetSpecification     VARIANT NOT NULL,

    -- Calculation parameters
    AllocationBasis         VARCHAR(30) NULL,
    AllocationPercentage    DECIMAL(8,6) NULL,
    RoundingMethod          VARCHAR(10) NOT NULL DEFAULT 'NEAREST',  -- NEAREST, UP, DOWN, NONE
    RoundingPrecision       TINYINT NOT NULL DEFAULT 2,
    MinimumAmount           DECIMAL(19,4) NULL,

    -- Execution order for step-down allocations
    ExecutionSequence       INT NOT NULL DEFAULT 100,
    DependsOnRuleID         INT NULL,

    -- Validity
    EffectiveFromDate       DATE NOT NULL,
    EffectiveToDate         DATE NULL,
    IsActive                BOOLEAN NOT NULL DEFAULT TRUE,  -- Was BIT

    -- Audit
    CreatedByUserID         INT NULL,
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),  -- Was DATETIME2(7) with SYSUTCDATETIME()
    ModifiedByUserID        INT NULL,
    ModifiedDateTime        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),  -- Was DATETIME2(7) with SYSUTCDATETIME()

    CONSTRAINT PK_AllocationRule PRIMARY KEY (AllocationRuleID),
    CONSTRAINT UQ_AllocationRule_Code UNIQUE (RuleCode),
    CONSTRAINT FK_AllocationRule_SourceCC FOREIGN KEY (SourceCostCenterID)
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT FK_AllocationRule_DependsOn FOREIGN KEY (DependsOnRuleID)
        REFERENCES Planning.AllocationRule (AllocationRuleID),
    CONSTRAINT CK_AllocationRule_Type CHECK (RuleType IN ('DIRECT','STEP_DOWN','RECIPROCAL','ACTIVITY_BASED')),
    CONSTRAINT CK_AllocationRule_Rounding CHECK (RoundingMethod IN ('NEAREST','UP','DOWN','NONE'))
);


