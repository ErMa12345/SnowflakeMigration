/*
    CostCenter - Organizational hierarchy for cost allocation
    Dependencies: None (base table, self-referential)

    Migration Notes:
    - HIERARCHYID replaced with materialized path (VARCHAR) pattern: '/1/2/3/'
    - Computed column HierarchyLevel converted to regular column (updated via triggers/procedures)
    - Temporal table (SYSTEM_VERSIONING) replaced with Snowflake Time Travel + history pattern
    - Created separate history table for manual tracking beyond 90-day Time Travel limit
    - Added HierarchyPathLevel as regular column instead of computed
*/

CREATE OR REPLACE TABLE Planning.CostCenter (
    CostCenterID            INT AUTOINCREMENT NOT NULL,
    CostCenterCode          VARCHAR(20) NOT NULL,
    CostCenterName          VARCHAR(100) NOT NULL,
    ParentCostCenterID      INT NULL,

    -- HierarchyPath: Materialized path pattern (e.g., '/1/', '/1/2/', '/1/2/3/')
    -- This replaces SQL Server's HIERARCHYID type
    HierarchyPath           VARCHAR(500) NULL,

    -- HierarchyLevel: Count of '/' minus 1 (e.g., '/1/2/' = level 2)
    -- Was computed column in SQL Server, now updated via procedure/trigger
    HierarchyLevel          INT NULL,

    ManagerEmployeeID       INT NULL,
    DepartmentCode          VARCHAR(10) NULL,
    IsActive                BOOLEAN NOT NULL DEFAULT TRUE,
    EffectiveFromDate       DATE NOT NULL,
    EffectiveToDate         DATE NULL,
    AllocationWeight        DECIMAL(5,4) NOT NULL DEFAULT 1.0000,

    -- Temporal tracking fields (Snowflake Time Travel covers 90 days, manual history for longer)
    ValidFrom               TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ValidTo                 TIMESTAMP_NTZ NULL DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),

    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ModifiedDateTime        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_CostCenter PRIMARY KEY (CostCenterID),
    CONSTRAINT UQ_CostCenter_Code UNIQUE (CostCenterCode),
    CONSTRAINT FK_CostCenter_Parent FOREIGN KEY (ParentCostCenterID)
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT CK_CostCenter_Weight CHECK (AllocationWeight BETWEEN 0 AND 1)
);

-- Clustering key for hierarchy queries
ALTER TABLE Planning.CostCenter CLUSTER BY (HierarchyPath);

-- History table for long-term temporal tracking (beyond 90-day Time Travel)
CREATE TABLE IF NOT EXISTS Planning.CostCenterHistory (
    HistoryID               INT AUTOINCREMENT NOT NULL,
    CostCenterID            INT NOT NULL,
    CostCenterCode          VARCHAR(20) NOT NULL,
    CostCenterName          VARCHAR(100) NOT NULL,
    ParentCostCenterID      INT NULL,
    HierarchyPath           VARCHAR(500) NULL,
    HierarchyPathLevel      INT NULL,
    ManagerEmployeeID       INT NULL,
    DepartmentCode          VARCHAR(10) NULL,
    IsActive                BOOLEAN NOT NULL,
    EffectiveFromDate       DATE NOT NULL,
    EffectiveToDate         DATE NULL,
    AllocationWeight        DECIMAL(5,4) NOT NULL,
    ValidFrom               TIMESTAMP_NTZ NOT NULL,
    ValidTo                 TIMESTAMP_NTZ NOT NULL,
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL,
    ModifiedDateTime        TIMESTAMP_NTZ NOT NULL,

    CONSTRAINT PK_CostCenterHistory PRIMARY KEY (HistoryID)
);

