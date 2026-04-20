/*
    BudgetHeader - Budget version and scenario header
    Dependencies: FiscalPeriod

    Migration Notes:
    - Computed persisted column (IsLocked) converted to regular column updated via trigger/procedure
    - XML column (ExtendedProperties) replaced with VARIANT type
    - XML indexes removed (use VARIANT, access with colon notation)
    - SYSUTCDATETIME() replaced with CURRENT_TIMESTAMP()
*/

CREATE TABLE IF NOT EXISTS Planning.BudgetHeader (
    BudgetHeaderID          INT AUTOINCREMENT NOT NULL,
    BudgetCode              VARCHAR(30) NOT NULL,
    BudgetName              VARCHAR(100) NOT NULL,
    BudgetType              VARCHAR(20) NOT NULL,  -- ANNUAL, QUARTERLY, ROLLING, FORECAST
    ScenarioType            VARCHAR(20) NOT NULL,  -- BASE, OPTIMISTIC, PESSIMISTIC, STRETCH
    FiscalYear              SMALLINT NOT NULL,
    StartPeriodID           INT NOT NULL,
    EndPeriodID             INT NOT NULL,
    BaseBudgetHeaderID      INT NULL,  -- For variance calculations
    StatusCode              VARCHAR(15) NOT NULL DEFAULT 'DRAFT',
    SubmittedByUserID       INT NULL,
    SubmittedDateTime       TIMESTAMP_NTZ NULL,
    ApprovedByUserID        INT NULL,
    ApprovedDateTime        TIMESTAMP_NTZ NULL,
    LockedDateTime          TIMESTAMP_NTZ NULL,
    -- Apparently snowflake virtual columns are not persistent, use this instead and edit in procedures
    IsLocked                BOOLEAN NOT NULL DEFAULT FALSE,
    VersionNumber           INT NOT NULL DEFAULT 1,
    Notes                   VARCHAR NULL,
    -- Store JSON format: {"department": "Finance", "region": "EMEA"}
    ExtendedProperties      VARIANT NULL,
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ModifiedDateTime        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_BudgetHeader PRIMARY KEY (BudgetHeaderID),
    CONSTRAINT UQ_BudgetHeader_Code_Year UNIQUE (BudgetCode, FiscalYear, VersionNumber),
    CONSTRAINT FK_BudgetHeader_StartPeriod FOREIGN KEY (StartPeriodID)
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetHeader_EndPeriod FOREIGN KEY (EndPeriodID)
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_BudgetHeader_BaseBudget FOREIGN KEY (BaseBudgetHeaderID)
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT CK_BudgetHeader_Status CHECK (StatusCode IN ('DRAFT','SUBMITTED','APPROVED','REJECTED','LOCKED','ARCHIVED'))
);



