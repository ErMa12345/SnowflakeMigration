/*
    Migration Notes:
    - IDENTITY replaced with AUTOINCREMENT
    - ROWVERSION replaced with VERSION column using sequence
    - SYSUTCDATETIME() replaced with CURRENT_TIMESTAMP()
    - Filtered indexes removed (not supported in Snowflake)
    - INCLUDE columns removed (not needed in Snowflake)
    - Clustering key added for performance on date queries
*/

-- Create sequence for version tracking (replacement for ROWVERSION)
CREATE SEQUENCE IF NOT EXISTS Planning.seq_FiscalPeriod_Version START = 1 INCREMENT = 1;

CREATE TABLE IF NOT EXISTS Planning.FiscalPeriod (
    FiscalPeriodID          INT AUTOINCREMENT NOT NULL,
    FiscalYear              SMALLINT NOT NULL,
    FiscalQuarter           TINYINT NOT NULL,
    FiscalMonth             TINYINT NOT NULL,
    PeriodName              VARCHAR(50) NOT NULL,
    PeriodStartDate         DATE NOT NULL,
    PeriodEndDate           DATE NOT NULL,
    IsClosed                BOOLEAN NOT NULL DEFAULT FALSE,
    ClosedByUserID          INT NULL,
    ClosedDateTime          TIMESTAMP_NTZ NULL,
    IsAdjustmentPeriod      BOOLEAN NOT NULL DEFAULT FALSE,
    WorkingDays             TINYINT NULL,
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ModifiedDateTime        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    RowVersionStamp         INT NOT NULL DEFAULT Planning.seq_FiscalPeriod_Version.NEXTVAL,
    CONSTRAINT PK_FiscalPeriod PRIMARY KEY (FiscalPeriodID),
    CONSTRAINT UQ_FiscalPeriod_YearMonth UNIQUE (FiscalYear, FiscalMonth),
    CONSTRAINT CK_FiscalPeriod_Quarter CHECK (FiscalQuarter BETWEEN 1 AND 4),
    CONSTRAINT CK_FiscalPeriod_Month CHECK (FiscalMonth BETWEEN 1 AND 13), -- 13 for adjustment period
    CONSTRAINT CK_FiscalPeriod_DateRange CHECK (PeriodEndDate >= PeriodStartDate)
);

ALTER TABLE Planning.FiscalPeriod CLUSTER BY (PeriodStartDate, PeriodEndDate);


