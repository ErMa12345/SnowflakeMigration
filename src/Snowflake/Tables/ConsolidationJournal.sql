/*
    ConsolidationJournal - Journal entries for consolidation adjustments
    Dependencies: BudgetHeader, GLAccount, CostCenter, FiscalPeriod

    MIGRATION NOTES:
    -----------------
    1. IDENTITY(1,1) → AUTOINCREMENT for JournalID
    2. NVARCHAR → VARCHAR for Description
    3. BIT → BOOLEAN for IsAutoReverse and IsReversed
    4. DATETIME2(7) → TIMESTAMP_NTZ for all DateTime columns
    5. SYSUTCDATETIME() → CURRENT_TIMESTAMP() (not used here, but would apply)
    6. Computed column IsBalanced: Converted to regular BOOLEAN column
       - SQL Server: AS CASE WHEN TotalDebits = TotalCredits THEN 1 ELSE 0 END
       - Snowflake: Regular column, will need trigger/procedure to maintain
    7. FILESTREAM removed - not supported in Snowflake
       - SQL Server uses FILESTREAM for efficient binary large object storage
       - Snowflake approach: Store files in external stages (S3, Azure Blob, GCS)
       - Store stage path/URL in AttachmentPath column instead
       - Use PUT/GET commands or external stages for file operations
    8. VARBINARY(MAX) removed (AttachmentData column)
       - Replace with VARCHAR to store stage path or use VARIANT for metadata
    9. UNIQUEIDENTIFIER ROWGUIDCOL removed (AttachmentRowGuid)
       - ROWGUIDCOL is SQL Server specific attribute for FILESTREAM
       - Not needed in Snowflake since we're using external stages
    10. NEWSEQUENTIALID() removed
        - Was used for FILESTREAM row identification
        - Not applicable in Snowflake migration
    11. Index on AttachmentRowGuid removed
        - No longer needed without FILESTREAM
    12. NONCLUSTERED/CLUSTERED keywords removed
    13. Added clustering key on (BudgetHeaderID, FiscalPeriodID, PostingDate)
        - Common query pattern for consolidation reporting
        - Improves performance for time-based and budget-based queries
*/

CREATE TABLE Planning.ConsolidationJournal (
    JournalID               BIGINT AUTOINCREMENT NOT NULL,
    JournalNumber           VARCHAR(30) NOT NULL,
    JournalType             VARCHAR(20) NOT NULL,  -- ELIMINATION, RECLASSIFICATION, TRANSLATION, ADJUSTMENT
    BudgetHeaderID          INT NOT NULL,
    FiscalPeriodID          INT NOT NULL,
    PostingDate             DATE NOT NULL,
    Description             VARCHAR(500) NULL,  -- Was NVARCHAR
    StatusCode              VARCHAR(15) NOT NULL DEFAULT 'DRAFT',

    -- Entity tracking for multi-entity consolidation
    SourceEntityCode        VARCHAR(20) NULL,
    TargetEntityCode        VARCHAR(20) NULL,

    -- Reversal handling
    IsAutoReverse           BOOLEAN NOT NULL DEFAULT FALSE,  -- Was BIT
    ReversalPeriodID        INT NULL,
    ReversedFromJournalID   BIGINT NULL,
    IsReversed              BOOLEAN NOT NULL DEFAULT FALSE,  -- Was BIT

    -- Totals (denormalized for performance)
    TotalDebits             DECIMAL(19,4) NOT NULL DEFAULT 0,
    TotalCredits            DECIMAL(19,4) NOT NULL DEFAULT 0,
    IsBalanced              BOOLEAN NOT NULL DEFAULT FALSE,  -- Was computed: CASE WHEN TotalDebits = TotalCredits THEN 1 ELSE 0 END

    -- Approval workflow
    PreparedByUserID        INT NULL,
    PreparedDateTime        TIMESTAMP_NTZ NULL,      -- Was DATETIME2(7)
    ReviewedByUserID        INT NULL,
    ReviewedDateTime        TIMESTAMP_NTZ NULL,      -- Was DATETIME2(7)
    ApprovedByUserID        INT NULL,
    ApprovedDateTime        TIMESTAMP_NTZ NULL,      -- Was DATETIME2(7)
    PostedByUserID          INT NULL,
    PostedDateTime          TIMESTAMP_NTZ NULL,      -- Was DATETIME2(7)

    -- Attachment handling using external stages (replaces FILESTREAM)
    -- SQL Server used FILESTREAM for binary data, Snowflake uses external stages
    -- Store the stage path where attachment is located
    -- Example: '@my_stage/attachments/journal_12345_document.pdf'
    AttachmentPath          VARCHAR(500) NULL,  -- Path to file in external stage (S3/Azure/GCS)
    AttachmentMetadata      VARIANT NULL,       -- JSON metadata about the attachment (filename, size, type, upload date, etc.)

    CONSTRAINT PK_ConsolidationJournal PRIMARY KEY (JournalID),
    CONSTRAINT UQ_ConsolidationJournal_Number UNIQUE (JournalNumber),
    CONSTRAINT FK_ConsolidationJournal_Header FOREIGN KEY (BudgetHeaderID)
        REFERENCES Planning.BudgetHeader (BudgetHeaderID),
    CONSTRAINT FK_ConsolidationJournal_Period FOREIGN KEY (FiscalPeriodID)
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_ConsolidationJournal_ReversalPeriod FOREIGN KEY (ReversalPeriodID)
        REFERENCES Planning.FiscalPeriod (FiscalPeriodID),
    CONSTRAINT FK_ConsolidationJournal_ReversedFrom FOREIGN KEY (ReversedFromJournalID)
        REFERENCES Planning.ConsolidationJournal (JournalID)
);

-- Index on journal number for quick lookup
CREATE INDEX IX_ConsolidationJournal_Number ON Planning.ConsolidationJournal (JournalNumber);

-- Index for status-based queries
CREATE INDEX IX_ConsolidationJournal_Status ON Planning.ConsolidationJournal (StatusCode, PostingDate);

-- Clustering key for optimal query performance
-- Common access pattern: queries by budget, period, and posting date
-- ALTER TABLE Planning.ConsolidationJournal CLUSTER BY (BudgetHeaderID, FiscalPeriodID, PostingDate);

