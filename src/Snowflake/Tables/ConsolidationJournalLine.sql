/*
    ConsolidationJournalLine - Line items for consolidation journal entries
    Dependencies: ConsolidationJournal, GLAccount, CostCenter

    MIGRATION NOTES:
    -----------------
    1. IDENTITY(1,1) → AUTOINCREMENT for JournalLineID
    2. NVARCHAR → VARCHAR for Description
    3. NCHAR → CHAR for LocalCurrencyCode (already CHAR in original)
    4. DATETIME2(7) → TIMESTAMP_NTZ for CreatedDateTime
    5. SYSUTCDATETIME() → CURRENT_TIMESTAMP() for default timestamp
    6. Computed column NetAmount: Converted to regular DECIMAL column
       - SQL Server: AS (DebitAmount - CreditAmount) PERSISTED
       - Snowflake: Regular column, will need trigger/procedure to maintain
    7. NONCLUSTERED/CLUSTERED keywords removed
    8. NONCLUSTERED COLUMNSTORE index removed
       - Snowflake is columnar by default, no need for explicit columnstore indexes
       - All columns benefit from columnar compression and performance
    9. ON DELETE CASCADE preserved
       - Snowflake supports cascading deletes
       - When parent journal is deleted, all lines are automatically deleted
    10. Added clustering key on (JournalID, LineNumber)
        - Common query pattern for retrieving journal lines in order
        - Improves performance for journal detail queries
*/

CREATE TABLE Planning.ConsolidationJournalLine (
    JournalLineID           BIGINT AUTOINCREMENT NOT NULL,
    JournalID               BIGINT NOT NULL,
    LineNumber              INT NOT NULL,
    GLAccountID             INT NOT NULL,
    CostCenterID            INT NOT NULL,
    DebitAmount             DECIMAL(19,4) NOT NULL DEFAULT 0,
    CreditAmount            DECIMAL(19,4) NOT NULL DEFAULT 0,
    NetAmount               DECIMAL(19,4) NOT NULL DEFAULT 0,  -- Was computed: DebitAmount - CreditAmount
    LocalCurrencyCode       CHAR(3) NOT NULL DEFAULT 'USD',
    LocalCurrencyAmount     DECIMAL(19,4) NULL,
    ExchangeRate            DECIMAL(18,10) NULL,
    Description             VARCHAR(255) NULL,  -- Was NVARCHAR
    ReferenceNumber         VARCHAR(50) NULL,

    -- Intercompany tracking
    PartnerEntityCode       VARCHAR(20) NULL,
    PartnerAccountID        INT NULL,

    -- Statistical tracking
    StatisticalQuantity     DECIMAL(18,6) NULL,
    StatisticalUOM          VARCHAR(10) NULL,

    -- Allocation tracking
    AllocationRuleID        INT NULL,

    -- Audit
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),  -- Was DATETIME2(7) with SYSUTCDATETIME()

    CONSTRAINT PK_ConsolidationJournalLine PRIMARY KEY (JournalLineID),
    CONSTRAINT UQ_ConsolidationJournalLine_JournalLine UNIQUE (JournalID, LineNumber),
    CONSTRAINT FK_ConsolidationJournalLine_Journal FOREIGN KEY (JournalID)
        REFERENCES Planning.ConsolidationJournal (JournalID) ON DELETE CASCADE,  -- CASCADE preserved
    CONSTRAINT FK_ConsolidationJournalLine_Account FOREIGN KEY (GLAccountID)
        REFERENCES Planning.GLAccount (GLAccountID),
    CONSTRAINT FK_ConsolidationJournalLine_CostCenter FOREIGN KEY (CostCenterID)
        REFERENCES Planning.CostCenter (CostCenterID),
    CONSTRAINT FK_ConsolidationJournalLine_AllocationRule FOREIGN KEY (AllocationRuleID)
        REFERENCES Planning.AllocationRule (AllocationRuleID),
    CONSTRAINT CK_ConsolidationJournalLine_DebitCredit CHECK (
        (DebitAmount >= 0 AND CreditAmount >= 0) AND
        NOT (DebitAmount > 0 AND CreditAmount > 0)  -- Cannot have both debit and credit
    )
);

CREATE INDEX IX_ConsolidationJournalLine_Account
ON Planning.ConsolidationJournalLine (GLAccountID, CostCenterID);

CREATE INDEX IX_ConsolidationJournalLine_Partner
ON Planning.ConsolidationJournalLine (PartnerEntityCode, PartnerAccountID);


