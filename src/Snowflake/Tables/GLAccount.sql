/*
    GLAccount - General Ledger Account master
    Dependencies: None (base table, self-referential)

    Migration Notes:
    - SPARSE columns removed (not supported in Snowflake, but no performance penalty without it)
    - Columnstore index removed (Snowflake stores all data in columnar format automatically)
    - SYSUTCDATETIME() replaced with CURRENT_TIMESTAMP()
    - BIT replaced with BOOLEAN
*/

CREATE TABLE IF NOT EXISTS Planning.GLAccount (
    GLAccountID             INT AUTOINCREMENT NOT NULL,
    AccountNumber           VARCHAR(20) NOT NULL,
    AccountName             VARCHAR(150) NOT NULL,
    AccountType             CHAR(1) NOT NULL,  -- A=Asset, L=Liability, E=Equity, R=Revenue, X=Expense
    AccountSubType          VARCHAR(30) NULL,
    ParentAccountID         INT NULL,
    AccountLevel            TINYINT NOT NULL DEFAULT 1,
    IsPostable              BOOLEAN NOT NULL DEFAULT TRUE,
    IsBudgetable            BOOLEAN NOT NULL DEFAULT TRUE,
    IsStatistical           BOOLEAN NOT NULL DEFAULT FALSE,
    NormalBalance           CHAR(1) NOT NULL DEFAULT 'D',  -- D=Debit, C=Credit
    CurrencyCode            CHAR(3) NOT NULL DEFAULT 'USD',
    ConsolidationAccountID  INT NULL,
    IntercompanyFlag        BOOLEAN NOT NULL DEFAULT FALSE,
    IsActive                BOOLEAN NOT NULL DEFAULT TRUE,
    CreatedDateTime         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ModifiedDateTime        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    -- Sparse columns - SPARSE keyword removed (not needed in Snowflake)
    TaxCode                 VARCHAR(20) NULL,
    StatutoryAccountCode    VARCHAR(30) NULL,
    IFRSAccountCode         VARCHAR(30) NULL,

    CONSTRAINT PK_GLAccount PRIMARY KEY (GLAccountID),
    CONSTRAINT UQ_GLAccount_Number UNIQUE (AccountNumber),
    CONSTRAINT FK_GLAccount_Parent FOREIGN KEY (ParentAccountID)
        REFERENCES Planning.GLAccount (GLAccountID),
    CONSTRAINT CK_GLAccount_Type CHECK (AccountType IN ('A','L','E','R','X')),
    CONSTRAINT CK_GLAccount_Balance CHECK (NormalBalance IN ('D','C'))
);

ALTER TABLE Planning.GLAccount CLUSTER BY (AccountType, AccountNumber);
