CREATE OR REPLACE PROCEDURE Planning.usp_BulkImportBudgetData(
    FILE_PATH VARCHAR,                    -- Path to staged file (e.g., '@BUDGET_IMPORT_STAGE/file.csv')
    TARGET_BUDGET_HEADER_ID INT,
    VALIDATION_MODE VARCHAR DEFAULT 'STRICT',     -- STRICT, LENIENT, NONE
    DUPLICATE_HANDLING VARCHAR DEFAULT 'REJECT',  -- REJECT, UPDATE, SKIP
    BATCH_SIZE INT DEFAULT 10000
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    START_TIME TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    IMPORT_BATCH_ID VARCHAR := UUID_STRING();
    TOTAL_ROWS INT := 0;
    VALID_ROWS INT := 0;
    INVALID_ROWS INT := 0;
    ROWS_IMPORTED INT := 0;
    ROWS_REJECTED INT := 0;
    PROCESSED_BATCHES INT := 0;
    ERROR_MESSAGE VARCHAR := NULL;
    RESULT_JSON VARCHAR;
BEGIN

    -- =====================================================================
    -- Step 1: Create temporary staging tables
    -- =====================================================================

    -- Staging table for imported data
    CREATE OR REPLACE TEMPORARY TABLE IMPORT_STAGING (
        ROW_ID INT AUTOINCREMENT,
        GL_ACCOUNT_ID INT,
        ACCOUNT_NUMBER VARCHAR(20),
        COST_CENTER_ID INT,
        COST_CENTER_CODE VARCHAR(20),
        FISCAL_PERIOD_ID INT,
        FISCAL_YEAR SMALLINT,
        FISCAL_MONTH TINYINT,
        ORIGINAL_AMOUNT DECIMAL(19,4),
        ADJUSTED_AMOUNT DECIMAL(19,4),
        SPREAD_METHOD_CODE VARCHAR(10),
        NOTES VARCHAR(500),
        -- Validation tracking
        IS_VALID BOOLEAN DEFAULT TRUE,
        VALIDATION_ERRORS VARCHAR,
        -- Processing tracking
        IS_PROCESSED BOOLEAN DEFAULT FALSE,
        PROCESSED_DATETIME TIMESTAMP_NTZ,
        RESULT_LINE_ITEM_ID BIGINT
    );

    -- Error tracking table
    CREATE OR REPLACE TEMPORARY TABLE IMPORT_ERRORS (
        ERROR_ID INT AUTOINCREMENT,
        ROW_ID INT,
        ERROR_CODE VARCHAR(20),
        ERROR_MESSAGE VARCHAR(500),
        COLUMN_NAME VARCHAR(128),
        ORIGINAL_VALUE VARCHAR(500),
        SEVERITY VARCHAR(10)  -- ERROR, WARNING
    );

    -- =====================================================================
    -- Step 2: Load data from CSV file using COPY INTO
    -- =====================================================================

    BEGIN
        EXECUTE IMMEDIATE '
            COPY INTO IMPORT_STAGING (
                ACCOUNT_NUMBER,
                COST_CENTER_CODE,
                FISCAL_YEAR,
                FISCAL_MONTH,
                ORIGINAL_AMOUNT,
                ADJUSTED_AMOUNT,
                SPREAD_METHOD_CODE,
                NOTES
            )
            FROM ' || :FILE_PATH || '
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_DELIMITER = '','',
                RECORD_DELIMITER = ''\\n'',
                SKIP_HEADER = 1,
                FIELD_OPTIONALLY_ENCLOSED_BY = ''"'',
                TRIM_SPACE = TRUE,
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,
                EMPTY_FIELD_AS_NULL = TRUE
            )
            ON_ERROR = CONTINUE
            RETURN_FAILED_ONLY = FALSE;
        ';

        SELECT COUNT(*) INTO :TOTAL_ROWS FROM IMPORT_STAGING;

    EXCEPTION
        WHEN OTHER THEN
            ERROR_MESSAGE := 'Failed to load CSV file: ' || SQLERRM;
            RETURN OBJECT_CONSTRUCT(
                'success', FALSE,
                'error', :ERROR_MESSAGE,
                'rows_imported', 0,
                'rows_rejected', 0
            )::VARCHAR;
    END;

    -- =====================================================================
    -- Step 3: Resolve lookups (IDs from codes)
    -- =====================================================================

    -- Resolve GLAccountID from AccountNumber
    UPDATE IMPORT_STAGING stg
    SET GL_ACCOUNT_ID = gla.GLAccountID
    FROM Planning.GLAccount gla
    WHERE stg.ACCOUNT_NUMBER = gla.AccountNumber
      AND stg.GL_ACCOUNT_ID IS NULL
      AND stg.ACCOUNT_NUMBER IS NOT NULL;

    -- Resolve CostCenterID from CostCenterCode
    UPDATE IMPORT_STAGING stg
    SET COST_CENTER_ID = cc.CostCenterID
    FROM Planning.CostCenter cc
    WHERE stg.COST_CENTER_CODE = cc.CostCenterCode
      AND stg.COST_CENTER_ID IS NULL
      AND stg.COST_CENTER_CODE IS NOT NULL;

    -- Resolve FiscalPeriodID from Year/Month
    UPDATE IMPORT_STAGING stg
    SET FISCAL_PERIOD_ID = fp.FiscalPeriodID
    FROM Planning.FiscalPeriod fp
    WHERE stg.FISCAL_YEAR = fp.FiscalYear
      AND stg.FISCAL_MONTH = fp.FiscalMonth
      AND stg.FISCAL_PERIOD_ID IS NULL
      AND stg.FISCAL_YEAR IS NOT NULL
      AND stg.FISCAL_MONTH IS NOT NULL;

    -- =====================================================================
    -- Step 4: Validate data
    -- =====================================================================

    IF (VALIDATION_MODE <> 'NONE') THEN

        -- Check for missing required fields
        INSERT INTO IMPORT_ERRORS (ROW_ID, ERROR_CODE, ERROR_MESSAGE, COLUMN_NAME, SEVERITY)
        SELECT
            ROW_ID,
            'MISSING_ACCOUNT',
            'GL Account not found or not specified',
            'GL_ACCOUNT_ID',
            CASE WHEN :VALIDATION_MODE = 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
        FROM IMPORT_STAGING
        WHERE GL_ACCOUNT_ID IS NULL;

        INSERT INTO IMPORT_ERRORS (ROW_ID, ERROR_CODE, ERROR_MESSAGE, COLUMN_NAME, SEVERITY)
        SELECT
            ROW_ID,
            'MISSING_COSTCENTER',
            'Cost Center not found or not specified',
            'COST_CENTER_ID',
            CASE WHEN :VALIDATION_MODE = 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
        FROM IMPORT_STAGING
        WHERE COST_CENTER_ID IS NULL;

        INSERT INTO IMPORT_ERRORS (ROW_ID, ERROR_CODE, ERROR_MESSAGE, COLUMN_NAME, SEVERITY)
        SELECT
            ROW_ID,
            'MISSING_PERIOD',
            'Fiscal Period not found or not specified',
            'FISCAL_PERIOD_ID',
            CASE WHEN :VALIDATION_MODE = 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
        FROM IMPORT_STAGING
        WHERE FISCAL_PERIOD_ID IS NULL;

        -- Check for invalid amounts
        INSERT INTO IMPORT_ERRORS (ROW_ID, ERROR_CODE, ERROR_MESSAGE, COLUMN_NAME, SEVERITY)
        SELECT ROW_ID, 'INVALID_AMOUNT', 'Amount is NULL', 'ORIGINAL_AMOUNT', 'ERROR'
        FROM IMPORT_STAGING
        WHERE ORIGINAL_AMOUNT IS NULL;

        -- Check account is budgetable
        INSERT INTO IMPORT_ERRORS (ROW_ID, ERROR_CODE, ERROR_MESSAGE, COLUMN_NAME, SEVERITY)
        SELECT stg.ROW_ID, 'NON_BUDGETABLE', 'Account is not marked as budgetable', 'GL_ACCOUNT_ID', 'WARNING'
        FROM IMPORT_STAGING stg
        INNER JOIN Planning.GLAccount gla ON stg.GL_ACCOUNT_ID = gla.GLAccountID
        WHERE gla.IsBudgetable = FALSE;

        -- Check cost center is active
        INSERT INTO IMPORT_ERRORS (ROW_ID, ERROR_CODE, ERROR_MESSAGE, COLUMN_NAME, SEVERITY)
        SELECT
            stg.ROW_ID,
            'INACTIVE_CC',
            'Cost Center is inactive',
            'COST_CENTER_ID',
            CASE WHEN :VALIDATION_MODE = 'STRICT' THEN 'ERROR' ELSE 'WARNING' END
        FROM IMPORT_STAGING stg
        INNER JOIN Planning.CostCenter cc ON stg.COST_CENTER_ID = cc.CostCenterID
        WHERE cc.IsActive = FALSE;

        -- Check period is not closed
        INSERT INTO IMPORT_ERRORS (ROW_ID, ERROR_CODE, ERROR_MESSAGE, COLUMN_NAME, SEVERITY)
        SELECT stg.ROW_ID, 'CLOSED_PERIOD', 'Fiscal period is closed', 'FISCAL_PERIOD_ID', 'ERROR'
        FROM IMPORT_STAGING stg
        INNER JOIN Planning.FiscalPeriod fp ON stg.FISCAL_PERIOD_ID = fp.FiscalPeriodID
        WHERE fp.IsClosed = TRUE;

        -- Check for duplicates within import
        INSERT INTO IMPORT_ERRORS (ROW_ID, ERROR_CODE, ERROR_MESSAGE, SEVERITY)
        SELECT
            ROW_ID,
            'DUPLICATE_IN_BATCH',
            'Duplicate entry within import batch',
            'WARNING'
        FROM (
            SELECT
                ROW_ID,
                ROW_NUMBER() OVER (
                    PARTITION BY GL_ACCOUNT_ID, COST_CENTER_ID, FISCAL_PERIOD_ID
                    ORDER BY ROW_ID
                ) AS ROW_NUM
            FROM IMPORT_STAGING
            WHERE GL_ACCOUNT_ID IS NOT NULL
              AND COST_CENTER_ID IS NOT NULL
              AND FISCAL_PERIOD_ID IS NOT NULL
        )
        WHERE ROW_NUM > 1;

        -- Check for existing records in target
        INSERT INTO IMPORT_ERRORS (ROW_ID, ERROR_CODE, ERROR_MESSAGE, SEVERITY)
        SELECT
            stg.ROW_ID,
            'ALREADY_EXISTS',
            'Record already exists in target budget',
            CASE WHEN :DUPLICATE_HANDLING = 'REJECT' THEN 'ERROR' ELSE 'WARNING' END
        FROM IMPORT_STAGING stg
        INNER JOIN Planning.BudgetLineItem bli
            ON stg.GL_ACCOUNT_ID = bli.GLAccountID
            AND stg.COST_CENTER_ID = bli.CostCenterID
            AND stg.FISCAL_PERIOD_ID = bli.FiscalPeriodID
        WHERE bli.BudgetHeaderID = :TARGET_BUDGET_HEADER_ID;

        -- Aggregate validation errors to staging table
        UPDATE IMPORT_STAGING stg
        SET
            IS_VALID = CASE
                WHEN EXISTS (
                    SELECT 1 FROM IMPORT_ERRORS e
                    WHERE e.ROW_ID = stg.ROW_ID AND e.SEVERITY = 'ERROR'
                ) THEN FALSE
                ELSE TRUE
            END,
            VALIDATION_ERRORS = (
                SELECT LISTAGG(ERROR_CODE || ': ' || ERROR_MESSAGE, '; ')
                FROM IMPORT_ERRORS e
                WHERE e.ROW_ID = stg.ROW_ID
            )
        WHERE ROW_ID = stg.ROW_ID;

    END IF;

    -- Count valid/invalid
    SELECT
        SUM(CASE WHEN IS_VALID = TRUE THEN 1 ELSE 0 END),
        SUM(CASE WHEN IS_VALID = FALSE THEN 1 ELSE 0 END)
    INTO :VALID_ROWS, :INVALID_ROWS
    FROM IMPORT_STAGING;

    -- =====================================================================
    -- Step 5: Process imports based on duplicate handling
    -- =====================================================================

    IF (DUPLICATE_HANDLING = 'UPDATE') THEN

        -- Use MERGE for upsert
        MERGE INTO Planning.BudgetLineItem AS target
        USING (
            SELECT
                :TARGET_BUDGET_HEADER_ID AS BudgetHeaderID,
                GL_ACCOUNT_ID AS GLAccountID,
                COST_CENTER_ID AS CostCenterID,
                FISCAL_PERIOD_ID AS FiscalPeriodID,
                ORIGINAL_AMOUNT AS OriginalAmount,
                COALESCE(ADJUSTED_AMOUNT, 0) AS AdjustedAmount,
                ORIGINAL_AMOUNT + COALESCE(ADJUSTED_AMOUNT, 0) AS FinalAmount,
                SPREAD_METHOD_CODE AS SpreadMethodCode,
                'BULK_IMPORT' AS SourceSystem,
                :IMPORT_BATCH_ID AS SourceReference,
                :IMPORT_BATCH_ID AS ImportBatchID
            FROM IMPORT_STAGING
            WHERE IS_VALID = TRUE
        ) AS source
        ON target.BudgetHeaderID = source.BudgetHeaderID
           AND target.GLAccountID = source.GLAccountID
           AND target.CostCenterID = source.CostCenterID
           AND target.FiscalPeriodID = source.FiscalPeriodID
        WHEN MATCHED THEN
            UPDATE SET
                OriginalAmount = source.OriginalAmount,
                AdjustedAmount = source.AdjustedAmount,
                FinalAmount = source.FinalAmount,
                SpreadMethodCode = source.SpreadMethodCode,
                SourceReference = source.SourceReference,
                LastModifiedDateTime = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
                OriginalAmount, AdjustedAmount, FinalAmount, SpreadMethodCode,
                SourceSystem, SourceReference, ImportBatchID, LastModifiedDateTime
            )
            VALUES (
                source.BudgetHeaderID, source.GLAccountID, source.CostCenterID,
                source.FiscalPeriodID, source.OriginalAmount, source.AdjustedAmount,
                source.FinalAmount, source.SpreadMethodCode, source.SourceSystem,
                source.SourceReference, source.ImportBatchID, CURRENT_TIMESTAMP()
            );

        SELECT COUNT(*) INTO :ROWS_IMPORTED FROM IMPORT_STAGING WHERE IS_VALID = TRUE;

    ELSE  -- SKIP or REJECT duplicates

        -- Insert only non-duplicate records
        INSERT INTO Planning.BudgetLineItem (
            BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
            OriginalAmount, AdjustedAmount, FinalAmount, SpreadMethodCode,
            SourceSystem, SourceReference, ImportBatchID, LastModifiedDateTime
        )
        SELECT
            :TARGET_BUDGET_HEADER_ID,
            GL_ACCOUNT_ID,
            COST_CENTER_ID,
            FISCAL_PERIOD_ID,
            ORIGINAL_AMOUNT,
            COALESCE(ADJUSTED_AMOUNT, 0),
            ORIGINAL_AMOUNT + COALESCE(ADJUSTED_AMOUNT, 0),
            SPREAD_METHOD_CODE,
            'BULK_IMPORT',
            :IMPORT_BATCH_ID,
            :IMPORT_BATCH_ID,
            CURRENT_TIMESTAMP()
        FROM IMPORT_STAGING stg
        WHERE IS_VALID = TRUE
          AND (:DUPLICATE_HANDLING = 'REJECT'
               OR NOT EXISTS (
                   SELECT 1 FROM Planning.BudgetLineItem bli
                   WHERE bli.BudgetHeaderID = :TARGET_BUDGET_HEADER_ID
                     AND bli.GLAccountID = stg.GL_ACCOUNT_ID
                     AND bli.CostCenterID = stg.COST_CENTER_ID
                     AND bli.FiscalPeriodID = stg.FISCAL_PERIOD_ID
               ));

        ROWS_IMPORTED := SQLROWCOUNT;

    END IF;

    -- Set output parameters
    SELECT
        SUM(CASE WHEN IS_VALID = FALSE THEN 1 ELSE 0 END)
    INTO :ROWS_REJECTED
    FROM IMPORT_STAGING;

    -- =====================================================================
    -- Step 6: Build results JSON
    -- =====================================================================

    LET error_summary VARIANT := (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
            'error_code', ERROR_CODE,
            'count', CNT,
            'max_severity', MAX_SEVERITY
        ))
        FROM (
            SELECT
                ERROR_CODE,
                COUNT(*) AS CNT,
                MAX(SEVERITY) AS MAX_SEVERITY
            FROM IMPORT_ERRORS
            GROUP BY ERROR_CODE
        )
    );

    LET rejected_sample VARIANT := (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
            'row_id', ROW_ID,
            'account', ACCOUNT_NUMBER,
            'cost_center', COST_CENTER_CODE,
            'year', FISCAL_YEAR,
            'month', FISCAL_MONTH,
            'amount', ORIGINAL_AMOUNT,
            'errors', VALIDATION_ERRORS
        ))
        FROM (
            SELECT *
            FROM IMPORT_STAGING
            WHERE IS_VALID = FALSE
            ORDER BY ROW_ID
            LIMIT 100
        )
    );

    RESULT_JSON := OBJECT_CONSTRUCT(
        'success', TRUE,
        'batch_id', :IMPORT_BATCH_ID,
        'source', 'FILE',
        'target_budget_id', :TARGET_BUDGET_HEADER_ID,
        'duration_ms', DATEDIFF(MILLISECOND, :START_TIME, CURRENT_TIMESTAMP()),
        'summary', OBJECT_CONSTRUCT(
            'total_rows', :TOTAL_ROWS,
            'valid_rows', :VALID_ROWS,
            'invalid_rows', :INVALID_ROWS,
            'imported_rows', :ROWS_IMPORTED,
            'rejected_rows', :ROWS_REJECTED
        ),
        'error_summary', :error_summary,
        'rejected_sample', :rejected_sample
    )::VARCHAR;

    -- Cleanup temp tables
    DROP TABLE IF EXISTS IMPORT_STAGING;
    DROP TABLE IF EXISTS IMPORT_ERRORS;

    RETURN :RESULT_JSON;

EXCEPTION
    WHEN OTHER THEN
        ERROR_MESSAGE := SQLERRM;
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'error', :ERROR_MESSAGE,
            'rows_imported', 0,
            'rows_rejected', COALESCE(:TOTAL_ROWS, 0)
        )::VARCHAR;
END;
$$;
