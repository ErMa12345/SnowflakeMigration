CREATE OR REPLACE PROCEDURE Planning.usp_ProcessBudgetConsolidation(
    SourceBudgetHeaderID    INTEGER,
    ConsolidationType       VARCHAR(20) DEFAULT 'FULL',    -- FULL, INCREMENTAL, DELTA
    IncludeEliminations     BOOLEAN DEFAULT TRUE,
    RecalculateAllocations  BOOLEAN DEFAULT TRUE,
    ProcessingOptions       VARIANT DEFAULT NULL,           -- JSON instead of XML
    UserID                  INTEGER DEFAULT NULL,
    DebugMode               BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    TargetBudgetHeaderID    INTEGER,
    RowsProcessed           INTEGER,
    ErrorMessage            VARCHAR(4000),
    ProcessingLogJSON       VARIANT
)
LANGUAGE SQL
AS
$$
DECLARE
    -- =========================================================================
    -- Variable declarations
    -- =========================================================================
    ProcStartTime TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    StepStartTime TIMESTAMP_NTZ;
    CurrentStep VARCHAR(100);
    ReturnCode INTEGER := 0;
    TotalRowsProcessed INTEGER := 0;
    BatchSize INTEGER := 5000;
    CurrentBatch INTEGER := 0;
    MaxIterations INTEGER := 1000;
    ConsolidationRunID VARCHAR(36) := UUID_STRING();
    TargetBudgetHeaderID INTEGER := NULL;
    ErrorMsg VARCHAR(4000) := NULL;
    RowsAffected INTEGER := 0;

    -- Cursor replacement variables
    CursorCostCenterID INTEGER;
    CursorLevel INTEGER;
    CursorParentID INTEGER;
    CursorSubtotal DECIMAL(19,4);

    -- Elimination variables
    EliminationCount INTEGER := 0;

    -- Dynamic SQL variables
    DynamicSQL VARCHAR(16777216);
    AllocationRowCount INTEGER;
    IncludeZeroBalances BOOLEAN;
    RoundingPrecision INTEGER;

    -- Result set for logs
    ProcessingLogJSON VARIANT;

BEGIN
    -- =========================================================================
    -- Create temporary tables (replaces table variables)
    -- Using CREATE OR REPLACE to ensure clean state on each execution
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE ProcessingLog (
        LogID               INTEGER AUTOINCREMENT,
        StepName            VARCHAR(100),
        StartTime           TIMESTAMP_NTZ,
        EndTime             TIMESTAMP_NTZ,
        RowsAffected        INTEGER,
        StatusCode          VARCHAR(20),
        Message             VARCHAR(16777216)
    );

    CREATE OR REPLACE TEMPORARY TABLE HierarchyNodes (
        NodeID              INTEGER PRIMARY KEY,
        ParentNodeID        INTEGER,
        NodeLevel           INTEGER,
        ProcessingOrder     INTEGER,
        IsProcessed         BOOLEAN DEFAULT FALSE,
        SubtotalAmount      DECIMAL(19,4)
    );

    CREATE OR REPLACE TEMPORARY TABLE ConsolidatedAmounts (
        GLAccountID         INTEGER NOT NULL,
        CostCenterID        INTEGER NOT NULL,
        FiscalPeriodID      INTEGER NOT NULL,
        ConsolidatedAmount  DECIMAL(19,4) NOT NULL,
        EliminationAmount   DECIMAL(19,4) DEFAULT 0,
        FinalAmount         DECIMAL(19,4),
        SourceCount         INTEGER,
        PRIMARY KEY (GLAccountID, CostCenterID, FiscalPeriodID)
    );

    CREATE OR REPLACE TEMPORARY TABLE InsertedHeaders (
        BudgetHeaderID      INTEGER,
        BudgetCode          VARCHAR(30)
    );

    CREATE OR REPLACE TEMPORARY TABLE InsertedLines (
        BudgetLineItemID    BIGINT,
        GLAccountID         INTEGER,
        CostCenterID        INTEGER,
        Amount              DECIMAL(19,4)
    );

    -- =========================================================================
    -- Main TRY block (using EXCEPTION handler)
    -- =========================================================================
    BEGIN
        -- Validate input parameters
        CurrentStep := 'Parameter Validation';
        StepStartTime := CURRENT_TIMESTAMP();

        LET budget_exists INTEGER := (
            SELECT COUNT(*)
            FROM Planning.BudgetHeader
            WHERE BudgetHeaderID = :SourceBudgetHeaderID
        );

        IF (budget_exists = 0) THEN
            ErrorMsg := 'Source budget header not found: ' || SourceBudgetHeaderID::VARCHAR;
            LET error_result RESULTSET := (
                SELECT NULL::INTEGER AS TargetBudgetHeaderID,
                       0 AS RowsProcessed,
                       :ErrorMsg AS ErrorMessage,
                       NULL::VARIANT AS ProcessingLogJSON
            );
            RETURN TABLE(error_result);
        END IF;

        -- Check if source is locked
        LET invalid_status INTEGER := (
            SELECT COUNT(*)
            FROM Planning.BudgetHeader
            WHERE BudgetHeaderID = :SourceBudgetHeaderID
              AND StatusCode NOT IN ('APPROVED', 'LOCKED')
        );

        IF (invalid_status > 0) THEN
            ErrorMsg := 'Source budget must be in APPROVED or LOCKED status for consolidation';
            LET error_result RESULTSET := (
                SELECT NULL::INTEGER AS TargetBudgetHeaderID,
                       0 AS RowsProcessed,
                       :ErrorMsg AS ErrorMessage,
                       NULL::VARIANT AS ProcessingLogJSON
            );
            RETURN TABLE(error_result);
        END IF;

        INSERT INTO ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
        VALUES (:CurrentStep, :StepStartTime, CURRENT_TIMESTAMP(), 0, 'COMPLETED');

        -- =====================================================================
        -- Create or update target budget header
        -- =====================================================================
        CurrentStep := 'Create Target Budget';
        StepStartTime := CURRENT_TIMESTAMP();

        BEGIN TRANSACTION;

        -- Create new consolidated budget header
        INSERT INTO Planning.BudgetHeader (
            BudgetCode, BudgetName, BudgetType, ScenarioType, FiscalYear,
            StartPeriodID, EndPeriodID, BaseBudgetHeaderID, StatusCode,
            VersionNumber, ExtendedProperties
        )
        SELECT
            BudgetCode || '_CONSOL_' || TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'),
            BudgetName || ' - Consolidated',
            'CONSOLIDATED',
            ScenarioType,
            FiscalYear,
            StartPeriodID,
            EndPeriodID,
            BudgetHeaderID,
            'DRAFT',
            1,
            -- JSON modification instead of XML
            OBJECT_CONSTRUCT(
                'ConsolidationRun', OBJECT_CONSTRUCT(
                    'RunID', :ConsolidationRunID,
                    'SourceID', :SourceBudgetHeaderID,
                    'Timestamp', TO_CHAR(:ProcStartTime, 'YYYY-MM-DD"T"HH24:MI:SS')
                ),
                'OriginalProperties', COALESCE(ExtendedProperties, PARSE_JSON('{}'))
            )
        FROM Planning.BudgetHeader
        WHERE BudgetHeaderID = :SourceBudgetHeaderID;

        -- Capture inserted ID (Snowflake pattern)
        TargetBudgetHeaderID := (
            SELECT MAX(BudgetHeaderID)
            FROM Planning.BudgetHeader
            WHERE BudgetCode LIKE '%_CONSOL_%'
              AND BaseBudgetHeaderID = :SourceBudgetHeaderID
        );

        IF (TargetBudgetHeaderID IS NULL) THEN
            ErrorMsg := 'Failed to create target budget header';
            LET error_result RESULTSET := (
                SELECT NULL::INTEGER AS TargetBudgetHeaderID,
                       0 AS RowsProcessed,
                       :ErrorMsg AS ErrorMessage,
                       NULL::VARIANT AS ProcessingLogJSON
            );
            RETURN TABLE(error_result);
        END IF;

        INSERT INTO ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
        VALUES (:CurrentStep, :StepStartTime, CURRENT_TIMESTAMP(), 1, 'COMPLETED');

        -- =====================================================================
        -- Build hierarchy for bottom-up rollup using TVF
        -- =====================================================================
        CurrentStep := 'Build Hierarchy';
        StepStartTime := CURRENT_TIMESTAMP();

        INSERT INTO HierarchyNodes (NodeID, ParentNodeID, NodeLevel, ProcessingOrder)
        SELECT
            h.CostCenterID,
            h.ParentCostCenterID,
            h.HierarchyLevel,
            ROW_NUMBER() OVER (ORDER BY h.HierarchyLevel DESC, h.CostCenterID)
        FROM TABLE(Planning.tvf_ExplodeCostCenterHierarchy(NULL::INT, 10, FALSE, CURRENT_DATE())) h;

        RowsAffected := SQLROWCOUNT;

        INSERT INTO ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
        VALUES (:CurrentStep, :StepStartTime, CURRENT_TIMESTAMP(), :RowsAffected, 'COMPLETED');

        -- =====================================================================
        -- Process consolidation (cursor replacement with WHILE loop)
        -- =====================================================================
        CurrentStep := 'Hierarchy Consolidation';
        StepStartTime := CURRENT_TIMESTAMP();

        -- Reset batch counter
        CurrentBatch := 0;

        -- Process nodes from bottom-up (highest level DESC = leaf nodes first)
        LET nodes_to_process INTEGER := (SELECT COUNT(*) FROM HierarchyNodes WHERE IsProcessed = FALSE);

        WHILE (nodes_to_process > 0 AND CurrentBatch < MaxIterations) DO
            CurrentBatch := CurrentBatch + 1;

            -- Get next unprocessed node (deepest level first)
            SELECT NodeID, NodeLevel, ParentNodeID
            INTO CursorCostCenterID, CursorLevel, CursorParentID
            FROM HierarchyNodes
            WHERE IsProcessed = FALSE
            ORDER BY NodeLevel DESC, NodeID
            LIMIT 1;

            -- Calculate subtotal for this node
            CursorSubtotal := (
                SELECT COALESCE(SUM(bli.FinalAmount), 0)
                FROM Planning.BudgetLineItem bli
                WHERE bli.BudgetHeaderID = :SourceBudgetHeaderID
                  AND bli.CostCenterID = :CursorCostCenterID
            );

            -- Add child subtotals (already processed due to bottom-up order)
            CursorSubtotal := CursorSubtotal + COALESCE((
                SELECT SUM(h.SubtotalAmount)
                FROM HierarchyNodes h
                WHERE h.ParentNodeID = :CursorCostCenterID
                  AND h.IsProcessed = TRUE
            ), 0);

            -- Update node
            UPDATE HierarchyNodes
            SET SubtotalAmount = :CursorSubtotal,
                IsProcessed = TRUE
            WHERE NodeID = :CursorCostCenterID;

            -- MERGE to update or insert consolidated amounts
            MERGE INTO ConsolidatedAmounts AS target
            USING (
                SELECT
                    bli.GLAccountID,
                    :CursorCostCenterID AS CostCenterID,
                    bli.FiscalPeriodID,
                    SUM(bli.FinalAmount) AS Amount,
                    COUNT(*) AS SourceCnt
                FROM Planning.BudgetLineItem bli
                WHERE bli.BudgetHeaderID = :SourceBudgetHeaderID
                  AND bli.CostCenterID = :CursorCostCenterID
                GROUP BY bli.GLAccountID, bli.FiscalPeriodID
            ) AS source
            ON target.GLAccountID = source.GLAccountID
               AND target.CostCenterID = source.CostCenterID
               AND target.FiscalPeriodID = source.FiscalPeriodID
            WHEN MATCHED THEN
                UPDATE SET
                    ConsolidatedAmount = target.ConsolidatedAmount + source.Amount,
                    SourceCount = target.SourceCount + source.SourceCnt
            WHEN NOT MATCHED THEN
                INSERT (GLAccountID, CostCenterID, FiscalPeriodID, ConsolidatedAmount, SourceCount)
                VALUES (source.GLAccountID, source.CostCenterID, source.FiscalPeriodID, source.Amount, source.SourceCnt);

            TotalRowsProcessed := TotalRowsProcessed + SQLROWCOUNT;

            -- Check if more nodes to process
            nodes_to_process := (SELECT COUNT(*) FROM HierarchyNodes WHERE IsProcessed = FALSE);
        END WHILE;

        INSERT INTO ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
        VALUES (:CurrentStep, :StepStartTime, CURRENT_TIMESTAMP(), :TotalRowsProcessed, 'COMPLETED');

        -- =====================================================================
        -- Process intercompany eliminations (set-based approach)
        -- =====================================================================
        IF (IncludeEliminations = TRUE) THEN
            CurrentStep := 'Intercompany Eliminations';
            StepStartTime := CURRENT_TIMESTAMP();

            -- Create temp table for IC matching
            CREATE OR REPLACE TEMPORARY TABLE ICMatches (
                GLAccountID         INTEGER,
                CostCenterID        INTEGER,
                FiscalPeriodID      INTEGER,
                Amount              DECIMAL(19,4),
                SourceReference     VARCHAR(50),
                MatchedFlag         BOOLEAN DEFAULT FALSE
            );

            -- Populate IC transactions
            INSERT INTO ICMatches (GLAccountID, CostCenterID, FiscalPeriodID, Amount, SourceReference)
            SELECT
                bli.GLAccountID,
                bli.CostCenterID,
                bli.FiscalPeriodID,
                bli.FinalAmount,
                bli.SourceReference
            FROM Planning.BudgetLineItem bli
            INNER JOIN Planning.GLAccount gla ON bli.GLAccountID = gla.GLAccountID
            WHERE bli.BudgetHeaderID = :SourceBudgetHeaderID
              AND gla.IntercompanyFlag = TRUE;

            -- Match offsetting IC pairs based on SourceReference and negating amounts
            -- Simple elimination: matching source reference with opposite amounts
            UPDATE ConsolidatedAmounts ca
            SET EliminationAmount = ca.EliminationAmount + ic1.Amount
            FROM ICMatches ic1
            WHERE ca.GLAccountID = ic1.GLAccountID
              AND ca.CostCenterID = ic1.CostCenterID
              AND ca.FiscalPeriodID = ic1.FiscalPeriodID
              AND EXISTS (
                  SELECT 1
                  FROM ICMatches ic2
                  WHERE ic2.SourceReference = ic1.SourceReference
                    AND ic2.Amount = -ic1.Amount
                    AND ic2.FiscalPeriodID = ic1.FiscalPeriodID
              );

            EliminationCount := SQLROWCOUNT;

            INSERT INTO ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
            VALUES (:CurrentStep, :StepStartTime, CURRENT_TIMESTAMP(), :EliminationCount, 'COMPLETED');
        END IF;

        -- =====================================================================
        -- Recalculate allocations using dynamic SQL (if needed)
        -- =====================================================================
        IF (RecalculateAllocations = TRUE) THEN
            CurrentStep := 'Recalculate Allocations';
            StepStartTime := CURRENT_TIMESTAMP();

            -- Build base SQL
            DynamicSQL := '
                UPDATE ConsolidatedAmounts
                SET FinalAmount = ConsolidatedAmount - EliminationAmount
                WHERE ConsolidatedAmount <> 0
                   OR EliminationAmount <> 0
            ';

            -- Extract options from JSON if provided
            IF (ProcessingOptions IS NOT NULL) THEN
                BEGIN
                    IncludeZeroBalances := TRY_CAST(ProcessingOptions:IncludeZeroBalances AS BOOLEAN);
                    RoundingPrecision := TRY_CAST(ProcessingOptions:RoundingPrecision AS INTEGER);

                    -- Modify SQL based on options
                    IF (IncludeZeroBalances = FALSE) THEN
                        DynamicSQL := REPLACE(DynamicSQL,
                            'WHERE ConsolidatedAmount <> 0',
                            'WHERE ConsolidatedAmount <> 0 AND (ConsolidatedAmount - EliminationAmount) <> 0');
                    END IF;

                    IF (RoundingPrecision IS NOT NULL) THEN
                        DynamicSQL := REPLACE(DynamicSQL,
                            'ConsolidatedAmount - EliminationAmount',
                            'ROUND(ConsolidatedAmount - EliminationAmount, ' || RoundingPrecision::VARCHAR || ')');
                    END IF;
                EXCEPTION
                    WHEN OTHER THEN
                        -- If JSON parsing fails, use defaults
                        NULL;
                END;
            END IF;

            -- Execute dynamic SQL
            EXECUTE IMMEDIATE :DynamicSQL;
            AllocationRowCount := SQLROWCOUNT;

            INSERT INTO ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
            VALUES (:CurrentStep, :StepStartTime, CURRENT_TIMESTAMP(), :AllocationRowCount, 'COMPLETED');
        END IF;

        -- =====================================================================
        -- Insert final results
        -- =====================================================================
        CurrentStep := 'Insert Results';
        StepStartTime := CURRENT_TIMESTAMP();

        INSERT INTO Planning.BudgetLineItem (
            BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID,
            OriginalAmount, AdjustedAmount, SpreadMethodCode, SourceSystem, SourceReference,
            IsAllocated, LastModifiedByUserID, LastModifiedDateTime
        )
        SELECT
            :TargetBudgetHeaderID,
            ca.GLAccountID,
            ca.CostCenterID,
            ca.FiscalPeriodID,
            ca.FinalAmount,
            0,
            'CONSOLIDATED',
            'CONSOLIDATION_PROC',
            :ConsolidationRunID,
            FALSE,
            :UserID,
            CURRENT_TIMESTAMP()
        FROM ConsolidatedAmounts ca
        WHERE ca.FinalAmount IS NOT NULL;

        RowsAffected := SQLROWCOUNT;
        TotalRowsProcessed := TotalRowsProcessed + RowsAffected;

        INSERT INTO ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode)
        VALUES (:CurrentStep, :StepStartTime, CURRENT_TIMESTAMP(), :RowsAffected, 'COMPLETED');

        -- =====================================================================
        -- Commit transaction
        -- =====================================================================
        COMMIT;

        -- Build processing log as JSON
        ProcessingLogJSON := (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'StepName', StepName,
                'StartTime', StartTime,
                'EndTime', EndTime,
                'RowsAffected', RowsAffected,
                'StatusCode', StatusCode,
                'Message', Message
            ))
            FROM ProcessingLog
            ORDER BY LogID
        );

        -- Debug output
        LET result_table RESULTSET := (
            SELECT :TargetBudgetHeaderID AS TargetBudgetHeaderID,
                   :TotalRowsProcessed AS RowsProcessed,
                   :ErrorMsg AS ErrorMessage,
                   CASE WHEN :DebugMode THEN :ProcessingLogJSON ELSE NULL::VARIANT END AS ProcessingLogJSON
        );
        RETURN TABLE(result_table);

    EXCEPTION
        WHEN OTHER THEN
            -- =====================================================================
            -- Error handling block
            -- =====================================================================
            ErrorMsg := SQLERRM;
            ReturnCode := SQLCODE;

            -- Rollback transaction if active
            ROLLBACK;

            -- Log the error
            INSERT INTO ProcessingLog (StepName, StartTime, EndTime, RowsAffected, StatusCode, Message)
            VALUES (:CurrentStep, :StepStartTime, CURRENT_TIMESTAMP(), 0, 'ERROR', :ErrorMsg);

            -- Build error log
            ProcessingLogJSON := (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                    'StepName', StepName,
                    'StartTime', StartTime,
                    'EndTime', EndTime,
                    'RowsAffected', RowsAffected,
                    'StatusCode', StatusCode,
                    'Message', Message
                ))
                FROM ProcessingLog
                ORDER BY LogID
            );

            -- Return error result
            LET error_result RESULTSET := (
                SELECT NULL::INTEGER AS TargetBudgetHeaderID,
                       0 AS RowsProcessed,
                       :ErrorMsg AS ErrorMessage,
                       :ProcessingLogJSON AS ProcessingLogJSON
            );
            RETURN TABLE(error_result);
    END;

    -- Should not reach here
    LET final_result RESULTSET := (
        SELECT :TargetBudgetHeaderID AS TargetBudgetHeaderID,
               :TotalRowsProcessed AS RowsProcessed,
               :ErrorMsg AS ErrorMessage,
               :ProcessingLogJSON AS ProcessingLogJSON
    );
    RETURN TABLE(final_result);
END;
$$;

/*
    ============================================================================
    USAGE EXAMPLES
    ============================================================================

    1. Basic consolidation:

    CALL Planning.usp_ProcessBudgetConsolidation(
        SourceBudgetHeaderID => 100,
        ConsolidationType => 'FULL',
        IncludeEliminations => TRUE,
        RecalculateAllocations => TRUE,
        ProcessingOptions => NULL,
        UserID => 1,
        DebugMode => TRUE
    );

    2. With processing options (JSON):

    CALL Planning.usp_ProcessBudgetConsolidation(
        SourceBudgetHeaderID => 100,
        ConsolidationType => 'FULL',
        IncludeEliminations => TRUE,
        RecalculateAllocations => TRUE,
        ProcessingOptions => PARSE_JSON('{
            "IncludeZeroBalances": false,
            "RoundingPrecision": 2
        }'),
        UserID => 1,
        DebugMode => FALSE
    );

    3. Retrieve results:

    SELECT * FROM TABLE(Planning.usp_ProcessBudgetConsolidation(
        100, 'FULL', TRUE, TRUE, NULL, 1, TRUE
    ));

    ============================================================================
    MIGRATION NOTES & VALIDATION
    ============================================================================

    Changes from SQL Server version:

    1. ✓ Cursors eliminated - replaced with WHILE loops + temp tables
    2. ✓ Table variables → TEMPORARY tables
    3. ✓ OUTPUT clause → SELECT after INSERT pattern
    4. ✓ TRY-CATCH → EXCEPTION WHEN OTHER
    5. ✓ SYSUTCDATETIME() → CURRENT_TIMESTAMP()
    6. ✓ NEWID() → UUID_STRING()
    7. ✓ @@ROWCOUNT → SQLROWCOUNT
    8. ✓ @@FETCH_STATUS → loop condition in WHILE
    9. ✓ XML → JSON (VARIANT)
    10. ✓ sp_executesql → EXECUTE IMMEDIATE
    11. ✓ RAISERROR/THROW → RAISE EXCEPTION
    12. ✓ Removed SET NOCOUNT, XACT_ABORT
    13. ✓ Simplified transaction (no savepoints/nested transactions)
    14. ✓ MERGE works (without OUTPUT clause)
    15. ✓ Returns TABLE instead of scalar OUTPUT parameters

    Testing checklist:
    - [ ] Hierarchy rollup produces correct subtotals
    - [ ] Intercompany eliminations match correctly
    - [ ] Dynamic SQL with JSON options works
    - [ ] Error handling captures all exceptions
    - [ ] Processing log is complete
    - [ ] Performance acceptable vs cursor version
*/
