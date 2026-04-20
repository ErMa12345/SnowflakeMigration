/*
    tvf_ExplodeCostCenterHierarchy - Explodes cost center hierarchy
    Dependencies: CostCenter

    Migration Notes:
    - SQL Server Multi-statement TVF → Snowflake JavaScript UDF
    - WHILE loop logic converted to recursive CTE approach for better performance
    - Alternative implementation using recursive CTE instead of procedural loops
    - Table variable → RESULTSET
    - SCHEMABINDING removed
    - BIT → BOOLEAN
    - GETDATE() → CURRENT_DATE()

    Note: This is implemented as a TABLE function using recursive CTE which is
    more efficient in Snowflake than procedural loops
*/

CREATE OR REPLACE FUNCTION Planning.tvf_ExplodeCostCenterHierarchy(
    RootCostCenterID    INT,
    MaxDepth            INT,
    IncludeInactive     BOOLEAN,
    AsOfDate            DATE
)
RETURNS TABLE (
    CostCenterID        INT,
    CostCenterCode      VARCHAR(20),
    CostCenterName      VARCHAR(100),
    ParentCostCenterID  INT,
    HierarchyLevel      INT,
    HierarchyPath       VARCHAR(500),
    SortPath            VARCHAR(500),
    IsLeaf              BOOLEAN,
    ChildCount          INT,
    CumulativeWeight    DECIMAL(18,10)
)
AS
$$
    WITH RECURSIVE HierarchyBase AS (
        -- Anchor: Get root level
        SELECT
            cc.CostCenterID,
            cc.CostCenterCode,
            cc.CostCenterName,
            cc.ParentCostCenterID,
            0 AS HierarchyLevel,
            cc.CostCenterName AS HierarchyPath,
            LPAD(cc.CostCenterID::VARCHAR, 10, '0') AS SortPath,
            cc.AllocationWeight AS CumulativeWeight
        FROM Planning.CostCenter cc
        WHERE ((RootCostCenterID IS NULL AND cc.ParentCostCenterID IS NULL)
            OR cc.CostCenterID = RootCostCenterID)
          AND (cc.IsActive = TRUE OR IncludeInactive = TRUE)
          AND cc.EffectiveFromDate <= COALESCE(AsOfDate, CURRENT_DATE())
          AND (cc.EffectiveToDate IS NULL OR cc.EffectiveToDate >= COALESCE(AsOfDate, CURRENT_DATE()))

        UNION ALL

        -- Recursive: Get children
        SELECT
            cc.CostCenterID,
            cc.CostCenterCode,
            cc.CostCenterName,
            cc.ParentCostCenterID,
            h.HierarchyLevel + 1 AS HierarchyLevel,
            h.HierarchyPath || ' > ' || cc.CostCenterName AS HierarchyPath,
            h.SortPath || '/' || LPAD(cc.CostCenterID::VARCHAR, 10, '0') AS SortPath,
            h.CumulativeWeight * cc.AllocationWeight AS CumulativeWeight
        FROM Planning.CostCenter cc
        INNER JOIN HierarchyBase h ON cc.ParentCostCenterID = h.CostCenterID
        WHERE h.HierarchyLevel < COALESCE(MaxDepth, 10) - 1
          AND (cc.IsActive = TRUE OR IncludeInactive = TRUE)
          AND cc.EffectiveFromDate <= COALESCE(AsOfDate, CURRENT_DATE())
          AND (cc.EffectiveToDate IS NULL OR cc.EffectiveToDate >= COALESCE(AsOfDate, CURRENT_DATE()))
    ),
    HierarchyWithChildren AS (
        SELECT
            h.*,
            -- Calculate if leaf (no children)
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM Planning.CostCenter cc
                    WHERE cc.ParentCostCenterID = h.CostCenterID
                      AND (cc.IsActive = TRUE OR IncludeInactive = TRUE)
                      AND cc.EffectiveFromDate <= COALESCE(AsOfDate, CURRENT_DATE())
                      AND (cc.EffectiveToDate IS NULL OR cc.EffectiveToDate >= COALESCE(AsOfDate, CURRENT_DATE()))
                ) THEN FALSE
                ELSE TRUE
            END AS IsLeaf,
            -- Calculate child count from hierarchy result
            (
                SELECT COUNT(*)
                FROM HierarchyBase c
                WHERE c.ParentCostCenterID = h.CostCenterID
            ) AS ChildCount
        FROM HierarchyBase h
    )
    SELECT
        CostCenterID,
        CostCenterCode,
        CostCenterName,
        ParentCostCenterID,
        HierarchyLevel,
        HierarchyPath,
        SortPath,
        IsLeaf,
        ChildCount,
        CumulativeWeight
    FROM HierarchyWithChildren
    ORDER BY SortPath
$$;


