/*
    vw_AllocationRuleTargets - Parses target specifications from allocation rules
    Dependencies: AllocationRule, CostCenter

    Migration Notes:
    - XML column (TargetSpecification) → VARIANT in Snowflake
    - XML .nodes() → FLATTEN() with VARIANT
    - XML .value() → Variant path notation (col:path::type)
    - XML .exist() → Check if path exists in VARIANT
    - XML .query() → Extract sub-VARIANT
    - CROSS APPLY → LATERAL FLATTEN

    IMPORTANT: This assumes AllocationRule.TargetSpecification is stored as JSON in VARIANT
    Expected JSON format:
    {
        "AllocationTargets": {
            "Target": [
                {
                    "CostCenterID": 123,
                    "CostCenterCode": "CC001",
                    "AllocationPercentage": 0.5,
                    "Priority": 1,
                    "AccountFilter": "6000-6999",
                    "ExcludePattern": "6500",
                    "Conditions": [...]
                },
                ...
            ]
        }
    }
*/

CREATE OR REPLACE VIEW Planning.vw_AllocationRuleTargets
AS
SELECT
    ar.AllocationRuleID,
    ar.RuleCode,
    ar.RuleName,
    ar.RuleType,
    ar.AllocationMethod,
    ar.ExecutionSequence,
    -- Parse VARIANT target specifications using path notation
    target.value:CostCenterID::INT AS TargetCostCenterID,
    target.value:CostCenterCode::VARCHAR(20) AS TargetCostCenterCode,
    target.value:AllocationPercentage::DECIMAL(8,6) AS TargetAllocationPct,
    target.value:Priority::INT AS TargetPriority,
    target.value:AccountFilter::VARCHAR(50) AS AccountFilter,
    target.value:ExcludePattern::VARCHAR(50) AS ExcludePattern,
    -- Check for conditional allocations
    CASE
        WHEN target.value:Conditions IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS HasConditions,
    target.value:Conditions AS ConditionsVariant,
    -- Join to cost center for target details
    cc.CostCenterName AS TargetCostCenterName,
    cc.ParentCostCenterID AS TargetParentCostCenterID,
    cc.IsActive AS TargetIsActive,
    ar.EffectiveFromDate,
    ar.EffectiveToDate,
    ar.IsActive AS RuleIsActive
FROM Planning.AllocationRule ar
    LEFT JOIN LATERAL FLATTEN(input => ar.TargetSpecification:AllocationTargets.Target, outer => TRUE) target
    LEFT JOIN Planning.CostCenter cc
        ON cc.CostCenterID = target.value:CostCenterID::INT
           OR cc.CostCenterCode = target.value:CostCenterCode::VARCHAR(20);


