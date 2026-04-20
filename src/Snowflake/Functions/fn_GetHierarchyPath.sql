/*
    fn_GetHierarchyPath - Builds the full hierarchy path string for a cost center
    Dependencies: CostCenter

    Migration Notes:
    - Converted from T-SQL to Snowflake SQL UDF
    - WHILE loop remains (Snowflake supports procedural SQL in UDFs)
    - NVARCHAR → VARCHAR
    - Could alternatively be implemented as recursive CTE in views/queries
*/

CREATE OR REPLACE FUNCTION Planning.fn_GetHierarchyPath(
    CostCenterID_param  FLOAT,
    Delimiter           VARCHAR(5)
)
RETURNS VARCHAR(1000)
LANGUAGE JAVASCRIPT
AS
$$
    // Return NULL if input is NULL
    if (COSTCENTERID_PARAM == null) {
        return null;
    }

    // Default delimiter if NULL
    var delimiter = DELIMITER || ' > ';

    var path = '';
    var currentID = COSTCENTERID_PARAM;
    var depth = 0;
    var maxDepth = 20;  // Prevent infinite loops

    // Traverse up the hierarchy
    while (currentID != null && depth < maxDepth) {
        var stmt = snowflake.createStatement({
            sqlText: `SELECT CostCenterName, ParentCostCenterID
                      FROM Planning.CostCenter
                      WHERE CostCenterID = :1`,
            binds: [currentID]
        });

        var result = stmt.execute();

        if (!result.next()) {
            // Cost center not found, exit
            break;
        }

        var name = result.getColumnValue(1);
        var parentID = result.getColumnValue(2);

        if (name == null) {
            break;
        }

        if (path == '') {
            path = name;
        } else {
            path = name + delimiter + path;
        }

        currentID = parentID;
        depth++;
    }

    return path;
$$;

