-- INITIAL SETUP

USE WAREHOUSE SNOWFLAKETAKEHOME; 

CREATE DATABASE IF NOT EXISTS FINANCIAL_PLANNING;
USE DATABASE FINANCIAL_PLANNING;

CREATE SCHEMA IF NOT EXISTS Planning;
USE SCHEMA Planning;

CREATE SEQUENCE IF NOT EXISTS Planning.seq_FiscalPeriod_Version START = 1 INCREMENT = 1;

!source src/Snowflake/Schema/Planning.sql
!source src/Snowflake/Tables/FiscalPeriod.sql
!source src/Snowflake/Tables/CostCenter.sql
!source src/Snowflake/Tables/GLAccount.sql

!source src/Snowflake/Tables/BudgetHeader.sql    
!source src/Snowflake/Tables/AllocationRule.sql    
!source src/Snowflake/Tables/BudgetLineItem.sql  

!source src/Snowflake/Tables/ConsolidationJournal.sql
!source src/Snowflake/Tables/ConsolidationJournalLine.sql


!source src/Snowflake/Functions/fn_GetAllocationFactor.sql;
!source src/Snowflake/Functions/fn_GetHierarchyPath.sql;
!source src/Snowflake/Functions/tvf_GetBudgetVariance.sql;
!source src/Snowflake/Functions/tvf_ExplodeCostCenterHierarchy.sql;


!source src/Snowflake/Views/vw_BudgetConsolidationSummary.sql;
!source src/Snowflake/Views/vw_AllocationRuleTargets.sql;

