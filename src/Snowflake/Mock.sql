USE WAREHOUSE SNOWFLAKETAKEHOME;
USE DATABASE FINANCIAL_PLANNING;
USE SCHEMA Planning;

-- Clear existing data (in reverse order of dependencies)
DELETE FROM Planning.BudgetLineItem;
DELETE FROM Planning.BudgetHeader;
DELETE FROM Planning.FiscalPeriod;
DELETE FROM Planning.CostCenter;
DELETE FROM Planning.GLAccount;

-- Insert GL Accounts
INSERT INTO Planning.GLAccount (AccountNumber, AccountName, AccountType, AccountSubType, ParentAccountID, AccountLevel, IsPostable, IsBudgetable, IsStatistical, NormalBalance, CurrencyCode, IntercompanyFlag, IsActive)
VALUES
-- Revenue Accounts
('4000', 'Total Revenue', 'R', 'Revenue', NULL, 1, FALSE, TRUE, FALSE, 'C', 'USD', FALSE, TRUE),
('4100', 'Product Revenue', 'R', 'Operating Revenue', 1, 2, TRUE, TRUE, FALSE, 'C', 'USD', FALSE, TRUE),
('4200', 'Service Revenue', 'R', 'Operating Revenue', 1, 2, TRUE, TRUE, FALSE, 'C', 'USD', FALSE, TRUE),
('4300', 'Consulting Revenue', 'R', 'Operating Revenue', 1, 2, TRUE, TRUE, FALSE, 'C', 'USD', FALSE, TRUE),
-- Expense Accounts
('5000', 'Total Expenses', 'X', 'Expense', NULL, 1, FALSE, TRUE, FALSE, 'D', 'USD', FALSE, TRUE),
('5100', 'Salaries', 'X', 'Personnel', 5, 2, TRUE, TRUE, FALSE, 'D', 'USD', FALSE, TRUE),
('5200', 'Benefits', 'X', 'Personnel', 5, 2, TRUE, TRUE, FALSE, 'D', 'USD', FALSE, TRUE),
('5300', 'Marketing', 'X', 'Operating', 5, 2, TRUE, TRUE, FALSE, 'D', 'USD', FALSE, TRUE),
('5400', 'Travel', 'X', 'Operating', 5, 2, TRUE, TRUE, FALSE, 'D', 'USD', FALSE, TRUE),
('5500', 'IT Infrastructure', 'X', 'Operating', 5, 2, TRUE, TRUE, FALSE, 'D', 'USD', FALSE, TRUE),
-- Asset Accounts
('1000', 'Total Assets', 'A', 'Asset', NULL, 1, FALSE, TRUE, FALSE, 'D', 'USD', FALSE, TRUE),
('1100', 'Cash', 'A', 'Current Asset', 11, 2, TRUE, TRUE, FALSE, 'D', 'USD', FALSE, TRUE);

-- Insert Cost Centers
INSERT INTO Planning.CostCenter (CostCenterCode, CostCenterName, ParentCostCenterID, HierarchyPath, HierarchyLevel, ManagerEmployeeID, DepartmentCode, IsActive, EffectiveFromDate, EffectiveToDate, AllocationWeight)
VALUES
('CC-000', 'Corporate', NULL, '/1/', 1, NULL, 'CORP', TRUE, '2024-01-01', NULL, 1.0000),
('CC-100', 'Engineering', 1, '/1/2/', 2, 1001, 'ENG', TRUE, '2024-01-01', NULL, 0.4000),
('CC-200', 'Sales', 1, '/1/3/', 2, 1002, 'SALES', TRUE, '2024-01-01', NULL, 0.3000),
('CC-300', 'Marketing', 1, '/1/4/', 2, 1003, 'MKT', TRUE, '2024-01-01', NULL, 0.2000),
('CC-400', 'Operations', 1, '/1/5/', 2, 1004, 'OPS', TRUE, '2024-01-01', NULL, 0.1000),
('CC-110', 'Product Dev', 2, '/1/2/6/', 3, 1005, 'PDEV', TRUE, '2024-01-01', NULL, 0.6000),
('CC-120', 'QA', 2, '/1/2/7/', 3, 1006, 'QA', TRUE, '2024-01-01', NULL, 0.4000);

-- Insert Fiscal Periods for 2024
INSERT INTO Planning.FiscalPeriod (FiscalYear, FiscalQuarter, FiscalMonth, PeriodName, PeriodStartDate, PeriodEndDate, IsClosed, IsAdjustmentPeriod, WorkingDays)
VALUES
(2024, 1, 1, 'January 2024', '2024-01-01', '2024-01-31', FALSE, FALSE, 22),
(2024, 1, 2, 'February 2024', '2024-02-01', '2024-02-29', FALSE, FALSE, 21),
(2024, 1, 3, 'March 2024', '2024-03-01', '2024-03-31', FALSE, FALSE, 21),
(2024, 2, 4, 'April 2024', '2024-04-01', '2024-04-30', FALSE, FALSE, 22),
(2024, 2, 5, 'May 2024', '2024-05-01', '2024-05-31', FALSE, FALSE, 23),
(2024, 2, 6, 'June 2024', '2024-06-01', '2024-06-30', FALSE, FALSE, 20),
(2024, 3, 7, 'July 2024', '2024-07-01', '2024-07-31', FALSE, FALSE, 23),
(2024, 3, 8, 'August 2024', '2024-08-01', '2024-08-31', FALSE, FALSE, 22),
(2024, 3, 9, 'September 2024', '2024-09-01', '2024-09-30', FALSE, FALSE, 21),
(2024, 4, 10, 'October 2024', '2024-10-01', '2024-10-31', FALSE, FALSE, 23),
(2024, 4, 11, 'November 2024', '2024-11-01', '2024-11-30', FALSE, FALSE, 21),
(2024, 4, 12, 'December 2024', '2024-12-01', '2024-12-31', FALSE, FALSE, 22);

-- Insert Budget Headers
INSERT INTO Planning.BudgetHeader (BudgetCode, BudgetName, BudgetType, ScenarioType, FiscalYear, StartPeriodID, EndPeriodID, BaseBudgetHeaderID, StatusCode, VersionNumber, Notes)
VALUES
('BUD-2024-001', '2024 Annual Operating Budget', 'ANNUAL', 'BASE', 2024, 1, 12, NULL, 'APPROVED', 1, 'Base case budget for 2024'),
('BUD-2024-002', '2024 Q1 Forecast', 'QUARTERLY', 'BASE', 2024, 1, 3, 1, 'APPROVED', 1, 'Q1 detailed forecast'),
('BUD-2024-003', '2024 Optimistic Scenario', 'ANNUAL', 'OPTIMISTIC', 2024, 1, 12, 1, 'APPROVED', 1, 'High growth scenario');

-- Insert Budget Line Items
-- Budget 1: Annual Operating Budget
INSERT INTO Planning.BudgetLineItem (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID, OriginalAmount, AdjustedAmount, FinalAmount, SpreadMethodCode, IsAllocated)
VALUES
-- Engineering Product Revenue - Monthly
(1, 2, 2, 1, 100000.00, 0, 100000.00, 'EVEN', FALSE),
(1, 2, 2, 2, 100000.00, 0, 100000.00, 'EVEN', FALSE),
(1, 2, 2, 3, 100000.00, 5000.00, 105000.00, 'EVEN', FALSE),
-- Engineering Salaries
(1, 6, 2, 1, 150000.00, 0, 150000.00, 'EVEN', FALSE),
(1, 6, 2, 2, 150000.00, 0, 150000.00, 'EVEN', FALSE),
(1, 6, 2, 3, 150000.00, 10000.00, 160000.00, 'EVEN', FALSE),
-- Sales Service Revenue
(1, 3, 3, 1, 80000.00, 0, 80000.00, 'SEASONAL', FALSE),
(1, 3, 3, 2, 85000.00, 0, 85000.00, 'SEASONAL', FALSE),
(1, 3, 3, 3, 90000.00, 0, 90000.00, 'SEASONAL', FALSE),
-- Marketing Expenses
(1, 8, 4, 1, 25000.00, 0, 25000.00, 'EVEN', FALSE),
(1, 8, 4, 2, 25000.00, 5000.00, 30000.00, 'EVEN', FALSE),
(1, 8, 4, 3, 25000.00, 0, 25000.00, 'EVEN', FALSE),
-- Operations IT Infrastructure
(1, 10, 5, 1, 30000.00, 0, 30000.00, 'EVEN', FALSE),
(1, 10, 5, 2, 30000.00, 0, 30000.00, 'EVEN', FALSE),
(1, 10, 5, 3, 30000.00, 0, 30000.00, 'EVEN', FALSE);

-- Budget 2: Q1 Forecast (more detailed, all cost centers)
INSERT INTO Planning.BudgetLineItem (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID, OriginalAmount, AdjustedAmount, FinalAmount, SpreadMethodCode, IsAllocated)
VALUES
-- Product Dev Revenue
(2, 2, 6, 1, 60000.00, 0, 60000.00, 'EVEN', FALSE),
(2, 2, 6, 2, 65000.00, 0, 65000.00, 'EVEN', FALSE),
(2, 2, 6, 3, 70000.00, 2000.00, 72000.00, 'EVEN', FALSE),
-- QA Salaries
(2, 6, 7, 1, 45000.00, 0, 45000.00, 'EVEN', FALSE),
(2, 6, 7, 2, 45000.00, 0, 45000.00, 'EVEN', FALSE),
(2, 6, 7, 3, 45000.00, 3000.00, 48000.00, 'EVEN', FALSE),
-- Corporate Travel
(2, 9, 1, 1, 15000.00, 0, 15000.00, 'EVEN', FALSE),
(2, 9, 1, 2, 12000.00, 0, 12000.00, 'EVEN', FALSE),
(2, 9, 1, 3, 18000.00, 0, 18000.00, 'EVEN', FALSE);

-- Budget 3: Optimistic Scenario
INSERT INTO Planning.BudgetLineItem (BudgetHeaderID, GLAccountID, CostCenterID, FiscalPeriodID, OriginalAmount, AdjustedAmount, FinalAmount, SpreadMethodCode, IsAllocated)
VALUES
-- Higher revenue projections
(3, 2, 2, 1, 120000.00, 0, 120000.00, 'EVEN', FALSE),
(3, 2, 2, 2, 125000.00, 0, 125000.00, 'EVEN', FALSE),
(3, 2, 2, 3, 130000.00, 0, 130000.00, 'EVEN', FALSE),
-- Corresponding higher expenses
(3, 6, 2, 1, 180000.00, 0, 180000.00, 'EVEN', FALSE),
(3, 6, 2, 2, 185000.00, 0, 185000.00, 'EVEN', FALSE),
(3, 6, 2, 3, 190000.00, 5000.00, 195000.00, 'EVEN', FALSE);

-- Verify data was inserted
SELECT 'GLAccount' AS TableName, COUNT(*) AS RecordCount FROM Planning.GLAccount
UNION ALL
SELECT 'CostCenter', COUNT(*) FROM Planning.CostCenter
UNION ALL
SELECT 'FiscalPeriod', COUNT(*) FROM Planning.FiscalPeriod
UNION ALL
SELECT 'BudgetHeader', COUNT(*) FROM Planning.BudgetHeader
UNION ALL
SELECT 'BudgetLineItem', COUNT(*) FROM Planning.BudgetLineItem;
-- Should be 12, 7, 12, 3, 30
