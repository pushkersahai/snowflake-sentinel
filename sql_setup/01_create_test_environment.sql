-- Snowflake Sentinel - Test Environment Setup
-- Purpose: Create failing tasks for AI agent detection and remediation

-- Step 1: Create test database and schema
CREATE DATABASE IF NOT EXISTS SENTINEL_TEST;
CREATE SCHEMA IF NOT EXISTS SENTINEL_TEST.FAILURES;

USE SCHEMA SENTINEL_TEST.FAILURES;

-- Step 2: Create sample sales table with edge cases
CREATE OR REPLACE TABLE sales (
    order_id INT,
    revenue FLOAT,
    orders INT,
    region VARCHAR(50)
);

-- Step 3: Insert test data (includes edge cases that cause failures)
INSERT INTO sales VALUES
    (1, 100.0, 5, 'WEST'),
    (2, 200.0, 0, 'EAST'),
    (3, 150.0, 3, 'WEST'),
    (4, 300.0, 10, 'NORTH'),
    (5, 50.0, 0, 'SOUTH');

-- Step 4: Create intentionally failing tasks

-- Task 1: Division by zero error
CREATE OR REPLACE TASK task_broken_division
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Fails due to division by zero when orders = 0'
AS
    SELECT 
        order_id,
        revenue / orders AS avg_per_order
    FROM sales;

-- Task 2: Column doesn't exist error
CREATE OR REPLACE TASK task_missing_column
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Fails due to non-existent column reference'
AS
    SELECT 
        order_id,
        nonexistent_column
    FROM sales;

-- Task 3: Table doesn't exist error
CREATE OR REPLACE TASK task_missing_table
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Fails due to missing table reference'
AS
    SELECT * FROM fake_table_that_does_not_exist;

-- Step 5: Activate the tasks
ALTER TASK task_broken_division RESUME;
ALTER TASK task_missing_column RESUME;
ALTER TASK task_missing_table RESUME;

-- Step 6: Verify tasks are created and active
SHOW TASKS IN SCHEMA SENTINEL_TEST.FAILURES;