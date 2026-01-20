"""
Demo data for Snowflake Sentinel
Cached results from actual analysis runs - no API calls needed
"""

DEMO_RESULTS = [
    {
        'task_name': 'TASK_BROKEN_DIVISION',
        'error_message': 'Division by zero',
        'investigation': {
            'query_text': '''SELECT 
    order_id,
    revenue / orders AS avg_per_order
FROM sales''',
            'error_message': 'Division by zero',
            'diagnosis': '''STEP 1 - ANALYZE ERROR
The error message "Division by zero" indicates that the query attempted to divide a number by zero, which is mathematically undefined and causes the query to fail.

STEP 2 - CONTEXT CHECK
The query is calculating average revenue per order by dividing the revenue column by the orders column from the sales table. The table DDL shows that the orders column is defined as INT, which allows zero values.

STEP 3 - ROOT CAUSE IDENTIFICATION
The root cause is that the sales table contains rows where orders = 0. When the query attempts to compute revenue / orders for these rows, it triggers a division-by-zero error. This is a data quality issue combined with insufficient null/zero handling in the SQL logic.

STEP 4 - PROPOSE FIX
```sql
SELECT 
    order_id,
    CASE 
        WHEN orders = 0 THEN 0
        ELSE revenue / orders
    END AS avg_per_order
FROM sales;
```
The fix wraps the division operation in a CASE statement that checks if orders equals zero. If true, it returns 0 as the average (or NULL if that's more appropriate for your business logic). If false, it performs the division safely.

STEP 5 - VALIDATION
This fix resolves the division-by-zero error by handling the edge case before the division occurs. The CASE statement evaluates the denominator first and provides a safe default value. This approach maintains data integrity while preventing query failures. Alternative approaches could use NULLIF(orders, 0) which would return NULL instead of 0 for zero-order records.''',
            'reasoning_steps': {
                'step1_analyze_error': '''STEP 1 - ANALYZE ERROR
The error message "Division by zero" indicates that the query attempted to divide a number by zero, which is mathematically undefined and causes the query to fail.''',
                'step2_context_check': '''STEP 2 - CONTEXT CHECK
The query is calculating average revenue per order by dividing the revenue column by the orders column from the sales table. The table DDL shows that the orders column is defined as INT, which allows zero values.''',
                'step3_root_cause': '''STEP 3 - ROOT CAUSE IDENTIFICATION
The root cause is that the sales table contains rows where orders = 0. When the query attempts to compute revenue / orders for these rows, it triggers a division-by-zero error. This is a data quality issue combined with insufficient null/zero handling in the SQL logic.''',
                'step4_propose_fix': '''STEP 4 - PROPOSE FIX
```sql
SELECT 
    order_id,
    CASE 
        WHEN orders = 0 THEN 0
        ELSE revenue / orders
    END AS avg_per_order
FROM sales;
```
The fix wraps the division operation in a CASE statement that checks if orders equals zero. If true, it returns 0 as the average. If false, it performs the division safely.''',
                'step5_validation': '''STEP 5 - VALIDATION
This fix resolves the division-by-zero error by handling the edge case before the division occurs. The CASE statement evaluates the denominator first and provides a safe default value. This approach maintains data integrity while preventing query failures.'''
            },
            'fixed_sql': '''SELECT 
    order_id,
    CASE 
        WHEN orders = 0 THEN 0
        ELSE revenue / orders
    END AS avg_per_order
FROM sales;''',
            'execution_time_ms': 152
        },
        'savings': {
            'task_name': 'TASK_BROKEN_DIVISION',
            'warehouse_size': 'X-Small',
            'avg_execution_time_seconds': 0.15,
            'estimated_improvement_pct': 5,
            'time_saved_per_run_seconds': 0.01,
            'credits_saved_per_run': 0.000002,
            'executions_per_year': 105120,
            'annual_credits_saved': 0.21,
            'annual_cost_saved_usd': 0.63
        }
    },
    {
        'task_name': 'TASK_MISSING_COLUMN',
        'error_message': 'SQL compilation error: error line 3 at position 8 invalid identifier \'NONEXISTENT_COLUMN\'',
        'investigation': {
            'query_text': '''SELECT 
    order_id,
    nonexistent_column
FROM sales''',
            'error_message': 'SQL compilation error: invalid identifier NONEXISTENT_COLUMN',
            'diagnosis': '''STEP 1 - ANALYZE ERROR
The error is a SQL compilation error indicating that the identifier "NONEXISTENT_COLUMN" is invalid, meaning Snowflake cannot find a column with that name in the referenced table.

STEP 2 - CONTEXT CHECK
The query attempts to select order_id and nonexistent_column from the sales table. Based on the table DDL, the sales table contains columns: order_id, revenue, orders, and region. There is no column named "nonexistent_column".

STEP 3 - ROOT CAUSE IDENTIFICATION
This is a schema mismatch error caused by referencing a column that does not exist in the table. This commonly occurs when: (1) the column name is misspelled, (2) the schema changed and the query wasn't updated, or (3) the developer referenced the wrong column name.

STEP 4 - PROPOSE FIX
Without knowing the intended business logic, the most likely fixes are:

Option 1 - Remove the invalid column:
```sql
SELECT 
    order_id
FROM sales;
```

Option 2 - Replace with a valid column (e.g., revenue):
```sql
SELECT 
    order_id,
    revenue
FROM sales;
```

Option 3 - Add the missing column to the table schema if it should exist.

STEP 5 - VALIDATION
The fix depends on business requirements. If the column reference was a typo, correcting the column name resolves the error. If the column is truly not needed, removing it from the SELECT clause prevents the compilation error. This type of error is caught at query compilation time, so no data is processed or credits wasted beyond the compilation attempt.''',
            'reasoning_steps': {
                'step1_analyze_error': '''STEP 1 - ANALYZE ERROR
The error is a SQL compilation error indicating that the identifier "NONEXISTENT_COLUMN" is invalid, meaning Snowflake cannot find a column with that name in the referenced table.''',
                'step2_context_check': '''STEP 2 - CONTEXT CHECK
The query attempts to select order_id and nonexistent_column from the sales table. Based on the table DDL, the sales table contains columns: order_id, revenue, orders, and region.''',
                'step3_root_cause': '''STEP 3 - ROOT CAUSE IDENTIFICATION
This is a schema mismatch error caused by referencing a column that does not exist in the table. This commonly occurs when the column name is misspelled, the schema changed, or the developer referenced the wrong column name.''',
                'step4_propose_fix': '''STEP 4 - PROPOSE FIX
```sql
SELECT 
    order_id,
    revenue
FROM sales;
```
Replace the invalid column reference with a valid column from the table schema.''',
                'step5_validation': '''STEP 5 - VALIDATION
This fix resolves the compilation error by referencing only columns that exist in the table schema. The error is caught at compilation time, so no compute resources are wasted.'''
            },
            'fixed_sql': '''SELECT 
    order_id,
    revenue
FROM sales;''',
            'execution_time_ms': 0
        },
        'savings': {
            'task_name': 'TASK_MISSING_COLUMN',
            'warehouse_size': 'X-Small',
            'avg_execution_time_seconds': 0.0,
            'estimated_improvement_pct': 100,
            'time_saved_per_run_seconds': 0.0,
            'credits_saved_per_run': 0.0,
            'executions_per_year': 105120,
            'annual_credits_saved': 0.0,
            'annual_cost_saved_usd': 0.0
        }
    },
    {
        'task_name': 'TASK_MISSING_TABLE',
        'error_message': "SQL compilation error: Object 'FAKE_TABLE_THAT_DOES_NOT_EXIST' does not exist or not authorized",
        'investigation': {
            'query_text': '''SELECT * FROM fake_table_that_does_not_exist''',
            'error_message': "Object 'FAKE_TABLE_THAT_DOES_NOT_EXIST' does not exist",
            'diagnosis': '''STEP 1 - ANALYZE ERROR
The error indicates that Snowflake cannot find an object (table or view) named "FAKE_TABLE_THAT_DOES_NOT_EXIST" in the current schema, or the user lacks authorization to access it.

STEP 2 - CONTEXT CHECK
The query attempts to select all columns from a table that does not exist in the database. This is a fundamental dependency issue where the query references a non-existent data source.

STEP 3 - ROOT CAUSE IDENTIFICATION
The root cause is a missing table dependency. This typically happens when: (1) the table name is misspelled, (2) the table exists in a different schema/database, (3) the table was dropped or renamed, or (4) the task was created before the dependent table was created.

STEP 4 - PROPOSE FIX
Without additional context about the intended table, the fixes are:

Option 1 - Correct the table reference:
```sql
SELECT * FROM sales;  -- Use correct table name
```

Option 2 - Fully qualify the table name:
```sql
SELECT * FROM SENTINEL_TEST.FAILURES.correct_table_name;
```

Option 3 - Create the missing table before running the task.

STEP 5 - VALIDATION
This error is caught during the compilation phase, preventing any query execution. The fix requires either correcting the table reference to an existing table or ensuring the dependent table is created before the task runs. Proper dependency management in task scheduling can prevent this type of error.''',
            'reasoning_steps': {
                'step1_analyze_error': '''STEP 1 - ANALYZE ERROR
The error indicates that Snowflake cannot find an object named "FAKE_TABLE_THAT_DOES_NOT_EXIST" in the current schema, or the user lacks authorization to access it.''',
                'step2_context_check': '''STEP 2 - CONTEXT CHECK
The query attempts to select all columns from a table that does not exist in the database. This is a fundamental dependency issue.''',
                'step3_root_cause': '''STEP 3 - ROOT CAUSE IDENTIFICATION
The root cause is a missing table dependency. This happens when the table name is misspelled, exists in a different schema, was dropped, or the task was created before the table existed.''',
                'step4_propose_fix': '''STEP 4 - PROPOSE FIX
```sql
SELECT * FROM sales;
```
Correct the table reference to point to an existing table in the schema.''',
                'step5_validation': '''STEP 5 - VALIDATION
This error is caught during compilation, preventing execution. The fix requires correcting the table reference or ensuring dependent tables exist before task execution.'''
            },
            'fixed_sql': '''SELECT * FROM sales;''',
            'execution_time_ms': 0
        },
        'savings': {
            'task_name': 'TASK_MISSING_TABLE',
            'warehouse_size': 'X-Small',
            'avg_execution_time_seconds': 0.0,
            'estimated_improvement_pct': 100,
            'time_saved_per_run_seconds': 0.0,
            'credits_saved_per_run': 0.0,
            'executions_per_year': 105120,
            'annual_credits_saved': 0.0,
            'annual_cost_saved_usd': 0.0
        }
    }
]