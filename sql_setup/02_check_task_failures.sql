-- Snowflake Sentinel - Check Task Failures
-- Purpose: Query task execution history to identify failures

-- Option 1: Check using INFORMATION_SCHEMA (near real-time)
SELECT 
    name,
    state,
    error_code,
    error_message,
    scheduled_time,
    completed_time
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
WHERE database_name = 'SENTINEL_TEST'
    AND schema_name = 'FAILURES'
    AND state = 'FAILED'
ORDER BY scheduled_time DESC;

-- Option 2: Check using ACCOUNT_USAGE (15-45 min lag, but more complete)
SELECT 
    name,
    state,
    error_code,
    error_message,
    scheduled_time,
    completed_time,
    query_id
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE database_name = 'SENTINEL_TEST'
    AND schema_name = 'FAILURES'
    AND state = 'FAILED'
    AND scheduled_time > DATEADD('hour', -2, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC
LIMIT 20;

-- Option 3: Get full query text for a failed task
SELECT 
    th.name,
    th.error_message,
    qh.query_text,
    qh.execution_time,
    qh.warehouse_size
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY th
JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
    ON th.query_id = qh.query_id
WHERE th.database_name = 'SENTINEL_TEST'
    AND th.state = 'FAILED'
    AND th.scheduled_time > DATEADD('hour', -2, CURRENT_TIMESTAMP())
ORDER BY th.scheduled_time DESC;
