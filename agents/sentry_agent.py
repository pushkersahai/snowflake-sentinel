import os
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta

load_dotenv()

class SentryAgent:
    """
    Detects failed Snowflake tasks by querying TASK_HISTORY.
    Returns a list of failed task executions for investigation.
    """
    
    def __init__(self):
        self.conn = None
        
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA')
            )
            print("SentryAgent connected to Snowflake")
        except Exception as e:
            print(f"Connection error: {str(e)}")
            raise
    
    def detect_failures(self, hours_back=2):
        """
        Query TASK_HISTORY for failed tasks in the last N hours.
        Returns a DataFrame of failures.
        """
        if not self.conn:
            self.connect()
        
        query = f"""
        SELECT 
            name as task_name,
            state,
            error_code,
            error_message,
            scheduled_time,
            query_id,
            database_name,
            schema_name
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
        WHERE state = 'FAILED'
            AND scheduled_time > DATEADD('hour', -{hours_back}, CURRENT_TIMESTAMP())
        ORDER BY scheduled_time DESC
        """
        
        cursor = self.conn.cursor()
        cursor.execute(query)
        
        columns = [col[0] for col in cursor.description]
        failures = cursor.fetchall()
        
        cursor.close()
        
        df = pd.DataFrame(failures, columns=columns)
        
        print(f"Detected {len(df)} failed task executions")
        
        return df
    
    def get_latest_failure_per_task(self, hours_back=2):
        """
        Get only the most recent failure for each unique task.
        Useful for avoiding duplicate investigations.
        """
        all_failures = self.detect_failures(hours_back)
        
        if all_failures.empty:
            return all_failures
        
        latest = all_failures.groupby('TASK_NAME').first().reset_index()
        
        print(f"Found {len(latest)} unique failed tasks")
        
        return latest
    
    def close(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            print("SentryAgent disconnected")

if __name__ == "__main__":
    agent = SentryAgent()
    
    failures = agent.get_latest_failure_per_task(hours_back=24)
    
    print("\nFailed Tasks:")
    print(failures[['TASK_NAME', 'ERROR_CODE', 'ERROR_MESSAGE', 'SCHEDULED_TIME']])
    
    agent.close()