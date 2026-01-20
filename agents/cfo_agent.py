import os
from dotenv import load_dotenv
import snowflake.connector
import re

load_dotenv()

class CFOAgent:
    """
    Calculates potential cost savings from fixing failed tasks.
    Estimates credits saved based on query execution time reduction.
    """
    
    def __init__(self, credit_price_usd=3.0):
        self.sf_conn = None
        self.credit_price = credit_price_usd
        
    def connect_snowflake(self):
        """Establish Snowflake connection"""
        try:
            self.sf_conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
            )
            print("CFOAgent connected to Snowflake")
        except Exception as e:
            print(f"Snowflake connection error: {str(e)}")
            raise
    
    def get_warehouse_credits_per_hour(self, warehouse_size):
        """
        Map warehouse size to credits consumed per hour.
        Based on Snowflake's standard pricing.
        """
        warehouse_credits = {
            'X-Small': 1,
            'Small': 2,
            'Medium': 4,
            'Large': 8,
            'X-Large': 16,
            '2X-Large': 32,
            '3X-Large': 64,
            '4X-Large': 128
        }
        return warehouse_credits.get(warehouse_size, 1)
    
    def get_task_execution_stats(self, task_name, database, schema, hours_back=24):
        """
        Get execution statistics for a task over the past N hours.
        Returns average execution time and warehouse size.
        """
        if not self.sf_conn:
            self.connect_snowflake()
        
        query = f"""
        SELECT 
            AVG(qh.execution_time) as avg_execution_time_ms,
            qh.warehouse_size,
            COUNT(*) as execution_count
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY th
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
            ON th.query_id = qh.query_id
        WHERE th.name = '{task_name}'
            AND th.database_name = '{database}'
            AND th.schema_name = '{schema}'
            AND th.scheduled_time > DATEADD('hour', -{hours_back}, CURRENT_TIMESTAMP())
        GROUP BY qh.warehouse_size
        """
        
        cursor = self.sf_conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return {
                'avg_execution_time_ms': result[0],
                'warehouse_size': result[1],
                'execution_count': result[2]
            }
        return None
    
    def estimate_runtime_improvement(self, original_sql, fixed_sql):
        """
        Estimate runtime improvement percentage based on fix type.
        This is a heuristic - in production you'd run EXPLAIN PLAN.
        """
        
        improvements = {
            'division by zero': 5,
            'missing column': 100,
            'missing table': 100,
            'case statement': 5,
            'coalesce': 5,
            'index': 30,
            'partition': 40
        }
        
        fixed_lower = fixed_sql.lower()
        
        for keyword, improvement_pct in improvements.items():
            if keyword in fixed_lower:
                return improvement_pct
        
        return 10
    
    def calculate_savings(self, task_name, database, schema, original_sql, fixed_sql, task_schedule='5 MINUTE'):
        """
        Calculate estimated annual savings from fixing a failed task.
        
        Args:
            task_name: Name of the failed task
            database: Database name
            schema: Schema name
            original_sql: Original failing SQL
            fixed_sql: Proposed fixed SQL
            task_schedule: How often task runs (e.g., '5 MINUTE', '1 HOUR')
        """
        
        print(f"\nCalculating savings for: {task_name}")
        
        stats = self.get_task_execution_stats(task_name, database, schema)
        
        if not stats:
            print("No execution history found, using estimates")
            stats = {
                'avg_execution_time_ms': 10000,
                'warehouse_size': 'X-Small',
                'execution_count': 1
            }
        
        avg_time_seconds = float(stats['avg_execution_time_ms']) / 1000
        warehouse_size = stats['warehouse_size']
        credits_per_hour = self.get_warehouse_credits_per_hour(warehouse_size)
        
        executions_per_year = self._parse_schedule_to_annual_runs(task_schedule)
        
        improvement_pct = self.estimate_runtime_improvement(original_sql, fixed_sql)
        
        time_saved_seconds = float(avg_time_seconds) * (improvement_pct / 100)
        
        credits_per_run_before = (avg_time_seconds / 3600) * credits_per_hour
        credits_per_run_after = ((avg_time_seconds - time_saved_seconds) / 3600) * credits_per_hour
        credits_saved_per_run = credits_per_run_before - credits_per_run_after
        
        annual_credits_saved = credits_saved_per_run * executions_per_year
        annual_cost_saved = annual_credits_saved * self.credit_price
        
        return {
            'task_name': task_name,
            'warehouse_size': warehouse_size,
            'avg_execution_time_seconds': round(avg_time_seconds, 2),
            'estimated_improvement_pct': improvement_pct,
            'time_saved_per_run_seconds': round(time_saved_seconds, 2),
            'credits_saved_per_run': round(credits_saved_per_run, 6),
            'executions_per_year': executions_per_year,
            'annual_credits_saved': round(annual_credits_saved, 2),
            'annual_cost_saved_usd': round(annual_cost_saved, 2)
        }
    
    def _parse_schedule_to_annual_runs(self, schedule):
        """Convert schedule string to annual execution count"""
        
        schedule_lower = schedule.lower()
        
        if 'minute' in schedule_lower:
            minutes = int(re.search(r'\d+', schedule_lower).group())
            return int((365 * 24 * 60) / minutes)
        elif 'hour' in schedule_lower:
            hours = int(re.search(r'\d+', schedule_lower).group())
            return int((365 * 24) / hours)
        elif 'day' in schedule_lower:
            days = int(re.search(r'\d+', schedule_lower).group())
            return int(365 / days)
        else:
            return 365
    
    def close(self):
        """Close connections"""
        if self.sf_conn:
            self.sf_conn.close()
            print("CFOAgent disconnected")


if __name__ == "__main__":
    from sentry_agent import SentryAgent
    from forensic_agent import ForensicAgent
    
    print("Step 1: Detecting failures...")
    sentry = SentryAgent()
    failures = sentry.get_latest_failure_per_task(hours_back=24)
    sentry.close()
    
    if not failures.empty:
        print(f"\nStep 2: Investigating and calculating ROI...\n")
        
        forensic = ForensicAgent()
        cfo = CFOAgent(credit_price_usd=3.0)
        
        for idx, row in failures.head(1).iterrows():
            investigation = forensic.investigate(
                task_name=row['TASK_NAME'],
                query_id=row['QUERY_ID'],
                database=row['DATABASE_NAME'],
                schema=row['SCHEMA_NAME'],
                error_message=row['ERROR_MESSAGE']
            )
            
            fixed_sql = "SELECT order_id, CASE WHEN orders = 0 THEN 0 ELSE revenue / orders END FROM sales"
            
            savings = cfo.calculate_savings(
                task_name=row['TASK_NAME'],
                database=row['DATABASE_NAME'],
                schema=row['SCHEMA_NAME'],
                original_sql=investigation['query_text'],
                fixed_sql=fixed_sql,
                task_schedule='5 MINUTE'
            )
            
            print("\n" + "="*60)
            print(f"COST SAVINGS ANALYSIS: {savings['task_name']}")
            print("="*60)
            print(f"Warehouse Size: {savings['warehouse_size']}")
            print(f"Current Execution Time: {savings['avg_execution_time_seconds']}s")
            print(f"Estimated Improvement: {savings['estimated_improvement_pct']}%")
            print(f"Time Saved Per Run: {savings['time_saved_per_run_seconds']}s")
            print(f"Credits Saved Per Run: {savings['credits_saved_per_run']}")
            print(f"Annual Executions: {savings['executions_per_year']:,}")
            print(f"\nANNUAL SAVINGS: ${savings['annual_cost_saved_usd']:,.2f}")
            print("="*60)
        
        forensic.close()
        cfo.close()