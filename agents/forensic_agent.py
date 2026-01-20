import os
from dotenv import load_dotenv
import snowflake.connector
from anthropic import Anthropic

load_dotenv()

class ForensicAgent:
    """
    Investigates failed tasks by analyzing query text and error messages.
    Uses Claude to diagnose root cause and propose SQL fixes.
    """
    
    def __init__(self):
        self.sf_conn = None
        self.claude = Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
        
    def connect_snowflake(self):
        """Establish Snowflake connection"""
        try:
            self.sf_conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
            )
            print("ForensicAgent connected to Snowflake")
        except Exception as e:
            print(f"Snowflake connection error: {str(e)}")
            raise
    
    def get_query_text(self, query_id):
        """Fetch the actual SQL query text from QUERY_HISTORY"""
        if not self.sf_conn:
            self.connect_snowflake()
        
        query = f"""
        SELECT 
            query_text,
            database_name,
            schema_name,
            execution_time,
            error_message
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE query_id = '{query_id}'
        """
        
        cursor = self.sf_conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return {
                'query_text': result[0],
                'database': result[1],
                'schema': result[2],
                'execution_time': result[3],
                'error_message': result[4]
            }
        return None
    
    def get_table_ddl(self, database, schema, table_name):
        """Get table structure using GET_DDL function"""
        if not self.sf_conn:
            self.connect_snowflake()
        
        try:
            cursor = self.sf_conn.cursor()
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")
            
            ddl_query = f"SELECT GET_DDL('TABLE', '{table_name}')"
            cursor.execute(ddl_query)
            
            result = cursor.fetchone()
            cursor.close()
            
            return result[0] if result else None
        except Exception as e:
            print(f"Could not fetch DDL: {str(e)}")
            return None
    
    def investigate_with_claude(self, failure_context):
        """
        Send failure context to Claude for root cause analysis and fix proposal.
        
        Args:
            failure_context: Dict with query_text, error_message, table_ddl
        """
        
        prompt = f"""You are a Snowflake SQL expert analyzing a failed task.

FAILED QUERY:
{failure_context['query_text']}

ERROR MESSAGE:
{failure_context['error_message']}

TABLE DDL (if available):
{failure_context.get('table_ddl', 'Not available')}

Please provide:
1. ROOT CAUSE: Explain what caused this failure in 2-3 sentences
2. FIXED SQL: Provide the corrected SQL query
3. EXPLANATION: Explain what you changed and why

Format your response clearly with these three sections."""

        try:
            message = self.claude.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1000,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            return message.content[0].text
            
        except Exception as e:
            print(f"Claude API error: {str(e)}")
            return None
    
    def investigate(self, task_name, query_id, database, schema, error_message):
        """
        Full investigation workflow for a failed task.
        Returns diagnosis and proposed fix.
        """
        
        print(f"\nInvestigating: {task_name}")
        print(f"Query ID: {query_id}")
        
        query_details = self.get_query_text(query_id)
        
        if not query_details:
            return {"error": "Could not fetch query details"}
        
        print(f"Retrieved query text: {len(query_details['query_text'])} characters")
        
        table_ddl = None
        if 'FROM' in query_details['query_text'].upper():
            try:
                table_name = query_details['query_text'].upper().split('FROM')[1].split()[0].strip()
                table_ddl = self.get_table_ddl(database, schema, table_name)
            except:
                pass
        
        failure_context = {
            'query_text': query_details['query_text'],
            'error_message': error_message,
            'table_ddl': table_ddl
        }
        
        print("Sending to Claude for analysis...")
        diagnosis = self.investigate_with_claude(failure_context)
        
        return {
            'task_name': task_name,
            'query_text': query_details['query_text'],
            'error_message': error_message,
            'diagnosis': diagnosis,
            'execution_time_ms': query_details['execution_time']
        }
    
    def close(self):
        """Close connections"""
        if self.sf_conn:
            self.sf_conn.close()
            print("ForensicAgent disconnected")


if __name__ == "__main__":
    from sentry_agent import SentryAgent
    
    print("Step 1: Detecting failures...")
    sentry = SentryAgent()
    failures = sentry.get_latest_failure_per_task(hours_back=24)
    sentry.close()
    
    if failures.empty:
        print("No failures detected")
    else:
        print(f"\nStep 2: Investigating {len(failures)} failed tasks...\n")
        
        forensic = ForensicAgent()
        
        for idx, row in failures.iterrows():
            result = forensic.investigate(
                task_name=row['TASK_NAME'],
                query_id=row['QUERY_ID'],
                database=row['DATABASE_NAME'],
                schema=row['SCHEMA_NAME'],
                error_message=row['ERROR_MESSAGE']
            )
            
            print("\n" + "="*60)
            print(f"TASK: {result['task_name']}")
            print("="*60)
            print(result['diagnosis'])
            print("\n")
        
        forensic.close()