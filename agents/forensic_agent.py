import os
from dotenv import load_dotenv
import snowflake.connector
from anthropic import Anthropic

load_dotenv()

class ForensicAgent:
    """
    Investigates failed tasks by analyzing query text and error messages.
    Uses Claude to diagnose root cause and propose SQL fixes with explicit reasoning chain.
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
        Send failure context to Claude for root cause analysis with explicit reasoning chain.
        
        Args:
            failure_context: Dict with query_text, error_message, table_ddl
        
        Returns:
            Dict with diagnosis and reasoning_steps
        """
        
        prompt = f"""You are a senior database engineer performing root cause analysis on a failed Snowflake task.

Follow this reasoning process and document each step:

STEP 1 - ANALYZE ERROR
Examine the error message and identify the specific failure type.

STEP 2 - CONTEXT CHECK  
Review the SQL query and table DDL to understand what the query was attempting to do.

STEP 3 - ROOT CAUSE IDENTIFICATION
Determine the exact condition that triggered the failure.

STEP 4 - PROPOSE FIX
Provide the corrected SQL query with inline comments explaining the changes.

STEP 5 - VALIDATION
Explain why this fix resolves the issue and what edge cases it handles.

---

FAILED QUERY:
{failure_context['query_text']}

ERROR MESSAGE:
{failure_context['error_message']}

TABLE DDL:
{failure_context.get('table_ddl', 'Not available')}

---

Provide your analysis following the 5-step structure above. Be specific and technical."""

        try:
            message = self.claude.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1500,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            response_text = message.content[0].text
            
            # Parse reasoning steps
            reasoning_steps = self._parse_reasoning_steps(response_text)
            
            return {
                'full_analysis': response_text,
                'reasoning_steps': reasoning_steps,
                'fixed_sql': self._extract_fixed_sql(response_text)
            }
            
        except Exception as e:
            print(f"Claude API error: {str(e)}")
            return None
    
    def _parse_reasoning_steps(self, response_text):
        """Extract structured reasoning steps from Claude's response"""
        
        steps = {
            'step1_analyze_error': '',
            'step2_context_check': '',
            'step3_root_cause': '',
            'step4_propose_fix': '',
            'step5_validation': ''
        }
        
        step_markers = [
            ('STEP 1', 'step1_analyze_error'),
            ('STEP 2', 'step2_context_check'),
            ('STEP 3', 'step3_root_cause'),
            ('STEP 4', 'step4_propose_fix'),
            ('STEP 5', 'step5_validation')
        ]
        
        for i, (marker, key) in enumerate(step_markers):
            if marker in response_text:
                start = response_text.find(marker)
                # Find the next step marker or end of text
                if i + 1 < len(step_markers):
                    next_marker = step_markers[i + 1][0]
                    end = response_text.find(next_marker) if next_marker in response_text else len(response_text)
                else:
                    end = len(response_text)
                
                steps[key] = response_text[start:end].strip()
        
        return steps
    
    def _extract_fixed_sql(self, response_text):
        """Extract fixed SQL from response"""
        
        # Look for SQL code blocks
        if '```sql' in response_text:
            parts = response_text.split('```sql')
            if len(parts) > 1:
                sql_part = parts[1].split('```')[0]
                return sql_part.strip()
        
        # Fallback: look for SELECT statements in STEP 4
        if 'STEP 4' in response_text:
            step4_section = response_text.split('STEP 4')[1]
            if 'SELECT' in step4_section.upper():
                # Extract first complete SELECT statement
                lines = step4_section.split('\n')
                sql_lines = []
                in_sql = False
                for line in lines:
                    if 'SELECT' in line.upper():
                        in_sql = True
                    if in_sql:
                        sql_lines.append(line)
                        if ';' in line:
                            break
                return '\n'.join(sql_lines).strip()
        
        return None
    
    def investigate(self, task_name, query_id, database, schema, error_message):
        """
        Full investigation workflow for a failed task.
        Returns diagnosis and proposed fix with reasoning chain.
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
            'diagnosis': diagnosis['full_analysis'] if diagnosis else None,
            'reasoning_steps': diagnosis['reasoning_steps'] if diagnosis else {},
            'fixed_sql': diagnosis['fixed_sql'] if diagnosis else None,
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
            
            if result.get('reasoning_steps'):
                for step_key, step_text in result['reasoning_steps'].items():
                    if step_text:
                        print(f"\n{step_text}\n")
            else:
                print(result['diagnosis'])
            
            if result.get('fixed_sql'):
                print("\nFIXED SQL:")
                print(result['fixed_sql'])
            
            print("\n")
        
        forensic.close()