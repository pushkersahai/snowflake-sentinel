import os
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process
from agents.sentry_agent import SentryAgent
from agents.forensic_agent import ForensicAgent
from agents.cfo_agent import CFOAgent
from agents.notification_agent import NotificationAgent

load_dotenv()

class SnowflakeSentinelCrew:
    """
    Multi-agent crew for detecting, diagnosing, and calculating ROI 
    for failed Snowflake tasks.
    """
    
    def __init__(self):
        self.sentry = SentryAgent()
        self.forensic = ForensicAgent()
        self.cfo = CFOAgent(credit_price_usd=3.0)
        self.notifier = NotificationAgent()
        
    def create_agents(self):
        """Define CrewAI agents with roles and goals"""
        
        sentry_agent = Agent(
            role="Failure Detection Specialist",
            goal="Monitor Snowflake task executions and identify all failures",
            backstory="""You are an expert at monitoring data pipeline health.
            You continuously scan Snowflake task history to catch failures before 
            they impact business operations. You have deep knowledge of Snowflake's
            metadata views and can quickly identify problematic tasks.""",
            verbose=True,
            allow_delegation=False
        )
        
        forensic_agent = Agent(
            role="SQL Debugging Expert",
            goal="Diagnose root causes of failed tasks and propose fixes",
            backstory="""You are a senior database engineer with 15 years of experience.
            You excel at reading error messages, analyzing SQL queries, and identifying
            bugs. You use AI assistance (Claude) to rapidly diagnose issues and propose
            battle-tested solutions.""",
            verbose=True,
            allow_delegation=False
        )
        
        cfo_agent = Agent(
            role="Cost Optimization Analyst",
            goal="Calculate financial impact and ROI of proposed fixes",
            backstory="""You are a financial analyst specializing in cloud cost optimization.
            You understand Snowflake's credit-based pricing model and can estimate the
            cost savings from performance improvements. You translate technical fixes
            into business value.""",
            verbose=True,
            allow_delegation=False
        )
        
        return sentry_agent, forensic_agent, cfo_agent
    
    def create_tasks(self, sentry_agent, forensic_agent, cfo_agent):
        """Define tasks for each agent"""
        
        detect_task = Task(
            description="""Scan Snowflake TASK_HISTORY for failed tasks in the last 24 hours.
            Return a list of unique failed tasks with their query IDs, error messages,
            and metadata.""",
            agent=sentry_agent,
            expected_output="List of failed tasks with query_id, error_message, database, schema"
        )
        
        diagnose_task = Task(
            description="""For each failed task identified, perform root cause analysis:
            1. Retrieve the actual SQL query text
            2. Get table DDL if applicable
            3. Use Claude AI to diagnose the issue
            4. Propose a fixed SQL query
            Return diagnosis with original SQL, fixed SQL, and explanation.""",
            agent=forensic_agent,
            expected_output="Diagnosis report with root cause, fixed SQL, and explanation"
        )
        
        calculate_task = Task(
            description="""For each diagnosed failure with proposed fix:
            1. Get task execution statistics (runtime, warehouse size)
            2. Estimate performance improvement from the fix
            3. Calculate credits saved per run
            4. Calculate annual cost savings
            Return financial impact analysis.""",
            agent=cfo_agent,
            expected_output="ROI analysis with annual cost savings in USD"
        )
        
        return [detect_task, diagnose_task, calculate_task]
    
    def run_investigation(self):
        """Execute the full investigation workflow"""
        
        print("\n" + "="*60)
        print("SNOWFLAKE SENTINEL - AI-POWERED TASK HEALING")
        print("="*60 + "\n")
        
        print("Phase 1: Detecting failures...")
        failures = self.sentry.get_latest_failure_per_task(hours_back=24)
        
        if failures.empty:
            print("No failures detected. All tasks are healthy.")
            return []
        
        print(f"Detected {len(failures)} failed task(s)\n")
        
        results = []
        
        for idx, row in failures.iterrows():
            print(f"\n{'='*60}")
            print(f"INVESTIGATING: {row['TASK_NAME']}")
            print(f"{'='*60}\n")
            
            print("Phase 2: Forensic analysis...")
            investigation = self.forensic.investigate(
                task_name=row['TASK_NAME'],
                query_id=row['QUERY_ID'],
                database=row['DATABASE_NAME'],
                schema=row['SCHEMA_NAME'],
                error_message=row['ERROR_MESSAGE']
            )
            
            print("\nClaude's Diagnosis:")
            print(investigation['diagnosis'])
            
            print("\nPhase 3: Calculating ROI...")
            
            fixed_sql = self._extract_fixed_sql(investigation['diagnosis'])
            
            savings = self.cfo.calculate_savings(
                task_name=row['TASK_NAME'],
                database=row['DATABASE_NAME'],
                schema=row['SCHEMA_NAME'],
                original_sql=investigation['query_text'],
                fixed_sql=fixed_sql if fixed_sql else investigation['query_text'],
                task_schedule='5 MINUTE'
            )
            
            print(f"\nFinancial Impact:")
            print(f"  Estimated Improvement: {savings['estimated_improvement_pct']}%")
            print(f"  Annual Executions: {savings['executions_per_year']:,}")
            print(f"  Annual Savings: ${savings['annual_cost_saved_usd']:,.2f}")
            
            results.append({
                'task_name': row['TASK_NAME'],
                'error_message': row['ERROR_MESSAGE'],
                'investigation': investigation,
                'savings': savings
            })
            # Send human-in-the-loop notification
            print("\nPhase 4: Sending notification to data team...")
            notification_sent = self.notifier.send_fix_proposal_email({
                'task_name': row['TASK_NAME'],
                'error_message': row['ERROR_MESSAGE'],
                'original_sql': investigation['query_text'],
                'fixed_sql': fixed_sql if fixed_sql else 'No automated fix available',
                'savings': savings
            })
            
            if notification_sent:
                print("Notification sent successfully")
        
        self._print_summary(results)
        
        return results
    
    def _extract_fixed_sql(self, diagnosis_text):
        """Extract the fixed SQL from Claude's response"""
        if not diagnosis_text:
            return None
        
        if "FIXED SQL:" in diagnosis_text:
            parts = diagnosis_text.split("FIXED SQL:")
            if len(parts) > 1:
                sql_part = parts[1].split("EXPLANATION:")[0] if "EXPLANATION:" in parts[1] else parts[1]
                return sql_part.strip()
        
        return None
    
    def _print_summary(self, results):
        """Print executive summary"""
        
        total_savings = sum([r['savings']['annual_cost_saved_usd'] for r in results])
        
        print("\n" + "="*60)
        print("EXECUTIVE SUMMARY")
        print("="*60)
        print(f"Total Failed Tasks Analyzed: {len(results)}")
        print(f"Total Annual Savings Potential: ${total_savings:,.2f}")
        print("\nRecommendation: Review and approve fixes in Streamlit dashboard")
        print("="*60 + "\n")
    
    def close(self):
        """Close all agent connections"""
        self.sentry.close()
        self.forensic.close()
        self.cfo.close()


if __name__ == "__main__":
    crew = SnowflakeSentinelCrew()
    
    try:
        results = crew.run_investigation()
    finally:
        crew.close()