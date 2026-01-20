import os
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

load_dotenv()

class NotificationAgent:
    """
    Sends human-in-the-loop notifications via email when fixes are proposed.
    Alerts data engineering teams for approval before deployment.
    """
    
    def __init__(self):
        self.from_email = os.getenv('NOTIFICATION_EMAIL_FROM')
        self.to_email = os.getenv('NOTIFICATION_EMAIL_TO')
        self.password = os.getenv('NOTIFICATION_EMAIL_PASSWORD')
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
    
    def send_fix_proposal_email(self, incident_details):
        """
        Send email notification when a fix is proposed.
        
        Args:
            incident_details: Dict with task_name, error_message, 
                            original_sql, fixed_sql, savings
        """
        
        task_name = incident_details['task_name']
        error_msg = incident_details['error_message']
        original_sql = incident_details['original_sql']
        fixed_sql = incident_details['fixed_sql']
        savings = incident_details['savings']
        
        subject = f"[Snowflake Sentinel] Fix Proposed: {task_name}"
        
        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6;">
            <h2 style="color: #1e88e5;">Snowflake Task Failure Detected</h2>
            
            <div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 20px 0;">
                <strong>Task:</strong> {task_name}<br>
                <strong>Error:</strong> {error_msg}<br>
                <strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </div>
            
            <h3 style="color: #333;">AI-Proposed Fix</h3>
            
            <div style="margin: 20px 0;">
                <h4>Original SQL (Broken):</h4>
                <pre style="background-color: #f5f5f5; padding: 15px; border: 1px solid #ddd; overflow-x: auto;">
{original_sql}
                </pre>
            </div>
            
            <div style="margin: 20px 0;">
                <h4>Fixed SQL:</h4>
                <pre style="background-color: #e8f5e9; padding: 15px; border: 1px solid #4caf50; overflow-x: auto;">
{fixed_sql if fixed_sql else 'No automated fix available'}
                </pre>
            </div>
            
            <h3 style="color: #333;">Financial Impact</h3>
            
            <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
                <tr style="background-color: #f5f5f5;">
                    <td style="padding: 10px; border: 1px solid #ddd;"><strong>Warehouse Size</strong></td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{savings['warehouse_size']}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;"><strong>Current Runtime</strong></td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{savings['avg_execution_time_seconds']:.2f} seconds</td>
                </tr>
                <tr style="background-color: #f5f5f5;">
                    <td style="padding: 10px; border: 1px solid #ddd;"><strong>Estimated Improvement</strong></td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{savings['estimated_improvement_pct']}%</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;"><strong>Annual Executions</strong></td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{savings['executions_per_year']:,}</td>
                </tr>
                <tr style="background-color: #e8f5e9;">
                    <td style="padding: 10px; border: 1px solid #ddd;"><strong>Annual Savings</strong></td>
                    <td style="padding: 10px; border: 1px solid #ddd; font-weight: bold; color: #4caf50;">
                        ${savings['annual_cost_saved_usd']:,.2f}
                    </td>
                </tr>
            </table>
            
            <div style="background-color: #e3f2fd; padding: 15px; border-left: 4px solid #1e88e5; margin: 20px 0;">
                <strong>Next Steps:</strong><br>
                1. Review the proposed SQL fix above<br>
                2. Log into the Snowflake Sentinel dashboard to approve or reject<br>
                3. If approved, the fix will be deployed automatically
            </div>
            
            <p style="color: #666; font-size: 12px; margin-top: 30px;">
                This notification was sent by Snowflake Sentinel AI Agent System<br>
                Powered by Claude 3.5 Sonnet
            </p>
        </body>
        </html>
        """
        
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = self.to_email
            
            html_part = MIMEText(html_body, 'html')
            msg.attach(html_part)
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.from_email, self.password)
                server.send_message(msg)
            
            print(f"Email notification sent to {self.to_email}")
            return True
            
        except Exception as e:
            print(f"Failed to send email: {str(e)}")
            return False
    
    def send_summary_email(self, all_incidents):
        """
        Send a summary email with all detected failures.
        
        Args:
            all_incidents: List of incident detail dicts
        """
        
        total_savings = sum([inc['savings']['annual_cost_saved_usd'] for inc in all_incidents])
        
        subject = f"[Snowflake Sentinel] Daily Summary: {len(all_incidents)} Tasks Need Attention"
        
        incident_rows = ""
        for inc in all_incidents:
            incident_rows += f"""
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;">{inc['task_name']}</td>
                <td style="padding: 10px; border: 1px solid #ddd;">{inc['error_message'][:50]}...</td>
                <td style="padding: 10px; border: 1px solid #ddd;">${inc['savings']['annual_cost_saved_usd']:,.2f}</td>
            </tr>
            """
        
        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6;">
            <h2 style="color: #1e88e5;">Snowflake Sentinel - Daily Summary</h2>
            
            <div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 20px 0;">
                <strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d')}<br>
                <strong>Failed Tasks:</strong> {len(all_incidents)}<br>
                <strong>Total Potential Savings:</strong> ${total_savings:,.2f}/year
            </div>
            
            <h3>Incidents Detected</h3>
            
            <table style="border-collapse: collapse; width: 100%; margin: 20px 0;">
                <tr style="background-color: #1e88e5; color: white;">
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Task Name</th>
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Error</th>
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Annual Savings</th>
                </tr>
                {incident_rows}
            </table>
            
            <div style="background-color: #e3f2fd; padding: 15px; border-left: 4px solid #1e88e5; margin: 20px 0;">
                <strong>Action Required:</strong><br>
                Log into the Snowflake Sentinel dashboard to review and approve fixes
            </div>
        </body>
        </html>
        """
        
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = self.to_email
            
            html_part = MIMEText(html_body, 'html')
            msg.attach(html_part)
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.from_email, self.password)
                server.send_message(msg)
            
            print(f"Summary email sent to {self.to_email}")
            return True
            
        except Exception as e:
            print(f"Failed to send summary email: {str(e)}")
            return False


if __name__ == "__main__":
    # Test the notification system
    notifier = NotificationAgent()
    
    test_incident = {
        'task_name': 'TASK_BROKEN_DIVISION',
        'error_message': 'Division by zero',
        'original_sql': 'SELECT revenue / orders FROM sales',
        'fixed_sql': 'SELECT CASE WHEN orders = 0 THEN 0 ELSE revenue / orders END FROM sales',
        'savings': {
            'warehouse_size': 'X-Small',
            'avg_execution_time_seconds': 0.15,
            'estimated_improvement_pct': 5,
            'executions_per_year': 105120,
            'annual_cost_saved_usd': 0.21
        }
    }
    
    notifier.send_fix_proposal_email(test_incident)