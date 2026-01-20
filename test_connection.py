import os
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()

# Test connection
def test_snowflake_connection():
    try:
        print("Connecting to Snowflake...")
        
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
        )
        
        print("Connected successfully!")
        
        # Test a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        print(f"Snowflake version: {version[0]}")
        
        cursor.close()
        conn.close()
        
        print("Connection test passed!")
        
    except Exception as e:
        print(f"Connection failed: {str(e)}")

if __name__ == "__main__":
    test_snowflake_connection()