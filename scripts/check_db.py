# Run this using: docker-compose exec airflow-scheduler python /opt/airflow/scripts/check_db.py
from sqlalchemy import create_engine, inspect, text
import sys
import os

def check_database(verbose=True):
    """Simple database connectivity and schema check"""
    connection_url = os.environ.get('DATABASE_URL', 'postgresql://airflow:airflow@postgres/job_collection')
    
    try:
        # Test connection
        engine = create_engine(connection_url)
        connection = engine.connect()
        
        if verbose:
            print("✅ Database connection successful")
        
        # Check tables
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        if verbose:
            print(f"Found {len(tables)} tables: {', '.join(tables)}")
            
        # Verify required tables
        required_tables = ['companies', 'jobs', 'applications']
        missing_tables = [table for table in required_tables if table not in tables]
        
        if missing_tables:
            print(f"❌ Missing tables: {', '.join(missing_tables)}")
            return False
            
        # Basic record count check
        for table in required_tables:
            if table in tables:
                count = connection.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                if verbose:
                    print(f"Table '{table}' has {count} records")
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Database error: {str(e)}")
        return False

if __name__ == "__main__":
    success = check_database()
    sys.exit(0 if success else 1)