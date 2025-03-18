from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dags.job_utils.db.models import Base
import os
import sys
import time

def init_database():
    # Get database URL from environment or use default
    db_url = os.getenv('DATABASE_URL', 'postgresql://airflow:airflow@postgres/job_collection')
    
    print(f"Attempting to initialize database using: {db_url}")
    
    # Add retry logic for database connection
    max_retries = 5
    retry_interval = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            # Create engine
            engine = create_engine(db_url)
            
            # Test connection
            connection = engine.connect()
            connection.close()
            
            # If we get here, connection was successful
            print("Successfully connected to database")
            
            # Import models here to avoid potential circular imports
            try:
                print("Successfully imported models")
                
                # Create tables
                Base.metadata.create_all(engine)
                print("Database tables created successfully")
                
                return True
            except ImportError as e:
                print(f"Error importing models: {e}")
                print(f"Python path: {sys.path}")
                return False
                
        except SQLAlchemyError as e:
            print(f"Database connection attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                print("Maximum retries reached. Could not connect to database.")
                return False
    
    return False

if __name__ == "__main__":
    success = init_database()
    if not success:
        print("Database initialization failed")
        # Optionally exit with error code
        sys.exit(1)
    else:
        print("Database initialization completed successfully")