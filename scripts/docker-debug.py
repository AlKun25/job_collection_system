#!/usr/bin/env python3
"""
Debug script designed for Docker-based Airflow environment.
Run this inside the airflow-scheduler or airflow-webserver container.
"""

import os
import sys
import logging
import pandas as pd
import traceback
from datetime import datetime
from sqlalchemy import create_engine, text, inspect

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/opt/airflow/data/debug_log.txt')
    ]
)
logger = logging.getLogger(__name__)

def check_environment():
    """Check the Docker environment setup."""
    logger.info("Checking environment setup...")
    
    # Check Python path
    logger.info(f"PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")
    
    # Check directory structure
    dirs_to_check = [
        '/opt/airflow/dags',
        '/opt/airflow/data',
        '/opt/airflow/data/job_boards'
    ]
    
    for directory in dirs_to_check:
        if os.path.exists(directory):
            logger.info(f"✅ Directory exists: {directory}")
            # Check permissions
            try:
                test_file = f"{directory}/test_write_permission.txt"
                with open(test_file, 'w') as f:
                    f.write("test")
                os.remove(test_file)
                logger.info(f"✅ Directory is writable: {directory}")
            except Exception as e:
                logger.error(f"❌ Directory not writable: {directory} - {str(e)}")
        else:
            logger.warning(f"⚠️ Directory does not exist: {directory}")
            # Try to create it
            try:
                os.makedirs(directory, exist_ok=True)
                logger.info(f"✅ Created directory: {directory}")
            except Exception as e:
                logger.error(f"❌ Failed to create directory: {directory} - {str(e)}")

def check_database_connection():
    """Check database connections for both Airflow and job_collection."""
    logger.info("Checking database connections...")
    
    # Database URLs from docker-compose
    db_urls = {
        'airflow': 'postgresql://airflow:airflow@postgres/airflow',
        'job_collection': 'postgresql://airflow:airflow@postgres/job_collection'
    }
    
    results = {}
    
    for db_name, db_url in db_urls.items():
        logger.info(f"Testing connection to {db_name} database...")
        try:
            engine = create_engine(db_url)
            conn = engine.connect()
            
            # Test connection
            result = conn.execute(text("SELECT 1")).fetchone()
            if result and result[0] == 1:
                logger.info(f"✅ Connected to {db_name} database")
                
                # Check tables
                inspector = inspect(engine)
                tables = inspector.get_table_names()
                logger.info(f"Tables in {db_name}: {tables}")
                
                if db_name == 'job_collection':
                    # Check companies table
                    if 'companies' in tables:
                        logger.info("✅ companies table exists")
                        
                        # Check schema
                        columns = [c['name'] for c in inspector.get_columns('companies')]
                        logger.info(f"companies columns: {columns}")
                        
                        # Check constraints
                        constraints = inspector.get_unique_constraints('companies')
                        logger.info(f"companies constraints: {constraints}")
                        
                        # Check if empty
                        count = conn.execute(text("SELECT COUNT(*) FROM companies")).fetchone()[0]
                        logger.info(f"companies record count: {count}")
                    else:
                        logger.warning("⚠️ companies table does not exist")
                
                results[db_name] = True
            else:
                logger.error(f"❌ Failed to connect to {db_name} database")
                results[db_name] = False
            
            conn.close()
        except Exception as e:
            logger.error(f"❌ Error connecting to {db_name} database: {str(e)}")
            logger.error(traceback.format_exc())
            results[db_name] = False
    
    return results

def test_job_board_search():
    """Test the job_board_search.py module."""
    logger.info("Testing job_board_search module...")
    
    try:
        # Check if module can be imported
        sys.path.append('/opt/airflow')
        sys.path.append('/opt/airflow/dags')
        
        # Try to import 
        try:
            from dags.job_utils.job_board_search import search_job_boards, URL_PATTERNS, ROLES
            logger.info("✅ Successfully imported job_board_search module")
            
            # Check URL_PATTERNS and ROLES
            logger.info(f"URL_PATTERNS: {URL_PATTERNS}")
            logger.info(f"ROLES: {ROLES}")
            
            # Test search function
            platform = list(URL_PATTERNS.keys())[0]
            url_pattern = URL_PATTERNS[platform]
            role = ROLES[0]
            
            logger.info(f"Testing search_job_boards with {platform}, {role}")
            
            results = search_job_boards(
                url_pattern=url_pattern, 
                role=role,
                platform=platform,
                max_companies=2  # Small number for quick test
            )
            
            if results:
                logger.info(f"✅ search_job_boards returned {len(results)} results")
                logger.info(f"Sample result: {results[0] if results else None}")
                
                # Save results to CSV for testing
                output_dir = "/opt/airflow/data/job_boards/"
                os.makedirs(output_dir, exist_ok=True)
                
                df = pd.DataFrame(results, columns=["Company", "URL", "Platform"])
                test_file = f"{output_dir}test_results.csv"
                df.to_csv(test_file, index=False)
                logger.info(f"✅ Saved test results to {test_file}")
                
                return True
            else:
                logger.warning("⚠️ search_job_boards returned no results")
                return False
                
        except ImportError as e:
            logger.error(f"❌ Failed to import job_board_search: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"❌ Error in test_job_board_search: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def test_database_save():
    """Test saving to the companies database table."""
    logger.info("Testing database save operation...")
    
    try:
        # Import models
        sys.path.append('/opt/airflow')
        sys.path.append('/opt/airflow/dags')
        
        try:
            from dags.job_utils.db.models import Company
            logger.info("✅ Successfully imported Company model")
            
            # Connect to database
            db_url = 'postgresql://airflow:airflow@postgres/job_collection'
            engine = create_engine(db_url)
            
            # Create session
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=engine)
            session = Session()
            
            # Create test company
            test_company = Company(
                name=f"TestCompany-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                platform="test_platform",
                updated_at=datetime.now()
            )
            
            try:
                session.add(test_company)
                session.commit()
                logger.info(f"✅ Successfully added test company to database: {test_company.name}")
                
                # Verify it was added
                result = session.query(Company).filter_by(name=test_company.name).first()
                if result:
                    logger.info(f"✅ Successfully retrieved test company from database")
                    return True
                else:
                    logger.warning("⚠️ Test company was not found after saving")
                    return False
            except Exception as e:
                session.rollback()
                logger.error(f"❌ Error adding test company to database: {str(e)}")
                return False
            finally:
                session.close()
        except ImportError as e:
            logger.error(f"❌ Failed to import Company model: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"❌ Error in test_database_save: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def check_airflow_dag():
    """Check Airflow DAG configuration."""
    logger.info("Checking Airflow DAG configuration...")
    
    try:
        # Try to import the DAG
        sys.path.append('/opt/airflow')
        sys.path.append('/opt/airflow/dags')
        
        try:
            # Import the DAG module without running it
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "job_board_discovery_dag", 
                "/opt/airflow/dags/job_board_discovery_dag.py"
            )
            dag_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(dag_module)
            
            # Check DAG object
            if hasattr(dag_module, 'dag'):
                logger.info(f"✅ DAG object found: {dag_module.dag.dag_id}")
                logger.info(f"DAG schedule: {dag_module.dag.schedule_interval}")
                logger.info(f"DAG start date: {dag_module.dag.start_date}")
                
                # Check tasks
                tasks = dag_module.dag.tasks
                logger.info(f"DAG tasks: {[t.task_id for t in tasks]}")
                
                return True
            else:
                logger.warning("⚠️ No DAG object found in module")
                return False
        except Exception as e:
            logger.error(f"❌ Error importing DAG module: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"❌ Error in check_airflow_dag: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def main():
    """Run all debug checks."""
    logger.info("=== Starting Docker Environment Debug ===")
    
    # Check environment
    check_environment()
    
    # Check database connections
    db_results = check_database_connection()
    
    # Test job board search
    search_result = test_job_board_search()
    
    # Test database save
    db_save_result = test_database_save()
    
    # Check Airflow DAG
    dag_result = check_airflow_dag()
    
    # Summary
    logger.info("=== Debug Summary ===")
    logger.info(f"Airflow DB Connection: {'✅' if db_results.get('airflow', False) else '❌'}")
    logger.info(f"Job Collection DB Connection: {'✅' if db_results.get('job_collection', False) else '❌'}")
    logger.info(f"Job Board Search Test: {'✅' if search_result else '❌'}")
    logger.info(f"Database Save Test: {'✅' if db_save_result else '❌'}")
    logger.info(f"Airflow DAG Check: {'✅' if dag_result else '❌'}")
    
    logger.info("Debug log saved to: /opt/airflow/data/debug_log.txt")

if __name__ == "__main__":
    main()