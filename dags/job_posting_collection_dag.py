from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import json
import os
import sys
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add the dags directory to the Python path
sys.path.append('/opt/airflow/dags')

# Import custom modules
from dags.job_utils.boards.ashby import AshbyAPI
from dags.job_utils.db.models import Company, Job
from dags.job_utils.db.manager import DBManager

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'job_posting_collection',
    default_args=default_args,
    description='Collect job postings from company job boards',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['job_search'],
)

# Function to get companies to process
def get_companies_to_process(**kwargs):
    # Connect to database
    engine = create_engine('postgresql://airflow:airflow@postgres/job_collection')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Get companies to process
        companies = session.query(Company).filter(
            Company.platform == 'Ashby'  # Start with just Ashby
        ).all()
        
        # Convert to list of dictionaries
        companies_list = [
            {
                "id": company.id,
                "name": company.name,
                "platform": company.platform
            }
            for company in companies
        ]
        
        return companies_list
    
    except Exception as e:
        print(f"Error getting companies: {e}")
        return []
    
    finally:
        session.close()

# Function to collect job postings for a company
def collect_job_postings(company_data, **kwargs):
    # Create DB manager with user ID from context
    db_manager = DBManager(
        user_id=kwargs['run_id'],
        connection_url='postgresql://airflow:airflow@postgres/job_collection'
    )
    
    # Use your existing DB manager for database operations
    company_code = company_data["name"]
    platform = company_data["platform"]
    
    # Initialize the appropriate API client
    if platform == "Ashby":
        api_client = AshbyAPI(db=db_manager, company_code=company_code)
    else:
        print(f"Unsupported platform: {platform}")
        return []
    
    # Get job postings
    job_postings = api_client.process_jobs()
    
    # Save to JSON file
    if job_postings:
        output_dir = f"/opt/airflow/data/job_postings/{platform.lower()}"
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = f"{output_dir}/{company_code}.json"
        with open(output_file, 'w') as f:
            json.dump(job_postings, f, indent=2)
        
        print(f"Saved {len(job_postings)} job postings for {company_code} to {output_file}")
        
        return {
            "company_id": company_data["id"],
            "company_code": company_code,
            "platform": platform,
            "job_count": len(job_postings),
            "file_path": output_file
        }
    
    return {
        "company_id": company_data["id"],
        "company_code": company_code,
        "platform": platform,
        "job_count": 0,
        "file_path": None
    }

# Function to process companies in batches
def process_companies_batch(**kwargs):
    ti = kwargs['ti']
    companies = ti.xcom_pull(task_ids='get_companies_to_process')
    
    if not companies:
        print("No companies to process")
        return []
    
    # Process in batches with rate limiting
    batch_size = int(Variable.get("companies_batch_size", default_var=10))
    results = []
    
    for i in range(0, len(companies), batch_size):
        batch = companies[i:i+batch_size]
        
        for company_data in batch:
            result = collect_job_postings(company_data)
            results.append(result)
            
            # Rate limiting
            time.sleep(5)  # 5 second delay between API calls
        
        # Batch delay
        if i + batch_size < len(companies):
            time.sleep(30)  # 30 second delay between batches
    
    return results

# Function to save job postings to database
def save_jobs_to_database(**kwargs):
    ti = kwargs['ti']
    results = ti.xcom_pull(task_ids='process_companies_batch')
    
    if not results:
        print("No job posting results to save")
        return {"jobs_added": 0}
    
    # Connect to database
    engine = create_engine('postgresql://airflow:airflow@postgres/job_collection')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    jobs_added = 0
    
    try:
        for result in results:
            if result["job_count"] > 0 and result["file_path"]:
                # Load job postings from file
                with open(result["file_path"], 'r') as f:
                    job_postings = json.load(f)
                
                for posting in job_postings:
                    # Check if job already exists
                    existing_job = session.query(Job).filter_by(
                        company_id=result["company_id"],
                        platform_job_id=posting["platform_job_id"]
                    ).first()
                    
                    if not existing_job:
                        # Add new job
                        job = Job(
                            title=posting["title"],
                            company_id=result["company_id"],
                            employment_type=posting["employment_type"],
                            platform_job_id=posting["platform_job_id"],
                            location=posting["location"],
                            published_date=datetime.fromisoformat(posting["published_date"]),
                            compensation=posting["compensation"],
                            description=posting["description"],
                            url=posting["url"],
                            score=-1  # Default score
                        )
                        
                        # Save description to Parquet
                        description_dir = f"/opt/airflow/data/job_postings/descriptions/{result['platform'].lower()}/{result['company_code']}"
                        os.makedirs(description_dir, exist_ok=True)
                        
                        description_path = f"{description_dir}/{posting['platform_job_id']}.parquet"
                        
                        # Create a DataFrame with the description
                        desc_df = pd.DataFrame({
                            "job_id": [posting["platform_job_id"]],
                            "description": [posting["description"]]
                        })
                        
                        # Save to Parquet
                        desc_df.to_parquet(description_path, index=False)
                        
                        # Set the description path
                        job.description_path = description_path
                        
                        session.add(job)
                        jobs_added += 1
            
            # Update company last checked time
            company = session.query(Company).filter_by(id=result["company_id"]).first()
            if company:
                company.updated_at = datetime.now()
        
        session.commit()
        print(f"Added {jobs_added} new jobs to database")
        
        return {"jobs_added": jobs_added}
    
    except Exception as e:
        session.rollback()
        print(f"Error saving jobs to database: {e}")
        return {"jobs_added": 0}
    
    finally:
        session.close()

# Create tasks
get_companies_task = PythonOperator(
    task_id='get_companies_to_process',
    python_callable=get_companies_to_process,
    dag=dag,
)

process_batch_task = PythonOperator(
    task_id='process_companies_batch',
    python_callable=process_companies_batch,
    dag=dag,
)

save_jobs_task = PythonOperator(
    task_id='save_jobs_to_database',
    python_callable=save_jobs_to_database,
    dag=dag,
)

# Set task dependencies
get_companies_task >> process_batch_task >> save_jobs_task