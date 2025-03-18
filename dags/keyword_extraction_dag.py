from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import json
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add the dags directory to the Python path
sys.path.append('/opt/airflow/dags')

# Import custom modules
from dags.job_utils.keyword_extractor import KeywordExtractor
from dags.job_utils.db.models import Job

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
    'keyword_extraction',
    default_args=default_args,
    description='Extract keywords from job descriptions',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['job_search'],
)

# Function to get jobs that need keyword extraction
def get_jobs_for_extraction(**kwargs):
    # Connect to database
    engine = create_engine('postgresql://airflow:airflow@postgres/job_collection')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Get jobs that need keyword extraction (score = -1)
        jobs = session.query(Job).filter(
            Job.score == -1
        ).all()
        
        job_data = []
        for job in jobs:
            if job.description_path:
                job_data.append({
                    "id": job.id,
                    "description_path": job.description_path
                })
        
        return job_data
    
    except Exception as e:
        print(f"Error getting jobs: {e}")
        return []
    
    finally:
        session.close()

# Function to extract keywords from job descriptions
def extract_keywords(**kwargs):
    ti = kwargs['ti']
    jobs = ti.xcom_pull(task_ids='get_jobs_for_extraction')
    
    if not jobs:
        print("No jobs to process")
        return []
    
    # Process in batches
    batch_size = int(Variable.get("keyword_batch_size", default_var=50))
    keyword_extractor = KeywordExtractor()
    
    results = []
    for i in range(0, len(jobs), batch_size):
        batch = jobs[i:i+batch_size]
        
        # Load descriptions from Parquet files
        job_ids = []
        descriptions = []
        
        for job in batch:
            try:
                # Read description from Parquet
                desc_df = pd.read_parquet(job["description_path"])
                description = desc_df["description"][0]
                
                job_ids.append(job["id"])
                descriptions.append(description)
            except Exception as e:
                print(f"Error reading description for job {job['id']}: {e}")
        
        # Process batch
        batch_results = keyword_extractor.process_job_batch(job_ids, descriptions)
        results.extend(batch_results)
    
    # Save results
    output_dir = "/opt/airflow/data/job_postings/keywords"
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = f"{output_dir}/extracted_keywords_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    return {
        "job_count": len(results),
        "file_path": output_file
    }

# Function to update job scores in database
def update_job_scores(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='extract_keywords')
    
    if not result or result["job_count"] == 0:
        print("No keywords to save")
        return {"jobs_updated": 0}
    
    # Load keywords from file
    with open(result["file_path"], 'r') as f:
        keyword_data = json.load(f)
    
    # Connect to database
    engine = create_engine('postgresql://airflow:airflow@postgres/job_collection')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    jobs_updated = 0
    
    try:
        # Loop through keywords and update jobs
        for job_data in keyword_data:
            job_id = job_data["job_id"]
            keywords = job_data["keywords"]
            
            # Calculate a keyword score based on diversity and relevance
            keyword_score = 0.0
            if keywords:
                # Get unique keywords
                unique_keywords = set(kw["keyword"] for kw in keywords)
                
                # Calculate score based on number of unique keywords
                # Higher score for more unique keywords
                keyword_score = min(1.0, len(unique_keywords) / 20.0)
            
            # Update job in database
            job = session.query(Job).filter_by(id=job_id).first()
            if job:
                # Store keywords as JSON in a column or as a separate table
                # For now, just update the score
                job.score = keyword_score
                jobs_updated += 1
        
        session.commit()
        print(f"Updated {jobs_updated} jobs with keyword scores")
        
        return {"jobs_updated": jobs_updated}
    
    except Exception as e:
        session.rollback()
        print(f"Error updating job scores: {e}")
        return {"jobs_updated": 0}
    
    finally:
        session.close()

# Create tasks
get_jobs_task = PythonOperator(
    task_id='get_jobs_for_extraction',
    python_callable=get_jobs_for_extraction,
    dag=dag,
)

extract_keywords_task = PythonOperator(
    task_id='extract_keywords',
    python_callable=extract_keywords,
    dag=dag,
)

update_scores_task = PythonOperator(
    task_id='update_job_scores',
    python_callable=update_job_scores,
    dag=dag,
)

# Set task dependencies
get_jobs_task >> extract_keywords_task >> update_scores_task