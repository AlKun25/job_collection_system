from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import sys

# Add the dags directory to the Python path
sys.path.append('/opt/airflow/dags')

# Import custom modules
from dags.job_utils.job_board_search import search_job_boards, URL_PATTERNS, ROLES
from dags.job_utils.db.models import Company

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create DAG
dag = DAG(
    'job_board_discovery',
    default_args=default_args,
    description='Discover job boards for companies',
    schedule_interval='0 0 * * 0',  # Weekly on Sunday
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['job_search'],
)

# Function to search for job boards for a specific URL pattern and role
def search_and_save_job_boards(url_pattern: str, role: str, platform:str, **kwargs):
    max_companies = int(Variable.get("max_companies_per_search", default_var=100))
    results = search_job_boards(url_pattern=url_pattern, role=role, platform=platform, max_companies=max_companies)
    
    # Save results to CSV
    if results:
        df = pd.DataFrame(results, columns=["Company", "URL", "Platform"])
        output_path = f"/opt/airflow/data/job_boards/{url_pattern.split('//')[1].split('.')[0]}_{role.replace(' ', '_')}.csv"
        df.to_csv(output_path, index=False)
        
        return {"count": len(results), "file_path": output_path}
    
    return {"count": 0, "file_path": None}

# Function to merge all search results
def merge_search_results(**kwargs):
    ti = kwargs['ti']
    all_results = []
    
    for platform,_ in URL_PATTERNS.items():
        for role in ROLES:
            task_id = f"search_{platform}_{role.replace(' ', '_')}"
            result = ti.xcom_pull(task_ids=task_id)
            
            if result and result['count'] > 0:
                df = pd.read_csv(result['file_path'])
                all_results.append(df)
    
    if all_results:
        # Combine all results and remove duplicates
        combined_df = pd.concat(all_results, ignore_index=True)
        combined_df.drop_duplicates(subset=["Company", "Platform"], inplace=True)
        
        # Save combined results
        output_path = "/opt/airflow/data/job_boards/all_companies.csv"
        combined_df.to_csv(output_path, index=False)
        
        return {"count": len(combined_df), "file_path": output_path}
    
    return {"count": 0, "file_path": None}

# Function to save companies to database
def save_companies_to_db(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='merge_search_results')
    
    if result and result['count'] > 0:
        df = pd.read_csv(result['file_path'])
        
        # Connect to database
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        engine = create_engine('postgresql://airflow:airflow@postgres/job_collection')
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Add companies to database
        companies_added = 0
        try:
            for _, row in df.iterrows():
                company = Company(
                    name=row['Company'],
                    platform=row['Platform'],
                    updated_at=datetime.now()
                )
                
                # Check if company already exists
                existing = session.query(Company).filter_by(
                    name=row['Company'], 
                    platform=row['Platform']
                ).first()
                
                if not existing:
                    session.add(company)
                    companies_added += 1
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
        
        return {"companies_added": companies_added}
    
    return {"companies_added": 0}

# Create tasks for each URL pattern and role combination
search_tasks = []
for platform,url_pattern in URL_PATTERNS.items():
    for role in ROLES:
        # platform = url_pattern.split('//')[1].split('.')[0]
        task_id = f"search_{platform}_{role.replace(' ', '_')}"
        
        task = PythonOperator(
            task_id=task_id,
            python_callable=search_and_save_job_boards,
            op_kwargs={'url_pattern': url_pattern, 'role': role, 'platform': platform},
            dag=dag,
        )
        
        search_tasks.append(task)

# Merge results task
merge_task = PythonOperator(
    task_id='merge_search_results',
    python_callable=merge_search_results,
    dag=dag,
)

# Save to database task
save_db_task = PythonOperator(
    task_id='save_companies_to_db',
    python_callable=save_companies_to_db,
    dag=dag,
)

# Set task dependencies
for task in search_tasks:
    task >> merge_task

merge_task >> save_db_task