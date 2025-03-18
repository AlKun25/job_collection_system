import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add the dags directory to the Python path
sys.path.append("/opt/airflow/dags")

# Import custom modules
from dags.job_utils.job_board_search import search_job_boards, URL_PATTERNS, ROLES
from dags.job_utils.db.models import Company

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Create DAG
dag = DAG(
    "job_board_discovery",
    default_args=default_args,
    description="Discover job boards for companies",
    schedule_interval="0 0 * * 0",  # Weekly on Sunday
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["job_search"],
)

# Function to get database session
def get_db_session():
    engine = create_engine("postgresql://airflow:airflow@postgres/job_collection")
    Session = sessionmaker(bind=engine)
    return Session()

# Function to search for job boards and directly save to the database
def search_and_save_to_db(url_pattern: str, role: str, platform: str, **kwargs):
    max_companies = int(Variable.get("max_companies_per_search", default_var=100))
    results = search_job_boards(url_pattern=url_pattern, role=role, platform=platform, max_companies=max_companies)
    
    # Save results directly to database
    session = get_db_session()
    companies_added = 0
    companies_updated = 0
    
    try:
        for company, url, platform_name in results:
            # Check if company already exists
            existing = session.query(Company).filter_by(
                name=company, 
                platform=platform_name
            ).first()
            
            if not existing:
                # Add new company
                new_company = Company(
                    name=company,
                    platform=platform_name,
                    url=url,
                    updated_at=datetime.now()
                )
                session.add(new_company)
                companies_added += 1
            else:
                # Update existing company
                existing.updated_at = datetime.now()
                existing.url = url
                companies_updated += 1
        
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
    
    return {
        "companies_added": companies_added, 
        "companies_updated": companies_updated,
        "platform": platform, 
        "role": role
    }

# Function to generate a report of all search operations
def generate_report(**kwargs):
    ti = kwargs['ti']
    total_companies_added = 0
    total_companies_updated = 0
    report = []
    
    # Gather results from all search tasks
    for platform, _ in URL_PATTERNS.items():
        for role in ROLES:
            task_id = f"search_save_{platform}_{role.replace(' ', '_')}"
            result = ti.xcom_pull(task_ids=task_id)
            
            if result:
                total_companies_added += result.get('companies_added', 0)
                total_companies_updated += result.get('companies_updated', 0)
                report.append({
                    'platform': result.get('platform', ''),
                    'role': result.get('role', ''),
                    'companies_added': result.get('companies_added', 0),
                    'companies_updated': result.get('companies_updated', 0)
                })
    
    # Get current total number of companies in the database
    session = get_db_session()
    try:
        total_in_db = session.query(Company).count()
    finally:
        session.close()
    
    return {
        "total_companies_added": total_companies_added,
        "total_companies_updated": total_companies_updated,
        "total_companies_in_db": total_in_db,
        "report": report
    }

# Create tasks for each URL pattern and role combination
search_tasks = []
for platform, url_pattern in URL_PATTERNS.items():
    for role in ROLES:
        # platform = url_pattern.split('//')[1].split('.')[0]
        task_id = f"search_save_{platform}_{role.replace(' ', '_')}"

        task = PythonOperator(
            task_id=task_id,
            python_callable=search_and_save_to_db,
            op_kwargs={"url_pattern": url_pattern, "role": role, "platform": platform},
            dag=dag,
        )

        search_tasks.append(task)

report_task = PythonOperator(
    task_id="generate_report",
    python_callable=generate_report,
    dag=dag,
)

# Set task dependencies
for task in search_tasks:
    task >> report_task