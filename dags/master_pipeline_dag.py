from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from dags.job_utils.db.db_logger import logger

def check_database_health():
    """Verify database is accessible before running DAG tasks"""
    from sqlalchemy import create_engine, text
    import os
    
    connection_url = os.environ.get('DATABASE_URL', 'postgresql://airflow:airflow@postgres/job_collection')
    
    try:
        engine = create_engine(connection_url)
        with engine.connect() as conn:
            # Simple query to test connectivity
            result = conn.execute(text("SELECT 1")).scalar()
            
            # Check required tables exist
            table_check = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name IN ('companies', 'jobs')"
            )).scalar()
            
            if table_check < 2:
                raise Exception(f"Missing required tables. Found {table_check}/2 tables.")
                
        logger.info("Database health check passed")
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        return False


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
    'master_job_pipeline',
    default_args=default_args,
    description='Master pipeline to orchestrate all job posting collection processes',
    schedule_interval='0 0 * * 1',  # Weekly on Monday
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['job_search'],
)

# Start task
start = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Add health check task to DAG
db_health_check = PythonOperator(
    task_id='database_health_check',
    python_callable=check_database_health,
    dag=dag,
)

# Trigger Job Board Discovery DAG
trigger_discovery = TriggerDagRunOperator(
    task_id='trigger_job_board_discovery',
    trigger_dag_id='job_board_discovery',
    wait_for_completion=True,
    dag=dag,
)

# Wait for Job Board Discovery to complete
wait_for_discovery = ExternalTaskSensor(
    task_id='wait_for_job_board_discovery',
    external_dag_id='job_board_discovery',
    external_task_id='send_email',
    timeout=3600,
    mode='reschedule',
    poke_interval=60,
    dag=dag,
)

# Trigger Job Posting Collection DAG
trigger_collection = TriggerDagRunOperator(
    task_id='trigger_job_posting_collection',
    trigger_dag_id='job_posting_collection',
    wait_for_completion=True,
    dag=dag,
)

# Wait for Job Posting Collection to complete
wait_for_collection = ExternalTaskSensor(
    task_id='wait_for_job_posting_collection',
    external_dag_id='job_posting_collection',
    external_task_id='save_jobs_to_database',
    timeout=3600,
    mode='reschedule',
    poke_interval=60,
    dag=dag,
)

# Trigger Keyword Extraction DAG
trigger_extraction = TriggerDagRunOperator(
    task_id='trigger_keyword_extraction',
    trigger_dag_id='keyword_extraction',
    wait_for_completion=True,
    dag=dag,
)

# Wait for Keyword Extraction to complete
wait_for_extraction = ExternalTaskSensor(
    task_id='wait_for_keyword_extraction',
    external_dag_id='keyword_extraction',
    external_task_id='update_job_scores',
    timeout=3600,
    mode='reschedule',
    poke_interval=60,
    dag=dag,
)

# End task
end = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Set task dependencies
start >> db_health_check >> trigger_discovery >> wait_for_discovery >> trigger_collection >> wait_for_collection >> trigger_extraction >> wait_for_extraction >> end