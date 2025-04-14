from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import yagmail
from dags.job_utils.job_board_search import search_job_boards, URL_PATTERNS, ROLES
from dags.job_utils.db.models import Company
from dags.job_utils.db.manager import DBContext
from dotenv import load_dotenv

load_dotenv()


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
    schedule_interval="0 3 * * *",  # Daily at 3am
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["job_search"],
)


# Function to search for job boards and directly save to the database
def search_and_save_to_db(url_pattern: str, role: str, platform: str, **kwargs):
    max_companies = int(Variable.get("max_companies_per_search", default_var=100))
    companies_added, companies_updated = search_job_boards(
        url_pattern=url_pattern,
        role=role,
        platform=platform,
        max_companies=max_companies,
    )

    return {
        "companies_added": companies_added,
        "companies_updated": companies_updated,
        "platform": platform,
        "role": role,
    }


# Function to generate a report of all search operations
def generate_report(**kwargs):
    ti = kwargs["ti"]
    total_companies_added = 0
    total_companies_updated = 0
    report = []

    # Gather results from all search tasks
    for platform, _ in URL_PATTERNS.items():
        for role in ROLES:
            task_id = f"search_save_{platform}_{role.replace(' ', '_')}"
            result = ti.xcom_pull(task_ids=task_id)

            if result:
                total_companies_added += result.get("companies_added", 0)
                total_companies_updated += result.get("companies_updated", 0)
                report.append(
                    {
                        "platform": result.get("platform", ""),
                        "role": result.get("role", ""),
                        "companies_added": result.get("companies_added", 0),
                        "companies_updated": result.get("companies_updated", 0),
                    }
                )

    # Get current total number of companies in the database
    # For counting total records, we'll need to use a database session directly
    # since DBContext doesn't expose a count method
    total_in_db = 0
    with DBContext(connection_url="postgresql://airflow:airflow@postgres/job_collection") as db_manager:
        all_companies = db_manager.get_by_filter(model=Company, return_all=True)
        if all_companies:  # handling None
            total_in_db = len(all_companies)
        else:
            total_in_db = 0
    return {
        "total_companies_added": total_companies_added,
        "total_companies_updated": total_companies_updated,
        "total_companies_in_db": total_in_db,
        "report": report,
    }


def send_email(**kwargs):
    ti = kwargs["ti"]
    result = ti.xcom_pull(task_ids="generate_report")
    if result["total_companies_added"] > 0:
        receiver = os.getenv("RECEIVER_EMAIL")
        body = """
        Here are the results of the job board discovery run on {date}:
        
        Total companies added: {total_companies_added}
        Total companies updated: {total_companies_updated}
        Overall companies in database: {total_companies_in_db}
        
        Report:
        {report}
        """.format(
            date=datetime.now().strftime("%Y-%m-%d"),
            total_companies_added=result.get("total_companies_added", 0),
            total_companies_updated=result.get("total_companies_updated", 0),
            total_companies_in_db=result.get("total_companies_in_db", 0),
            report="\n".join(
                [
                    f"Platform: {r['platform']}, Role: {r['role']}, Companies Added: {r['companies_added']}, Companies Updated: {r['companies_updated']}"
                    for r in result.get("report", [])
                ]
            ),
        )
        # !: Authentication not working - keyring registration required
        yag = yagmail.SMTP(os.getenv("SENDER_USERNAME"), oauth2_file=os.getenv("SENDER_OAUTH2_FILE"))
        try:
            yag.send(
                to=receiver,
                subject=f"YAGMAIL TEST: Job Board Discovery Report {datetime.now().strftime('%Y-%m-%d')}",
                contents=body,
            )
        except Exception as e:
            raise e
        
        return {"companies_added": result["total_companies_added"], "email_sent": True}
    else:
        return {"companies_added": 0, "email_sent": False}


# Create tasks for each URL pattern and role combination
search_tasks = []
for platform, url_pattern in URL_PATTERNS.items():
    for role in ROLES:
        task_id = f"search_save_{platform}_{role.replace(' ', '_')}"

        task = PythonOperator(
            task_id=task_id,
            python_callable=search_and_save_to_db,
            op_kwargs={"url_pattern": url_pattern, "role": role, "platform": platform},
            dag=dag,
        )

        search_tasks.append(task)

# Merge results and generate report
report_task = PythonOperator(
    task_id="generate_report",
    python_callable=generate_report,
    dag=dag,
)

# Send email on success task.
email_task = PythonOperator(
    task_id="send_email",
    python_callable=send_email,
    dag=dag,
)

# Set task dependencies
for task in search_tasks:
    task >> report_task

report_task >> email_task