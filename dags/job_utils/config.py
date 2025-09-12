from airflow.models import Variable
import os
from dotenv import load_dotenv

load_dotenv()

# Default values
DEFAULT_CONFIG = {
    # Database
    "sqlalchemy_conn": os.getenv("POSTGRES_SQLALCHEMY_CONN", "postgresql://airflow:airflow@localhost:5432/job_collection"),
    "database_url": os.getenv("POSTGRES_DATABASE_URL", "postgresql://airflow:airflow@localhost:5432/job_collection"),
    
    # API settings
    "min_wait_time": 2,
    "max_wait_time": 5,
    "max_companies_per_search": 100,
    "companies_batch_size": 10,
    "keyword_batch_size": 50,
    
    # Task settings
    "task_timeout": 3600,
    "task_poke_interval": 60,
    
    # Email settings
    "receiver_email": "kunalm.jobs@gmail.com",
    "sender_username": "alkungg.official",
    ""
    
    # Job board platforms
    "enabled_platforms": ["Ashby", "Greenhouse", "Lever", "Workable", "Rippling"],
    
    # Role titles to search for
    "search_roles": [
        "Software Engineer",
        "AI Engineer",
        "Machine Learning Engineer",
        "Backend Engineer"
    ],
}

# Environment-based configuration system
def get_config(key):
    """
    Get configuration value with fallback mechanism:
    1. Check Airflow Variable
    2. Check environment variable
    3. Fall back to default value
    """
    # Try Airflow Variable first
    value = Variable.get(key, default_var=None)
    if value is not None:
        return value
    
    # Try environment variable (with naming convention)
    env_key = f"JOB_BOARD_{key.upper()}"
    if env_key in os.environ:
        return os.environ[env_key]
    
    # Fall back to default
    if key in DEFAULT_CONFIG:
        return DEFAULT_CONFIG[key]
    
    # No value found
    raise KeyError(f"Configuration key '{key}' not found")