from datetime import datetime
import os
import shutil
import time
import random
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from sqlalchemy import create_engine, text
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import WebDriverException

from dags.job_utils.db.manager import DBContext
from dags.job_utils.db.models import Company

# URL patterns and roles configuration
URL_PATTERNS = {
    # "Ashby": "https://jobs.ashbyhq.com",
    # "Greenhouse": "https://boards.greenhouse.io",
    # "Lever": "https://jobs.lever.co",
    "Workable": "https://apply.workable.com",
    # "Rippling": "https://ats.rippling.com",
}

ROLES = [
    # "Software Engineer",
    # "AI Engineer",
    # "Machine Learning Engineer",
    # "Backend Engineer",
]

# Wait time range (seconds)
MIN_WAIT = 2
MAX_WAIT = 5

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def initialize_driver():
    """Set up Firefox WebDriver with WebDriverManager and Docker configurations."""
    logger.info("Setting up Firefox WebDriver...")

    # Configure Firefox options for headless mode in Docker
    options = Options()
    # options.add_argument("--headless")
    
    in_docker = os.path.exists("/.dockerenv")
    if in_docker:
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource issues
        # Find Firefox binary
        firefox_binary = shutil.which("firefox")
        if firefox_binary:
            options.binary_location = firefox_binary
            logger.info(f"Using Firefox binary at: {firefox_binary}")
        else:
            logger.error("Firefox binary not found! Ensure it's installed.")
            # return None

    try:
        # Initialize WebDriver using WebDriverManager
        logger.info("Initializing WebDriver with WebDriverManager...")
        driver = webdriver.Firefox(
            service=FirefoxService(GeckoDriverManager().install()), options=options
        )
        logger.info("✅ Firefox WebDriver initialized successfully.")
        return driver

    except WebDriverException as e:
        logger.error(f"Failed to initialize WebDriver: {str(e.stacktrace)}")
        return None


def cleanup_driver(driver):
    """Safely close the webdriver."""
    if driver:
        driver.quit()
        logger.info("Driver closed")


def save_companies_to_db(results, url_pattern: str, platform: str):
    """
    Save the companies found in search results to the database.

    The function takes a list of search result elements, a URL pattern and a platform as arguments.
    It extracts the company name from the href attribute of the link element in each result,
    and checks if the company already exists in the database. If it does, it updates the
    updated_at timestamp. If not, it adds the company to the database.

    Args:
        results (list): A list of search result elements.
        url_pattern (str): A URL pattern to filter the results.
        platform (str): The job board platform.

    Returns:
        tuple: A tuple containing the number of companies added and updated.
    """
    add_count = 0
    update_count = 0
    try:
        with DBContext(
            connection_url="postgresql://airflow:airflow@localhost:5432/job_collection"
        ) as db_manager:
            for result in results:
                links = result.find_elements(By.XPATH, "./div[2]/div/div/a")
                if not links:
                    raise RuntimeError("Failed to find links in result")

                link = links[0]
                href = link.get_attribute("href")
                if not href:
                    raise RuntimeError("Failed to find href in link")

                if href.startswith(url_pattern):
                    company_name = href.removeprefix(url_pattern).split("/")[1]
                    if company_name:
                        if db_manager.get_by_filter(
                            model=Company, name=company_name, platform=platform
                        ): # check if company exists in DB
                            db_manager.update(
                                obj_id=db_manager.get_by_filter(
                                    model=Company, name=company_name, platform=platform
                                ).id,
                                model=Company,
                                updated_at=datetime.now(),
                            )
                            update_count += 1
                        else: # company does not exist in DB or batch
                            db_manager.add(obj=Company(name=company_name, platform=platform, updated_at=datetime.now()))
                            add_count += 1
                            logger.info(f"Found company: {company_name}")
    except Exception as e:
        logger.error(f"Error processing result: {e}")
    return add_count, update_count


def search_job_boards(url_pattern: str, role: str, platform: str, max_companies=100):
    """
    Search for job boards matching the given URL pattern and role.

    Args:
        url_pattern: Job board URL pattern to search for
        role: Job role to search for
        max_companies: Maximum number of companies to collect (default: 100)

    Returns:
        List of tuples containing (company, url, platform)
    """
    companies_added = 0
    companies_updated = 0
    driver = None

    try:
        driver = initialize_driver()
        logger.info(f"Searching for {role} on {url_pattern}")

        # Navigate to DuckDuckGo
        if driver is None:
            raise RuntimeError("Failed to initialize WebDriver")
        driver.get("https://duckduckgo.com")
        time.sleep(random.randint(MIN_WAIT, MAX_WAIT))

        # Find search input and enter query
        search_form = driver.find_element(By.ID, "searchbox_input")
        if search_form is None:
            raise RuntimeError("Failed to find search input")
        search_form.send_keys(f"{role} site:{url_pattern}")
        search_form.submit()
        time.sleep(random.randint(MIN_WAIT, MAX_WAIT))

        # Loop till you hit the desired number of companies or when you have viewed 2-3x search results.
        more_results_clicks = 0
        result_count = 0
        while companies_added < max_companies:  # *: Condition 1
            try:
                more_results_button = driver.find_element(By.ID, "more-results")
                if more_results_button is None:
                    raise RuntimeError("Failed to find 'more results' button")
                more_results_button.click()
                time.sleep(random.randint(MIN_WAIT, MAX_WAIT))
                more_results_clicks += 1
                # logger.info(f"Clicked 'more results' {more_results_clicks}x times")
                print(f"Clicked 'more results' {more_results_clicks}x times")
            except Exception as e:
                # logger.warning(f"Could not click 'more results' button: {e}")
                print(f"Could not click 'more results' button: {e}")
                break
            # Extract company information from search results
            results = [
                web_element
                for web_element in driver.find_elements(By.TAG_NAME, "article")
                if web_element.get_attribute("data-testid") == "result"
            ]
            if results is None or len(results) == 0:
                raise RuntimeError("Failed to find search results")
            # Slice results from last viewed search result
            results = results[result_count:]
            results_added, results_updated = save_companies_to_db(results, url_pattern, platform)
            # Update counters
            companies_added += results_added
            companies_updated += results_updated
            result_count += len(results)
            # *: Condition 2
            if result_count >= 2 * max_companies:
                break
    except Exception as e:
        logger.error(f"Error in search function: {e}")
    finally:
        if driver:
            cleanup_driver(driver)
    return companies_added, companies_updated


def main():
    """Main function to test the job scraping functionality."""
    all_results = []

    for platform, url_pattern in URL_PATTERNS.items():
        for role in ROLES:
            results = search_job_boards(url_pattern, role, platform, max_companies=30)
            all_results.extend(results)
    print(f"Found {len(all_results)} results")

    # Retrieve companies from database
    connection_url = os.environ.get('DATABASE_URL', 'postgresql://airflow:airflow@localhost:5432/job_collection')
    engine = create_engine(connection_url)
    connection = engine.connect()

    count = connection.execute(text("SELECT COUNT(*) FROM companies")).scalar()
    print(f"Number of companies: {count}")

    # Retrieve all companies
    companies = connection.execute(text("SELECT name, platform FROM companies")).fetchall()
    print(companies)



if __name__ == "__main__":
    main()
