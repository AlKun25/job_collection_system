import shutil
import time
import random
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import WebDriverException
from tqdm import tqdm

# URL patterns and roles configuration
URL_PATTERNS = {
    "Ashby": "https://jobs.ashbyhq.com",
    # "Greenhouse": "https://boards.greenhouse.io",
    # "Lever": "https://jobs.lever.co",
    # "Rippling": "https://ats.rippling.com",
}

ROLES = [
    "Software Engineer",
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
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource issues
    # options.add_argument("--remote-debugging-port=9222")
    # options.add_argument("--disable-gpu")  # Necessary for some environments
    # options.add_argument("--window-size=1920,1080")

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
    companies = set()
    driver = None

    try:
        driver = initialize_driver()
        logger.info(f"Searching for {role} on {url_pattern}")

        # Navigate to DuckDuckGo
        if driver is None:
            raise RuntimeError("Failed to initialize WebDriver")
        driver.get("https://duckduckgo.com")
        driver.set_page_load_timeout(7)

        # Find search input and enter query
        search_form = driver.find_element(By.ID, "searchbox_input")
        if search_form is None:
            raise RuntimeError("Failed to find search input")
        search_form.send_keys(f"{role} site:{url_pattern}")
        search_form.submit()
        time.sleep(random.randint(MIN_WAIT, MAX_WAIT))

        # Click "more results" button multiple times to load more results
        more_results_clicks = 5
        for i in range(more_results_clicks):
            if len(companies) >= max_companies:
                break

            try:
                more_results_button = driver.find_element(By.ID, "more-results")
                if more_results_button is None:
                    raise RuntimeError("Failed to find 'more results' button")
                more_results_button.click()
                time.sleep(random.randint(MIN_WAIT, MAX_WAIT))
                logger.info(f"Clicked 'more results' ({i + 1}/{more_results_clicks})")
            except Exception as e:
                logger.warning(f"Could not click 'more results' button: {e}")
                break

        # Extract company information from search results
        results = driver.find_elements(By.TAG_NAME, "article")
        if results is None or len(results) == 0:
            raise RuntimeError("Failed to find search results")
        logger.info(f"Found {len(results)} results for {role} on {url_pattern}")

        for result in tqdm(
            results, total=len(results), desc=f"{role} on {url_pattern}"
        ):
            if len(companies) >= max_companies:
                break

            try:
                links = result.find_elements(By.XPATH, "./div[2]/div/div/a")
                if links is None or len(links) == 0:
                    raise RuntimeError("Failed to find links in result")

                if links[0].get_attribute("href"):
                    url = links[0].get_attribute("href")

                    # Check if URL matches the pattern we're looking for
                    if url.startswith(url_pattern):
                        company = url.removeprefix(url_pattern).split("/")[1]
                        if company:
                            # platform = url_pattern.split("//")[1].split(".")[0]
                            companies.add(
                                (company, f"{url_pattern}/{company}", platform)
                            )
                            logger.info(f"Added company: {company}")
            except Exception as e:
                logger.error(f"Error processing result: {e}")

    except Exception as e:
        logger.error(f"Error in search function: {e}")
    finally:
        if driver:
            cleanup_driver(driver)

    return list(companies)

def main():
    """Main function to test the job scraping functionality."""
    all_results = []
    
    for platform, url_pattern in URL_PATTERNS.items():
        for role in ROLES:
            results = search_job_boards(url_pattern, role, platform, max_companies=10)
            all_results.extend(results)
    
    # Convert results to DataFrame and save as CSV
    df = pd.DataFrame(all_results, columns=["Company", "URL", "Platform"])
    df.to_csv("job_scrape_results.csv", index=False)
    
    logger.info(f"Scraping completed. Found {len(all_results)} job postings.")
    
    print(df.head())  # Display first few results
    

if __name__ == "__main__":
    main()
