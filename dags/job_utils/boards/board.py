from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import geopy.geocoders as geocoders
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from dags.job_utils.db.models import Job, Company
from dags.job_utils.boards.schemas import JobPosting
from dags.job_utils.utils.location_cache import LocationCache
import logging

geocoders.options.default_timeout = 3


class JobBoardAPI(ABC):
    roles = [
        "software engineer",
        "ai engineer",
        "machine learning engineer",
        "backend engineer",
        "data scientist",
        "data engineer",
    ]
    location_cache = LocationCache()
    geolocator = geocoders.Photon()

    def __init__(self, db, platform: str, company_code: str):
        self.db = db
        self.platform = platform
        self.company_code = company_code

        # Create session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _make_api_request(
        self, url: str, params: Optional[Dict] = None, timeout: int = 10
    ) -> Optional[Dict]:
        """Make API request with error handling and retries.

        Args:
            url: API endpoint URL
            params: Query parameters
            timeout: Request timeout in seconds

        Returns:
            Response JSON or None if failed
        """
        try:
            response = self.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(
                f"API request failed for {self.platform}/{self.company_code}: {str(e)}"
            )
            return None

    def get_or_create_company(self) -> Company:
        """Get existing company or create new one.
        
        Returns:
            Company object from database.
        """
        company = self.db.get_by_filter(
            Company, name=self.company_code, platform=self.platform
        )
        
        if not company:
            company = self.db.add(
                Company(
                    name=self.company_code,
                    platform=self.platform,
                    updated_at=datetime.now(timezone.utc),
                )
            )
        
        return company

    def process_jobs(self) -> List[Dict[str, Any]]:
        """Process jobs from the API and return standardized format.
        
        This is a template method that defines the workflow.
        
        Returns:
            List of standardized job dictionaries.
        """
        # Get the company ID
        company = self.get_or_create_company()
        
        # Get raw job postings
        raw_jobs = self.get_job_postings()
        if not raw_jobs:
            return []
        
        # Convert to standardized format
        processed_jobs = []
        for job_data in raw_jobs:
            try:
                # Convert based on platform
                if self.platform == "Ashby":
                    posting = JobPosting.from_ashby(job_data, company.id)
                elif self.platform == "Greenhouse":
                    posting = JobPosting.from_greenhouse(job_data, company.id)
                elif self.platform == "Lever":
                    posting = JobPosting.from_lever(job_data, company.id)
                elif self.platform == "Workable":
                    posting = JobPosting.from_workable(job_data, company.id)
                else:
                    continue  # Skip unsupported platforms
                
                # Add to results
                processed_jobs.append(posting.to_job_model())
            except Exception as e:
                logging.error(f"Error processing job: {e}")
                continue
        
        return processed_jobs

    @classmethod
    def check_country_code(cls, query: str) -> bool:
        # Check in cache first
        is_us = cls.location_cache.is_us_location(query)
        if is_us is not None:  # If found in either cache
            return is_us

        # Handle remote locations
        if "remote" in query.lower():
            if "United States" in query or cls.country_code in query:
                cls.location_cache.add_location(query, is_us=True)
                return True

        # Check with geolocation service
        try:
            location = cls.geolocator.geocode(query)
            if location and "properties" in location.raw:
                is_us = location.raw["properties"]["countrycode"] == cls.country_code
                cls.location_cache.add_location(query, is_us=is_us)
                return is_us
        except Exception:
            pass  # Handle exception silently

        return False  # Default to False if can't determine

    @abstractmethod
    def get_job_postings(self, company_code: str):
        """Fetch job postings from the API"""
        pass

    @abstractmethod
    def transform_job_posting(
        self, data, company_code: str, job_type: str
    ) -> Optional["Job"]:
        """Transform API response to Job model"""
        pass

    @abstractmethod
    def filter_job_titles(self, jobs: List) -> List:
        # TODO : filter on job titles, like SDE or not.
        if not jobs:
            return []

        filtered_jobs = []
        for job in jobs:
            title_field = self._get_title_field()
            if not title_field or title_field not in job:
                continue

            job_title = job[title_field].lower()
            for title in self.roles:
                if title in job_title:
                    filtered_jobs.append(job)
                    break
        return filtered_jobs

    @abstractmethod
    def filter_employment_type(self, jobs: List) -> List:
        """Filter jobs based on employment type.

        Args:
            jobs: List of job posting data.

        Returns:
            Filtered list of jobs.
        """
        if not jobs:
            return []

        # Default implementation that child classes can override
        return jobs

    @abstractmethod
    def filter_locations(self, jobs: List) -> List:
        # TODO : filter on job locations, mostly US jobs or not.
        pass

    # @abstractmethod
    # def filter_job_description(self, jobs: List) -> List:
    #     # TODO : filter on job description, SDE or not.
    #     pass
