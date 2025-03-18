from abc import ABC, abstractmethod
from typing import List, Optional
import geopy.geocoders as geocoders
from dags.job_utils.db.models import Job
from dags.job_utils.utils.location_cache import LocationCache

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
        pass
    
    @abstractmethod
    def filter_employment_type(self, jobs: List) -> List:
        # TODO : filter on job types, like FT or not
        pass

    @abstractmethod
    def filter_locations(self, jobs: List) -> List:
        # TODO : filter on job locations, mostly US jobs or not.
        pass
    
    # @abstractmethod
    # def filter_job_description(self, jobs: List) -> List:
    #     # TODO : filter on job description, SDE or not.
    #     pass
    
    # def process_company_jobs(self, company_code: str) -> List["Job"]:
    #     """Template method for processing jobs"""
    #     data = self.get_job_postings(company_code)
    #     if not data:
    #         return []

    #     jobs = []
    #     for job_data in data:
    #         job = self.transform_job_posting(job_data, company_code, self.job_type)
    #         if job:
    #             jobs.append(self.db.add(job))
    #     return jobs
