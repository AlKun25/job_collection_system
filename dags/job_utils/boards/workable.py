# url = "https://apply.workable.com/butterflymx/#jobs"
# best structured API response

from datetime import datetime, timezone
import json
import time
from typing import List
from dags.job_utils.boards.board import JobBoardAPI
from dags.job_utils.utils.constants import JSON_DIR
from dags.job_utils.db.models import Company, Job
from dags.job_utils.db.manager import DBManager
import pandas as pd
import requests
from tqdm import tqdm
# TODO: implement _get_title_field

class WorkableAPI(JobBoardAPI):
    def __init__(self, db, company_code: str, job_type: str = "FT", country_code: str = "US"):
        super().__init__(db=db, platform="Workable", company_code=company_code)
        self.job_type = job_type
        self.country_code = country_code
        self.job_type_mapping = { "FT": "Full-time" }

    def get_job_postings(self):
        response = requests.get(
            f"https://www.workable.com/api/accounts/{self.company_code}?details=true", # true for job description
            headers={"accept": "application/json"},
        )
        
        # data = response.json()["jobs"] if response.status_code == 200 else None

        if not response or "jobs" not in response:
            return None
        
        jobs = response["jobs"]
        # add keyword search in job_title
        jobs = self.filter_locations(jobs)
        jobs = self.filter_employment_type(jobs)
        jobs = self.filter_job_titles(jobs)
        # TODO: add filter for time period - updated_at, published_at or created_at
        return jobs if len(jobs) > 0 else None
    
    def filter_job_titles(self, jobs: List) -> List:
        filtered_jobs = []
        for job in jobs:
            for title in self.roles:
                if title in job["title"].lower():
                    filtered_jobs.append(job)
                    break
        return filtered_jobs
    
    def filter_employment_type(self, jobs: List) -> List:
        filtered_jobs = []
        for job in jobs:
            if job["employment_type"]:
                if job["employment_type"] == self.job_type_mapping[self.job_type]:
                    filtered_jobs.append(job)
            else:
                filtered_jobs.append(job)
        return filtered_jobs
    
    def filter_locations(self, jobs: List) -> List:
        filtered_jobs = []
        for job in jobs:
            if self.country_code == job["locations"][0]["countryCode"]:
                filtered_jobs.append(job)
            elif self.check_country_code(job["country"]):
                filtered_jobs.append(job)
            else:
                print("No location found for job:", job["title"])
                print("Job ID:", job["shortcode"])
        return filtered_jobs
    
    def transform_job_posting(self, data, company_code: str) -> Job | None:
        company = self.db.get_by_filter(
            Company, name=company_code, platform=self.platform
        )
        
        if not company:
            company = self.db.add(
                Company(
                    name=company_code,
                    platform=self.platform,
                    updated_at=datetime.now(timezone.utc),
                )
            )
        
        return Job(
            title=data["title"],
            company_id=company.id,
            employment_type=self.job_type,
            location=self.country_code,
            published_date=datetime.fromisoformat(data["published_on"]),
            # ?: compensation needs to be NLP'd from Description
            description=data["description"],
            url=data["url"],
        )
        
if __name__ == "__main__":
    db = DBManager("user123", "postgresql://user:pass@localhost/dbname")
    companies = (
        pd.read_csv("companies_classified.csv")
        .loc[pd.read_csv("companies_classified.csv")["Platform"] == "Workable"][
            "Company"
        ]
        .tolist()
    )
    print("Number of Workable companies:", len(companies))
    count = 0
    for company in tqdm(companies, total=len(companies)):  # companies:
        workable = WorkableAPI(db, company_code=company)
        data = workable.get_job_postings()
        if data:
            # TODO: save data to json in the same directory as this script
            with open(f"{JSON_DIR}/workable_{company}.json", "w") as f:
                json.dump(data, f, indent=4)
            # destroy the WorkableAPI object
            del workable
        else:
            print("No data for company:", company)
        count += 1
        if count == 10:
            break
        time.sleep(5)
    # TODO: save it to database