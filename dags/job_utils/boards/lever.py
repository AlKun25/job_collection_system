# Example page: https://jobs.lever.co/lime
import json
import time
from tqdm import tqdm
from typing import List
import requests
from datetime import datetime, timezone
import pandas as pd
from dags.job_utils.boards.board import JobBoardAPI
from dags.job_utils.db.manager import DBManager
from dags.job_utils.db.models import Company, Job
from dags.job_utils.utils.constants import JSON_DIR

class LeverAPI(JobBoardAPI):
    def __init__(self, db, company_code: str, job_type: str = "FT", country_code: str = "US"):
        super().__init__(db=db, platform="Lever", company_code=company_code)
        self.job_type = job_type
        self.country_code = country_code
        self.job_type_mapping = {"FT": "Full-time"}

    def get_job_postings(self):
        api_endpoint = f"https://api.lever.co/v0/postings/{self.company_code}"
        response = requests.get(api_endpoint)
        jobs = response.json() if response.status_code == 200 else None
        # *: Debugging stuff
        # print(type(data), type(data[0]), data[0].keys(), data[0]["categories"].keys())
        
        # Filter job postings based on user needs
        jobs = self.filter_locations(jobs)
        jobs = self.filter_employment_type(jobs)
        jobs = self.filter_job_titles(jobs)
        # TODO: add filter for time period - updated_at, published_at or created_at
        return jobs if len(jobs) > 0 else None

    def filter_job_titles(self, jobs: List) -> List:
        # filter on job titles like "software"engineer"
        filtered_jobs = []
        for job in jobs:
            for title in self.roles:
                if title in job["text"].lower():
                    filtered_jobs.append(job)
                    break
        return filtered_jobs
    
    def filter_employment_type(self, jobs: List) -> List:
        # filter on job types, like FT or not
        filtered_jobs = []
        for job in jobs:
            if "categories" in job and "commitment" in job["categories"]:
                if self.job_type_mapping[self.job_type] in job["categories"]["commitment"]:
                    filtered_jobs.append(job)
            elif "categories" not in job or "commitment" not in job["categories"]:
                    filtered_jobs.append(job)
            else:
                print("No job type found for job:", job["text"])
                print("Job ID:", job["id"])
        return filtered_jobs

    def filter_locations(self, jobs: List) -> List:
        # filter on job locations, mostly US jobs or not.
        filtered_jobs = []
        for job in jobs:
            if self.country_code == job["country"]:
                filtered_jobs.append(job)
            # ? : elif pass for trying "allLocations"
            else:
                print("No location found for job:", job["text"])
                print("Job ID:", job["id"])
        return filtered_jobs

    def transform_job_posting(self, data) -> Job | None:
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

        return Job(
            title=data["text"],
            company_id=company.id,
            employment_type=self.job_type,
            location=self.country_code,
            published_date=datetime.fromisoformat(data["createdAt"]),
            compensation=-1,  # NLP data["additionaPlain"] further later on.
            description=data["descriptionPlain"],
            url=data["hostedUrl"],
        )


if __name__ == "__main__":
    db = DBManager("user123", "postgresql://user:pass@localhost/dbname")
    companies = (
        pd.read_csv("companies_classified.csv")
        .loc[pd.read_csv("companies_classified.csv")["Platform"] == "Lever"][
            "Company"
        ]
        .tolist()
    )
    print("Number of Lever companies:", len(companies))
    count = 0
    for company in tqdm(companies, total=len(companies)):  # companies:
        lever = LeverAPI(db, company_code=company)
        data = lever.get_job_postings()
        if data:
            # TODO: save data to json in the same directory as this script
            with open(f"{JSON_DIR}/lever_{company}.json", "w") as f:
                json.dump(data, f, indent=4)
            # destroy the LeverAPI object
            del lever
        else:
            print("No data for company:", company)
        count += 1
        if count == 10:
            break
        time.sleep(5)
