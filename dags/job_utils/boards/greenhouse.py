# Example page: https://boards.greenhouse.io/eyecarecenter
import json
import time
from typing import List
import pandas as pd
import requests
from datetime import datetime, timezone
from boards.board import JobBoardAPI
from db.manager import DBManager
from dags.job_utils.db.models import Company, Job
from dags.job_utils.utils.constants import JSON_DIR
from tqdm import tqdm


class GreenhouseAPI(JobBoardAPI):
    def __init__(self, db, company_code: str, job_type: str = "FT", country_code: str = "US"):
        super().__init__(db=db, platform="Greenhouse", company_code=company_code)
        self.job_type = job_type
        self.country_code = country_code
        self.job_type_mapping = {"FT": "FullTime"}

    def get_job_postings(self):
        api_endpoint = f"https://boards-api.greenhouse.io/v1/boards/{self.company_code}/jobs"
        response = requests.get(api_endpoint, params={"content": "true"})
        # response = requests.get(api_endpoint)
        data = response.json()["jobs"] if response.status_code == 200 else None
        # add keyword search in job_title
        data = self.filter_locations(data)
        data = self.filter_employment_type(data)
        data = self.filter_job_titles(data)
        return data

    def filter_job_titles(self, jobs: List):
        filtered_jobs = []
        for job in jobs:
            for title in self.roles:
                if title in job["title"].lower():
                    filtered_jobs.append(job)
                    break
        return filtered_jobs

    # ? : maybe try fuzzy matching on job description
    def filter_employment_type(self, jobs: List) -> List:
        filtered_jobs = []
        for job in jobs:
            if job["title"].lower().find(("intern", "contract", "temporary")) == -1:
                filtered_jobs.append(job)
            else:
                print("This job may not be full-time: ", job["name"], job["url"])
        return super().filter_employment_type(jobs)

    def filter_locations(self, jobs: List) -> List:
        # filter on job locations, mostly US jobs or not.
        filtered_jobs = []
        for job in jobs:
            if "location" in job or "name" in job["location"]:
                if self.check_country_code(job["location"]["name"]):
                    filtered_jobs.append(job)
            else:
                print(" location found for job:", job["title"])
                print("Job ID:", job["id"])
                filtered_jobs.append(job)
        return filtered_jobs

    # ? : replace company_code & data["metadata"][1]["value"]
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
            title=data["title"],
            company_id=company.id,
            employment_type=self.job_type,
            location=self.country_code,
            published_date=datetime.fromisoformat(data["updated_at"]),
            compensation=-1,
            description=data["content"],
            url=data["absolute_url"],
        )


if __name__ == "__main__":
    db = DBManager("user123", "postgresql://user:pass@localhost/dbname")
    companies = (
        pd.read_csv("companies_classified.csv")
        .loc[pd.read_csv("companies_classified.csv")["Platform"] == "Greenhouse"][
            "Company"
        ]
        .tolist()
    )
    print("Number of Greenhouse companies:", len(companies))
    count = 0
    for company in tqdm(companies, total=len(companies)):  # companies:
        greenhouse = GreenhouseAPI(db, company_code=company)
        data = greenhouse.get_job_postings()
        if data:
            # TODO: save data to json in the same directory as this script
            with open(f"{JSON_DIR}/greenhouse_{company}.json", "w") as f:
                json.dump(data, f, indent=4)
            # destroy the GreenhouseAPI object
            del greenhouse
        else:
            print("No data for company:", company)
        count += 1
        if count == 10:
            break
        time.sleep(5)
    # TODO: save it to database
