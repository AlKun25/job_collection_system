# Example page: https://jobs.ashbyhq.com/writer
# models.py stays the same

# ashby.py
import json
import time
from typing import List
from tqdm import tqdm
import requests
from datetime import datetime, timezone
from dags.job_utils.db.models import Company, Job
from dags.job_utils.db.manager import DBManager
from dags.job_utils.boards.board import JobBoardAPI
from dags.job_utils.utils.constants import JSON_DIR
import pandas as pd

class AshbyAPI(JobBoardAPI):
    def __init__(self, db, company_code: str, job_type: str = "FT", country_code: str = "US"):
        super().__init__(db=db, platform="Ashby", company_code=company_code)
        self.job_type = job_type
        self.country_code = country_code
        self.job_type_mapping = {"FT": "FullTime"}

    def get_job_postings(self):
        response = requests.get(
            f"https://api.ashbyhq.com/posting-api/job-board/{self.company_code}",
            params={"includeCompensation": "true"},
        )
        # there could be some metadata that might be useful, but is lost in here.
        data = response.json()["jobs"] if response.status_code == 200 else None
        data = self.filter_locations(data)
        data = self.filter_employment_type(data)
        data = self.filter_job_titles(data)
        return data if len(data) > 0 else None

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
            if job["employmentType"]:
                if job["employmentType"] == self.job_type_mapping[self.job_type]:
                    filtered_jobs.append(job)
            else:
                filtered_jobs.append(job)
        return filtered_jobs

    def filter_locations(self, jobs: List) -> List:
        filtered_jobs = []
        for job in jobs:
            # ? : can we also add if case for "location"
            if self.check_country_code(query=job["location"]):
                filtered_jobs.append(job)
            elif job["address"]:
                country = job["address"]["postalAddress"]["addressCountry"]
                if self.check_country_code(query=country):
                    filtered_jobs.append(job)
            else:
                print("No location or address found for job:", job["title"])
                print("Job ID:", job["id"])
        return filtered_jobs

    def transform_job_posting(self, data):
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
            published_date=datetime.fromisoformat(data["publishedAt"]),
            compensation=data["compensation"]["compensationTierSummary"]
            if data["shouldDisplayCompensationOnJobPostings"]
            else -1,
            description=data["descriptionHtml"],
            url=data["jobUrl"],
        )


if __name__ == "__main__":
    db = DBManager("user123", "postgresql://user:pass@localhost/dbname")
    # TODO : read companies_classified.csv and filter by board type = ashby
    companies = (
        pd.read_csv("companies_classified.csv")
        .loc[pd.read_csv("companies_classified.csv")["Platform"] == "Ashby"]["Company"]
        .tolist()
    )
    print("Number of Ashby companies:", len(companies))
    count = 0
    for company in tqdm(companies, total=len(companies)):  # companies:
        print("Processing company:", company)
        ashby = AshbyAPI(db, company_code=company)
        data = ashby.get_job_postings()
        if data:
            # TODO: save data to json in the same directory as this script
            with open(f"{JSON_DIR}/ashby_{company}.json", "w") as f:
                json.dump(data, f, indent=4)
            # destroy the AshbyAPI object
            del ashby
        else:
            print("No job postings found for ", company)
        count += 1
        if count == 10:
            break
        time.sleep(5)

    # TODO: filter the job roles and description for our use case
    # TODO: save it to database
