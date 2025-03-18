# Example page: https://ats.rippling.com/mytra
from datetime import datetime, timezone
import json
import time
from typing import List
from boards.board import JobBoardAPI
from classes.models import Company, Job
from db.manager import DBManager
import pandas as pd
import requests
from langchain_community.document_loaders import SeleniumURLLoader
from classes.constants import JSON_DIR
from tqdm import tqdm

class RipplingAPI(JobBoardAPI):
    
    def __init__(self, db, company_code: str):
        super().__init__(db, platform="Rippling", company_code=company_code)

    def get_job_postings(self):
        api_endpoint = (
            f"https://api.rippling.com/platform/api/ats/v1/board/{self.company_code}/jobs"
        )
        response = requests.get(api_endpoint)
        data = response.json() if response.status_code == 200 else None
        # print(type(data), type(data[0]), data[0].keys())
        data = self.filter_locations(data) if data is not None else None
        data = self.filter_employment_type(data)
        data = self.filter_job_titles(data) if data is not None else None
        return data
    
    @staticmethod
    def get_job_description(url: str) -> str:
        loader = SeleniumURLLoader(urls=[url], headless=True, continue_on_failure=True, browser="firefox")
        description = loader.load()[0].page_content if loader.load() else ""
        return description

    def filter_job_titles(self, jobs: List) -> List:
        filtered_jobs = []
        for job in jobs:
            for title in self.roles:
                if title in job["name"].lower():
                    filtered_jobs.append(job)
                    break
        return filtered_jobs

# ? : maybe try fuzzy matching on job description
    def filter_employment_type(self, jobs: List) -> List:
        filtered_jobs = []
        for job in jobs:
            if job["name"].lower().find(("intern", "contract", "temporary")) == -1:
                filtered_jobs.append(job)
            else:
                print("This job may not be full-time: ", job["name"], job["url"])
        return super().filter_employment_type(jobs)

    def filter_locations(self, jobs: List) -> List:
        filtered_jobs = []
        for job in jobs:
            if self.check_country_code(job["workLocation"]["label"]):
                filtered_jobs.append(job)
            else:
                print("No location found for job:", job["name"])
                print("Job Location:", job["workLocation"])
                print("Job ID:", job["uuid"])
        return filtered_jobs

    # TODO: Not consistent with other job boards
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
            title=data["name"],
            company_id=company.id,
            employment_type=self.job_type,
            location=self.country_code,
            # *: No published date for Rippling, even in description.
            # ? : NLP on scraped description further for compensation.
            description=self.get_job_description(url=data["url"]),
            url=data["url"],
        )



if __name__ == "__main__":
    db = DBManager("user123", "postgresql://user:pass@localhost/dbname")
    companies = (
        pd.read_csv("companies_classified.csv")
        .loc[pd.read_csv("companies_classified.csv")["Platform"] == "Rippling"][
            "Company"
        ]
        .tolist()
    )
    print("Number of Rippling companies:", len(companies))
    count = 0
    for company in tqdm(companies, total=len(companies)): 
        rippling = RipplingAPI(db, company_code=company)
        data = rippling.get_job_postings()
        if data:
            # TODO: save data to json in the same directory as this script
            with open(f"{JSON_DIR}/rippling_{company}.json", "w") as f:
                json.dump(data, f, indent=4)
            # destroy the RipplingAPI object
            del rippling
        else:
            print("No data for company:", company)
        count += 1
        if count == 10:
            break
        time.sleep(5)
    
        # TODO: filter the job roles and description for our use case
        # TODO: save it to database
