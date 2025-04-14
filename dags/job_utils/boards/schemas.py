from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, Union
from datetime import datetime


class JobPosting(BaseModel):
    """Standardized model for job postings across different platforms."""

    platform_job_id: str
    title: str
    location: Optional[str] = None
    employment_type: Optional[str] = "FT"
    description: Optional[str] = None
    published_date: Optional[datetime] = None
    compensation: Union[str, int, float, Dict, None] = -1
    url: str
    company_id: Optional[int] = None

    # Store original data for reference
    raw_data: Dict[str, Any] = Field(default_factory=dict, exclude=True)

    @classmethod
    def from_ashby(
        cls, data: Dict[str, Any], company_id: Optional[int] = None
    ) -> "JobPosting":
        """Convert Ashby API data to standardized job posting."""
        return cls(
            platform_job_id=data["id"],
            title=data["title"],
            location=data.get("location", ""),
            employment_type="FT"
            if data.get("employmentType") == "FullTime"
            else data.get("employmentType", "FT"),
            description=data.get("descriptionHtml", ""),
            published_date=datetime.fromisoformat(data["publishedAt"])
            if "publishedAt" in data
            else None,
            compensation=data.get("compensation", {}).get("compensationTierSummary", -1)
            if data.get("shouldDisplayCompensationOnJobPostings", False)
            else -1,
            url=data["jobUrl"],
            company_id=company_id,
            raw_data=data,
        )

    @classmethod
    def from_greenhouse(
        cls, data: Dict[str, Any], company_id: Optional[int] = None
    ) -> "JobPosting":
        """Convert Greenhouse API data to standardized job posting."""
        location = (
            data.get("location", {}).get("name", "")
            if isinstance(data.get("location"), dict)
            else ""
        )

        return cls(
            platform_job_id=str(data["id"]),
            title=data["title"],
            location=location,
            employment_type="FT",  # Default since Greenhouse doesn't clearly specify
            description=data.get("content", ""),
            published_date=datetime.fromisoformat(data["updated_at"])
            if "updated_at" in data
            else None,
            compensation=-1,  # Greenhouse doesn't provide compensation data
            url=data["absolute_url"],
            company_id=company_id,
            raw_data=data,
        )

    @classmethod
    def from_lever(
        cls, data: Dict[str, Any], company_id: Optional[int] = None
    ) -> "JobPosting":
        """Convert Lever API data to standardized job posting."""
        return cls(
            platform_job_id=data["id"],
            title=data["text"],
            location=data.get("categories", {}).get("location", ""),
            employment_type="FT",  # Default or extract from categories
            description=data.get("descriptionPlain", ""),
            published_date=datetime.fromisoformat(data["createdAt"])
            if "createdAt" in data
            else None,
            compensation=-1,  # Need NLP to extract from description
            url=data["hostedUrl"],
            company_id=company_id,
            raw_data=data,
        )

    @classmethod
    def from_workable(
        cls, data: Dict[str, Any], company_id: Optional[int] = None
    ) -> "JobPosting":
        """Convert Workable API data to standardized job posting."""
        return cls(
            platform_job_id=data.get("shortcode", ""),
            title=data["title"],
            location=data.get("locations", [{}])[0].get("location", "")
            if data.get("locations")
            else "",
            employment_type="FT"
            if data.get("employment_type") == "Full-time"
            else data.get("employment_type", "FT"),
            description=data.get("description", ""),
            published_date=datetime.fromisoformat(data["published_on"])
            if "published_on" in data
            else None,
            compensation=-1,  # Workable doesn't provide standard compensation data
            url=data["url"],
            company_id=company_id,
            raw_data=data,
        )

    def to_job_model(self) -> Dict[str, Any]:
        """Convert to a dictionary ready for the Job ORM model."""
        return {
            "title": self.title,
            "employment_type": self.employment_type,
            "company_id": self.company_id,
            "platform_job_id": self.platform_job_id,
            "location": self.location,
            "published_date": self.published_date,
            "compensation": self.compensation,
            "description": self.description,
            "url": self.url,
            "score": -1,  # Default score for new jobs
        }
