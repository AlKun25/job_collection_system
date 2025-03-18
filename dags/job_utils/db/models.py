from sqlalchemy import func, Column, Integer, String, DateTime, ForeignKey, Text, Float, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    platform = Column(String(50), nullable=False)
    updated_at = Column(DateTime, server_default=func.now())
    jobs = relationship("Job", back_populates="company")
    __table_args__ = (UniqueConstraint("name", "platform", name="uix_company_board"),)

class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    employment_type = Column(String(50), default="IDK")
    company_id = Column(Integer, ForeignKey("companies.id"))
    platform_job_id = Column(String(255))  # Added platform_job_id field
    location = Column(String(255))
    published_date = Column(DateTime, server_default=func.now())
    compensation = Column(Integer, default=-1)
    description_path = Column(String(255))
    # description = Column(Text, nullable=False)
    url = Column(Text, nullable=False)
    score = Column(Float, default=-1)
    company = relationship("Company", back_populates="jobs")
    # Removed applications relationship

class Application(Base):
    __tablename__ = "applications"
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"))
    status = Column(String(50))
    applied_date = Column(DateTime, server_default=func.now())
    notes = Column(Text)
    job = relationship("Job")  # One-way relationship to Job