# Job Board Collection System

A distributed system for discovering, collecting, and analyzing job postings from various job boards.

## Architecture

This project implements an ETL pipeline for job postings using Apache Airflow for orchestration. It follows a three-phase approach:

1. **Discovery**: Identifies company job boards across multiple platforms (Ashby, Greenhouse, Lever, etc.)
2. **Collection**: Extracts job postings from discovered boards with platform-specific APIs
3. **Analysis**: Performs keyword extraction and scoring using Spark NLP

## Components

- **Master Pipeline**: Orchestrates the entire workflow (`master_pipeline_dag.py`)
- **Board-Specific APIs**: Adapters for various job board platforms
- **DB Layer**: SQLAlchemy models and data access components
- **Keyword Extractor**: Spark NLP based text analysis

## Infrastructure

The system runs in a containerized environment with:
- Apache Airflow for workflow orchestration
- PostgreSQL for persistent storage
- Apache Spark for text processing
- Kafka for event streaming (future)

## Getting Started

1. Clone this repository
2. Install Docker and Docker Compose
3. Run `docker-compose up -d`
4. Access Airflow UI at http://localhost:8080 (user: admin, password: admin)

## Development

- Add new job board platforms by extending `JobBoardAPI`
- Customize job filtering by modifying the filtering strategy
- Adjust scoring algorithms in `keyword_extractor.py`

## Configuration

Configuration is managed through:
- Environment variables
- Airflow variables
- Platform-specific settings in `constants.py`
