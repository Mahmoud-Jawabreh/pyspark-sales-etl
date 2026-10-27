# PySpark Sales Data Engineering Pipeline

A hands-on data engineering project built to explore production-style batch and streaming pipelines using PySpark, Delta Lake, Kafka, Airflow, testing, and data quality practices.

> **Status:** In active development

## Current Features

- PySpark batch ETL pipeline
- Explicit schemas and reusable transformation modules
- Parquet output
- Data quality checks and rejection thresholds
- Pipeline metrics and structured logging
- CDC transformation logic
- SCD Type 2 logic
- Unit and integration testing with pytest
- Airflow orchestration
- Airflow retries and failure propagation
- FileSensor-based source waiting
- Airflow Connections
- Dockerized Airflow environment
- Environment-specific configuration

## Architecture

Current batch workflow:

```text
Source CSV
    ↓
Airflow FileSensor
    ↓
Input Validation
    ↓
PySpark ETL Pipeline
    ↓
Data Quality Checks
    ↓
Parquet Output
    ↓
Output Validation
```

## Project Structure

```text
pyspark-sales-etl/
├── airflow/
│   ├── dags/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── requirements.txt
├── configs/
│   ├── dev.yaml
│   └── docker.yaml
├── data/
├── src/
│   ├── cdc/
│   ├── config/
│   ├── dimensions/
│   ├── ingestion/
│   ├── pipelines/
│   ├── quality/
│   ├── transformations/
│   └── utils/
├── tests/
├── requirements.txt
└── run_batch_pipeline.py
```

## Technologies

- Python
- PySpark
- Apache Spark
- Apache Airflow
- Docker
- Delta Lake
- Apache Kafka
- pytest
- Parquet
- YAML configuration

## Testing

Run the test suite with:

```bash
python -m pytest -v
```

## Running the Batch Pipeline

Locally:

```bash
python run_batch_pipeline.py
```

The project also supports execution through Airflow using the Docker-based environment.

## Roadmap

Planned improvements include:

- Production streaming refinements
- Streaming recovery and checkpoint management
- Streaming health and lag monitoring
- Databricks / cloud data engineering
- Expanded Delta Lake architecture
- End-to-end production-style data platform
- Final architecture documentation and diagrams

## Security

Secrets and environment-specific credentials are excluded from Git. Files such as `.env`, Airflow runtime logs, Spark output, checkpoints, and local virtual environments are ignored through `.gitignore`.