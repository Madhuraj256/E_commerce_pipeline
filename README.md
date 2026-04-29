# E-commerce Data Pipeline (Medallion Architecture)

This project implements an end-to-end data pipeline using PySpark, Docker, and Apache Airflow following the Medallion Architecture (Bronze → Silver → Gold).
The pipeline ingests raw e-commerce data, performs transformations, and produces business-level insights such as customer lifetime value and product performance.

## Tech Stack

- PySpark (data processing)
- Docker (containerization)
- Apache Airflow (orchestration)
- Python
- Delta Lake (optional if used)

- ## Dataset

- Brazilian E-commerce Dataset (Olist)
- Contains orders, customers, products, and payments data

- ## Pipeline Design

### Bronze Layer
- Raw data ingestion
- No transformations

### Silver Layer
- Data cleaning and standardization
- Handling null values and duplicates

### Gold Layer
- Aggregations and business metrics
- Customer Lifetime Value (CLV)
- Product performance

- ## How to Run

1. Start Docker containers:
   docker-compose up

2. Run pipeline:
   docker exec -it bd-pyspark-jupyter-lab spark-submit /opt/project/main.py

3. Access Airflow:
   http://localhost:8088
