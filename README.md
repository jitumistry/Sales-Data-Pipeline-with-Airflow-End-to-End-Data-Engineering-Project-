# 🚀 Sales Data Pipeline with Airflow (End-to-End Data Engineering Project)

## 📌 Overview
This project demonstrates a production-style data engineering pipeline built using:

- Apache Airflow (orchestration)
- PostgreSQL (source + warehouse)
- Docker (environment setup)
- Python + Pandas (ETL logic)
- SQLAlchemy (DB connectivity)

It simulates a real-world incremental ETL pipeline with:
- SCD Type 2 (Slowly Changing Dimensions)
- Fact table loading
- Metadata-driven incremental loads

---

## 🏗️ Architecture

Source DB (Postgres) → ETL (Python) → Warehouse DB (Postgres) → Airflow Orchestration

---

## 📂 Project Structure
