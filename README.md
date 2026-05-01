
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

```

project/
│
├── dags/
│   ├── sales_pipeline_dag.py
│   └── utils/
│       ├── etl.py
│       └── db.py
│
├── scripts/
│   ├── create_tables.sql
│   └── seed_data.py
│
├── docker-compose.yml
├── requirements.txt
└── README.md

```

---

## ⚙️ Features

### ✅ Incremental Load (Fact Table)
- Uses updated_at column
- Loads only new/updated records
- Tracks last run using metadata table

### ✅ SCD Type 2 (Dimension Table)
- Tracks historical changes
- Maintains:
  - start_date
  - end_date
  - is_current

### ✅ Metadata Table
Tracks ETL state:
```

etl_metadata

* table_name
* last_run

```

### ✅ Airflow DAG
- Fully orchestrated pipeline
- Tasks:
  - Extract
  - Transform
  - Load

---

## 🧱 Database Schema

### 🔹 Source Tables
```

customers
orders

```

### 🔹 Warehouse Tables
```

dim_customers
fact_orders
etl_metadata

```

---

## 🚀 Setup Instructions

### 1. Clone Repo
```

git clone <your-repo-link>
cd project

```

### 2. Start Services
```

docker compose up -d

```

### 3. Create Tables
Run inside Postgres:
```

scripts/create_tables.sql

```

### 4. Seed Data
```

python scripts/seed_data.py

```

### 5. Test ETL Locally
```

python dags/test_etl.py

```

### 6. Run Airflow
- Open: http://localhost:8080
- Enable DAG: sales_pipeline_dag
- Trigger run

---

## 📊 DAG Workflow

```

extract → transform → load

```

- Extract: Reads from source DB
- Transform: Applies SCD2 and incremental logic
- Load: Writes into warehouse

---

## ⚠️ Common Issues & Fixes

### ❌ Connection refused (localhost)
✔ Use Docker service names:
```

source_postgres
warehouse_postgres

```

### ❌ Table not found
✔ Run:
```

create_tables.sql

```

### ❌ Module not found (utils)
✔ Ensure correct structure:
```

dags/utils/

```

---

## 💡 Future Improvements

- Add data validation layer
- Implement logging and monitoring
- Add dbt transformations
- Use cloud storage (S3 or ADLS)
- CI/CD pipeline integration

---

## 🧠 Skills Demonstrated

- Data Engineering
- ETL Pipeline Design
- Airflow Orchestration
- PostgreSQL
- Docker
- Incremental Processing
- Dimensional Modeling

---

## 📢 Connect

If you found this useful or want to collaborate, feel free to connect 🚀
```
