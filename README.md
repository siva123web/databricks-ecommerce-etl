# 🛒 E-Commerce ETL Pipeline — Azure Databricks + Delta Lake → Azure SQL

> **Author:** Siva Patti | Senior Data Engineer
> **Stack:** Python · PySpark · Databricks · Delta Lake · Azure SQL · Azure Data Factory

A production-grade **end-to-end ETL pipeline** that ingests e-commerce data from a REST API, processes it through the **Medallion Architecture** (Bronze → Silver → Gold) using Apache Spark on Azure Databricks, and serves business-ready analytics tables to Azure SQL Database.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        E-COMMERCE ETL PIPELINE                              │
│                                                                             │
│  ┌──────────────┐     ┌─────────────────────────────────────────────────┐   │
│  │  FakeStore   │     │              Azure Databricks                   │   │
│  │  REST API    │────▶│                                                 │   │
│  │              │     │  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │   │
│  │ /products    │     │  │ 🥉 BRONZE│  │ 🥈 SILVER│  │  🥇 GOLD     │  │   │
│  │ /carts       │────▶│  │ Raw JSON │─▶│  Parsed  │─▶│  Aggregated  │  │   │
│  │ /users       │     │  │ Delta    │  │  Delta   │  │  Delta       │  │   │
│  └──────────────┘     │  └──────────┘  └──────────┘  └──────┬───────┘  │   │
│                       └────────────────────────────────────── │ ────────┘   │
│                                                               │             │
│                                              ┌────────────────▼───────────┐ │
│                                              │      Azure SQL Database    │ │
│                                              │  ┌─────────────────────┐  │ │
│                                              │  │ gold_orders_enriched│  │ │
│                                              │  │ gold_category_revenue│  │ │
│                                              │  │ gold_customer_summary│  │ │
│                                              │  │ gold_top_products   │  │ │
│                                              │  │ gold_daily_trends   │  │ │
│                                              │  └─────────────────────┘  │ │
│                                              └────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘

Orchestration: Databricks Jobs (databricks.yml) / Azure Data Factory
Scheduling:    Daily at 06:00 UTC
```

---

## 📁 Project Structure

```
databricks-ecommerce-etl/
├── notebooks/
│   ├── 01_bronze_ingest.py      # Ingest raw JSON from FakeStore API → Delta Bronze
│   ├── 02_silver_transform.py   # Parse, cleanse, DQ checks → Delta Silver
│   ├── 03_gold_aggregate.py     # Business aggregations → Delta Gold + Azure SQL
│   └── 04_pipeline_runner.py    # Orchestrator — runs all stages sequentially
├── config/
│   └── config.py                # All configuration (Bronze/Silver/Gold/AzureSQL)
├── utils/
│   ├── __init__.py
│   └── helpers.py               # Shared helpers: logger, Delta writer, DQ checks
├── tests/
│   └── test_pipeline.py         # pytest unit tests (chispa + PySpark local)
├── databricks.yml               # Databricks Asset Bundle — jobs & cluster config
├── requirements.txt             # Python dependencies
└── README.md
```

---

## 🔄 Pipeline Layers

### 🥉 Bronze — Raw Ingestion
- Calls FakeStore API endpoints: `/products`, `/carts`, `/users`
- Stores raw JSON strings as-is into Delta tables on DBFS
- Adds metadata: `ingestion_timestamp`, `source_endpoint`, `pipeline_run_id`
- Retry logic with configurable max attempts

### 🥈 Silver — Cleanse & Transform
- Parses raw JSON using explicit PySpark schemas
- Explodes nested `cart.products[]` array into line-item rows
- Applies data quality flags (`dq_flag`: PASS / INVALID_PRICE / NULL_USER etc.)
- Deduplicates records by natural keys
- Flattens nested structs (user address, rating)
- Casts and coerces all types (dates, doubles, integers)

### 🥇 Gold — Business Aggregations → Azure SQL
| Table | Description |
|---|---|
| `gold_orders_enriched` | Joined fact: carts × products × users |
| `gold_category_revenue` | Revenue, order count, avg order value by category |
| `gold_customer_summary` | Customer lifetime value (CLV), order frequency |
| `gold_top_products` | Top 10 products by revenue with rank |
| `gold_daily_order_trends` | Daily revenue, order volume, unique customers |

---

## ⚙️ Setup & Deployment

### 1. Prerequisites
- Azure Databricks workspace (Standard or Premium tier)
- Azure SQL Database (Basic tier is sufficient for dev)
- Python 3.9+
- Databricks CLI v0.18+

### 2. Clone & install
```bash
git clone https://github.com/siva123web/databricks-ecommerce-etl.git
cd databricks-ecommerce-etl
pip install -r requirements.txt
```

### 3. Configure Azure SQL credentials
```bash
databricks secrets create-scope --scope ecommerce-etl
databricks secrets put --scope ecommerce-etl --key sql-server
databricks secrets put --scope ecommerce-etl --key sql-user
databricks secrets put --scope ecommerce-etl --key sql-password
```

### 4. Deploy to Databricks
```bash
databricks configure --token
databricks bundle deploy --target dev
databricks bundle run --target dev ecommerce_etl_pipeline
```

---

## 🔑 Key Design Decisions

- **Medallion Architecture** — clear separation of raw / curated / serving layers
- **Schema enforcement on read** — raw JSON preserved in Bronze; schemas applied in Silver
- **DQ flags not drops** — bad records flagged, not dropped, for auditability
- **Databricks Asset Bundles** — infrastructure-as-code for job definitions
- **Secret Scope** — no credentials in code; all secrets via `dbutils.secrets.get()`
- **Idempotent writes** — overwrite mode + `overwriteSchema=true` makes reruns safe

---

## 👤 Author

**Siva Patti** — Senior Data Engineer (7+ years)
Azure · AWS · GCP · Databricks · Snowflake · Kafka · Airflow

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/siva-patti-931182229/)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/siva123web)
