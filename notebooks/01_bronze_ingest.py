# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer — Raw Data Ingestion
# MAGIC **Pipeline:** E-Commerce Orders ETL
# MAGIC **Layer:** Bronze (Raw Zone)
# MAGIC **Source:** FakeStore REST API (https://fakestoreapi.com)
# MAGIC **Sink:** Delta Lake — Bronze tables on DBFS
# MAGIC
# MAGIC Ingests raw JSON from three endpoints:
# MAGIC - `/products`  → `bronze.products`
# MAGIC - `/carts`     → `bronze.carts`
# MAGIC - `/users`     → `bronze.users`

# COMMAND ----------
import requests
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StringType
import sys
sys.path.insert(0, "/Workspace/Repos/databricks-ecommerce-etl")

from config.config import BronzeConfig
from utils.helpers import get_logger, write_delta, create_database_if_not_exists

# COMMAND ----------
# MAGIC %md ## 1. Initialise

logger = get_logger("bronze_ingest")
spark = SparkSession.builder.appName("EcommerceETL_Bronze").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")

RUN_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
logger.info(f"Bronze ingestion started at {RUN_TS}")

# COMMAND ----------
# MAGIC %md ## 2. Create Bronze Database

create_database_if_not_exists(spark, BronzeConfig.DATABASE, BronzeConfig.STORAGE_PATH)

# COMMAND ----------
# MAGIC %md ## 3. Helper — Fetch from API

def fetch_api(endpoint: str) -> list:
    """Fetch JSON array from FakeStore API with retry logic."""
    url = f"{BronzeConfig.API_BASE_URL}/{endpoint}"
    logger.info(f"Fetching: {url}")
    for attempt in range(1, BronzeConfig.MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=BronzeConfig.REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"  ✅ Fetched {len(data)} records from /{endpoint}")
            return data
        except Exception as e:
            logger.warning(f"  Attempt {attempt} failed: {e}")
            if attempt == BronzeConfig.MAX_RETRIES:
                raise RuntimeError(f"Failed to fetch /{endpoint} after {attempt} attempts") from e

# COMMAND ----------
# MAGIC %md ## 4. Ingest Products

raw_products = fetch_api("products")

df_products_raw = spark.createDataFrame(
    [(json.dumps(rec),) for rec in raw_products],
    schema=["raw_json"]
).withColumn("source_endpoint", lit("products")) \
 .withColumn("ingestion_timestamp", lit(RUN_TS)) \
 .withColumn("pipeline_run_id", lit(dbutils.widgets.get("pipeline_run_id") if "dbutils" in dir() else "local"))

write_delta(
    df=df_products_raw,
    path=f"{BronzeConfig.STORAGE_PATH}/products",
    table_name=f"{BronzeConfig.DATABASE}.products",
    mode="overwrite"
)
logger.info("✅ bronze.products written")

# COMMAND ----------
# MAGIC %md ## 5. Ingest Carts

raw_carts = fetch_api("carts")

df_carts_raw = spark.createDataFrame(
    [(json.dumps(rec),) for rec in raw_carts],
    schema=["raw_json"]
).withColumn("source_endpoint", lit("carts")) \
 .withColumn("ingestion_timestamp", lit(RUN_TS)) \
 .withColumn("pipeline_run_id", lit(dbutils.widgets.get("pipeline_run_id") if "dbutils" in dir() else "local"))

write_delta(
    df=df_carts_raw,
    path=f"{BronzeConfig.STORAGE_PATH}/carts",
    table_name=f"{BronzeConfig.DATABASE}.carts",
    mode="overwrite"
)
logger.info("✅ bronze.carts written")

# COMMAND ----------
# MAGIC %md ## 6. Ingest Users

raw_users = fetch_api("users")

df_users_raw = spark.createDataFrame(
    [(json.dumps(rec),) for rec in raw_users],
    schema=["raw_json"]
).withColumn("source_endpoint", lit("users")) \
 .withColumn("ingestion_timestamp", lit(RUN_TS)) \
 .withColumn("pipeline_run_id", lit(dbutils.widgets.get("pipeline_run_id") if "dbutils" in dir() else "local"))

write_delta(
    df=df_users_raw,
    path=f"{BronzeConfig.STORAGE_PATH}/users",
    table_name=f"{BronzeConfig.DATABASE}.users",
    mode="overwrite"
)
logger.info("✅ bronze.users written")

# COMMAND ----------
# MAGIC %md ## 7. Audit

for table in ["products", "carts", "users"]:
    count = spark.table(f"{BronzeConfig.DATABASE}.{table}").count()
    logger.info(f"  bronze.{table}: {count} rows")

logger.info("🏁 Bronze ingestion complete")

# COMMAND ----------
dbutils.notebook.exit("SUCCESS")
