# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer — Cleanse & Transform
# MAGIC **Pipeline:** E-Commerce Orders ETL
# MAGIC **Layer:** Silver (Curated Zone)
# MAGIC **Source:** Bronze Delta tables
# MAGIC **Sink:** Delta Lake — Silver tables on DBFS
# MAGIC
# MAGIC Transformations applied:
# MAGIC - Parse raw JSON strings into typed columns
# MAGIC - Enforce schemas and cast data types
# MAGIC - Deduplicate records
# MAGIC - Apply data quality checks (nulls, ranges)
# MAGIC - Flatten nested structures (cart items, user address)

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, when, lit, trim, upper,
    to_date, current_timestamp, round as spark_round,
    regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, ArrayType
)
import sys
sys.path.insert(0, "/Workspace/Repos/databricks-ecommerce-etl")

from config.config import BronzeConfig, SilverConfig
from utils.helpers import get_logger, write_delta, create_database_if_not_exists, run_dq_checks

# COMMAND ----------
logger = get_logger("silver_transform")
spark = SparkSession.builder.appName("EcommerceETL_Silver").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")
logger.info("Silver transformation started")
create_database_if_not_exists(spark, SilverConfig.DATABASE, SilverConfig.STORAGE_PATH)

# COMMAND ----------
# MAGIC %md ## 2. Define Schemas

PRODUCT_SCHEMA = StructType([
    StructField("id",          IntegerType(), True),
    StructField("title",       StringType(),  True),
    StructField("price",       DoubleType(),  True),
    StructField("description", StringType(),  True),
    StructField("category",    StringType(),  True),
    StructField("image",       StringType(),  True),
    StructField("rating", StructType([
        StructField("rate",  DoubleType(),  True),
        StructField("count", IntegerType(), True),
    ]), True),
])

CART_PRODUCT_SCHEMA = StructType([
    StructField("productId", IntegerType(), True),
    StructField("quantity",  IntegerType(), True),
])

CART_SCHEMA = StructType([
    StructField("id",       IntegerType(), True),
    StructField("userId",   IntegerType(), True),
    StructField("date",     StringType(),  True),
    StructField("products", ArrayType(CART_PRODUCT_SCHEMA), True),
])

ADDRESS_SCHEMA = StructType([
    StructField("city",    StringType(), True),
    StructField("street",  StringType(), True),
    StructField("zipcode", StringType(), True),
])

NAME_SCHEMA = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname",  StringType(), True),
])

USER_SCHEMA = StructType([
    StructField("id",      IntegerType(), True),
    StructField("email",   StringType(),  True),
    StructField("username",StringType(),  True),
    StructField("phone",   StringType(),  True),
    StructField("address", ADDRESS_SCHEMA, True),
    StructField("name",    NAME_SCHEMA,    True),
])

# COMMAND ----------
# MAGIC %md ## 3. Transform Products

df_bronze_products = spark.table(f"{BronzeConfig.DATABASE}.products")
df_silver_products = (
    df_bronze_products
    .withColumn("parsed", from_json(col("raw_json"), PRODUCT_SCHEMA))
    .select(
        col("parsed.id").alias("product_id"),
        trim(col("parsed.title")).alias("product_title"),
        col("parsed.price").alias("price_usd"),
        trim(upper(col("parsed.category"))).alias("category"),
        col("parsed.description").alias("description"),
        col("parsed.rating.rate").alias("rating_score"),
        col("parsed.rating.count").alias("rating_count"),
        current_timestamp().alias("etl_processed_at"),
    )
    .withColumn("dq_flag",
        when(col("product_id").isNull(), lit("NULL_PRODUCT_ID"))
        .when(col("price_usd") <= 0, lit("INVALID_PRICE"))
        .otherwise(lit("PASS")))
    .dropDuplicates(["product_id"])
)
run_dq_checks(df_silver_products, "products", logger)
write_delta(df_silver_products, f"{SilverConfig.STORAGE_PATH}/products",
            f"{SilverConfig.DATABASE}.products", mode="overwrite")
logger.info(f"silver.products done")

# COMMAND ----------
# MAGIC %md ## 4. Transform Carts

df_bronze_carts = spark.table(f"{BronzeConfig.DATABASE}.carts")
df_silver_carts = (
    df_bronze_carts
    .withColumn("parsed", from_json(col("raw_json"), CART_SCHEMA))
    .select(col("parsed.id").alias("cart_id"), col("parsed.userId").alias("user_id"),
            to_date(col("parsed.date")).alias("cart_date"),
            explode(col("parsed.products")).alias("line_item"))
    .select("cart_id", "user_id", "cart_date",
            col("line_item.productId").alias("product_id"),
            col("line_item.quantity").alias("quantity"),
            current_timestamp().alias("etl_processed_at"))
    .withColumn("dq_flag",
        when(col("quantity") <= 0, lit("INVALID_QTY"))
        .when(col("user_id").isNull(), lit("NULL_USER"))
        .otherwise(lit("PASS")))
    .dropDuplicates(["cart_id", "product_id"])
)
write_delta(df_silver_carts, f"{SilverConfig.STORAGE_PATH}/carts",
            f"{SilverConfig.DATABASE}.carts", mode="overwrite")
logger.info("silver.carts done")

# COMMAND ----------
# MAGIC %md ## 5. Transform Users

df_bronze_users = spark.table(f"{BronzeConfig.DATABASE}.users")
df_silver_users = (
    df_bronze_users
    .withColumn("parsed", from_json(col("raw_json"), USER_SCHEMA))
    .select(
        col("parsed.id").alias("user_id"),
        col("parsed.email").alias("email"),
        col("parsed.username").alias("username"),
        col("parsed.name.firstname").alias("first_name"),
        col("parsed.name.lastname").alias("last_name"),
        col("parsed.address.city").alias("city"),
        col("parsed.address.street").alias("street"),
        col("parsed.address.zipcode").alias("zipcode"),
        current_timestamp().alias("etl_processed_at"),
    )
    .withColumn("dq_flag",
        when(col("user_id").isNull(), lit("NULL_USER_ID"))
        .when(col("email").isNull() | (col("email") == ""), lit("MISSING_EMAIL"))
        .otherwise(lit("PASS")))
    .dropDuplicates(["user_id"])
)
write_delta(df_silver_users, f"{SilverConfig.STORAGE_PATH}/users",
            f"{SilverConfig.DATABASE}.users", mode="overwrite")
logger.info("silver.users done")

# COMMAND ----------
logger.info("Silver transformation complete")
dbutils.notebook.exit("SUCCESS")
