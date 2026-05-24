# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer — Cleanse & Transform
# MAGIC **Pipeline:** E-Commerce Orders ETL  **Layer:** Silver (Curated Zone)
# MAGIC **Source:** Bronze Delta tables  **Sink:** Delta Lake — Silver tables

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, when, lit, trim, upper,
    to_date, current_timestamp, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
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

USER_SCHEMA = StructType([
    StructField("id",      IntegerType(), True),
    StructField("email",   StringType(),  True),
    StructField("username",StringType(),  True),
    StructField("address", StructType([
        StructField("city",    StringType(), True),
        StructField("street",  StringType(), True),
        StructField("zipcode", StringType(), True),
    ]), True),
    StructField("name", StructType([
        StructField("firstname", StringType(), True),
        StructField("lastname",  StringType(), True),
    ]), True),
])

# COMMAND ----------
# MAGIC %md ## 3. Transform Products

df_bronze_products = spark.table(f"{BronzeConfig.DATABASE}.products")
df_silver_products = (
    df_bronze_products
    .withColumn("p", from_json(col("raw_json"), PRODUCT_SCHEMA))
    .select(
        col("p.id").alias("product_id"),
        trim(col("p.title")).alias("product_title"),
        col("p.price").alias("price_usd"),
        trim(upper(col("p.category"))).alias("category"),
        col("p.description").alias("description"),
        col("p.rating.rate").alias("rating_score"),
        col("p.rating.count").alias("rating_count"),
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

# COMMAND ----------
# MAGIC %md ## 4. Transform Carts

df_bronze_carts = spark.table(f"{BronzeConfig.DATABASE}.carts")
df_silver_carts = (
    df_bronze_carts
    .withColumn("c", from_json(col("raw_json"), CART_SCHEMA))
    .select(col("c.id").alias("cart_id"), col("c.userId").alias("user_id"),
            to_date(col("c.date")).alias("cart_date"),
            explode(col("c.products")).alias("item"))
    .select("cart_id", "user_id", "cart_date",
            col("item.productId").alias("product_id"),
            col("item.quantity").alias("quantity"),
            current_timestamp().alias("etl_processed_at"))
    .withColumn("dq_flag",
        when(col("quantity") <= 0, lit("INVALID_QTY"))
        .when(col("user_id").isNull(), lit("NULL_USER"))
        .otherwise(lit("PASS")))
    .dropDuplicates(["cart_id", "product_id"])
)
write_delta(df_silver_carts, f"{SilverConfig.STORAGE_PATH}/carts",
            f"{SilverConfig.DATABASE}.carts", mode="overwrite")

# COMMAND ----------
# MAGIC %md ## 5. Transform Users

df_bronze_users = spark.table(f"{BronzeConfig.DATABASE}.users")
df_silver_users = (
    df_bronze_users
    .withColumn("u", from_json(col("raw_json"), USER_SCHEMA))
    .select(
        col("u.id").alias("user_id"),
        col("u.email").alias("email"),
        col("u.username").alias("username"),
        col("u.name.firstname").alias("first_name"),
        col("u.name.lastname").alias("last_name"),
        col("u.address.city").alias("city"),
        col("u.address.street").alias("street"),
        col("u.address.zipcode").alias("zipcode"),
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

# COMMAND ----------
logger.info("Silver transformation complete")
dbutils.notebook.exit("SUCCESS")
