# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer — Business Aggregations & Azure SQL Sink
# MAGIC **Pipeline:** E-Commerce Orders ETL  **Layer:** Gold (Serving Zone)
# MAGIC **Source:** Silver Delta tables  **Sink:** Azure SQL Database + Gold Delta tables
# MAGIC
# MAGIC Gold tables: orders_enriched | category_revenue | customer_summary | top_products | daily_order_trends

# COMMAND ----------
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, sum as spark_sum, count, avg, round as spark_round,
    current_timestamp, concat_ws, to_date, desc, dense_rank
)
import sys
sys.path.insert(0, "/Workspace/Repos/databricks-ecommerce-etl")
from config.config import SilverConfig, GoldConfig, AzureSQLConfig
from utils.helpers import get_logger, write_delta, create_database_if_not_exists, write_to_azure_sql

# COMMAND ----------
logger = get_logger("gold_aggregate")
spark = SparkSession.builder.appName("EcommerceETL_Gold").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")
logger.info("Gold aggregation started")
create_database_if_not_exists(spark, GoldConfig.DATABASE, GoldConfig.STORAGE_PATH)

# COMMAND ----------
# MAGIC %md ## 2. Load Silver Tables

df_products = spark.table(f"{SilverConfig.DATABASE}.products").filter(col("dq_flag") == "PASS")
df_carts    = spark.table(f"{SilverConfig.DATABASE}.carts").filter(col("dq_flag") == "PASS")
df_users    = spark.table(f"{SilverConfig.DATABASE}.users").filter(col("dq_flag") == "PASS")

# COMMAND ----------
# MAGIC %md ## 3. Build orders_enriched (Fact Table)

df_orders_enriched = (
    df_carts
    .join(df_products, on="product_id", how="left")
    .join(df_users, on="user_id", how="left")
    .select(
        col("cart_id"), col("cart_date"),
        col("user_id"),
        concat_ws(" ", col("first_name"), col("last_name")).alias("customer_name"),
        col("email").alias("customer_email"),
        col("city").alias("customer_city"),
        col("product_id"), col("product_title"), col("category"),
        col("price_usd"), col("quantity"),
        spark_round(col("price_usd") * col("quantity"), 2).alias("line_total_usd"),
        col("rating_score"),
        current_timestamp().alias("etl_processed_at"),
    )
)
write_delta(df_orders_enriched, f"{GoldConfig.STORAGE_PATH}/orders_enriched",
            f"{GoldConfig.DATABASE}.orders_enriched", mode="overwrite")
write_to_azure_sql(df_orders_enriched, "gold_orders_enriched", logger)
logger.info(f"gold.orders_enriched done")

# COMMAND ----------
# MAGIC %md ## 4. Category Revenue

df_category_revenue = (
    df_orders_enriched.groupBy("category")
    .agg(
        spark_round(spark_sum("line_total_usd"), 2).alias("total_revenue_usd"),
        count("cart_id").alias("total_orders"),
        spark_round(avg("price_usd"), 2).alias("avg_product_price_usd"),
        spark_round(avg("line_total_usd"), 2).alias("avg_order_value_usd"),
    )
    .orderBy(desc("total_revenue_usd"))
    .withColumn("etl_processed_at", current_timestamp())
)
write_delta(df_category_revenue, f"{GoldConfig.STORAGE_PATH}/category_revenue",
            f"{GoldConfig.DATABASE}.category_revenue", mode="overwrite")
write_to_azure_sql(df_category_revenue, "gold_category_revenue", logger)

# COMMAND ----------
# MAGIC %md ## 5. Customer Summary (CLV)

df_customer_summary = (
    df_orders_enriched.groupBy("user_id", "customer_name", "customer_email", "customer_city")
    .agg(
        count("cart_id").alias("total_orders"),
        spark_round(spark_sum("line_total_usd"), 2).alias("lifetime_value_usd"),
        spark_round(avg("line_total_usd"), 2).alias("avg_order_value_usd"),
    )
    .orderBy(desc("lifetime_value_usd"))
    .withColumn("etl_processed_at", current_timestamp())
)
write_delta(df_customer_summary, f"{GoldConfig.STORAGE_PATH}/customer_summary",
            f"{GoldConfig.DATABASE}.customer_summary", mode="overwrite")
write_to_azure_sql(df_customer_summary, "gold_customer_summary", logger)

# COMMAND ----------
# MAGIC %md ## 6. Top 10 Products by Revenue

window_rank = Window.orderBy(desc("total_revenue_usd"))
df_top_products = (
    df_orders_enriched.groupBy("product_id", "product_title", "category", "price_usd")
    .agg(
        spark_round(spark_sum("line_total_usd"), 2).alias("total_revenue_usd"),
        spark_sum("quantity").alias("units_sold"),
        count("cart_id").alias("times_ordered"),
    )
    .withColumn("revenue_rank", dense_rank().over(window_rank))
    .filter(col("revenue_rank") <= 10)
    .orderBy("revenue_rank")
    .withColumn("etl_processed_at", current_timestamp())
)
write_delta(df_top_products, f"{GoldConfig.STORAGE_PATH}/top_products",
            f"{GoldConfig.DATABASE}.top_products", mode="overwrite")
write_to_azure_sql(df_top_products, "gold_top_products", logger)

# COMMAND ----------
# MAGIC %md ## 7. Daily Order Trends

df_daily_trends = (
    df_orders_enriched.groupBy("cart_date")
    .agg(
        count("cart_id").alias("order_count"),
        spark_round(spark_sum("line_total_usd"), 2).alias("daily_revenue_usd"),
        spark_round(avg("line_total_usd"), 2).alias("avg_order_value_usd"),
        count("user_id").alias("unique_customers"),
    )
    .orderBy("cart_date")
    .withColumn("etl_processed_at", current_timestamp())
)
write_delta(df_daily_trends, f"{GoldConfig.STORAGE_PATH}/daily_order_trends",
            f"{GoldConfig.DATABASE}.daily_order_trends", mode="overwrite")
write_to_azure_sql(df_daily_trends, "gold_daily_order_trends", logger)

# COMMAND ----------
logger.info("Gold aggregation complete — all 5 tables written to Delta + Azure SQL")
dbutils.notebook.exit("SUCCESS")
