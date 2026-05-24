"""
test_pipeline.py — Unit tests for Silver transformation logic.
Run with: pytest tests/ -v
"""

import pytest
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from chispa.dataframe_comparer import assert_df_equality


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("EcommerceETL_Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── Bronze fixtures ──────────────────────────────────────────────────────────

@pytest.fixture
def sample_products_raw(spark):
    products = [
        {"id": 1, "title": "T-Shirt", "price": 19.99, "category": "clothing",
         "description": "A shirt", "image": "img.png", "rating": {"rate": 4.2, "count": 120}},
        {"id": 2, "title": "Laptop",  "price": 999.0, "category": "electronics",
         "description": "A laptop", "image": "img2.png", "rating": {"rate": 4.7, "count": 300}},
    ]
    rows = [(json.dumps(p), "products", "2024-01-01T00:00:00Z") for p in products]
    return spark.createDataFrame(rows, ["raw_json", "source_endpoint", "ingestion_timestamp"])


@pytest.fixture
def sample_carts_raw(spark):
    carts = [
        {"id": 1, "userId": 1, "date": "2024-01-15",
         "products": [{"productId": 1, "quantity": 2}, {"productId": 2, "quantity": 1}]},
        {"id": 2, "userId": 2, "date": "2024-01-16",
         "products": [{"productId": 1, "quantity": 3}]},
    ]
    rows = [(json.dumps(c), "carts", "2024-01-01T00:00:00Z") for c in carts]
    return spark.createDataFrame(rows, ["raw_json", "source_endpoint", "ingestion_timestamp"])


# ── Product tests ─────────────────────────────────────────────────────────────

class TestProductTransform:

    def test_product_count(self, spark, sample_products_raw):
        from pyspark.sql.functions import from_json
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        schema = StructType([
            StructField("id",    IntegerType(), True),
            StructField("title", StringType(),  True),
            StructField("price", DoubleType(),  True),
        ])
        df = (sample_products_raw
              .withColumn("parsed", from_json(col("raw_json"), schema))
              .select(col("parsed.id").alias("product_id"),
                      col("parsed.price").alias("price_usd")))
        assert df.count() == 2

    def test_negative_price_flagged(self, spark):
        from pyspark.sql.functions import when, lit
        rows = [(1, 19.99), (2, -5.0), (3, 0.0)]
        df = spark.createDataFrame(rows, ["product_id", "price_usd"])
        df = df.withColumn("dq_flag",
            when(col("price_usd") <= 0, lit("INVALID_PRICE")).otherwise(lit("PASS")))
        assert df.filter(col("dq_flag") == "INVALID_PRICE").count() == 2

    def test_no_duplicate_products(self, spark):
        rows = [(1, "Shirt"), (1, "Shirt"), (2, "Laptop")]
        df = spark.createDataFrame(rows, ["product_id", "title"])
        assert df.dropDuplicates(["product_id"]).count() == 2


# ── Cart tests ────────────────────────────────────────────────────────────────

class TestCartTransform:

    def test_cart_explode_line_items(self, spark, sample_carts_raw):
        from pyspark.sql.functions import from_json, explode
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
        item_schema = StructType([
            StructField("productId", IntegerType(), True),
            StructField("quantity",  IntegerType(), True),
        ])
        cart_schema = StructType([
            StructField("id",       IntegerType(), True),
            StructField("userId",   IntegerType(), True),
            StructField("products", ArrayType(item_schema), True),
        ])
        df = (sample_carts_raw
              .withColumn("parsed", from_json(col("raw_json"), cart_schema))
              .select(col("parsed.id").alias("cart_id"),
                      explode(col("parsed.products")).alias("item"))
              .select("cart_id",
                      col("item.productId").alias("product_id"),
                      col("item.quantity").alias("quantity")))
        # cart 1 has 2 items, cart 2 has 1 → 3 rows total
        assert df.count() == 3

    def test_invalid_quantity_flagged(self, spark):
        from pyspark.sql.functions import when, lit
        rows = [(1, 1, 2), (2, 1, 0), (3, 2, -1)]
        df = spark.createDataFrame(rows, ["cart_id", "product_id", "quantity"])
        df = df.withColumn("dq_flag",
            when(col("quantity") <= 0, lit("INVALID_QTY")).otherwise(lit("PASS")))
        assert df.filter(col("dq_flag") == "INVALID_QTY").count() == 2


# ── Gold aggregation tests ────────────────────────────────────────────────────

class TestGoldAggregations:

    def test_category_revenue(self, spark):
        from pyspark.sql.functions import sum as spark_sum, round as spark_round
        rows = [("electronics", 999.0, 1), ("electronics", 499.0, 2), ("clothing", 19.99, 3)]
        df = spark.createDataFrame(rows, ["category", "line_total_usd", "cart_id"])
        agg = (df.groupBy("category")
               .agg(spark_round(spark_sum("line_total_usd"), 2).alias("total_revenue_usd")))
        electronics = agg.filter(col("category") == "electronics").collect()[0]["total_revenue_usd"]
        assert electronics == 1498.0

    def test_top_products_limit(self, spark):
        from pyspark.sql.functions import sum as spark_sum, dense_rank, desc
        from pyspark.sql import Window
        rows = [(i, f"Product {i}", float(i * 10)) for i in range(1, 16)]
        df = spark.createDataFrame(rows, ["product_id", "product_title", "line_total_usd"])
        w = Window.orderBy(desc("total_revenue"))
        agg = (df.groupBy("product_id", "product_title")
               .agg(spark_sum("line_total_usd").alias("total_revenue"))
               .withColumn("rank", dense_rank().over(w))
               .filter(col("rank") <= 10))
        assert agg.count() == 10
