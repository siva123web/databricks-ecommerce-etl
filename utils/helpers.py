"""
helpers.py — Shared utility functions for the E-Commerce ETL pipeline.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from config.config import AzureSQLConfig


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Return a configured logger that works both locally and on Databricks."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                              datefmt="%Y-%m-%d %H:%M:%S")
        )
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


# ---------------------------------------------------------------------------
# Delta Lake helpers
# ---------------------------------------------------------------------------
def create_database_if_not_exists(spark: SparkSession, db_name: str, location: str) -> None:
    """Create a Delta database at the given DBFS location if it does not exist."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{location}'")


def write_delta(df: DataFrame, path: str, table_name: str,
                mode: str = "overwrite",
                partition_by: Optional[list] = None) -> None:
    """
    Write a DataFrame to Delta Lake and register it as a metastore table.

    Args:
        df           : PySpark DataFrame to write.
        path         : DBFS path for the Delta table files.
        table_name   : Fully-qualified table name (database.table).
        mode         : Write mode — 'overwrite' | 'append'.
        partition_by : Optional list of columns to partition by.
    """
    writer = (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)

    # Register in metastore
    spark = df.sparkSession
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{path}'
    """)


# ---------------------------------------------------------------------------
# Azure SQL helpers
# ---------------------------------------------------------------------------
def write_to_azure_sql(df: DataFrame, table_name: str,
                       logger: logging.Logger,
                       mode: str = "overwrite") -> None:
    """
    Write a DataFrame to Azure SQL Database via JDBC.
    Credentials are resolved from AzureSQLConfig (Databricks secrets in prod).
    """
    cfg = AzureSQLConfig()
    logger.info(f"  Writing {table_name} to Azure SQL ({cfg.SERVER}/{cfg.DATABASE})...")
    try:
        (
            df.write
            .format("jdbc")
            .mode(mode)
            .option("url",      cfg.jdbc_url)
            .option("dbtable",  table_name)
            .option("user",     cfg.USER)
            .option("password", cfg.PASSWORD)
            .option("driver",   "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("batchsize",     10_000)
            .option("numPartitions", 4)
            .option("truncate",      mode == "overwrite")
            .save()
        )
        logger.info(f"  ✅ {table_name} written to Azure SQL")
    except Exception as e:
        logger.error(f"  ❌ Failed to write {table_name} to Azure SQL: {e}")
        raise


# ---------------------------------------------------------------------------
# Data Quality helpers
# ---------------------------------------------------------------------------
def run_dq_checks(df: DataFrame, table_name: str,
                  logger: logging.Logger) -> dict:
    """
    Run basic data quality checks and log results.
    Returns a dict with counts: total, pass, fail.
    """
    from pyspark.sql.functions import col
    total = df.count()
    if "dq_flag" in df.columns:
        passed   = df.filter(col("dq_flag") == "PASS").count()
        failed   = total - passed
        fail_rate = round(failed / total * 100, 2) if total > 0 else 0
        logger.info(f"  DQ [{table_name}] total={total} pass={passed} "
                    f"fail={failed} ({fail_rate}%)")
        if fail_rate > 10:
            logger.warning(f"  ⚠️  DQ failure rate {fail_rate}% exceeds 10% threshold!")
        return {"total": total, "pass": passed, "fail": failed, "fail_rate_pct": fail_rate}
    return {"total": total}


def assert_row_count(df: DataFrame, min_rows: int,
                     table_name: str, logger: logging.Logger) -> None:
    """Raise an error if the DataFrame has fewer rows than expected."""
    actual = df.count()
    if actual < min_rows:
        msg = (f"Row count assertion FAILED for {table_name}: "
               f"expected >={min_rows}, got {actual}")
        logger.error(msg)
        raise AssertionError(msg)
    logger.info(f"  ✅ Row count OK for {table_name}: {actual} rows")
