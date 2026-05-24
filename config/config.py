"""
config.py — Centralised configuration for the E-Commerce ETL pipeline.
All environment-specific values are read from Databricks Secrets or env vars.
"""

import os

# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------
class BronzeConfig:
    API_BASE_URL     = "https://fakestoreapi.com"
    REQUEST_TIMEOUT  = 30       # seconds
    MAX_RETRIES      = 3
    DATABASE         = "bronze_ecommerce"
    STORAGE_PATH     = "dbfs:/delta/ecommerce/bronze"


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------
class SilverConfig:
    DATABASE     = "silver_ecommerce"
    STORAGE_PATH = "dbfs:/delta/ecommerce/silver"


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------
class GoldConfig:
    DATABASE     = "gold_ecommerce"
    STORAGE_PATH = "dbfs:/delta/ecommerce/gold"


# ---------------------------------------------------------------------------
# Azure SQL — credentials sourced from Databricks Secret Scope
# ---------------------------------------------------------------------------
class AzureSQLConfig:
    """
    Store secrets in Databricks Secret Scope:
        databricks secrets put --scope ecommerce-etl --key sql-server
        databricks secrets put --scope ecommerce-etl --key sql-db
        databricks secrets put --scope ecommerce-etl --key sql-user
        databricks secrets put --scope ecommerce-etl --key sql-password
    """
    SCOPE    = "ecommerce-etl"

    # Fallback to env vars for local testing
    SERVER   = os.getenv("AZURE_SQL_SERVER",   "<your-server>.database.windows.net")
    DATABASE = os.getenv("AZURE_SQL_DATABASE", "ecommerce_dw")
    USER     = os.getenv("AZURE_SQL_USER",     "<your-sql-user>")
    PASSWORD = os.getenv("AZURE_SQL_PASSWORD", "<your-sql-password>")

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:sqlserver://{self.SERVER}:1433;"
            f"database={self.DATABASE};"
            "encrypt=true;trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        )

    @property
    def jdbc_properties(self) -> dict:
        return {
            "user":     self.USER,
            "password": self.PASSWORD,
            "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }


# ---------------------------------------------------------------------------
# Pipeline metadata
# ---------------------------------------------------------------------------
class PipelineConfig:
    PIPELINE_NAME    = "ecommerce-etl"
    VERSION          = "1.0.0"
    DEFAULT_ENV      = "dev"
    SUPPORTED_ENVS   = ["dev", "staging", "prod"]
    LOG_LEVEL        = "INFO"
