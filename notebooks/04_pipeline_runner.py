# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Pipeline Runner — Orchestrator
# MAGIC **Pipeline:** E-Commerce Orders ETL
# MAGIC Orchestrates Bronze → Silver → Gold notebooks sequentially.
# MAGIC
# MAGIC **Usage:** Set widgets then Run All.
# MAGIC - `env` : dev | staging | prod
# MAGIC - `force_full_reload` : true | false

# COMMAND ----------
import uuid
from datetime import datetime, timezone
import sys
sys.path.insert(0, "/Workspace/Repos/databricks-ecommerce-etl")
from utils.helpers import get_logger

# COMMAND ----------
# MAGIC %md ## Widgets

dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.dropdown("force_full_reload", "false", ["true", "false"], "Force Full Reload")

ENV               = dbutils.widgets.get("env")
FORCE_FULL_RELOAD = dbutils.widgets.get("force_full_reload").lower() == "true"
PIPELINE_RUN_ID   = str(uuid.uuid4())

logger = get_logger("pipeline_runner")
logger.info("=" * 70)
logger.info(f"  E-COMMERCE ETL PIPELINE STARTED")
logger.info(f"  Run ID  : {PIPELINE_RUN_ID}")
logger.info(f"  Env     : {ENV}")
logger.info(f"  Started : {datetime.now(timezone.utc).isoformat()}")
logger.info("=" * 70)

# COMMAND ----------
# MAGIC %md ## Pipeline Execution

NOTEBOOKS = [
    ("/Workspace/Repos/databricks-ecommerce-etl/notebooks/01_bronze_ingest",   "Bronze Ingestion"),
    ("/Workspace/Repos/databricks-ecommerce-etl/notebooks/02_silver_transform", "Silver Transform"),
    ("/Workspace/Repos/databricks-ecommerce-etl/notebooks/03_gold_aggregate",   "Gold Aggregation"),
]

results = {}

for notebook_path, stage_name in NOTEBOOKS:
    stage_start = datetime.now(timezone.utc)
    logger.info(f"▶ Starting: {stage_name}")
    try:
        result = dbutils.notebook.run(
            notebook_path,
            timeout_seconds=1800,
            arguments={
                "env": ENV,
                "pipeline_run_id": PIPELINE_RUN_ID,
                "force_full_reload": str(FORCE_FULL_RELOAD).lower(),
            }
        )
        elapsed = (datetime.now(timezone.utc) - stage_start).seconds
        results[stage_name] = {"status": "SUCCESS", "result": result, "elapsed_sec": elapsed}
        logger.info(f"  ✅ {stage_name} completed in {elapsed}s → {result}")
    except Exception as e:
        elapsed = (datetime.now(timezone.utc) - stage_start).seconds
        results[stage_name] = {"status": "FAILED", "error": str(e), "elapsed_sec": elapsed}
        logger.error(f"  ❌ {stage_name} FAILED after {elapsed}s: {e}")
        dbutils.notebook.exit(f"FAILED at stage: {stage_name} — {e}")

# COMMAND ----------
# MAGIC %md ## Pipeline Summary

logger.info("=" * 70)
logger.info("  PIPELINE SUMMARY")
total_elapsed = 0
all_success = True
for stage, info in results.items():
    status  = info["status"]
    elapsed = info["elapsed_sec"]
    total_elapsed += elapsed
    flag = "✅" if status == "SUCCESS" else "❌"
    logger.info(f"  {flag}  {stage:25s}  {status:8s}  {elapsed:>4}s")
    if status != "SUCCESS":
        all_success = False

logger.info(f"  Total elapsed: {total_elapsed}s")
logger.info(f"  Pipeline: {'✅ ALL STAGES PASSED' if all_success else '❌ PIPELINE FAILED'}")
logger.info("=" * 70)

final_status = "SUCCESS" if all_success else "PARTIAL_FAILURE"
dbutils.notebook.exit(final_status)
