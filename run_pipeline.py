import base64
import os
import sys
import time
import logging
from pathlib import Path

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    NotebookTask,
    RunLifeCycleState,
    RunResultState,
    SubmitTask,
)
from databricks.sdk.service.workspace import ImportFormat, Language

LOG_FILE = Path(__file__).parent / "app.log"
PARQUET_PATH = Path(__file__).parent / "ingestion" / "bronze_data.parquet"

_NOTEBOOKS = {
    "bronze_to_silver": {
        "local":  Path(__file__).parent / "databricks_process" / "bronze_to_silver.py",
        "remote": "/de_project_1_v2/bronze_to_silver",
    },
    "silver_to_gold": {
        "local":  Path(__file__).parent / "databricks_process" / "silver_to_gold.py",
        "remote": "/de_project_1_v2/silver_to_gold",
    },
}

_ACTIVE_STATES = {
    RunLifeCycleState.PENDING,
    RunLifeCycleState.RUNNING,
    RunLifeCycleState.BLOCKED,
    RunLifeCycleState.WAITING_FOR_RETRY,
}

# ================================================================================

class _ErrorMarkerFormatter(logging.Formatter):
    def format(self, record):
        # Prepends ">>> ERROR <<<" to error-level log lines for easy grep in app.log
        msg = super().format(record)
        return f">>> ERROR <<< {msg}" if record.levelno >= logging.ERROR else msg

# ================================================================================

def _setup_logging():
    # Configures dual logging: DEBUG+ to app.log with error markers, INFO+ to stdout
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.DEBUG)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"

    fh = logging.FileHandler(LOG_FILE)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(_ErrorMarkerFormatter(fmt))

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(fmt))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

# ── shared utilities ──────────────────────────────────────────────────────────

# ================================================================================

def _upload_notebook(client, notebook_key, logger):
    # Base64-encodes a local .py file and imports it to the Databricks workspace,
    # overwriting any existing notebook at the same remote path
    meta = _NOTEBOOKS[notebook_key]
    local_path  = meta["local"]
    remote_path = meta["remote"]

    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")

    client.workspace.import_(
        path=remote_path,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        content=content,
        overwrite=True,
    )
    logger.info(f"Uploaded {local_path.name} → {remote_path}")

# ================================================================================

def _submit_and_wait(client, notebook_key, logger, poll_interval=15):
    # Submits the notebook as a one-time Databricks job run and polls every
    # poll_interval seconds until it succeeds or fails
    remote_path = _NOTEBOOKS[notebook_key]["remote"]

    run = client.jobs.submit(
        run_name=f"pipeline_{notebook_key}",
        tasks=[
            SubmitTask(
                task_key=notebook_key,
                notebook_task=NotebookTask(notebook_path=remote_path),
            )
        ],
    )
    run_id = run.run_id
    logger.info(f"Notebook run submitted (run_id={run_id}), polling every {poll_interval}s...")

    while True:
        state = client.jobs.get_run(run_id)
        if state.state.life_cycle_state not in _ACTIVE_STATES:
            break
        elapsed = (state.run_duration // 1000) if state.run_duration else 0
        logger.info(f"  run_id={run_id} — {state.state.life_cycle_state} ({elapsed}s elapsed)")
        time.sleep(poll_interval)

    if state.state.result_state == RunResultState.SUCCESS:
        logger.info(f"Notebook run {run_id} completed successfully")
        return True

    logger.error(
        f"Notebook run {run_id} failed — result: {state.state.result_state}, "
        f"message: {state.state.state_message}"
    )
    return False

# ── pipeline steps ────────────────────────────────────────────────────────────

# ================================================================================

def step_ingestion(logger):
    # Step 1: calls the RAWG API across all pages and saves the result to bronze_data.parquet
    logger.info("=== STEP 1: Data Ingestion (RAWG API → parquet) ===")
    from ingestion.data_ingestion import fetch_all, save_parquet

    api_key = os.getenv("RAWG_API_KEY")
    if not api_key:
        logger.error("RAWG_API_KEY not set")
        return False

    records = fetch_all(api_key, logger)
    if not records:
        logger.error("No records fetched — aborting")
        return False

    save_parquet(records, PARQUET_PATH, logger)
    return True

# ================================================================================

def step_upload_and_bronze(client, warehouse_id, logger):
    # Step 2: uploads the local parquet to a UC Volume and creates the bronze Delta table
    logger.info("=== STEP 2: Upload + Bronze Delta Table (parquet → Databricks) ===")
    from ingestion.data_to_db import upload_to_volume, create_bronze_table

    if not PARQUET_PATH.exists():
        logger.error(f"Parquet not found at {PARQUET_PATH} — run step 1 first")
        return False

    upload_to_volume(client, logger)
    create_bronze_table(client, warehouse_id, logger)
    return True

# ================================================================================

def step_bronze_to_silver(client, logger):
    # Step 3: uploads the bronze_to_silver notebook to Databricks and runs it as a job
    logger.info("=== STEP 3: Bronze → Silver (Databricks notebook) ===")
    _upload_notebook(client, "bronze_to_silver", logger)
    return _submit_and_wait(client, "bronze_to_silver", logger)

# ================================================================================

def step_silver_to_gold(client, logger):
    # Step 4: uploads the silver_to_gold notebook to Databricks and runs it as a job
    logger.info("=== STEP 4: Silver → Gold (Databricks notebook) ===")
    _upload_notebook(client, "silver_to_gold", logger)
    return _submit_and_wait(client, "silver_to_gold", logger)

# ── main ──────────────────────────────────────────────────────────────────────

# ================================================================================

def main():
    # Entry point: loads .env, initializes the Databricks client, and runs all 4
    # pipeline steps in sequence; exits non-zero immediately if any step fails
    load_dotenv()
    logger = _setup_logging()

    host        = os.getenv("DATABRICKS_HOST")
    token       = os.getenv("DATABRICKS_TOKEN")
    http_path   = os.getenv("DATABRICKS_HTTP_PATH", "")
    warehouse_id = http_path.split("/")[-1]

    if not all([host, token, warehouse_id]):
        logger.error("Missing env vars: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH")
        sys.exit(1)

    client = WorkspaceClient(host=host, token=token)

    steps = [
        ("ingestion",        lambda: step_ingestion(logger)),
        ("upload + bronze",  lambda: step_upload_and_bronze(client, warehouse_id, logger)),
        ("bronze → silver",  lambda: step_bronze_to_silver(client, logger)),
        ("silver → gold",    lambda: step_silver_to_gold(client, logger)),
    ]

    for name, run_step in steps:
        if not run_step():
            logger.error(f"Pipeline aborted at step: {name}")
            sys.exit(1)

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
