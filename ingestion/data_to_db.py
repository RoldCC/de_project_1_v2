import os
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

LOG_FILE = Path(__file__).parent.parent / "app.log"
PARQUET_PATH = Path(__file__).parent / "bronze_data.parquet"
# Public DBFS root is disabled in this workspace — use Unity Catalog Volumes instead
VOLUME_PATH = "/Volumes/workspace/default/staging/bronze_data.parquet"
BRONZE_TABLE = "development.games_bronze.bronze_games"


class _ErrorMarkerFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        if record.levelno >= logging.ERROR:
            return f">>> ERROR <<< {msg}"
        return msg


def _setup_logging():
    logger = logging.getLogger("storage")
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


def _run_sql(client, warehouse_id, statement, logger, poll_interval=3, max_polls=60):
    resp = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout="50s",
    )
    for _ in range(max_polls):
        if resp.status.state not in (StatementState.PENDING, StatementState.RUNNING):
            break
        time.sleep(poll_interval)
        resp = client.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        logger.error(f"SQL failed — state: {resp.status.state}, error: {resp.status.error}")
    return resp


def upload_to_volume(client, logger):
    size_mb = PARQUET_PATH.stat().st_size / 1_048_576
    logger.info(f"Uploading {PARQUET_PATH.name} ({size_mb:.1f} MB) to {VOLUME_PATH}")
    with open(PARQUET_PATH, "rb") as f:
        client.files.upload(VOLUME_PATH, f, overwrite=True)
    logger.info("Upload complete")


def create_bronze_table(client, warehouse_id, logger):
    logger.info(f"Creating Delta table: {BRONZE_TABLE}")
    resp = _run_sql(
        client,
        warehouse_id,
        f"CREATE OR REPLACE TABLE {BRONZE_TABLE} USING DELTA "
        f"AS SELECT * FROM parquet.`{VOLUME_PATH}`",
        logger,
    )
    if resp.status.state == StatementState.SUCCEEDED:
        logger.info(f"Bronze table '{BRONZE_TABLE}' created successfully")

    # Row count confirmation
    resp = _run_sql(client, warehouse_id, f"SELECT COUNT(*) FROM {BRONZE_TABLE}", logger)
    if resp.status.state == StatementState.SUCCEEDED:
        count = resp.result.data_array[0][0]
        logger.info(f"Row count verified: {count}")


def main():
    load_dotenv()
    logger = _setup_logging()

    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    warehouse_id = http_path.split("/")[-1]

    if not all([host, token, warehouse_id]):
        logger.error("Missing env vars: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH")
        return

    if not PARQUET_PATH.exists():
        logger.error(f"bronze_data.parquet not found — run data_ingestion.py first")
        return

    client = WorkspaceClient(host=host, token=token)
    upload_to_volume(client, logger)
    create_bronze_table(client, warehouse_id, logger)


if __name__ == "__main__":
    main()
