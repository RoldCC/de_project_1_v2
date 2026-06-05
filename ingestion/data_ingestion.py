import os
import json
import time
import random
import logging
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from pathlib import Path

LOG_FILE = Path(__file__).parent.parent / "app.log"
BASE_URL = "https://api.rawg.io/api/games"
PAGE_SIZE = 40
TOTAL_PAGES = 700
MAX_RETRIES = 5
BASE_DELAY = 1.0
REQUEST_INTERVAL = 0.07  # ~14 req/s, safely under the 20 req/s limit

NESTED_FIELDS = [
    "ratings", "added_by_status", "platforms", "parent_platforms",
    "genres", "stores", "tags", "esrb_rating", "short_screenshots", "clip",
]


class _ErrorMarkerFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        if record.levelno >= logging.ERROR:
            return f">>> ERROR <<< {msg}"
        return msg


def _setup_logging():
    logger = logging.getLogger("ingestion")
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


def _fetch_page(session, api_key, page, logger):
    params = {"key": api_key, "page": page, "page_size": PAGE_SIZE}
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(BASE_URL, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json().get("results", [])
            if resp.status_code == 429 or resp.status_code >= 500:
                delay = BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    f"Page {page}: HTTP {resp.status_code}, retry {attempt + 1}/{MAX_RETRIES} in {delay:.1f}s"
                )
                time.sleep(delay)
            else:
                logger.error(f"Page {page}: unexpected HTTP {resp.status_code}, skipping")
                return []
        except requests.RequestException as exc:
            delay = BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
            logger.error(
                f"Page {page}: {exc}, retry {attempt + 1}/{MAX_RETRIES} in {delay:.1f}s"
            )
            time.sleep(delay)
    logger.error(f"Page {page}: exhausted {MAX_RETRIES} retries, skipping")
    return []


def _serialize_nested(records):
    for record in records:
        for field in NESTED_FIELDS:
            val = record.get(field)
            record[field] = json.dumps(val) if val is not None else None
    return records


def fetch_all(api_key, logger):
    records = []
    session = requests.Session()
    for page in range(1, TOTAL_PAGES + 1):
        logger.info(f"Fetching page {page}/{TOTAL_PAGES} ({len(records)} records so far)")
        results = _fetch_page(session, api_key, page, logger)
        if results:
            records.extend(_serialize_nested(results))
        time.sleep(REQUEST_INTERVAL)
    return records


def save_parquet(records, path, logger):
    table = pa.Table.from_pylist(records)
    pq.write_table(table, path)
    logger.info(f"Wrote {len(records)} records to {path}")


def main():
    load_dotenv()
    logger = _setup_logging()

    api_key = os.getenv("RAWG_API_KEY")
    if not api_key:
        logger.error("RAWG_API_KEY not set")
        return

    output = Path(__file__).parent / "bronze_data.parquet"
    logger.info(f"Starting extraction: {TOTAL_PAGES} pages x {PAGE_SIZE} records each")

    records = fetch_all(api_key, logger)
    logger.info(f"Total fetched: {len(records)} records")

    if not records:
        logger.error("No records fetched — parquet not written")
        return

    save_parquet(records, output, logger)


if __name__ == "__main__":
    main()
