# Games Analytics Pipeline

End-to-end data engineering project that extracts video game data from the [RAWG API](https://rawg.io/apidocs), processes it through a medallion architecture on Databricks Community Edition, and surfaces insights through an interactive AI/BI dashboard.

## Architecture

```
RAWG API
   │
   ▼
bronze_data.parquet  (local)
   │
   ▼
UC Volume (staging)  ──► Bronze Delta table   (gold_fact_games raw)
                                │
                                ▼
                         Silver layer (13 tables)
                         ├── silver_dim_games
                         ├── silver_dim_genres
                         ├── silver_dim_platforms
                         ├── silver_dim_stores
                         ├── silver_dim_tags
                         ├── silver_dim_esrb
                         ├── silver_bridge_game_genres
                         ├── silver_bridge_game_platforms
                         ├── silver_bridge_game_stores
                         ├── silver_bridge_game_tags
                         └── ... (remaining bridge tables)
                                │
                                ▼
                         Gold layer (star schema, 7 tables)
                         ├── gold_fact_games
                         ├── gold_bridge_game_genres
                         ├── gold_bridge_game_parent_platforms
                         ├── gold_bridge_game_stores
                         ├── gold_bridge_game_ratings
                         ├── gold_bridge_game_tags
                         └── gold_dim_esrb
                                │
                                ▼
                    Databricks AI/BI Dashboard (Lakeview)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Source | RAWG Video Games Database API |
| Ingestion | Python · `requests` · `pyarrow` |
| Staging | Databricks Unity Catalog Volume (Files API) |
| Storage | Databricks Delta Lake (Unity Catalog) |
| Processing | Databricks Serverless Notebooks (PySpark) |
| Orchestration | Python · `databricks-sdk` (Jobs API) |
| Visualization | Databricks AI/BI Lakeview Dashboard |

## Project Structure

```
de_project_1_v2/
├── ingestion/
│   ├── data_ingestion.py       # RAWG API → bronze_data.parquet (~28K games)
│   └── data_to_db.py           # parquet → UC Volume → bronze Delta table
├── databricks_process/
│   ├── bronze_to_silver.py     # Databricks notebook: bronze → 13 silver tables
│   └── silver_to_gold.py       # Databricks notebook: silver → 7 gold tables
├── visualization/
│   └── create_databricks_dashboard.py  # Builds + publishes AI/BI dashboard
├── run_pipeline.py             # End-to-end orchestration entry point
├── .env.example                # Credential template
├── requirements.txt
└── README.md
```

## Setup

### Prerequisites

- Python 3.10+
- Databricks Community Edition workspace with Unity Catalog enabled
- RAWG API key (free at [rawg.io](https://rawg.io/apidocs))

### Install dependencies

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Configure credentials

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

```env
RAWG_API_KEY=your_rawg_api_key
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your_personal_access_token
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
```

### Databricks setup required

Before running, ensure the following exist in your Databricks workspace:

1. **Unity Catalog schemas**: `development.games_bronze`, `development.games_silver`, `development.games_gold`
2. **UC Volume** for staging: `/Volumes/workspace/default/staging/`
3. **Notebook paths** for `bronze_to_silver.py` and `silver_to_gold.py` uploaded to `/de_project_1_v2/`

## Running the Pipeline

```bash
# Full pipeline: ingest → upload → bronze → silver → gold
python run_pipeline.py

# Dashboard only (after pipeline has run at least once)
python visualization/create_databricks_dashboard.py
```

`run_pipeline.py` sequences all steps and exits non-zero on the first failure, so downstream steps never run on stale data.

## Dashboard

The Databricks AI/BI dashboard is created and published programmatically via the Lakeview API. It includes:

- **KPI cards**: Total games, average rating, median playtime, average ratings count
- **Genre & Platform analysis**: Top 15 genres and platforms by game count and average rating
- **Release trend**: Games released per year (1990–present)
- **ESRB distribution**: Pie chart of content ratings
- **Player ratings breakdown**: Votes by rating category
- **Top tags & stores**: Most common tags and store availability
- **Multi-variable charts**: Rating vs. playtime scatter, genre × platform stacked bar
- **Top games table**: 20 highest-rated games (min. 10 ratings), sortable

**Filters**: Genre, Platform, ESRB Rating dropdowns + Released From / Released To date pickers — all filters apply simultaneously across every dataset on the dashboard.

## Logging

All pipeline steps write to `app.log`. Error lines are prefixed with `>>> ERROR <<<` for easy grep:

```bash
grep ">>> ERROR <<<" app.log
```
