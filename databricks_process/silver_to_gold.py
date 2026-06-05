import logging
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

_NULL_STRINGS = ("", "nan", "none", "null", "na", "n/a")

# ── helpers ───────────────────────────────────────────────────────────────────

class _ErrorMarker(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        return f">>> ERROR <<< {msg}" if record.levelno >= logging.ERROR else msg


def _setup_logging():
    logger = logging.getLogger("silver_to_gold")
    logger.setLevel(logging.DEBUG)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(_ErrorMarker("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)
    return logger


def _get_spark():
    try:
        return spark  # noqa: F821 — pre-initialized in Databricks notebook context
    except NameError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.appName("silver_to_gold").getOrCreate()


def _standardize_nulls(df):
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(
            c,
            F.when(F.lower(F.trim(F.col(c))).isin(*_NULL_STRINGS), None).otherwise(F.col(c)),
        )
    return df


def _write(df, table, logger):
    view = f"_tmp_{table}"
    df.createOrReplaceTempView(view)
    df.sparkSession.sql(
        f"CREATE OR REPLACE TABLE development.games_gold.{table} USING DELTA AS SELECT * FROM {view}"
    )
    logger.info(f"  {table}: written")


# ── table builders ────────────────────────────────────────────────────────────

def build_gold_fact_games(spark):
    """
    Central fact table. One row per game.
    Denormalizations vs silver:
      - esrb_name joined in so dashboards don't need a separate JOIN
      - release_year / release_month extracted for time-series analysis
    """
    fact = spark.table("development.games_silver.silver_fact_games")
    esrb = spark.table("development.games_silver.silver_dim_esrb_ratings")

    return (
        fact
        .join(
            esrb.select(F.col("esrb_id"), F.col("esrb_name")),
            fact["esrb_rating_id"] == esrb["esrb_id"],
            "left",
        )
        .drop("esrb_id")
        .withColumn("release_year",  F.year("released"))
        .withColumn("release_month", F.month("released"))
    )


def build_gold_bridge_game_genres(spark):
    """
    game_id + genre_id + genre_name.
    Name denormalized so a WHERE genre_name = '...' needs no extra JOIN.
    """
    bridge = spark.table("development.games_silver.silver_bridge_game_genres")
    dim    = spark.table("development.games_silver.silver_dim_genres")
    return bridge.join(dim, "genre_id", "left")


def build_gold_bridge_game_platforms(spark):
    bridge = spark.table("development.games_silver.silver_bridge_game_platforms")
    dim    = spark.table("development.games_silver.silver_dim_platforms")
    return bridge.join(dim, "platform_id", "left")


def build_gold_bridge_game_parent_platforms(spark):
    bridge = spark.table("development.games_silver.silver_bridge_game_parent_platforms")
    dim    = spark.table("development.games_silver.silver_dim_parent_platforms")
    return bridge.join(dim, "parent_platform_id", "left")


def build_gold_bridge_game_stores(spark):
    bridge = spark.table("development.games_silver.silver_bridge_game_stores")
    dim    = spark.table("development.games_silver.silver_dim_stores")
    return bridge.join(dim, "store_id", "left")


def build_gold_bridge_game_ratings(spark):
    """
    Rating breakdown per game (excellent / recommended / meh / skip).
    Passes through from silver — no structural change needed.
    """
    return spark.table("development.games_silver.silver_bridge_game_ratings")


def build_gold_agg_top_tags(spark):
    """
    Top 100 English tags by game count.
    Pre-aggregated because the raw bridge has millions of rows (28k games × many tags).
    English-only keeps tag names meaningful for display.
    """
    bridge = spark.table("development.games_silver.silver_bridge_game_tags")
    dim    = spark.table("development.games_silver.silver_dim_tags")

    return (
        bridge
        .join(dim, "tag_id", "left")
        .filter(F.col("tag_language") == "eng")
        .groupBy("tag_id", "tag_name")
        .agg(F.count("game_id").alias("game_count"))
        .orderBy(F.desc("game_count"))
        .limit(100)
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    logger = _setup_logging()
    spark  = _get_spark()

    logger.info("Building gold tables...")

    tables = {
        "gold_fact_games":                      build_gold_fact_games(spark),
        "gold_bridge_game_genres":              build_gold_bridge_game_genres(spark),
        "gold_bridge_game_platforms":           build_gold_bridge_game_platforms(spark),
        "gold_bridge_game_parent_platforms":    build_gold_bridge_game_parent_platforms(spark),
        "gold_bridge_game_stores":              build_gold_bridge_game_stores(spark),
        "gold_bridge_game_ratings":             build_gold_bridge_game_ratings(spark),
        "gold_agg_top_tags":                    build_gold_agg_top_tags(spark),
    }

    logger.info("Writing gold tables...")
    for table_name, df in tables.items():
        _write(_standardize_nulls(df), table_name, logger)

    logger.info("Silver → Gold complete")


if __name__ == "__main__":
    main()
