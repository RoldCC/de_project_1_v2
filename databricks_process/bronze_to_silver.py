import logging
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, LongType, StringType, StructField, StructType,
)

# ── JSON schemas (only fields we keep) ───────────────────────────────────────

_GENRES_SCHEMA = ArrayType(StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
]))

_PLATFORMS_SCHEMA = ArrayType(StructType([
    StructField("platform", StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
    ])),
]))

_PARENT_PLATFORMS_SCHEMA = ArrayType(StructType([
    StructField("platform", StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
    ])),
]))

_STORES_SCHEMA = ArrayType(StructType([
    StructField("store", StructType([
        StructField("id", LongType()),
        StructField("name", StringType()),
    ])),
]))

_TAGS_SCHEMA = ArrayType(StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("language", StringType()),
]))

_RATINGS_SCHEMA = ArrayType(StructType([
    StructField("id", LongType()),
    StructField("title", StringType()),
    StructField("count", LongType()),
    StructField("percent", DoubleType()),
]))

_ADDED_BY_STATUS_SCHEMA = StructType([
    StructField("yet", LongType()),
    StructField("owned", LongType()),
    StructField("beaten", LongType()),
    StructField("toplay", LongType()),
    StructField("dropped", LongType()),
    StructField("playing", LongType()),
])

_ESRB_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
])

_NULL_STRINGS = ("", "nan", "none", "null", "na", "n/a")

# ── helpers ───────────────────────────────────────────────────────────────────

class _ErrorMarker(logging.Formatter):
    def format(self, record):
        # Prepends ">>> ERROR <<<" to error-level log lines for easy grep in app.log
        msg = super().format(record)
        return f">>> ERROR <<< {msg}" if record.levelno >= logging.ERROR else msg

# ================================================================================

def _setup_logging():
    # Configures a stdout logger (Databricks captures stdout as notebook output)
    logger = logging.getLogger("bronze_to_silver")
    logger.setLevel(logging.DEBUG)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(_ErrorMarker("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)
    return logger

# ================================================================================

def _get_spark():
    # Returns the pre-existing Spark session injected by Databricks; falls back to
    # creating one locally for testing outside a notebook
    try:
        return spark  # noqa: F821 — pre-initialized in Databricks notebook context
    except NameError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.appName("bronze_to_silver").getOrCreate()

# ================================================================================

def _standardize_nulls(df):
    # Replaces common null-like strings ("", "nan", "none", etc.) with actual NULL
    # in every string column so downstream aggregations aren't skewed by them
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(
            c,
            F.when(F.lower(F.trim(F.col(c))).isin(*_NULL_STRINGS), None).otherwise(F.col(c)),
        )
    return df

# ================================================================================

def _parse_json(df):
    # Parses all JSON string columns into typed structs/arrays using pre-defined schemas
    return (
        df
        .withColumn("_genres",           F.from_json("genres",           _GENRES_SCHEMA))
        .withColumn("_platforms",         F.from_json("platforms",        _PLATFORMS_SCHEMA))
        .withColumn("_parent_platforms",  F.from_json("parent_platforms", _PARENT_PLATFORMS_SCHEMA))
        .withColumn("_stores",            F.from_json("stores",           _STORES_SCHEMA))
        .withColumn("_tags",              F.from_json("tags",             _TAGS_SCHEMA))
        .withColumn("_ratings",           F.from_json("ratings",          _RATINGS_SCHEMA))
        .withColumn("_added_by_status",   F.from_json("added_by_status",  _ADDED_BY_STATUS_SCHEMA))
        .withColumn("_esrb",              F.from_json("esrb_rating",      _ESRB_SCHEMA))
    )

# ================================================================================

def _write(df, table, logger):
    # Creates or replaces a silver Delta table in Unity Catalog from the given DataFrame
    view = f"_tmp_{table}"
    df.createOrReplaceTempView(view)
    df.sparkSession.sql(
        f"CREATE OR REPLACE TABLE development.games_silver.{table} USING DELTA AS SELECT * FROM {view}"
    )
    logger.info(f"  {table}: written")

# ── table builders ────────────────────────────────────────────────────────────

# ================================================================================

def build_fact_games(df):
    # Selects core game attributes and flattens added_by_status into columns;
    # ESRB name is denormalized at the gold layer, so only the id is kept here
    return df.select(
        F.col("id").alias("game_id"),
        F.col("name").alias("game_name"),
        F.to_date("released").alias("released"),
        F.col("tba"),
        F.col("rating"),
        F.col("ratings_count"),
        F.col("reviews_text_count"),
        F.col("reviews_count"),
        F.col("added"),
        F.col("playtime"),
        F.col("_added_by_status.yet").alias("added_yet"),
        F.col("_added_by_status.owned").alias("added_owned"),
        F.col("_added_by_status.beaten").alias("added_beaten"),
        F.col("_added_by_status.toplay").alias("added_toplay"),
        F.col("_added_by_status.dropped").alias("added_dropped"),
        F.col("_added_by_status.playing").alias("added_playing"),
        F.col("_esrb.id").alias("esrb_rating_id"),
    )

# ================================================================================

def build_dim_esrb_ratings(df):
    # Extracts a deduplicated ESRB rating dimension (id + name) from the parsed esrb column
    return (
        df.select(F.col("_esrb.id").alias("esrb_id"), F.col("_esrb.name").alias("esrb_name"))
        .filter(F.col("esrb_id").isNotNull())
        .distinct()
    )

# ================================================================================

def build_dim_genres(df):
    # Explodes the genres array to produce a deduplicated genre dimension (id + name)
    return (
        df.select(F.explode("_genres").alias("g"))
        .select(F.col("g.id").alias("genre_id"), F.col("g.name").alias("genre_name"))
        .filter(F.col("genre_id").isNotNull())
        .distinct()
    )

# ================================================================================

def build_bridge_game_genres(df):
    # Explodes the genres array to produce the game↔genre bridge table (game_id + genre_id)
    return (
        df.select("id", F.explode("_genres").alias("g"))
        .select(F.col("id").alias("game_id"), F.col("g.id").alias("genre_id"))
        .filter(F.col("genre_id").isNotNull())
    )

# ================================================================================

def build_dim_platforms(df):
    # Explodes the platforms array to produce a deduplicated platform dimension (id + name)
    return (
        df.select(F.explode("_platforms").alias("p"))
        .select(F.col("p.platform.id").alias("platform_id"), F.col("p.platform.name").alias("platform_name"))
        .filter(F.col("platform_id").isNotNull())
        .distinct()
    )

# ================================================================================

def build_bridge_game_platforms(df):
    # Explodes the platforms array to produce the game↔platform bridge table (game_id + platform_id)
    return (
        df.select("id", F.explode("_platforms").alias("p"))
        .select(F.col("id").alias("game_id"), F.col("p.platform.id").alias("platform_id"))
        .filter(F.col("platform_id").isNotNull())
    )

# ================================================================================

def build_dim_parent_platforms(df):
    # Explodes the parent_platforms array to produce a deduplicated parent platform dimension
    return (
        df.select(F.explode("_parent_platforms").alias("pp"))
        .select(F.col("pp.platform.id").alias("parent_platform_id"), F.col("pp.platform.name").alias("parent_platform_name"))
        .filter(F.col("parent_platform_id").isNotNull())
        .distinct()
    )

# ================================================================================

def build_bridge_game_parent_platforms(df):
    # Explodes the parent_platforms array to produce the game↔parent_platform bridge table
    return (
        df.select("id", F.explode("_parent_platforms").alias("pp"))
        .select(F.col("id").alias("game_id"), F.col("pp.platform.id").alias("parent_platform_id"))
        .filter(F.col("parent_platform_id").isNotNull())
    )

# ================================================================================

def build_dim_stores(df):
    # Explodes the stores array to produce a deduplicated store dimension (id + name)
    return (
        df.select(F.explode("_stores").alias("s"))
        .select(F.col("s.store.id").alias("store_id"), F.col("s.store.name").alias("store_name"))
        .filter(F.col("store_id").isNotNull())
        .distinct()
    )

# ================================================================================

def build_bridge_game_stores(df):
    # Explodes the stores array to produce the game↔store bridge table (game_id + store_id)
    return (
        df.select("id", F.explode("_stores").alias("s"))
        .select(F.col("id").alias("game_id"), F.col("s.store.id").alias("store_id"))
        .filter(F.col("store_id").isNotNull())
    )

# ================================================================================

def build_dim_tags(df):
    # Explodes the tags array to produce a deduplicated tag dimension (id + name + language)
    return (
        df.select(F.explode("_tags").alias("t"))
        .select(
            F.col("t.id").alias("tag_id"),
            F.col("t.name").alias("tag_name"),
            F.col("t.language").alias("tag_language"),
        )
        .filter(F.col("tag_id").isNotNull())
        .distinct()
    )

# ================================================================================

def build_bridge_game_tags(df):
    # Explodes the tags array to produce the game↔tag bridge table (game_id + tag_id)
    return (
        df.select("id", F.explode("_tags").alias("t"))
        .select(F.col("id").alias("game_id"), F.col("t.id").alias("tag_id"))
        .filter(F.col("tag_id").isNotNull())
    )

# ================================================================================

def build_bridge_game_ratings(df):
    # Explodes the ratings array to produce the game↔rating breakdown bridge table
    # (game_id + rating_id + title + count + percent)
    return (
        df.select("id", F.explode("_ratings").alias("r"))
        .select(
            F.col("id").alias("game_id"),
            F.col("r.id").alias("rating_id"),
            F.col("r.title").alias("rating_title"),
            F.col("r.count").alias("rating_count"),
            F.col("r.percent").alias("rating_percent"),
        )
        .filter(F.col("rating_id").isNotNull())
    )

# ── main ──────────────────────────────────────────────────────────────────────

# ================================================================================

def main():
    # Reads the bronze table, deduplicates by game id, parses all JSON columns,
    # then builds and writes all 13 silver tables
    logger = _setup_logging()
    spark = _get_spark()

    logger.info("Reading bronze_games...")
    bronze = spark.table("development.games_bronze.bronze_games")

    logger.info("Standardizing nulls...")
    bronze = _standardize_nulls(bronze)

    logger.info("Checking for duplicates on game id...")
    total = bronze.count()
    deduped = bronze.dropDuplicates(["id"])
    distinct_count = deduped.count()
    if total != distinct_count:
        logger.warning(f"Removed {total - distinct_count:,} duplicate game IDs")
        bronze = deduped
    else:
        logger.info(f"No duplicates found ({total:,} records)")

    logger.info("Parsing JSON columns...")
    bronze = _parse_json(bronze)

    logger.info("Writing silver tables...")
    tables = {
        "silver_fact_games":                   build_fact_games(bronze),
        "silver_dim_esrb_ratings":             build_dim_esrb_ratings(bronze),
        "silver_dim_genres":                   build_dim_genres(bronze),
        "silver_bridge_game_genres":           build_bridge_game_genres(bronze),
        "silver_dim_platforms":                build_dim_platforms(bronze),
        "silver_bridge_game_platforms":        build_bridge_game_platforms(bronze),
        "silver_dim_parent_platforms":         build_dim_parent_platforms(bronze),
        "silver_bridge_game_parent_platforms": build_bridge_game_parent_platforms(bronze),
        "silver_dim_stores":                   build_dim_stores(bronze),
        "silver_bridge_game_stores":           build_bridge_game_stores(bronze),
        "silver_dim_tags":                     build_dim_tags(bronze),
        "silver_bridge_game_tags":             build_bridge_game_tags(bronze),
        "silver_bridge_game_ratings":          build_bridge_game_ratings(bronze),
    }

    for table_name, df in tables.items():
        _write(df, table_name, logger)

    logger.info("Bronze → Silver complete")


if __name__ == "__main__":
    main()
