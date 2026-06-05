import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard

LOG_FILE     = Path(__file__).parent.parent / "app.log"
DASHBOARD_NAME = "Games Analytics Dashboard"
PARENT_PATH    = "/de_project_1_v2"

# ── logging ───────────────────────────────────────────────────────────────────

class _ErrorMarkerFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        return f">>> ERROR <<< {msg}" if record.levelno >= logging.ERROR else msg

def _setup_logging():
    logger = logging.getLogger("create_dashboard")
    logger.setLevel(logging.DEBUG)
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    fh = logging.FileHandler(LOG_FILE); fh.setLevel(logging.DEBUG)
    fh.setFormatter(_ErrorMarkerFormatter(fmt))
    ch = logging.StreamHandler(); ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(fmt))
    logger.addHandler(fh); logger.addHandler(ch)
    return logger

# ── parameter mapping ─────────────────────────────────────────────────────────
# param_date_from / param_date_to: filter by released date (ISO string vs DATE column).
# ds_scatter_genre and ds_genre_platform intentionally omit genre/platform/esrb
# (they are multi-variable overview charts). Date filter applies to all.

_DS_PARAMS = {
    "ds_kpi":                   ["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"],
    "ds_genre_stats":           ["param_platform", "param_esrb", "param_date_from", "param_date_to"],
    "ds_parent_platform_stats": ["param_genre", "param_esrb", "param_date_from", "param_date_to"],
    "ds_release_trend":         ["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"],
    "ds_esrb":                  ["param_genre", "param_platform", "param_date_from", "param_date_to"],
    "ds_rating_breakdown":      ["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"],
    "ds_top_tags":              ["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"],
    "ds_top_games":             ["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"],
    "ds_store_dist":            ["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"],
    "ds_scatter_genre":         ["param_date_from", "param_date_to"],
    "ds_genre_platform":        ["param_date_from", "param_date_to"],
}

_PARAM_TARGETS = {
    "param_genre":     [ds for ds, ps in _DS_PARAMS.items() if "param_genre"     in ps],
    "param_platform":  [ds for ds, ps in _DS_PARAMS.items() if "param_platform"  in ps],
    "param_esrb":      [ds for ds, ps in _DS_PARAMS.items() if "param_esrb"      in ps],
    "param_date_from": [ds for ds, ps in _DS_PARAMS.items() if "param_date_from" in ps],
    "param_date_to":   [ds for ds, ps in _DS_PARAMS.items() if "param_date_to"   in ps],
}

# ── dataset helpers ───────────────────────────────────────────────────────────

def _to_lines(sql: str) -> list:
    return [line + "\n" for line in sql.split("\n")]

def _str_param(keyword: str) -> dict:
    """STRING parameter with empty-string default (= 'show all' on initial load)."""
    return {
        "displayName": keyword,
        "keyword": keyword,
        "dataType": "STRING",
        "defaultSelection": {
            "values": {"dataType": "STRING", "values": [{"value": ""}]},
        },
    }

def _date_param(keyword: str) -> dict:
    """DATE parameter with null default (= no filter on initial load)."""
    return {
        "displayName": keyword,
        "keyword": keyword,
        "dataType": "DATE",
        "defaultSelection": {
            "values": {"dataType": "DATE", "values": [{"value": None}]},
        },
    }

def _dataset(name, display_name, sql, params=None):
    d = {
        "name": name,
        "displayName": display_name,
        "queryLines": _to_lines(sql),
    }
    if params:
        d["parameters"] = [
            _date_param(p) if p.startswith("param_date") else _str_param(p)
            for p in params
        ]
    return d

# ── SQL filter conditions ─────────────────────────────────────────────────────

def _genre_cond(p="param_genre"):
    return (f"AND (:{p} IS NULL OR :{p} = '' OR EXISTS ("
            f"SELECT 1 FROM development.games_gold.gold_bridge_game_genres _g "
            f"WHERE _g.game_id = f.game_id AND _g.genre_name = :{p}))\n")

def _platform_cond(p="param_platform"):
    return (f"AND (:{p} IS NULL OR :{p} = '' OR EXISTS ("
            f"SELECT 1 FROM development.games_gold.gold_bridge_game_parent_platforms _pp "
            f"WHERE _pp.game_id = f.game_id AND _pp.parent_platform_name = :{p}))\n")

def _esrb_cond(f_alias="f", p="param_esrb"):
    return (f"AND (:{p} IS NULL OR :{p} = '' OR "
            f"COALESCE({f_alias}.esrb_name, 'Not Rated') = :{p})\n")

def _date_from_cond(f_alias="f", p="param_date_from"):
    # TRY_CAST returns NULL for empty string/NULL — avoids CAST_INVALID_INPUT at planning time
    return f"AND (TRY_CAST(:{p} AS DATE) IS NULL OR {f_alias}.released >= TRY_CAST(:{p} AS DATE))\n"

def _date_to_cond(f_alias="f", p="param_date_to"):
    return f"AND (TRY_CAST(:{p} AS DATE) IS NULL OR {f_alias}.released <= TRY_CAST(:{p} AS DATE))\n"

# ── SQL datasets ──────────────────────────────────────────────────────────────

_DATASETS = [
    # ── filter option datasets (no params — always return full list) ──
    _dataset("ds_opt_genres", "Genre Options",
        "SELECT DISTINCT genre_name "
        "FROM development.games_gold.gold_bridge_game_genres "
        "WHERE genre_name IS NOT NULL ORDER BY genre_name"),

    _dataset("ds_opt_platforms", "Platform Options",
        "SELECT DISTINCT parent_platform_name "
        "FROM development.games_gold.gold_bridge_game_parent_platforms "
        "WHERE parent_platform_name IS NOT NULL ORDER BY parent_platform_name"),

    _dataset("ds_opt_esrb", "ESRB Options",
        "SELECT DISTINCT COALESCE(esrb_name, 'Not Rated') AS esrb_name "
        "FROM development.games_gold.gold_fact_games ORDER BY esrb_name"),

    # ── KPIs ──
    _dataset("ds_kpi", "KPIs",
        "SELECT COUNT(*) AS total_games,\n"
        "ROUND(AVG(rating), 2) AS avg_rating,\n"
        "ROUND(percentile_approx(CAST(playtime AS DOUBLE), 0.5), 1) AS median_playtime_hrs,\n"
        "ROUND(AVG(ratings_count), 0) AS avg_ratings_count\n"
        "FROM development.games_gold.gold_fact_games f\n"
        "WHERE release_year IS NOT NULL\n"
        + _genre_cond() + _platform_cond() + _esrb_cond()
        + _date_from_cond() + _date_to_cond(),
        params=["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"]),

    # ── Genre stats (cross-filter source for param_genre; filtered by platform + ESRB + date) ──
    _dataset("ds_genre_stats", "Genre Stats",
        "SELECT g.genre_name,\n"
        "COUNT(DISTINCT f.game_id) AS game_count,\n"
        "ROUND(AVG(f.rating), 2) AS avg_rating,\n"
        "ROUND(AVG(f.playtime), 1) AS avg_playtime\n"
        "FROM development.games_gold.gold_fact_games f\n"
        "JOIN development.games_gold.gold_bridge_game_genres g ON f.game_id = g.game_id\n"
        "WHERE g.genre_name IS NOT NULL\n"
        + _platform_cond() + _esrb_cond()
        + _date_from_cond() + _date_to_cond() +
        "GROUP BY g.genre_name ORDER BY game_count DESC LIMIT 15",
        params=["param_platform", "param_esrb", "param_date_from", "param_date_to"]),

    # ── Platform stats (cross-filter source for param_platform; filtered by genre + ESRB + date) ──
    _dataset("ds_parent_platform_stats", "Platform Stats",
        "SELECT pp.parent_platform_name,\n"
        "COUNT(DISTINCT f.game_id) AS game_count,\n"
        "ROUND(AVG(f.rating), 2) AS avg_rating\n"
        "FROM development.games_gold.gold_fact_games f\n"
        "JOIN development.games_gold.gold_bridge_game_parent_platforms pp ON f.game_id = pp.game_id\n"
        "WHERE pp.parent_platform_name IS NOT NULL\n"
        + _genre_cond() + _esrb_cond()
        + _date_from_cond() + _date_to_cond() +
        "GROUP BY pp.parent_platform_name ORDER BY game_count DESC",
        params=["param_genre", "param_esrb", "param_date_from", "param_date_to"]),

    # ── Release trend ──
    _dataset("ds_release_trend", "Release Trend",
        "SELECT f.release_year,\n"
        "COUNT(DISTINCT f.game_id) AS game_count,\n"
        "ROUND(AVG(f.rating), 2) AS avg_rating\n"
        "FROM development.games_gold.gold_fact_games f\n"
        "WHERE f.release_year IS NOT NULL\n"
        "AND f.release_year BETWEEN 1990 AND YEAR(CURRENT_DATE())\n"
        + _genre_cond() + _platform_cond() + _esrb_cond()
        + _date_from_cond() + _date_to_cond() +
        "GROUP BY f.release_year ORDER BY f.release_year",
        params=["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"]),

    # ── ESRB distribution (cross-filter source for param_esrb; filtered by genre + platform + date) ──
    _dataset("ds_esrb", "ESRB Distribution",
        "SELECT COALESCE(f.esrb_name, 'Not Rated') AS esrb_name,\n"
        "COUNT(DISTINCT f.game_id) AS game_count\n"
        "FROM development.games_gold.gold_fact_games f\n"
        "WHERE 1=1\n"
        + _genre_cond() + _platform_cond()
        + _date_from_cond() + _date_to_cond() +
        "GROUP BY esrb_name ORDER BY game_count DESC",
        params=["param_genre", "param_platform", "param_date_from", "param_date_to"]),

    # ── Rating breakdown ──
    _dataset("ds_rating_breakdown", "Rating Breakdown",
        "SELECT r.rating_title, SUM(r.rating_count) AS total_count\n"
        "FROM development.games_gold.gold_bridge_game_ratings r\n"
        "JOIN development.games_gold.gold_fact_games f ON r.game_id = f.game_id\n"
        "WHERE r.rating_title IS NOT NULL\n"
        + _genre_cond() + _platform_cond() + _esrb_cond()
        + _date_from_cond() + _date_to_cond() +
        "GROUP BY r.rating_title ORDER BY total_count DESC",
        params=["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"]),

    # ── Top tags ──
    _dataset("ds_top_tags", "Top Tags",
        "SELECT t.tag_name, COUNT(DISTINCT bt.game_id) AS game_count\n"
        "FROM development.games_silver.silver_dim_tags t\n"
        "JOIN development.games_silver.silver_bridge_game_tags bt ON t.tag_id = bt.tag_id\n"
        "JOIN development.games_gold.gold_fact_games f ON bt.game_id = f.game_id\n"
        "WHERE t.tag_language = 'eng'\n"
        + _genre_cond() + _platform_cond() + _esrb_cond()
        + _date_from_cond() + _date_to_cond() +
        "GROUP BY t.tag_name ORDER BY game_count DESC LIMIT 10",
        params=["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"]),

    # ── Store distribution ──
    _dataset("ds_store_dist", "Store Distribution",
        "SELECT s.store_name, COUNT(DISTINCT s.game_id) AS game_count\n"
        "FROM development.games_gold.gold_bridge_game_stores s\n"
        "JOIN development.games_gold.gold_fact_games f ON s.game_id = f.game_id\n"
        "WHERE s.store_name IS NOT NULL\n"
        + _genre_cond() + _platform_cond() + _esrb_cond()
        + _date_from_cond() + _date_to_cond() +
        "GROUP BY s.store_name ORDER BY game_count DESC",
        params=["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"]),

    # ── Top games ──
    _dataset("ds_top_games", "Top Games",
        "SELECT f.game_name, CAST(f.released AS STRING) AS released,\n"
        "ROUND(f.rating, 2) AS rating, f.ratings_count,\n"
        "f.playtime AS playtime_hrs,\n"
        "COALESCE(f.esrb_name, 'Not Rated') AS esrb_name\n"
        "FROM development.games_gold.gold_fact_games f\n"
        "WHERE f.ratings_count >= 10 AND f.rating IS NOT NULL\n"
        + _genre_cond() + _platform_cond() + _esrb_cond()
        + _date_from_cond() + _date_to_cond() +
        "GROUP BY f.game_name, f.released, f.rating, f.ratings_count, f.playtime, f.esrb_name\n"
        "ORDER BY f.rating DESC, f.ratings_count DESC LIMIT 20",
        params=["param_genre", "param_platform", "param_esrb", "param_date_from", "param_date_to"]),

    # ── Multi-variable: Avg Rating vs Avg Playtime by Genre (scatter — date-only filter) ──
    _dataset("ds_scatter_genre", "Rating vs Playtime by Genre",
        "SELECT g.genre_name,\n"
        "ROUND(AVG(f.rating), 2) AS avg_rating,\n"
        "ROUND(AVG(f.playtime), 1) AS avg_playtime,\n"
        "COUNT(DISTINCT f.game_id) AS game_count\n"
        "FROM development.games_gold.gold_fact_games f\n"
        "JOIN development.games_gold.gold_bridge_game_genres g ON f.game_id = g.game_id\n"
        "WHERE g.genre_name IS NOT NULL AND f.rating IS NOT NULL AND f.playtime > 0\n"
        + _date_from_cond() + _date_to_cond() +
        "GROUP BY g.genre_name",
        params=["param_date_from", "param_date_to"]),

    # ── Multi-variable: Top 5 Genres × Platform game count (stacked bar — date-only filter) ──
    _dataset("ds_genre_platform", "Genre x Platform",
        "WITH top_genres AS (\n"
        "  SELECT genre_name FROM development.games_gold.gold_bridge_game_genres\n"
        "  GROUP BY genre_name ORDER BY COUNT(DISTINCT game_id) DESC LIMIT 5\n"
        ")\n"
        "SELECT pp.parent_platform_name,\n"
        "CASE WHEN g.genre_name IN (SELECT genre_name FROM top_genres)\n"
        "     THEN g.genre_name ELSE 'Other' END AS genre_group,\n"
        "COUNT(DISTINCT f.game_id) AS game_count\n"
        "FROM development.games_gold.gold_fact_games f\n"
        "JOIN development.games_gold.gold_bridge_game_genres g ON f.game_id = g.game_id\n"
        "JOIN development.games_gold.gold_bridge_game_parent_platforms pp ON f.game_id = pp.game_id\n"
        "WHERE pp.parent_platform_name IS NOT NULL AND g.genre_name IS NOT NULL\n"
        + _date_from_cond() + _date_to_cond() +
        "GROUP BY pp.parent_platform_name, genre_group\n"
        "ORDER BY pp.parent_platform_name, game_count DESC",
        params=["param_date_from", "param_date_to"]),
]

# ── widget helpers ────────────────────────────────────────────────────────────

def _f(name):
    return {"name": name, "expression": f"`{name}`"}

def _q(dataset, fields):
    return {
        "name": "main_query",
        "query": {
            "datasetName": dataset,
            "fields": [_f(n) for n in fields],
            "disaggregated": False,
        },
    }

def _pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}

def _frame(title):
    return {"showDescription": False, "showTitle": True, "title": title}

# ── date picker filter widget ─────────────────────────────────────────────────

def _date_picker_filter(name, title, param_name, x, y, w=3, h=3):
    queries = []
    enc_fields = []
    for ds in _PARAM_TARGETS[param_name]:
        qname = f"{name}_{ds}"
        queries.append({
            "name": qname,
            "query": {
                "datasetName": ds,
                "parameters": [{"name": param_name, "keyword": param_name}],
                "disaggregated": False,
            }
        })
        enc_fields.append({"parameterName": param_name, "queryName": qname})
    return {
        "widget": {
            "name": name,
            "queries": queries,
            "spec": {
                "version": 2,
                "widgetType": "filter-date-picker",
                "encodings": {"fields": enc_fields},
                "frame": _frame(title),
            },
        },
        "position": _pos(x, y, w, h),
    }

# ── filter widget (single-select) ─────────────────────────────────────────────

def _filter(name, title, opt_dataset, opt_field, param_name, target_datasets, x, y, w=2, h=3):
    opt_qname = f"{name}_opts"
    queries = [{
        "name": opt_qname,
        "query": {
            "datasetName": opt_dataset,
            "fields": [_f(opt_field)],
            "disaggregated": False,
        },
    }]
    enc_fields = [{"displayName": title, "fieldName": opt_field, "queryName": opt_qname}]

    for ds in target_datasets:
        qname = f"{name}_{ds}"
        queries.append({
            "name": qname,
            "query": {
                "datasetName": ds,
                "parameters": [{"name": param_name, "keyword": param_name}],
                "disaggregated": False,
            },
        })
        enc_fields.append({"parameterName": param_name, "queryName": qname})

    return {
        "widget": {
            "name": name,
            "queries": queries,
            "spec": {
                "version": 2,
                "widgetType": "filter-single-select",
                "encodings": {"fields": enc_fields},
                "frame": _frame(title),
            },
        },
        "position": _pos(x, y, w, h),
    }

# ── chart widgets ─────────────────────────────────────────────────────────────

def _counter(name, title, dataset, field, x, y, w=3, h=4):
    return {
        "widget": {
            "name": name,
            "queries": [_q(dataset, [field])],
            "spec": {
                "version": 2, "widgetType": "counter",
                "encodings": {"target": {"displayName": title, "fieldName": field}},
                "frame": _frame(title),
            },
        },
        "position": _pos(x, y, w, h),
    }

def _bar(name, title, dataset, cat_field, val_field, cat_label, val_label,
         x, y, w=3, h=8, horizontal=False, color_field=None, color_label=None):
    fields = [cat_field, val_field]
    if color_field:
        fields.append(color_field)
    if horizontal:
        enc = {
            "x": {"displayName": val_label, "fieldName": val_field,
                  "scale": {"type": "quantitative"}, "axis": {"title": val_label}},
            "y": {"displayName": cat_label, "fieldName": cat_field,
                  "scale": {"type": "categorical"}, "axis": {"title": cat_label}},
        }
    else:
        enc = {
            "x": {"displayName": cat_label, "fieldName": cat_field,
                  "scale": {"type": "categorical"}, "axis": {"title": cat_label}},
            "y": {"displayName": val_label, "fieldName": val_field,
                  "scale": {"type": "quantitative"}, "axis": {"title": val_label}},
        }
    if color_field:
        enc["color"] = {
            "displayName": color_label or color_field,
            "fieldName": color_field,
            "scale": {"type": "categorical"},
            "legend": {"position": "right", "title": color_label or color_field},
        }
    return {
        "widget": {
            "name": name,
            "queries": [_q(dataset, fields)],
            "spec": {"version": 3, "widgetType": "bar", "encodings": enc, "frame": _frame(title)},
        },
        "position": _pos(x, y, w, h),
    }

def _line(name, title, dataset, x_field, y_field, x_label, y_label, x, y, w=6, h=8):
    return {
        "widget": {
            "name": name,
            "queries": [_q(dataset, [x_field, y_field])],
            "spec": {
                "version": 3, "widgetType": "line",
                "encodings": {
                    "x": {"displayName": x_label, "fieldName": x_field,
                          "scale": {"type": "quantitative"}, "axis": {"title": x_label}},
                    "y": {"displayName": y_label, "fieldName": y_field,
                          "scale": {"type": "quantitative"}, "axis": {"title": y_label}},
                },
                "frame": _frame(title),
            },
        },
        "position": _pos(x, y, w, h),
    }

def _pie(name, title, dataset, label_field, val_field, x, y, w=3, h=8):
    return {
        "widget": {
            "name": name,
            "queries": [_q(dataset, [label_field, val_field])],
            "spec": {
                "version": 3, "widgetType": "pie",
                "encodings": {
                    "angle": {"displayName": val_field, "fieldName": val_field,
                              "scale": {"type": "quantitative"}},
                    "color": {"displayName": label_field, "fieldName": label_field,
                              "scale": {"type": "categorical"}},
                },
                "frame": _frame(title),
            },
        },
        "position": _pos(x, y, w, h),
    }

def _scatter(name, title, dataset, x_field, y_field, color_field,
             x_label, y_label, color_label, x, y, w=3, h=8):
    return {
        "widget": {
            "name": name,
            "queries": [_q(dataset, [x_field, y_field, color_field])],
            "spec": {
                "version": 3, "widgetType": "scatter",
                "encodings": {
                    "x": {"displayName": x_label, "fieldName": x_field,
                          "scale": {"type": "quantitative"}, "axis": {"title": x_label}},
                    "y": {"displayName": y_label, "fieldName": y_field,
                          "scale": {"type": "quantitative"}, "axis": {"title": y_label}},
                    "color": {"displayName": color_label, "fieldName": color_field,
                              "scale": {"type": "categorical"},
                              "legend": {"position": "right", "title": color_label}},
                },
                "frame": _frame(title),
            },
        },
        "position": _pos(x, y, w, h),
    }

def _table_col(field_name, display_name, order, align="left"):
    return {
        "alignContent": align, "allowHTML": False, "allowSearch": True,
        "booleanValues": ["false", "true"], "dateTimeFormat": "YYYY-MM-DD",
        "displayAs": "string", "displayName": display_name, "fieldName": field_name,
        "highlightLinks": False, "imageHeight": "", "imageTitleTemplate": "{{ @ }}",
        "imageUrlTemplate": "{{ @ }}", "imageWidth": "", "linkOpenInNewTab": True,
        "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}",
        "linkUrlTemplate": "{{ @ }}", "order": order, "preserveWhitespace": False,
        "title": display_name, "type": "string", "useMonospaceFont": False, "visible": True,
    }

def _table(name, title, dataset, columns, x, y, w=6, h=10):
    fields = [c["fieldName"] for c in columns]
    return {
        "widget": {
            "name": name,
            "queries": [_q(dataset, fields)],
            "spec": {
                "version": 1, "widgetType": "table",
                "allowHTMLByDefault": False, "condensed": False,
                "invisibleColumns": [], "itemsPerPage": 20,
                "paginationSize": "default", "withRowNumber": False,
                "encodings": {"columns": columns},
                "frame": _frame(title),
            },
        },
        "position": _pos(x, y, w, h),
    }

# ── dashboard spec ────────────────────────────────────────────────────────────

def _build_spec():
    top_games_cols = [
        _table_col("game_name",     "Game",           10000, "left"),
        _table_col("released",      "Released",       10001, "center"),
        _table_col("rating",        "Rating",         10002, "right"),
        _table_col("ratings_count", "Ratings Count",  10003, "right"),
        _table_col("playtime_hrs",  "Playtime (hrs)", 10004, "right"),
        _table_col("esrb_name",     "ESRB",           10005, "center"),
    ]

    # Grid is 6 columns wide. Every row sums to exactly 6.
    layout = [
        # ── Row 0: Dropdown filters — 3 × w=2 = 6 (y 0–3) ───────────────
        _filter("flt_genre",    "Genre",         "ds_opt_genres",    "genre_name",
                "param_genre",    _PARAM_TARGETS["param_genre"],    0, 0, 2, 3),
        _filter("flt_platform", "Platform",      "ds_opt_platforms", "parent_platform_name",
                "param_platform", _PARAM_TARGETS["param_platform"], 2, 0, 2, 3),
        _filter("flt_esrb",     "ESRB Rating",   "ds_opt_esrb",      "esrb_name",
                "param_esrb",     _PARAM_TARGETS["param_esrb"],     4, 0, 2, 3),

        # ── Row 1: Date pickers — 2 × w=3 = 6 (y 3–6) ───────────────────
        _date_picker_filter("flt_date_from", "Released From", "param_date_from", 0, 3, 3, 3),
        _date_picker_filter("flt_date_to",   "Released To",   "param_date_to",   3, 3, 3, 3),

        # ── KPIs — w=2,1,1,2 = 6, one row (y 6–9) ────────────────────────
        _counter("kpi_total_games",     "Total Games",           "ds_kpi", "total_games",        0, 6, 2, 3),
        _counter("kpi_avg_rating",      "Avg Rating (0–5)",      "ds_kpi", "avg_rating",          2, 6, 1, 3),
        _counter("kpi_median_playtime", "Median Playtime (hrs)", "ds_kpi", "median_playtime_hrs", 3, 6, 1, 3),
        _counter("kpi_avg_ratings_cnt", "Avg Ratings Count",     "ds_kpi", "avg_ratings_count",   4, 6, 2, 3),

        # ── Top genres + platforms — 2 × w=3 = 6 (y 9–17) ───────────────
        _bar("bar_genre_count", "Top 15 Genres by Game Count",
             "ds_genre_stats", "genre_name", "game_count",
             "Genre", "Number of Games", 0, 9, 3, 8, horizontal=True),
        _bar("bar_platform_count", "Game Count by Platform",
             "ds_parent_platform_stats", "parent_platform_name", "game_count",
             "Platform", "Number of Games", 3, 9, 3, 8, horizontal=True),

        # ── Multi-variable — 2 × w=3 = 6 (y 17–25) ──────────────────────
        _bar("bar_genre_platform",
             "Top 5 Genres by Platform (Game Count)",
             "ds_genre_platform", "parent_platform_name", "game_count",
             "Platform", "Number of Games", 0, 17, 3, 8,
             horizontal=True, color_field="genre_group", color_label="Genre"),
        _scatter("scatter_rating_playtime",
                 "Avg Rating vs Avg Playtime by Genre",
                 "ds_scatter_genre", "avg_playtime", "avg_rating", "genre_name",
                 "Avg Playtime (hrs)", "Avg Rating (0–5)", "Genre",
                 3, 17, 3, 8),

        # ── Release trend — w=6 full width (y 25–33) ─────────────────────
        _line("line_release_trend", "Games Released per Year (1990–present)",
              "ds_release_trend", "release_year", "game_count",
              "Year", "Games Released", 0, 25, 6, 8),

        # ── ESRB + Rating breakdown — 2 × w=3 = 6 (y 33–41) ─────────────
        _pie("pie_esrb", "ESRB Rating Distribution",
             "ds_esrb", "esrb_name", "game_count", 0, 33, 3, 8),
        _bar("bar_rating_breakdown", "Player Rating Breakdown",
             "ds_rating_breakdown", "rating_title", "total_count",
             "Rating Category", "Total Votes", 3, 33, 3, 8),

        # ── Tags + Stores — 2 × w=3 = 6 (y 41–49) ───────────────────────
        _bar("bar_top_tags", "Top 10 Tags by Game Count",
             "ds_top_tags", "tag_name", "game_count",
             "Tag", "Number of Games", 0, 41, 3, 8, horizontal=True),
        _bar("bar_store_dist", "Games Available per Store",
             "ds_store_dist", "store_name", "game_count",
             "Store", "Number of Games", 3, 41, 3, 8, horizontal=True),

        # ── Avg rating — 2 × w=3 = 6 (y 49–57) ──────────────────────────
        _bar("bar_avg_rating_genre", "Avg Rating by Genre",
             "ds_genre_stats", "genre_name", "avg_rating",
             "Genre", "Avg Rating (0–5)", 0, 49, 3, 8, horizontal=True),
        _bar("bar_avg_rating_platform", "Avg Rating by Platform",
             "ds_parent_platform_stats", "parent_platform_name", "avg_rating",
             "Platform", "Avg Rating (0–5)", 3, 49, 3, 8, horizontal=True),

        # ── Top games table — w=6 full width (y 57–67) ───────────────────
        _table("table_top_games",
               "Top 20 Highest Rated Games (min. 10 ratings)",
               "ds_top_games", top_games_cols, 0, 57, 6, 10),
    ]

    return {
        "datasets": _DATASETS,
        "pages": [{"name": "main_page", "displayName": "Games Analytics", "layout": layout}],
    }

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    load_dotenv(Path(__file__).parent.parent / ".env")
    logger = _setup_logging()

    host         = os.getenv("DATABRICKS_HOST")
    token        = os.getenv("DATABRICKS_TOKEN")
    http_path    = os.getenv("DATABRICKS_HTTP_PATH", "")
    warehouse_id = http_path.split("/")[-1]

    if not all([host, token, warehouse_id]):
        logger.error("Missing env: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH")
        sys.exit(1)

    client = WorkspaceClient(host=host, token=token)

    for d in client.lakeview.list():
        if d.display_name == DASHBOARD_NAME:
            client.lakeview.trash(dashboard_id=d.dashboard_id)
            logger.info(f"Trashed old dashboard: {d.dashboard_id}")

    spec = _build_spec()
    logger.info(f"Creating '{DASHBOARD_NAME}'...")

    dashboard = client.lakeview.create(
        dashboard=Dashboard(
            display_name=DASHBOARD_NAME,
            serialized_dashboard=json.dumps(spec),
            warehouse_id=warehouse_id,
            parent_path=PARENT_PATH,
        )
    )
    dashboard_id = dashboard.dashboard_id
    logger.info(f"Dashboard created — id: {dashboard_id}")
    client.lakeview.publish(dashboard_id=dashboard_id, warehouse_id=warehouse_id)
    logger.info(f"Published: {host}/dashboardsv3/{dashboard_id}")

if __name__ == "__main__":
    main()
