# Visualization context

Dashboard creation into Databricks Community Edition using the AI/BI Lakeview API.
The dashboard stays ready for use (available when the cluster is on).

## Requirements

- Concise, insight-driven design with varied chart types.
- Cross-variable charts (games × genres × dates × ratings × platforms).
- Fully interactive: dropdown filters for Genre, Platform, ESRB Rating, and date range pickers (Released From / Released To) that apply to all datasets simultaneously.
- Horizontal bar charts when a category axis has many labels, to keep names readable.
- For any axis with many classes, limit to top 10/15.
- Data cards use statistically appropriate aggregation (avg vs. median).
- Date filters use ISO 8601 format (DATE parameter type).

## Implementation notes

- `create_databricks_dashboard.py` builds and publishes the full dashboard spec via the Databricks SDK (`WorkspaceClient.lakeview`).
- The spec uses `queryLines` (not `query`) for dataset SQL — required by the Lakeview API.
- Dataset-level parameters handle filtering: STRING params use empty-string defaults (`""`); DATE params use `null` defaults (no filter on initial load).
- SQL date conditions use `TRY_CAST(:param AS DATE) IS NULL` to safely handle null/unset date parameters without `CAST_INVALID_INPUT` errors.
- Cross-dataset filtering is achieved through shared parameters wired to all datasets, not through automatic Lakeview cross-filtering (which only works within the same dataset).
- Grid is 6 columns wide. All layout rows must sum to exactly 6.
