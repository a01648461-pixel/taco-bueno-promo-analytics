# Taco Bueno – Pricing & Promotion Analytics Pipeline

**Team RPM** · From the Kitchen to the Cloud Data Challenge (Sun Holdings × Databricks)

## Business objective
Taco Bueno wants to know **which promotions should live, die, or come back**.
This pipeline turns raw daily POS sales into business-ready tables that measure
promotional **lift**, **cannibalization** (discount dependency), **ROI per discount dollar**,
**halo effect** and **seasonality**. It runs automatically every time new sales data
arrives, so the metrics stay current without anyone re-running notebooks.

## Architecture (Medallion)

```
CSV files (daily POS export, ~85M lines of history)
        │  Unity Catalog Volume: /Volumes/workspace/default/datos_tacobueno/Taco Bueno - Pricing/
        ▼
BRONZE  st_sales_raw            streaming table · Auto Loader · standardized names · raw strings
        ▼
SILVER  st_sales_cleaned        streaming table · voids removed, typed, 24h hour, unique order_id,
        │                       line revenue, expectations (drop / warn), liquid clustering
        ▼
GOLD    mv_order_summary        one row per order (basket)
        ├─ mv_top_menu_items        Analysis A  – top revenue & best-selling items
        ├─ mv_promo_vs_regular      Analysis B  – discounted vs regular-price lines
        ├─ mv_channel_performance   Analysis C/G – channel revenue, ticket and promo share
        ├─ mv_promo_lift            Analysis D  – basket with vs without promo, by channel
        ├─ mv_discount_dependency   Analysis E  – % of units sold on discount (cannibalization)
        ├─ mv_promo_roi             Analysis H  – revenue per $1 of discount + LIVE / DIE label
        ├─ mv_halo_effect           Analysis I  – companion items in promo orders
        ├─ mv_hourly_seasonality    Analysis F/K – day of week × hour × daypart
        ├─ mv_monthly_item_trend    Analysis J  – monthly trend to find promos to bring BACK
        └─ mv_data_quality_summary  daily data-quality indicators
        ▼
Job task: validate_outputs      fails the job (and emails the team) if a check fails
        ▼
AI/BI Dashboard + Genie space   (read the gold tables)
```

## Repository structure

```
├── databricks.yml                       bundle config (catalog, schema, raw path)
├── notebooks/
│   ├── phase1_eda_limpieza_ventas.ipynb       Phase 1 original (kept as evidence, not corrected)
│   ├── Fase1_Corregida.ipynb                  Phase 1 corrected (order_id, line revenue, 24h hour, reconciliation)
│   └── phase2_analysis.ipynb                  final analysis + charts, reads gold tables
├── resources/
│   ├── jobs/daily_orchestration_job.yml       trigger, tasks, retries, alerts
│   └── pipelines/lakeflow_pipeline.yml        Lakeflow pipeline definition
└── src/
    ├── transformations/
    │   ├── bronze/st_sales_raw.py
    │   ├── silver/st_sales_cleaned.py
    │   └── gold/*.sql                         materialized views (one per analysis)
    └── checks/
        ├── profile_source.sql                 one-time source profiling (store/date, Total)
        └── validate_outputs.sql               post-pipeline validation (job task 2)
```

## How to run it

1. **Create the output schema** (SQL editor): `CREATE SCHEMA IF NOT EXISTS workspace.taco_bueno;`
   The raw CSVs stay where they already are, in the `datos_tacobueno` volume.
2. **Profile the source once**: run `src/checks/profile_source.sql`. It confirms the store/date
   column names and whether `Total` is a line or check-level amount.
3. **Create the pipeline**: Jobs & Pipelines → Create → ETL pipeline → add the `src/transformations` folder
   as source, default catalog `workspace`, schema `taco_bueno`, add configuration
   `raw_sales_path = /Volumes/workspace/default/datos_tacobueno/Taco Bueno - Pricing/`
   (plus `store_column` / `date_column` if auto-detection did not find them). Click **Run pipeline**.
   *(Or, with the Databricks CLI: `databricks bundle deploy && databricks bundle run taco_bueno_daily_job`.)*
4. **Create the job**: Jobs & Pipelines → Create → Job → task 1 = pipeline, task 2 = `validate_outputs` notebook
   depending on task 1. Add a **File arrival** trigger on the volume path.
5. **Analyze**: open `notebooks/phase2_analysis.ipynb` and run all; it reads only gold tables.
6. **Daily operation**: Taco Bueno drops each day's CSV into the volume → the job starts →
   only the new file is processed → all gold tables and the dashboard update.

## From Phase 1 to Phase 2

| Phase 1 notebook (`notebooks/phase1_eda_limpieza_ventas.ipynb`) | Phase 2 pipeline |
|---|---|
| `spark.read.csv` on the whole folder every time | Bronze: Auto Loader reads only new files |
| Rename columns + filter `Order_Voided` / `Item_Voided` | Silver: same logic, as counted expectations |
| `CAST(... AS DOUBLE)` repeated in every query; `'0.0'` cast error | Silver: typed once with `try_cast` |
| Hour from text + `ORDER BY AM_PM` (12 AM / 12 PM misplaced) | Silver: `order_hour` 0–23 |
| `GROUP BY Check_Number` | Silver: `order_id` = store + date + check |
| `SUM(Total)` | Silver: `line_revenue` = qty × price − discount, reconciled with `Total` |
| Ad-hoc SQL cells (A–H) | Gold: one materialized view per analysis + `phase2_analysis.ipynb` (adds Come Back and Live/Die/Come Back summary) |
| Manual "run all" | Job: file-arrival trigger, retries, validation, email alert |

## Data quality

| Dimension | Check | Where | Action |
|---|---|---|---|
| Completeness | check number, menu item, quantity, menu price not null | silver expectations | drop row |
| Validity | quantity > 0, hour 0–23, date parses | silver expectations | drop / warn |
| Accuracy | `Order_Voided` and `Item_Voided` lines removed | silver expectations | drop row |
| Accuracy | `Total` column vs computed line revenue | `mv_data_quality_summary` | monitor |
| Consistency | text → numbers with `try_cast`; non-numeric discounts flagged; discounts normalized to positive | silver | fix / warn |
| Consistency | discount ≤ gross price | silver expectation | warn |
| Uniqueness | `order_id` = store + date + check (check numbers repeat across stores/days) | silver + validation task | fail job |
| Uniqueness | re-uploaded files not reprocessed | Auto Loader checkpoint | automatic |
| Reconciliation | silver revenue = gold revenue | validation task | fail job |
| Monitoring | daily row counts, missing values, possible duplicates | `mv_data_quality_summary` | dashboard |

Expectation pass/fail counts are visible in the pipeline UI (Data quality tab) and in the pipeline event log.

## Technical best practices

- **Streaming tables** for bronze/silver: incremental, each file processed exactly once.
- **Materialized views** for gold: aggregations that Databricks refreshes incrementally when possible.
- **Liquid clustering** (`CLUSTER BY`) on `business_date` and `menu_item`, the most common filters.
- **Expectations** for declarative data quality with metrics, instead of silent filtering.
- **Triggered mode + file arrival trigger**: cost-efficient (no always-on cluster) yet automatic.
- **Retries** on the pipeline task, **no retries** on validation (a real data issue should alert, not loop).
- `max_concurrent_runs: 1` so two uploads never process at the same time.
- Bronze keeps raw data, so any logic change can be reprocessed with a **full refresh**.
- Code in Git, config in YAML (Databricks Asset Bundles): reproducible and reviewable.

## Failures and reruns
- Pipeline task fails → retried twice, 5 minutes apart → if still failing, email alert.
- Validation fails → job marked failed, email alert; gold tables keep the last good refresh for consumers to inspect.
- Fix the code or data → **Repair run** in the job UI (reruns only failed tasks).
- Logic changed → pipeline **Full refresh** rebuilds every table from bronze.

## Key findings (Phase 1 EDA, to be recomputed on gold tables)

Basket size, channel revenue and halo counts will change once orders use `order_id`
and revenue uses `line_revenue`; the item-level discount dependency results should hold.

- Drive-Thru is the #1 revenue channel; DoorDash ≈ 66% of third-party delivery revenue.
- Delivery items show 95–98% discount dependency; Steak Soft Taco 53%; sides 21–35%.
- Highest discount ROI: Steak Soft Taco (~$11 revenue per $1 discount) → scale up.
- Lowest: Diced Chicken (1 lb) (~$2) and Brisket side (~$6) → candidates to kill.
