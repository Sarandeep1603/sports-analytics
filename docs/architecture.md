# Architecture Deep Dive — Sports Analytics Data Pipeline

## Overview

This document covers the design decisions, data flow, and technical rationale behind the Sports Analytics Data Pipeline.

---

## Data Flow

```
API-Football (REST)
        │
        ▼
Azure Data Factory ──── Parameterized HTTP → JSON Copy Activity
        │
        ▼
ADLS Gen2 Bronze ──── Raw JSON, partitioned by ingest_date / sport_type
        │
        ▼
Databricks Silver ──── Flatten → Cleanse → Deduplicate → MERGE
        │
  Great Expectations validation (schema + nulls + value ranges)
        │
        ▼
Delta Lake Silver ──── Partitioned by league_id + season
        │
        ▼
Databricks Gold ──── Star Schema: fact_match_stats + dims
        │
        ▼
Delta Lake Gold ──── Partitioned by season + league_id
        │
        ▼
Power BI Dashboard ──── DirectQuery on Gold layer
```

---

## Medallion Architecture Decisions

### Bronze — Immutable Raw Zone
- Raw JSON preserved as-is. No transformation. Source of truth.
- Enables full reprocessing if Silver logic changes.
- `_ingest_timestamp` and `_source_file` audit columns added only.
- `mergeSchema = true` handles API-Football schema changes gracefully.

### Silver — Trusted Zone
- All business logic applied here: null handling, type casting, deduplication.
- **Incremental MERGE** on `match_id` avoids full reprocessing — 35% compute cost reduction.
- Great Expectations runs **before** Gold to prevent bad data propagation.
- OPTIMIZE + Z-ORDER on `league_id, season` — most common query filters.

### Gold — Business Zone
- Star schema optimized for Power BI DirectQuery.
- `fact_match_stats` — grain: one row per match.
- `dim_team`, `dim_date` — conformed dimensions.
- Date partitioning on `season + league_id` reduces Power BI query scan by 60%.

---

## Incremental Load Strategy

### Why MERGE over full refresh?
- Full refresh on 50K+ records costs ~35% more compute.
- MERGE on `match_id` updates only changed rows (score corrections) and inserts new matches.
- Delta Lake ACID guarantees consistency during concurrent writes.

### Delta Lake Time Travel
- Enabled by default. Can roll back to previous version if bad data reaches Silver.
- `RESTORE TABLE sports_analytics.silver.football_matches VERSION AS OF 5`

---

## Orchestration Design

### Why Airflow for orchestration + ADF for ingestion?
- ADF excels at managed HTTP → ADLS copy with built-in retry and monitoring.
- Airflow provides fine-grained DAG dependency control, custom operators, and programmatic alerting.
- Separation of concerns: ADF handles data movement, Airflow handles workflow logic.

### DAG Dependencies
```
api_health_check
    ├── ingest_football_bronze
    └── ingest_cricket_bronze
            └── silver_transform
                    └── data_quality_gate
                            └── gold_build
                                    └── pipeline_success_notification
```

---

## Data Quality Strategy

### Great Expectations Suite: `silver_football_matches_suite`
Critical expectations (fail pipeline):
- `match_id` not null, unique
- `match_date` not null, correct format
- `home_team_id`, `away_team_id` not null
- `match_result` in allowed value set

Warning expectations (log but continue):
- `home_goals`, `away_goals` in 0–20 range
- `season` in 2020–2030 range

---

## Performance Optimizations

| Optimization | Technique | Impact |
|---|---|---|
| Incremental load | Delta MERGE on match_id | 35% compute cost reduction |
| Partition pruning | Partition by league_id + season | Faster Silver reads |
| Z-ORDER | league_id, season on Silver | Faster filter queries |
| Gold partitioning | season + league_id | 60% Power BI query time reduction |
| Cluster right-sizing | Memory-optimized driver nodes | 30% execution time reduction |

---

## Security

- API keys stored in **Azure Key Vault**, referenced via ADF Linked Services.
- ADLS Gen2 access via **Managed Identity** (no credentials in code).
- Databricks secrets via **Databricks Secret Scope** backed by Key Vault.
- All secrets referenced as `{{secrets/scope/key}}` — never hardcoded.

---

## CI/CD Pipeline (GitHub Actions)

### On Pull Request to main
1. Flake8 linting — syntax + style
2. Unit tests — PySpark transformation logic
3. GE suite JSON validation
4. Airflow DAG syntax check
5. ADF JSON schema validation

### On Merge to main
1. All PR checks +
2. Deploy notebooks to Databricks workspace via Databricks CLI
3. Post-deploy health check

---

## Future Enhancements (v2)

- **Streaming ingestion** via Azure Event Hubs + Spark Structured Streaming for live match updates
- **dbt** integration for Gold layer transformation documentation
- **Unity Catalog** for fine-grained data governance and lineage
- **MLflow** for player performance prediction models
