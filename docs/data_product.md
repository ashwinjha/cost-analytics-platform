# Canonical Data Product

Name:
account_service_day_cost

Grain:
(account_id, service_name, usage_date)

Metric:
total_cost_usd

Description:
This table represents the authoritative daily cost per account and service.
All downstream analytics and reporting must depend on this dataset.

Update Model:
Daily batch processing.
Deterministic recomputation for late or corrected data.

## Dimensions

dim_account
- account_id
- org
- billing_owner

dim_service
- service_name
- service_category
- pricing_model

Rationale:
The fact table stores only metrics and join keys.
Attributes that evolve independently are stored in dimension tables
to avoid frequent rewrites of the fact dataset.

## Pipeline Stages

Stage 1: Ingest & Normalize
- Input: raw billing events
- Grain: event-level
- Responsibilities:
    - Schema normalization
    - Deduplication
    - No joins
    - No aggregations

Stage 2: Aggregate
- Input: normalized events
- Grain: account-service-day
- Responsibilities:
    - Compute daily cost per account and service
    - One row per grain

Stage 3: Publish Canonical Fact
- Input: aggregated data
- Responsibilities:
    - Write authoritative fact table
    - Enforce data quality checks
