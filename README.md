Data Model

This platform is designed around explicit canonical data models, rather than ad-hoc transformations. Each dataset has a clearly defined grain and ownership semantics.

Stage 1 — Normalized Billing Events

Grain: event_id

Characteristics:

Immutable raw ingestion

Deduplicated using latest‐ingestion‐wins semantics

Late and corrected events are preserved for recomputation

This stage serves as the system of record for all downstream recomputation.

Stage 2 — Daily Cost Fact

Table: account_service_day_cost

Grain: (account_id, service_name, usage_date)

Metric: total_cost_usd

This fact table is optimized for analytical queries and downstream joins. Aggregating at the account-service-day level prevents event-level explosion and ensures predictable query performance.

Stage 3 — Canonical Published Dataset

Table: daily_account_cost

Grain: (account_id, usage_date)

Columns:

total_cost_usd

data_complete

published_at

This dataset represents the authoritative daily cost view for downstream consumers. All SLAs, backfill guarantees, and data quality semantics are enforced at this layer.

Logical Dimensions

While not materialized in this project, the platform is designed to join against the following logical dimensions:

dim_account — account metadata and billing ownership

dim_service — service identifiers (EC2, S3, etc.)

dim_region — geographic cost attribution

dim_pricing_model — on-demand, reserved, spot

This separation allows facts to remain stable while dimensions evolve independently.

Why This Model

Account-day is the canonical financial grain required by finance and forecasting

Service-day is retained as an analytical fact for drill-downs

Explicit grains prevent double counting and ambiguous aggregations

Downstream SQL becomes simple, predictable, and performant