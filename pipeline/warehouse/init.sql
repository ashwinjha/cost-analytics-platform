CREATE SCHEMA IF NOT EXISTS finance;

CREATE TABLE IF NOT EXISTS finance.daily_account_cost (
    account_id TEXT,
    usage_date DATE,
    total_cost_usd DOUBLE PRECISION
);

