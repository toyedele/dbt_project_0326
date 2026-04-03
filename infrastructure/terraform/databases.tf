# =============================================================
# databases.tf — RAW and ANALYTICS databases + all schemas
# =============================================================

resource "snowflake_database" "raw" {
  name    = "RAW"
  comment = "Raw ingested data from sources"
}

resource "snowflake_database" "analytics" {
  name    = "ANALYTICS"
  comment = "Databases for staging, transformation, and marts following a medallion architecture"
}

# ── RAW schemas ───────────────────────────────────────────────

resource "snowflake_schema" "raw_events" {
  database = snowflake_database.raw.name
  name     = "EVENTS"
  comment  = "Events related data such as marketing clicks"
}

resource "snowflake_schema" "raw_user" {
  database = snowflake_database.raw.name
  name     = "USER"
  comment  = "Customer related data such as demographics and marketing subscription information"
}

resource "snowflake_schema" "raw_transactions" {
  database = snowflake_database.raw.name
  name     = "TRANSACTIONS"
  comment  = "Transactions related data such as orders, payments and refunds"
}

resource "snowflake_schema" "raw_product" {
  database = snowflake_database.raw.name
  name     = "PRODUCT"
  comment  = "Product related data such as order items and product information"
}

resource "snowflake_schema" "raw_finance" {
  database = snowflake_database.raw.name
  name     = "FINANCE"
  comment  = "Finance related data such as FX Rates"
}

# ── ANALYTICS schemas ─────────────────────────────────────────

resource "snowflake_schema" "analytics_analytics" {
  database = snowflake_database.analytics.name
  name     = "ANALYTICS"
  comment  = "For Marts dataset used for reporting and BI"
}
