# =============================================================
# stages.tf — Internal CSV stages per schema
# =============================================================

locals {
  csv_file_format = "TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1"
}

resource "snowflake_stage" "events" {
  database    = snowflake_database.raw.name
  schema      = snowflake_schema.raw_events.name
  name        = "EVENTS_STAGE"
  file_format = local.csv_file_format
  comment     = "Internal stage for ingesting events data"
}

resource "snowflake_stage" "user" {
  database    = snowflake_database.raw.name
  schema      = snowflake_schema.raw_user.name
  name        = "USER_STAGE"
  file_format = local.csv_file_format
  comment     = "Internal stage for ingesting user data"
}

resource "snowflake_stage" "transactions" {
  database    = snowflake_database.raw.name
  schema      = snowflake_schema.raw_transactions.name
  name        = "TRANSACTIONS_STAGE"
  file_format = local.csv_file_format
  comment     = "Internal stage for ingesting transactions data"
}

resource "snowflake_stage" "product" {
  database    = snowflake_database.raw.name
  schema      = snowflake_schema.raw_product.name
  name        = "PRODUCT_STAGE"
  file_format = local.csv_file_format
  comment     = "Internal stage for ingesting product data"
}

resource "snowflake_stage" "finance" {
  database    = snowflake_database.raw.name
  schema      = snowflake_schema.raw_finance.name
  name        = "FINANCE_STAGE"
  file_format = local.csv_file_format
  comment     = "Internal stage for ingesting finance data"
}
