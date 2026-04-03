# =============================================================
# warehouses.tf — INGESTION, TRANSFORM, REPORTING warehouses
# =============================================================

resource "snowflake_warehouse" "ingestion" {
  name                = "INGESTION_WH"
  warehouse_size      = "X-SMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  comment             = "Used by ingestion scripts to load data"
}

resource "snowflake_warehouse" "transform" {
  name                = "TRANSFORM_WH"
  warehouse_size      = "SMALL"
  auto_suspend        = 120
  auto_resume         = true
  initially_suspended = true
  comment             = "Used by dbt for data transformations"
}

resource "snowflake_warehouse" "reporting" {
  name                = "REPORTING_WH"
  warehouse_size      = "X-SMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  comment             = "Used by reader accounts and BI tools"
}
