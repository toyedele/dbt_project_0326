# =============================================================
# main.tf — Snowflake provisioning (medallion architecture)
# Pipeline: CSV → Bronze → Silver → Gold → Reader Account
# =============================================================

terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "2.14.1"
    }
  }
#  required_version = ">= 1.3"
}

provider "snowflake" {
  account_name      = var.snowflake_account
#  account           = var.snowflake_account_name
  organization_name = var.snowflake_organization
  user              = var.snowflake_setup_user
  role              = var.snowflake_setup_role
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(var.snowflake_private_key_path)
}

