# =============================================================
# variables.tf
# =============================================================

variable "snowflake_account" {
  description = "Snowflake account identifier (e.g. xy12345)"
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account identifier (e.g. xy12345)"
  type        = string
}

variable "snowflake_organization" {
  description = "Snowflake organization name"
  type        = string
}

variable "snowflake_setup_user" {
  description = "Snowflake user to authenticate as (must have ACCOUNTADMIN)"
  type        = string
}

variable "snowflake_setup_role" {
  description = "Snowflake role to use"
  type        = string
}

variable "snowflake_private_key_path" {
  description = "Path to the PEM private key file for key-pair auth"
  type        = string
}

