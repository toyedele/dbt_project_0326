# =============================================================
# roles.tf — Roles, grants, and service users
# =============================================================

# ── Roles ─────────────────────────────────────────────────────

resource "snowflake_account_role" "data_loader" {
  name    = "DATA_LOADER"
  comment = "Role for loading data into the Raw database"
}

resource "snowflake_account_role" "dbt_transformer" {
  name    = "DBT_TRANSFORMER"
  comment = "Role for running dbt transformations in the Analytics database"
}

resource "snowflake_account_role" "reporter" {
  name    = "REPORTER"
  comment = "Role for read-only access to the Analytics database for reporting and BI"
}

# ── Role hierarchy: SYSADMIN owns all custom roles ────────────

resource "snowflake_grant_account_role" "data_loader_to_sysadmin" {
  role_name        = snowflake_account_role.data_loader.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "dbt_transformer_to_sysadmin" {
  role_name        = snowflake_account_role.dbt_transformer.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "reporter_to_sysadmin" {
  role_name        = snowflake_account_role.reporter.name
  parent_role_name = "SYSADMIN"
}

# ── Warehouse grants ──────────────────────────────────────────

resource "snowflake_grant_privileges_to_account_role" "data_loader_ingestion_wh" {
  account_role_name = snowflake_account_role.data_loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.ingestion.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_transformer_transform_wh" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_reporting_wh" {
  account_role_name = snowflake_account_role.reporter.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.reporting.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_transformer_reporting_wh" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.reporting.name
  }
}

# ── DATA_LOADER grants on RAW ─────────────────────────────────

resource "snowflake_grant_privileges_to_account_role" "data_loader_raw_db" {
  account_role_name = snowflake_account_role.data_loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.raw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_loader_raw_schemas" {
  account_role_name = snowflake_account_role.data_loader.name
  all_privileges    = true
  on_schema {
    all_schemas_in_database = snowflake_database.raw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_loader_raw_future_schemas" {
  account_role_name = snowflake_account_role.data_loader.name
  all_privileges    = true
  on_schema {
    future_schemas_in_database = snowflake_database.raw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_loader_raw_tables" {
  account_role_name = snowflake_account_role.data_loader.name
  all_privileges    = true
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.raw.name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_loader_raw_future_tables" {
  account_role_name = snowflake_account_role.data_loader.name
  all_privileges    = true
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.raw.name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_loader_raw_stages" {
  account_role_name = snowflake_account_role.data_loader.name
  all_privileges    = true
  on_schema_object {
    all {
      object_type_plural = "STAGES"
      in_database        = snowflake_database.raw.name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "data_loader_raw_future_stages" {
  account_role_name = snowflake_account_role.data_loader.name
  all_privileges    = true
  on_schema_object {
    future {
      object_type_plural = "STAGES"
      in_database        = snowflake_database.raw.name
    }
  }
}

# ── DBT_TRANSFORMER grants on RAW (read-only) ─────────────────

resource "snowflake_grant_privileges_to_account_role" "dbt_raw_db" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.raw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_raw_schemas" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  all_privileges    = true
  on_schema {
    all_schemas_in_database = snowflake_database.raw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_raw_future_schemas" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  all_privileges    = true
  on_schema {
    future_schemas_in_database = snowflake_database.raw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_raw_tables" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.raw.name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_raw_future_tables" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.raw.name
    }
  }
}

# ── DBT_TRANSFORMER grants on ANALYTICS (full access) ─────────

resource "snowflake_grant_privileges_to_account_role" "dbt_analytics_db" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.analytics.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_analytics_schemas" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  all_privileges    = true
  on_schema {
    all_schemas_in_database = snowflake_database.analytics.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_analytics_future_schemas" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  all_privileges    = true
  on_schema {
    future_schemas_in_database = snowflake_database.analytics.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_analytics_tables" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  all_privileges    = true
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.analytics.name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_analytics_future_tables" {
  account_role_name = snowflake_account_role.dbt_transformer.name
  all_privileges    = true
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_database        = snowflake_database.analytics.name
    }
  }
}

# ── REPORTER grants on ANALYTICS (read-only) ──────────────────

resource "snowflake_grant_privileges_to_account_role" "reporter_analytics_db" {
  account_role_name = snowflake_account_role.reporter.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.analytics.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_analytics_schema" {
  account_role_name = snowflake_account_role.reporter.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.analytics.name}\".\"${snowflake_schema.analytics_analytics.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_analytics_tables" {
  account_role_name = snowflake_account_role.reporter.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.analytics.name}\".\"${snowflake_schema.analytics_analytics.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_analytics_future_tables" {
  account_role_name = snowflake_account_role.reporter.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.analytics.name}\".\"${snowflake_schema.analytics_analytics.name}\""
    }
  }
}

# ── Service users ─────────────────────────────────────────────

resource "snowflake_user" "loader" {
  name              = "LOADER"
  default_role      = snowflake_account_role.data_loader.name
  default_warehouse = snowflake_warehouse.ingestion.name
  comment           = "Service user for data ingestion"
}

resource "snowflake_user" "dbt" {
  name              = "DBT"
  default_role      = snowflake_account_role.dbt_transformer.name
  default_warehouse = snowflake_warehouse.transform.name
  comment           = "Service user for dbt transformations"
}

resource "snowflake_grant_account_role" "data_loader_to_loader_user" {
  role_name = snowflake_account_role.data_loader.name
  user_name = snowflake_user.loader.name
}

resource "snowflake_grant_account_role" "dbt_transformer_to_dbt_user" {
  role_name = snowflake_account_role.dbt_transformer.name
  user_name = snowflake_user.dbt.name
}
