"""
-- =============================================================
-- SNOWFLAKE INGESTION SCRIPT
-- Reads the configuration from file_config.py and performs the following steps:
    1. PUT file to internal Snowflake stage
    2. CREATE TABLE IF NOT EXISTS (all VARCHAR + metadata columns)
    3. COPY INTO table from stage
-- =============================================================
"""

import os
import csv
import sys
import time
import snowflake.connector
from dotenv import load_dotenv
from loguru import logger
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pathlib import Path
from file_config import FILE_CONFIG, RAW_DB

load_dotenv()

logger.add(
    sys.stdout,
    level="INFO",
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{message}</cyan>",
)

logger.add(
    "logs/ingestion_{time:YYYY-MM-DD}.log",
    level="DEBUG",
    rotation="00:00",       # new file every day at midnight
    retention="7 days",     # keep logs for 7 days
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)

def load_private_key():
    """
    Reads the private key file and returns the DER-encoded bytes
    that snowflake.connector expects.
    """
    key_path   = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]

    with open(key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
            backend=default_backend(),
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=load_private_key(),
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=RAW_DB,
    )

def get_csv_columns(file_path: str) -> list[str]:
    with open(file_path, newline="", encoding="utf-8") as f:
        return next(csv.reader(f))

def count_csv_rows(file_path: str) -> int:
    with open(file_path, encoding="utf-8") as f:
        return sum(1 for _ in f) - 1  

def create_table_sql(schema: str, table: str, columns: list[str]) -> str:
    col_defs = ",\n    ".join(f'"{col.upper()}" VARCHAR' for col in columns)
    return f"""
        create table if not exists {RAW_DB}.{schema}.{table} (
            {col_defs},
            _source_file  varchar,
            _loaded_at    timestamp,
            _row_hash     varchar
        )
    """

def copy_into_sql(schema: str, table: str, stage: str, filename: str, columns: list[str]) -> str:
    col_refs           = ", ".join(f"${i + 1}" for i in range(len(columns)))
    col_refs_coalesced = ", ".join(f"coalesce(${i + 1}, '')" for i in range(len(columns)))
    col_names          = ", ".join(f'"{col.upper()}"' for col in columns)

    return f"""
        copy into {RAW_DB}.{schema}.{table} (
            {col_names},
            _source_file,
            _loaded_at,
            _row_hash
        )
        from (
            select
                {col_refs},
                metadata$filename,
                current_timestamp(),
                md5(concat_ws('||', {col_refs_coalesced}))
            from @{stage}/{filename}
        )
        file_format = (type = csv, skip_header = 1, empty_field_as_null = true, null_if = ('', 'null', 'none'))
        on_error = abort_statement
        force    = false
    """

def validate_row_counts(cursor, schema: str, table: str, csv_rows: int) -> dict:
    cursor.execute(f"select count(*) from {RAW_DB}.{schema}.{table}")
    table_rows = cursor.fetchone()[0]
    passed     = csv_rows == table_rows

    if passed:
        logger.success("   Validation passed — CSV: {} | Snowflake: {}", csv_rows, table_rows)
    else:
        logger.error(
            "   Validation FAILED — CSV: {} | Snowflake: {} | Delta: {}",
            csv_rows, table_rows, abs(csv_rows - table_rows),
        )

    return {"csv_rows": csv_rows, "table_rows": table_rows, "passed": passed}

def ingest_file(cursor, name: str, config: dict) -> dict:
    file_path = config["file_path"]
    schema    = config["schema"]
    table     = config["table"]
    stage     = config["stage"]
    filename  = Path(file_path).name
    start     = time.time()

    logger.info("── Starting: {}", name)

    # Read CSV headers + count source rows
    columns  = get_csv_columns(file_path)
    csv_rows = count_csv_rows(file_path)
    logger.debug("   Columns ({}): {}", len(columns), columns)
    logger.info("   Source rows: {}", csv_rows)

    # PUT file to internal stage
    abs_path = Path(file_path).resolve().as_posix()
    logger.debug("   Staging file: {}", abs_path)
    cursor.execute(f"put file://{abs_path} @{stage} auto_compress=true overwrite=false")
    put_status = cursor.fetchone()[6]
    logger.info("   PUT status: {}", put_status)

    # Create table if not exists
    cursor.execute(create_table_sql(schema, table, columns))
    logger.info("   Table: {}.{}.{}", RAW_DB, schema, table)

    # Copy into table from stage
    cursor.execute(copy_into_sql(schema, table, stage, filename + ".gz", columns))
    logger.info("   COPY INTO complete")

    # Validation checks
    validation = validate_row_counts(cursor, schema, table, csv_rows)

    elapsed = round(time.time() - start, 2)
    logger.info("   Completed in {}s", elapsed)

    return {"name": name, "elapsed": elapsed, **validation}

def log_summary(results: list[dict]) -> bool:
    passed  = [r for r in results if r["passed"]]
    failed  = [r for r in results if not r["passed"]]
    errored = [r for r in results if r.get("error")]

    logger.info("")
    # For formatting nicely
    logger.info("  {:<22}  {:>10}  {:>10}  {:>8}  {}", "FILE", "CSV ROWS", "SF ROWS", "TIME(s)", "STATUS")
    logger.info("  {}", "─" * 66)

    for r in results:
        if r.get("error"):
            logger.error("  {:<22}  {:>10}  {:>10}  {:>8}  ✗ ERROR: {}", r["name"], "-", "-", "-", r["error"])
        else:
            status = "✓ PASS" if r["passed"] else "✗ FAIL"
            logger.info("  {:<22}  {:>10}  {:>10}  {:>8}  {}", r["name"], r["csv_rows"], r["table_rows"], r["elapsed"], status)

    logger.info("  {}", "─" * 66)
    logger.info("  Files:   {} total | {} passed | {} failed | {} errored",
                len(results), len(passed), len(failed), len(errored))

    all_passed = len(failed) == 0 and len(errored) == 0

    if all_passed:
        logger.success("  Overall: ✓ ALL PASSED")
    else:
        logger.error("  Overall: ✗ FAILURES DETECTED")
        if failed:
            logger.error("  Failed files: {}", [r["name"] for r in failed])
        if errored:
            logger.error("  Errored files: {}", [r["name"] for r in errored])

    return all_passed

def main():
    logger.info("=" * 60)
    logger.info("Snowflake Ingestion — database: {}", RAW_DB)
    logger.info("Files to load: {}", list(FILE_CONFIG.keys()))
    logger.info("=" * 60)

    try:
        logger.info("Connecting to Snowflake...")
        conn   = get_connection()
        cursor = conn.cursor()
        logger.success("Connected — account: {}", os.environ["SNOWFLAKE_ACCOUNT"])
    except Exception as e:
        logger.critical("Failed to connect to Snowflake: {}", e)
        raise SystemExit(1)

    results = []
    try:
        for name, config in FILE_CONFIG.items():
            try:
                result = ingest_file(cursor, name, config)
            except Exception as e:
                logger.error("Failed to ingest '{}': {}", name, e)
                results.append({"name": name, "error": str(e), "passed": False})
            else:
                results.append(result)
    finally:
        cursor.close()
        conn.close()
        logger.info("Connection closed.")

    if not log_summary(results):
        raise SystemExit(1)

if __name__ == "__main__":
    main()