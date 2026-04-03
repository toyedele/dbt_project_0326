"""
-- =============================================================
-- SNOWFLAKE SETUP RUNNER
-- Reads snowflake_setup.sql and executes it statement-by-statement
-- against Snowflake using key-pair auth (same pattern as ingestion.py).
-- Run as: python run_setup.py
-- Expected env vars (same .env as ingestion):
--   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH,
--   SNOWFLAKE_ROLE (should be ACCOUNTADMIN for this script)
-- =============================================================
"""

import os
import re
import sys
import snowflake.connector
from dotenv import load_dotenv
from loguru import logger
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pathlib import Path

load_dotenv()

SQL_FILE = Path(__file__).parent / "snowflake_setup.sql"

logger.remove()
logger.add(
    sys.stdout,
    level="INFO",
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{message}</cyan>",
)
logger.add(
    "logs/setup_{time:YYYY-MM-DD}.log",
    level="DEBUG",
    rotation="00:00",
    retention="7 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
)


def load_private_key() -> bytes:
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
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


def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user="TESTTASKS",  # Use a dedicated user for setup tasks
        private_key=load_private_key(),
        # Role must be ACCOUNTADMIN to run this provisioning script
        role="ACCOUNTADMIN", # Use a fixed role for setup, not from env, to avoid mistakes
    )


def parse_statements(sql: str) -> list[str]:
    """
    Strip single-line comments and blank lines, then split on semicolons.
    Returns only non-empty statements.
    """
    # Remove single-line comments (-- ...) but keep newlines for readability
    sql = re.sub(r"--[^\n]*", "", sql)
    statements = [s.strip() for s in sql.split(";")]
    return [s for s in statements if s]


def run_setup(sql_path: Path) -> None:
    logger.info("=" * 60)
    logger.info("Snowflake Setup — file: {}", sql_path.name)
    logger.info("=" * 60)

    sql = sql_path.read_text(encoding="utf-8")
    statements = parse_statements(sql)
    logger.info("Parsed {} statements to execute", len(statements))

    try:
        conn = get_connection()
        cursor = conn.cursor()
        logger.success("Connected — account: {}", os.environ["SNOWFLAKE_ACCOUNT"])
    except Exception as e:
        logger.critical("Failed to connect to Snowflake: {}", e)
        raise SystemExit(1)

    passed = 0
    failed = 0

    try:
        for i, stmt in enumerate(statements, start=1):
            preview = stmt[:80].replace("\n", " ")
            logger.info("[{}/{}] {}", i, len(statements), preview)
            try:
                cursor.execute(stmt)
                result = cursor.fetchone()
                if result:
                    logger.debug("   Result: {}", result)
                passed += 1
            except Exception as e:
                logger.error("   FAILED: {}", e)
                logger.error("   Statement: {}", stmt[:200])
                failed += 1
                # Continue executing remaining statements so we get a full picture
    finally:
        cursor.close()
        conn.close()
        logger.info("Connection closed.")

    logger.info("=" * 60)
    logger.info(
        "Summary: {} executed | {} passed | {} failed",
        len(statements), passed, failed,
    )

    if failed:
        logger.error("Setup completed WITH ERRORS — review logs above")
        raise SystemExit(1)
    else:
        logger.success("Setup completed successfully ✓")


if not SQL_FILE.exists():
    logger.critical("SQL file not found: {}", SQL_FILE)
    raise SystemExit(1)

run_setup(SQL_FILE)