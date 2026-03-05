"""
-- =============================================================
-- FILE CONFIGURATION FOR INGESTION
-- Defines the mapping of CSV files to Snowflake raw tables.
-- =============================================================
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'

RAW_DB = 'RAW'
# DATA_DIR = './dbt_project_0326/data'
FILE_CONFIG: dict[str, dict] = {
    'customer_data': {
        'file_path': f'{DATA_DIR}/customers.csv',
        'table': 'CUSTOMERS',
        'schema': 'USER',
        'stage': f'{RAW_DB}.USER.USER_STAGE',
    },
    'rates_data': {
        'file_path': f'{DATA_DIR}/fx_rates.csv',
        'table': 'FX_RATES',
        'schema': 'FINANCE',
        'stage': f'{RAW_DB}.FINANCE.FINANCE_STAGE',
    },
    'events_data': {
        'file_path': f'{DATA_DIR}/marketing_clicks.csv',
        'table': 'MARKETING_CLICKS',
        'schema': 'EVENTS',
        'stage': f'{RAW_DB}.EVENTS.EVENTS_STAGE',
    },
    'order_items_data': {
        'file_path': f'{DATA_DIR}/order_items.csv',
        'table': 'ORDER_ITEMS',
        'schema': 'PRODUCT',
        'stage': f'{RAW_DB}.PRODUCT.PRODUCT_STAGE',
    },
    'orders_data': {
        'file_path': f'{DATA_DIR}/orders.csv',
        'table': 'ORDERS',
        'schema': 'TRANSACTIONS',
        'stage': f'{RAW_DB}.TRANSACTIONS.TRANSACTIONS_STAGE',
    },
    'payments_data': {
        'file_path': f'{DATA_DIR}/payments.csv',
        'table': 'PAYMENTS',
        'schema': 'TRANSACTIONS',
        'stage': f'{RAW_DB}.TRANSACTIONS.TRANSACTIONS_STAGE',
    },
    'refunds_data': {
        'file_path': f'{DATA_DIR}/refunds.csv',
        'table': 'REFUNDS',
        'schema': 'TRANSACTIONS',
        'stage': f'{RAW_DB}.TRANSACTIONS.TRANSACTIONS_STAGE',
    },
    # Add more file configurations as needed
}
