"""
Configuration file for Operate Optima ETL Pipeline
Contains all file paths and Spark configuration settings
"""

import os
from pathlib import Path

# Base project directory
BASE_DIR = Path(__file__).parent.parent

# Data directories
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"

# File paths
RAW_CSV_PATH = RAW_DATA_DIR / "SampleSalesData.csv"
PROCESSED_CSV_PATH = PROCESSED_DATA_DIR / "cleaned_sales_data.csv"

# Spark Configuration
SPARK_CONFIG = {
    "spark.app.name": "OperateOptima-ETL",
    "spark.master": "local[*]",  # Use all available cores
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.execution.arrow.pyspark.enabled": "true"
}

# Data quality thresholds
MIN_UNITS_THRESHOLD = 1
MAX_UNIT_COST = 1000.0
MIN_TOTAL_AMOUNT = 0.01

# Column mappings for snake_case conversion
COLUMN_MAPPINGS = {
    "OrderDate": "order_date",
    "Region": "region", 
    "Rep": "sales_rep",
    "Item": "item",
    "Units": "units",
    "Unit Cost": "unit_cost",
    "Total": "total"
}

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
