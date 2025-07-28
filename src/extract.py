"""
Extract module for Operate Optima ETL Pipeline
Handles data extraction from CSV files using PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import logging
from typing import Optional
from src.config import RAW_CSV_PATH, SPARK_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction operations using PySpark"""
    
    def __init__(self):
        """Initialize Spark session with optimized configuration"""
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session for optimal performance"""
        logger.info("🚀 Initializing Spark Session...")
        
        spark = SparkSession.builder
        
        # Apply all configuration settings
        for key, value in SPARK_CONFIG.items():
            spark = spark.config(key, value)
            
        return spark.getOrCreate()
    
    def extract_csv_data(self, file_path: Optional[str] = None):
        """
        Extract data from CSV file with automatic schema inference
        
        Args:
            file_path: Path to CSV file. If None, uses default from config
            
        Returns:
            Spark DataFrame with extracted data
        """
        csv_path = file_path or str(RAW_CSV_PATH)
        
        logger.info(f"🔹 Extracting data from: {csv_path}")
        
        try:
            # Read CSV with header inference and automatic schema detection
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("multiline", "true") \
                .option("escape", '"') \
                .csv(csv_path)
            
            # Log extraction results
            row_count = df.count()
            column_count = len(df.columns)
            
            logger.info(f"✅ Successfully extracted {row_count:,} rows with {column_count} columns")
            logger.info(f"📊 Columns: {', '.join(df.columns)}")
            
            # Show sample data
            logger.info("📋 Sample data preview:")
            df.show(5, truncate=False)
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to extract data: {str(e)}")
            raise
    
    def get_data_schema(self, df):
        """Print detailed schema information"""
        logger.info("📝 Data Schema:")
        df.printSchema()
        
        # Show data types and null counts
        logger.info("🔍 Column Statistics:")
        for column in df.columns:
            null_count = df.filter(df[column].isNull()).count()
            total_count = df.count()
            null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
            
            logger.info(f"  {column}: {null_count:,} nulls ({null_percentage:.2f}%)")
    
    def close_session(self):
        """Close Spark session and release resources"""
        logger.info("🔒 Closing Spark session...")
        self.spark.stop()


def extract_data():
    """Main extraction function - can be called independently"""
    extractor = DataExtractor()
    try:
        df = extractor.extract_csv_data()
        extractor.get_data_schema(df)
        return df, extractor
    except Exception as e:
        extractor.close_session()
        raise e


if __name__ == "__main__":
    # Test extraction independently
    df, extractor = extract_data()
    extractor.close_session()
