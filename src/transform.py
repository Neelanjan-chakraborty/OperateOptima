"""
Transform module for Operate Optima ETL Pipeline
Handles data cleaning, transformation, and feature engineering using PySpark
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, 
    lower, trim, round as spark_round, to_date,
    sum as spark_sum, count as spark_count
)
from pyspark.sql.types import DoubleType, IntegerType
import logging
from typing import Tuple
from src.config import COLUMN_MAPPINGS, MIN_UNITS_THRESHOLD, MAX_UNIT_COST, MIN_TOTAL_AMOUNT

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles all data transformation operations"""
    
    def __init__(self):
        self.transformation_stats = {}
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Main transformation pipeline
        
        Args:
            df: Input Spark DataFrame
            
        Returns:
            Transformed Spark DataFrame
        """
        logger.info("🔹 Starting data transformation pipeline...")
        
        # Store original count for comparison
        original_count = df.count()
        self.transformation_stats['original_rows'] = original_count
        
        # Apply transformation steps
        df = self._rename_columns(df)
        df = self._clean_data_types(df)
        df = self._handle_null_values(df)
        df = self._apply_business_rules(df)
        df = self._create_derived_features(df)
        df = self._apply_data_quality_filters(df)
        
        # Store final count
        final_count = df.count()
        self.transformation_stats['final_rows'] = final_count
        self.transformation_stats['rows_removed'] = original_count - final_count
        self.transformation_stats['removal_percentage'] = (
            (original_count - final_count) / original_count * 100 
            if original_count > 0 else 0
        )
        
        logger.info(f"✅ Transformation completed: {original_count:,} → {final_count:,} rows")
        logger.info(f"📉 Removed {original_count - final_count:,} rows ({self.transformation_stats['removal_percentage']:.2f}%)")
        
        return df
    
    def _rename_columns(self, df: DataFrame) -> DataFrame:
        """Convert column names to snake_case"""
        logger.info("🔄 Renaming columns to snake_case...")
        
        for old_name, new_name in COLUMN_MAPPINGS.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        
        logger.info(f"📝 Renamed columns: {list(COLUMN_MAPPINGS.keys())}")
        return df
    
    def _clean_data_types(self, df: DataFrame) -> DataFrame:
        """Clean and convert data types"""
        logger.info("🧹 Cleaning data types...")
        
        # Convert unit_cost to double, handling any string formatting
        if 'unit_cost' in df.columns:
            df = df.withColumn('unit_cost', 
                regexp_replace(col('unit_cost'), '[$,]', '').cast(DoubleType()))
        
        # Convert total to double, handling any string formatting  
        if 'total' in df.columns:
            df = df.withColumn('total',
                regexp_replace(col('total'), '[$,]', '').cast(DoubleType()))
        
        # Ensure units is integer
        if 'units' in df.columns:
            df = df.withColumn('units', col('units').cast(IntegerType()))
        
        # Clean and standardize text fields
        text_columns = ['region', 'sales_rep', 'item']
        for column in text_columns:
            if column in df.columns:
                df = df.withColumn(column, trim(col(column)))
        
        # Convert order_date to proper date format
        if 'order_date' in df.columns:
            df = df.withColumn('order_date', to_date(col('order_date'), 'M/d/yy'))
        
        logger.info("✅ Data types cleaned and standardized")
        return df
    
    def _handle_null_values(self, df: DataFrame) -> DataFrame:
        """Handle null and missing values"""
        logger.info("🔍 Handling null values...")
        
        # Count nulls before cleaning
        null_counts_before = {}
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts_before[column] = null_count
        
        # Remove rows where critical fields are null
        critical_fields = ['units', 'unit_cost', 'total']
        for field in critical_fields:
            if field in df.columns:
                df = df.filter(col(field).isNotNull() & ~isnan(col(field)))
        
        # Fill missing text values with 'Unknown'
        text_fields = ['region', 'sales_rep', 'item']
        for field in text_fields:
            if field in df.columns:
                df = df.fillna('Unknown', subset=[field])
        
        logger.info(f"🧽 Cleaned null values in critical fields: {critical_fields}")
        return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business logic and validation rules"""
        logger.info("📋 Applying business rules...")
        
        # Ensure units and amounts are positive
        if 'units' in df.columns:
            df = df.filter(col('units') > 0)
        
        if 'unit_cost' in df.columns:
            df = df.filter(col('unit_cost') > 0)
        
        if 'total' in df.columns:
            df = df.filter(col('total') > 0)
        
        # Apply reasonable upper bounds
        if 'unit_cost' in df.columns:
            df = df.filter(col('unit_cost') <= MAX_UNIT_COST)
        
        logger.info("✅ Business rules applied")
        return df
    
    def _create_derived_features(self, df: DataFrame) -> DataFrame:
        """Create new calculated columns"""
        logger.info("⚡ Creating derived features...")
        
        # Calculate profit margin
        if all(col_name in df.columns for col_name in ['total', 'units', 'unit_cost']):
            df = df.withColumn('profit_margin', 
                col('total') - (col('units') * col('unit_cost')))
            
            # Calculate profit margin percentage
            df = df.withColumn('profit_margin_pct',
                spark_round((col('profit_margin') / col('total')) * 100, 2))
        
        # Calculate revenue per unit
        if all(col_name in df.columns for col_name in ['total', 'units']):
            df = df.withColumn('revenue_per_unit',
                spark_round(col('total') / col('units'), 2))
        
        # Add business categorization
        if 'total' in df.columns:
            df = df.withColumn('order_size_category',
                when(col('total') < 100, 'Small')
                .when(col('total') < 500, 'Medium')
                .when(col('total') < 1000, 'Large')
                .otherwise('Enterprise'))
        
        logger.info("✨ Created derived features: profit_margin, profit_margin_pct, revenue_per_unit, order_size_category")
        return df
    
    def _apply_data_quality_filters(self, df: DataFrame) -> DataFrame:
        """Apply final data quality filters"""
        logger.info("🎯 Applying data quality filters...")
        
        # Filter based on minimum thresholds
        if 'units' in df.columns:
            df = df.filter(col('units') >= MIN_UNITS_THRESHOLD)
        
        if 'total' in df.columns:
            df = df.filter(col('total') >= MIN_TOTAL_AMOUNT)
        
        # Remove extreme outliers (optional - could be made configurable)
        if 'profit_margin_pct' in df.columns:
            df = df.filter((col('profit_margin_pct') >= -100) & (col('profit_margin_pct') <= 500))
        
        logger.info("🎯 Data quality filters applied")
        return df
    
    def get_transformation_summary(self, df: DataFrame) -> dict:
        """Generate transformation summary statistics"""
        logger.info("📊 Generating transformation summary...")
        
        summary = self.transformation_stats.copy()
        
        # Add column statistics
        summary['final_columns'] = len(df.columns)
        summary['column_list'] = df.columns
        
        # Add basic statistics for numerical columns
        numerical_stats = {}
        for column in df.columns:
            if df.schema[column].dataType in [DoubleType(), IntegerType()]:
                stats = df.select(column).describe().collect()
                numerical_stats[column] = {row['summary']: row[column] for row in stats}
        
        summary['numerical_statistics'] = numerical_stats
        
        return summary


def transform_data(df: DataFrame) -> Tuple[DataFrame, dict]:
    """Main transformation function - can be called independently"""
    transformer = DataTransformer()
    transformed_df = transformer.transform_data(df)
    summary = transformer.get_transformation_summary(transformed_df)
    return transformed_df, summary


if __name__ == "__main__":
    # This would require an input DataFrame for testing
    logger.info("Transform module loaded. Use transform_data(df) function.")
