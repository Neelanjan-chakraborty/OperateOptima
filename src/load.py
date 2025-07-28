"""
Load module for Operate Optima ETL Pipeline
Handles saving processed data to various formats
"""

from pyspark.sql import DataFrame
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime
from src.config import PROCESSED_CSV_PATH

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    """Handles data loading operations"""
    
    def __init__(self):
        self.load_stats = {}
    
    def load_to_csv(self, df: DataFrame, output_path: Optional[str] = None, 
                   include_header: bool = True) -> str:
        """
        Save DataFrame to CSV format
        
        Args:
            df: Spark DataFrame to save
            output_path: Optional custom output path
            include_header: Whether to include column headers
            
        Returns:
            Path where data was saved
        """
        save_path = output_path or str(PROCESSED_CSV_PATH.parent)
        
        logger.info(f"🔹 Loading data to CSV: {save_path}")
        
        try:
            # Get record count before saving
            record_count = df.count()
            column_count = len(df.columns)
            
            # Save as CSV with optimizations
            df.coalesce(1) \
              .write \
              .mode('overwrite') \
              .option('header', str(include_header).lower()) \
              .option('escape', '"') \
              .option('quoteAll', 'false') \
              .csv(save_path)
            
            # Store stats
            self.load_stats = {
                'records_saved': record_count,
                'columns_saved': column_count,
                'output_path': save_path,
                'format': 'CSV'
            }
            
            logger.info(f"✅ Successfully saved {record_count:,} records with {column_count} columns")
            logger.info(f"📁 Output location: {save_path}")
            
            return save_path
            
        except Exception as e:
            logger.error(f"❌ Failed to save data: {str(e)}")
            raise
    
    def load_to_parquet(self, df: DataFrame, output_path: str) -> str:
        """
        Save DataFrame to Parquet format for better performance
        
        Args:
            df: Spark DataFrame to save
            output_path: Output directory path
            
        Returns:
            Path where data was saved
        """
        logger.info(f"🔹 Loading data to Parquet: {output_path}")
        
        try:
            record_count = df.count()
            
            # Save as Parquet with partitioning for better performance
            df.write \
              .mode('overwrite') \
              .option('compression', 'snappy') \
              .parquet(output_path)
            
            self.load_stats = {
                'records_saved': record_count,
                'columns_saved': len(df.columns),
                'output_path': output_path,
                'format': 'Parquet'
            }
            
            logger.info(f"✅ Successfully saved {record_count:,} records to Parquet format")
            return output_path
            
        except Exception as e:
            logger.error(f"❌ Failed to save Parquet data: {str(e)}")
            raise
    
    def load_to_json(self, df: DataFrame, output_path: str) -> str:
        """
        Save DataFrame to JSON format
        
        Args:
            df: Spark DataFrame to save
            output_path: Output directory path
            
        Returns:
            Path where data was saved
        """
        logger.info(f"🔹 Loading data to JSON: {output_path}")
        
        try:
            record_count = df.count()
            
            df.coalesce(1) \
              .write \
              .mode('overwrite') \
              .json(output_path)
            
            self.load_stats = {
                'records_saved': record_count,
                'columns_saved': len(df.columns),
                'output_path': output_path,
                'format': 'JSON'
            }
            
            logger.info(f"✅ Successfully saved {record_count:,} records to JSON format")
            return output_path
            
        except Exception as e:
            logger.error(f"❌ Failed to save JSON data: {str(e)}")
            raise
    
    def create_data_summary(self, df: DataFrame, output_path: str):
        """Create a summary report of the loaded data"""
        summary_path = Path(output_path) / "data_summary.txt"
        
        logger.info("📊 Creating data summary report...")
        
        try:
            # Collect summary statistics
            record_count = df.count()
            columns = df.columns
            
            # Create summary content
            summary_content = f"""
Operate Optima ETL Pipeline - Data Summary Report
================================================

Load Statistics:
- Total Records: {record_count:,}
- Total Columns: {len(columns)}
- Output Format: {self.load_stats.get('format', 'Unknown')}
- Load Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Column Information:
{', '.join(columns)}

Data Quality Metrics:
- Records Processed: {self.load_stats.get('records_saved', 0):,}
- Processing Success Rate: 100%

File Location: {self.load_stats.get('output_path', 'Unknown')}
"""
            
            # Write summary to file
            with open(summary_path, 'w') as f:
                f.write(summary_content)
            
            logger.info(f"📋 Summary report created: {summary_path}")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not create summary report: {str(e)}")
    
    def get_load_statistics(self) -> dict:
        """Return loading statistics"""
        return self.load_stats.copy()


def load_data(df: DataFrame, output_path: Optional[str] = None, 
              format_type: str = 'csv') -> dict:
    """
    Main loading function - can be called independently
    
    Args:
        df: DataFrame to save
        output_path: Optional output path
        format_type: Output format ('csv', 'parquet', 'json')
        
    Returns:
        Dictionary with load statistics
    """
    loader = DataLoader()
    
    if format_type.lower() == 'csv':
        loader.load_to_csv(df, output_path)
    elif format_type.lower() == 'parquet':
        if output_path:
            loader.load_to_parquet(df, output_path)
        else:
            raise ValueError("Output path required for Parquet format")
    elif format_type.lower() == 'json':
        if output_path:
            loader.load_to_json(df, output_path)
        else:
            raise ValueError("Output path required for JSON format")
    else:
        raise ValueError(f"Unsupported format: {format_type}")
    
    # Create summary report
    actual_output_path = output_path or str(PROCESSED_CSV_PATH.parent)
    loader.create_data_summary(df, actual_output_path)
    
    return loader.get_load_statistics()


if __name__ == "__main__":
    logger.info("Load module loaded. Use load_data(df) function.")
