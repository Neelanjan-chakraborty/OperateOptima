"""
Operate Optima - Main ETL Pipeline
High-Performance Apache Spark ETL Pipeline for Business Data Processing

This pipeline demonstrates enterprise-level data engineering practices:
- Efficient data extraction with schema inference
- Advanced data transformation and feature engineering  
- Performance benchmarking and monitoring
- Scalable architecture design
"""

import time
import logging
from pathlib import Path
import sys
from typing import Dict, Any, Optional

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / 'src'))

from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from src.config import RAW_CSV_PATH, PROCESSED_CSV_PATH

# Configure logging with professional formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


class OperateOptimaETL:
    """
    Main ETL Pipeline Class
    Orchestrates the complete Extract-Transform-Load process
    """
    
    def __init__(self):
        self.extractor = None
        self.transformer = None
        self.loader = None
        self.pipeline_stats = {}
        self.start_time = None
        
    def run_pipeline(self, input_path: Optional[str] = None, output_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline with performance monitoring
        
        Args:
            input_path: Optional custom input file path
            output_path: Optional custom output directory path
            
        Returns:
            Dictionary containing pipeline execution statistics
        """
        
        logger.info("=" * 80)
        logger.info("🚀 OPERATE OPTIMA ETL PIPELINE STARTING")
        logger.info("=" * 80)
        
        self.start_time = time.time()
        
        try:
            # Phase 1: Extract
            logger.info("\n📥 PHASE 1: DATA EXTRACTION")
            logger.info("-" * 40)
            
            extract_start = time.time()
            raw_df, self.extractor = self._extract_data(input_path)
            extract_time = time.time() - extract_start
            
            logger.info(f"⏱️ Extraction completed in {extract_time:.2f} seconds")
            
            # Phase 2: Transform
            logger.info("\n🔄 PHASE 2: DATA TRANSFORMATION") 
            logger.info("-" * 40)
            
            transform_start = time.time()
            clean_df, transform_stats = self._transform_data(raw_df)
            transform_time = time.time() - transform_start
            
            logger.info(f"⏱️ Transformation completed in {transform_time:.2f} seconds")
            
            # Phase 3: Load
            logger.info("\n💾 PHASE 3: DATA LOADING")
            logger.info("-" * 40)
            
            load_start = time.time()
            load_stats = self._load_data(clean_df, output_path)
            load_time = time.time() - load_start
            
            logger.info(f"⏱️ Loading completed in {load_time:.2f} seconds")
            
            # Generate final summary
            total_time = time.time() - self.start_time
            pipeline_summary = self._generate_pipeline_summary(
                extract_time, transform_time, load_time, total_time,
                transform_stats, load_stats
            )
            
            self._display_success_summary(pipeline_summary)
            
            return pipeline_summary
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            raise
        finally:
            self._cleanup_resources()
    
    def _extract_data(self, input_path: Optional[str] = None):
        """Extract data from source"""
        self.extractor = DataExtractor()
        
        # Use custom path if provided, otherwise use default
        file_path = input_path or str(RAW_CSV_PATH)
        
        # Verify file exists
        if not Path(file_path).exists():
            logger.warning(f"⚠️ Input file not found: {file_path}")
            logger.info("📋 Please ensure the CSV file is downloaded from Kaggle and placed in data/raw/")
            raise FileNotFoundError(f"Input file not found: {file_path}")
        
        df = self.extractor.extract_csv_data(file_path)
        self.extractor.get_data_schema(df)
        
        return df, self.extractor
    
    def _transform_data(self, df):
        """Transform and clean the data"""
        self.transformer = DataTransformer()
        
        transformed_df = self.transformer.transform_data(df)
        transform_stats = self.transformer.get_transformation_summary(transformed_df)
        
        # Show sample of transformed data
        logger.info("📋 Sample of transformed data:")
        transformed_df.show(5, truncate=False)
        
        return transformed_df, transform_stats
    
    def _load_data(self, df, output_path: Optional[str] = None):
        """Load data to output destination"""
        self.loader = DataLoader()
        
        # Use custom path if provided, otherwise use default
        save_path = output_path or str(PROCESSED_CSV_PATH.parent)
        
        load_stats = self.loader.load_to_csv(df, save_path)
        self.loader.create_data_summary(df, save_path)
        
        return self.loader.get_load_statistics()
    
    def _generate_pipeline_summary(self, extract_time: float, transform_time: float, 
                                 load_time: float, total_time: float,
                                 transform_stats: dict, load_stats: dict) -> dict:
        """Generate comprehensive pipeline summary"""
        
        return {
            'pipeline_name': 'Operate Optima ETL',
            'execution_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'performance_metrics': {
                'total_execution_time': round(total_time, 2),
                'extraction_time': round(extract_time, 2),
                'transformation_time': round(transform_time, 2),
                'loading_time': round(load_time, 2),
                'records_per_second': round(load_stats.get('records_saved', 0) / total_time, 2)
            },
            'data_metrics': {
                'input_records': transform_stats.get('original_rows', 0),
                'output_records': load_stats.get('records_saved', 0),
                'records_removed': transform_stats.get('rows_removed', 0),
                'data_quality_improvement': f"{transform_stats.get('removal_percentage', 0):.2f}%",
                'columns_processed': load_stats.get('columns_saved', 0)
            },
            'output_location': load_stats.get('output_path', 'Unknown'),
            'status': 'SUCCESS'
        }
    
    def _display_success_summary(self, summary: dict):
        """Display formatted success summary"""
        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        
        perf = summary['performance_metrics']
        data = summary['data_metrics']
        
        logger.info(f"⏱️  Total Execution Time: {perf['total_execution_time']:.2f} seconds")
        logger.info(f"📊 Processing Rate: {perf['records_per_second']:,.0f} records/second")
        logger.info(f"📈 Data Quality: {data['input_records']:,} → {data['output_records']:,} rows")
        logger.info(f"🎯 Quality Improvement: {data['data_quality_improvement']} data cleaned")
        logger.info(f"📁 Output Location: {summary['output_location']}")
        
        logger.info("\n🔥 PERFORMANCE BREAKDOWN:")
        logger.info(f"   Extract: {perf['extraction_time']:.2f}s | Transform: {perf['transformation_time']:.2f}s | Load: {perf['loading_time']:.2f}s")
        
        logger.info("\n✨ Ready for analysis and reporting!")
        logger.info("=" * 80)
    
    def _cleanup_resources(self):
        """Clean up Spark session and resources"""
        if self.extractor:
            try:
                self.extractor.close_session()
                logger.info("🔒 Resources cleaned up successfully")
            except Exception as e:
                logger.warning(f"⚠️ Warning during cleanup: {str(e)}")


def main():
    """Main entry point for the ETL pipeline"""
    
    # Create sample data file if it doesn't exist (for demonstration)
    if not RAW_CSV_PATH.exists():
        logger.warning("⚠️ Sample data file not found!")
        logger.info("📋 Please download the Kaggle dataset 'Customer Sales Data'")
        logger.info(f"📁 Expected location: {RAW_CSV_PATH}")
        logger.info("🔗 Kaggle link: https://www.kaggle.com/datasets/kyanyoga/sample-sales-data")
        
        # Create a small sample file for demonstration
        create_sample_data()
    
    # Initialize and run pipeline
    pipeline = OperateOptimaETL()
    
    try:
        results = pipeline.run_pipeline()
        logger.info("\n🎯 Pipeline execution completed successfully!")
        return results
        
    except Exception as e:
        logger.error(f"💥 Pipeline failed with error: {str(e)}")
        sys.exit(1)


def create_sample_data():
    """Create a sample CSV file for demonstration purposes"""
    logger.info("🔧 Creating sample data file for demonstration...")
    
    sample_data = """OrderDate,Region,Rep,Item,Units,Unit Cost,Total
1/6/19,East,Jones,Pencil,95,1.99,189.05
1/23/19,Central,Kivell,Binder,50,19.99,999.50
2/9/19,Central,Jardine,Pencil,36,4.99,179.64
2/26/19,Central,Gill,Pen,27,19.99,539.73
3/15/19,West,Sorvino,Pencil,56,2.99,167.44
4/1/19,East,Jones,Binder,60,4.99,299.40
4/18/19,Central,Andrews,Pencil,75,1.99,149.25
5/5/19,Central,Jardine,Pencil,90,4.99,449.10
5/22/19,West,Thompson,Pencil,32,1.99,63.68
6/8/19,East,Jones,Binder,60,8.99,539.40
6/25/19,Central,Morgan,Pencil,90,4.99,449.10
7/12/19,East,Howard,Binder,29,1.99,57.71
7/29/19,East,Parent,Binder,81,19.99,1619.19
8/15/19,East,Jones,Pencil,35,4.99,174.65
9/1/19,Central,Smith,Desk,2,125.00,250.00
9/18/19,East,Jones,Pen Set,16,15.99,255.84
10/5/19,Central,Morgan,Binder,28,8.99,251.72
10/22/19,East,Jones,Pen,64,8.99,575.36
11/8/19,East,Parent,Pen,15,19.99,299.85
11/25/19,Central,Kivell,Pen Set,96,4.99,479.04
12/12/19,Central,Smith,Pencil,67,1.29,86.43"""
    
    # Ensure directory exists
    RAW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Write sample data
    with open(RAW_CSV_PATH, 'w') as f:
        f.write(sample_data)
    
    logger.info(f"✅ Sample data created at: {RAW_CSV_PATH}")


if __name__ == "__main__":
    main()
