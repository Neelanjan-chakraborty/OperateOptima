"""
Comprehensive test suite for Operate Optima ETL Pipeline
Tests all components: Extract, Transform, Load, and Pipeline orchestration
"""

import pytest
import os
import sys
from pathlib import Path
import tempfile
import pandas as pd
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

from src.config import COLUMN_MAPPINGS, MIN_UNITS_THRESHOLD, MAX_UNIT_COST

# Skip all tests if Spark is not available
pytestmark = pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark not available")
    
    spark = SparkSession.builder \
        .appName("OperateOptima-Tests") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture
def sample_data():
    """Create sample test data"""
    return [
        ("1/6/19", "East", "Jones", "Pencil", 95, 1.99, 189.05),
        ("1/23/19", "Central", "Kivell", "Binder", 50, 19.99, 999.50),
        ("2/9/19", "Central", "Jardine", "Pencil", 36, 4.99, 179.64),
        ("2/26/19", "Central", "Gill", "Pen", 27, 19.99, 539.73),
        ("3/15/19", "West", "Sorvino", "Pencil", 0, 2.99, 0.00),  # Invalid data
        ("4/1/19", "East", "Jones", "", 60, 4.99, 299.40),  # Missing item
        (None, "Central", "Andrews", "Pencil", 75, 1.99, 149.25),  # Missing date
    ]


@pytest.fixture
def sample_df(spark_session, sample_data):
    """Create a sample DataFrame for testing"""
    columns = ["OrderDate", "Region", "Rep", "Item", "Units", "Unit Cost", "Total"]
    return spark_session.createDataFrame(sample_data, columns)


@pytest.fixture
def temp_csv_file(sample_data):
    """Create a temporary CSV file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        # Write header
        f.write("OrderDate,Region,Rep,Item,Units,Unit Cost,Total\n")
        # Write data
        for row in sample_data:
            f.write(",".join(str(x) if x is not None else "" for x in row) + "\n")
    
    yield f.name
    os.unlink(f.name)


class TestDataExtractor:
    """Test the data extraction functionality"""
    
    def test_spark_session_creation(self):
        """Test Spark session creation with proper configuration"""
        from src.extract import DataExtractor
        
        extractor = DataExtractor()
        assert extractor.spark is not None
        assert extractor.spark.conf.get("spark.app.name") == "OperateOptima-ETL"
        extractor.close_session()
    
    def test_csv_extraction(self, temp_csv_file):
        """Test CSV file extraction"""
        from src.extract import DataExtractor
        
        extractor = DataExtractor()
        df = extractor.extract_csv_data(temp_csv_file)
        
        assert df is not None
        assert df.count() > 0
        assert len(df.columns) == 7
        assert "OrderDate" in df.columns
        assert "Units" in df.columns
        
        extractor.close_session()
    
    def test_schema_detection(self, temp_csv_file):
        """Test automatic schema detection"""
        from src.extract import DataExtractor
        
        extractor = DataExtractor()
        df = extractor.extract_csv_data(temp_csv_file)
        
        # Check that numeric columns are properly detected
        schema_dict = {field.name: field.dataType for field in df.schema.fields}
        
        # Units should be integer
        assert isinstance(schema_dict.get("Units"), (IntegerType, type(IntegerType())))
        
        extractor.close_session()


class TestDataTransformer:
    """Test the data transformation functionality"""
    
    def test_column_renaming(self, sample_df):
        """Test column renaming to snake_case"""
        from src.transform import DataTransformer
        
        transformer = DataTransformer()
        result_df = transformer._rename_columns(sample_df)
        
        # Check that columns are renamed correctly
        for old_name, new_name in COLUMN_MAPPINGS.items():
            if old_name in sample_df.columns:
                assert new_name in result_df.columns
                assert old_name not in result_df.columns
    
    def test_data_type_cleaning(self, sample_df):
        """Test data type conversion and cleaning"""
        from src.transform import DataTransformer
        
        transformer = DataTransformer()
        # First rename columns
        df = transformer._rename_columns(sample_df)
        # Then clean data types
        result_df = transformer._clean_data_types(df)
        
        # Check that numeric columns are properly converted
        schema_dict = {field.name: field.dataType for field in result_df.schema.fields}
        assert isinstance(schema_dict.get("unit_cost"), (DoubleType, type(DoubleType())))
        assert isinstance(schema_dict.get("total"), (DoubleType, type(DoubleType())))
    
    def test_null_value_handling(self, sample_df):
        """Test null value handling"""
        from src.transform import DataTransformer
        
        transformer = DataTransformer()
        df = transformer._rename_columns(sample_df)
        df = transformer._clean_data_types(df)
        result_df = transformer._handle_null_values(df)
        
        # Check that rows with null critical values are removed
        null_units = result_df.filter(result_df.units.isNull()).count()
        assert null_units == 0
    
    def test_business_rules(self, sample_df):
        """Test business rule application"""
        from src.transform import DataTransformer
        
        transformer = DataTransformer()
        df = transformer._rename_columns(sample_df)
        df = transformer._clean_data_types(df)
        df = transformer._handle_null_values(df)
        result_df = transformer._apply_business_rules(df)
        
        # Check that invalid data is filtered out
        zero_units = result_df.filter(result_df.units <= 0).count()
        assert zero_units == 0
        
        zero_cost = result_df.filter(result_df.unit_cost <= 0).count()
        assert zero_cost == 0
    
    def test_feature_engineering(self, sample_df):
        """Test derived feature creation"""
        from src.transform import DataTransformer
        
        transformer = DataTransformer()
        df = transformer._rename_columns(sample_df)
        df = transformer._clean_data_types(df)
        df = transformer._handle_null_values(df)
        df = transformer._apply_business_rules(df)
        result_df = transformer._create_derived_features(df)
        
        # Check that new features are created
        assert "profit_margin" in result_df.columns
        assert "profit_margin_pct" in result_df.columns
        assert "revenue_per_unit" in result_df.columns
        assert "order_size_category" in result_df.columns
    
    def test_complete_transformation(self, sample_df):
        """Test the complete transformation pipeline"""
        from src.transform import DataTransformer
        
        transformer = DataTransformer()
        result_df = transformer.transform_data(sample_df)
        
        # Check that transformation produces valid results
        assert result_df.count() > 0
        assert "profit_margin" in result_df.columns
        
        # Check that invalid data is filtered out
        valid_data = result_df.filter(
            (result_df.units > 0) & 
            (result_df.unit_cost > 0) & 
            (result_df.total > 0)
        ).count()
        assert valid_data == result_df.count()


class TestDataLoader:
    """Test the data loading functionality"""
    
    def test_csv_loading(self, sample_df):
        """Test CSV file loading"""
        from src.load import DataLoader
        
        with tempfile.TemporaryDirectory() as temp_dir:
            loader = DataLoader()
            output_path = loader.load_to_csv(sample_df, temp_dir)
            
            assert output_path == temp_dir
            assert loader.load_stats['format'] == 'CSV'
            assert loader.load_stats['records_saved'] > 0
    
    def test_load_statistics(self, sample_df):
        """Test load statistics generation"""
        from src.load import DataLoader
        
        with tempfile.TemporaryDirectory() as temp_dir:
            loader = DataLoader()
            loader.load_to_csv(sample_df, temp_dir)
            stats = loader.get_load_statistics()
            
            assert 'records_saved' in stats
            assert 'columns_saved' in stats
            assert 'output_path' in stats
            assert 'format' in stats


class TestPipelineIntegration:
    """Test the complete pipeline integration"""
    
    def test_pipeline_execution(self, temp_csv_file):
        """Test complete pipeline execution"""
        # Import pipeline components
        sys.path.append(str(Path(__file__).parent.parent))
        from pipeline import OperateOptimaETL
        
        with tempfile.TemporaryDirectory() as temp_output:
            pipeline = OperateOptimaETL()
            
            try:
                results = pipeline.run_pipeline(
                    input_path=temp_csv_file,
                    output_path=temp_output
                )
                
                # Verify pipeline execution
                assert results['status'] == 'SUCCESS'
                assert 'performance_metrics' in results
                assert 'data_metrics' in results
                assert results['performance_metrics']['total_execution_time'] > 0
                
            finally:
                pipeline._cleanup_resources()


class TestPerformance:
    """Test performance benchmarks"""
    
    def test_processing_speed(self, spark_session):
        """Test processing speed with larger dataset"""
        # Create larger test dataset
        large_data = []
        for i in range(1000):
            large_data.append((
                f"1/{(i % 12) + 1}/19",
                "East" if i % 2 == 0 else "West",
                f"Rep{i % 10}",
                f"Item{i % 5}",
                i + 1,
                round(1.99 + (i % 50), 2),
                round((i + 1) * (1.99 + (i % 50)), 2)
            ))
        
        columns = ["OrderDate", "Region", "Rep", "Item", "Units", "Unit Cost", "Total"]
        large_df = spark_session.createDataFrame(large_data, columns)
        
        from src.transform import DataTransformer
        
        import time
        start_time = time.time()
        
        transformer = DataTransformer()
        result_df = transformer.transform_data(large_df)
        result_count = result_df.count()
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Performance assertions
        assert result_count > 0
        assert processing_time < 30  # Should process 1000 records in under 30 seconds
        
        records_per_second = result_count / processing_time
        assert records_per_second > 10  # Should process at least 10 records per second


class TestErrorHandling:
    """Test error handling and edge cases"""
    
    def test_missing_file_handling(self):
        """Test handling of missing input files"""
        from src.extract import DataExtractor
        
        extractor = DataExtractor()
        
        with pytest.raises(Exception):
            extractor.extract_csv_data("nonexistent_file.csv")
        
        extractor.close_session()
    
    def test_empty_dataframe_handling(self, spark_session):
        """Test handling of empty DataFrames"""
        from src.transform import DataTransformer
        
        # Create empty DataFrame
        columns = ["OrderDate", "Region", "Rep", "Item", "Units", "Unit Cost", "Total"]
        empty_df = spark_session.createDataFrame([], columns)
        
        transformer = DataTransformer()
        result_df = transformer.transform_data(empty_df)
        
        assert result_df.count() == 0
    
    def test_invalid_data_handling(self, spark_session):
        """Test handling of invalid data"""
        invalid_data = [
            (None, None, None, None, None, None, None),  # All nulls
            ("invalid_date", "Region", "Rep", "Item", -5, -1.99, -100),  # Negative values
        ]
        
        columns = ["OrderDate", "Region", "Rep", "Item", "Units", "Unit Cost", "Total"]
        invalid_df = spark_session.createDataFrame(invalid_data, columns)
        
        from src.transform import DataTransformer
        
        transformer = DataTransformer()
        result_df = transformer.transform_data(invalid_df)
        
        # Invalid data should be filtered out
        assert result_df.count() == 0


# Configuration for pytest
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
