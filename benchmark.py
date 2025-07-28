"""
Performance Benchmarking Script for Operate Optima ETL Pipeline
Measures processing speed, memory usage, and scalability metrics
"""

import time
import psutil
import sys
import json
from pathlib import Path
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import pandas as pd

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

try:
    from pyspark.sql import SparkSession
    from src.transform import DataTransformer
    from src.extract import DataExtractor
    from src.load import DataLoader
    SPARK_AVAILABLE = True
except ImportError:
    print("⚠️ Spark not available. Install pyspark to run benchmarks.")
    SPARK_AVAILABLE = False


class PerformanceBenchmark:
    """Performance benchmarking suite for the ETL pipeline"""
    
    def __init__(self):
        self.results = {}
        self.spark = None
        
    def setup_spark(self):
        """Initialize Spark session for benchmarking"""
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark not available")
            
        self.spark = SparkSession.builder \
            .appName("OperateOptima-Benchmark") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    
    def create_test_dataset(self, size: int):
        """Create a test dataset of specified size"""
        print(f"📊 Creating test dataset with {size:,} records...")
        
        import random
        from datetime import datetime, timedelta
        
        regions = ["East", "West", "Central", "North", "South"]
        reps = [f"Rep_{i}" for i in range(1, 21)]
        items = ["Pencil", "Pen", "Binder", "Desk", "Chair", "Paper", "Notebook"]
        
        data = []
        base_date = datetime(2019, 1, 1)
        
        for i in range(size):
            date = base_date + timedelta(days=random.randint(0, 365))
            data.append((
                date.strftime("%-m/%-d/%y"),
                random.choice(regions),
                random.choice(reps),
                random.choice(items),
                random.randint(1, 200),
                round(random.uniform(0.5, 50.0), 2),
                round(random.uniform(10, 1000), 2)
            ))
        
        columns = ["OrderDate", "Region", "Rep", "Item", "Units", "Unit Cost", "Total"]
        return self.spark.createDataFrame(data, columns)
    
    def measure_memory_usage(self) -> Dict[str, float]:
        """Measure current memory usage"""
        process = psutil.Process()
        memory_info = process.memory_info()
        
        return {
            "rss_mb": memory_info.rss / 1024 / 1024,  # Resident Set Size
            "vms_mb": memory_info.vms / 1024 / 1024,  # Virtual Memory Size
            "percent": process.memory_percent()
        }
    
    def benchmark_extraction(self, dataset_sizes: List[int]) -> Dict[str, List]:
        """Benchmark data extraction performance"""
        print("\n🔍 BENCHMARKING DATA EXTRACTION")
        print("=" * 50)
        
        results = {
            "dataset_size": [],
            "extraction_time": [],
            "records_per_second": [],
            "memory_usage_mb": []
        }
        
        for size in dataset_sizes:
            print(f"\n📈 Testing with {size:,} records...")
            
            # Create test dataset and save to temp file
            import tempfile
            test_df = self.create_test_dataset(size)
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                # Convert to Pandas and save as CSV
                test_df.toPandas().to_csv(f.name, index=False)
                temp_file = f.name
            
            # Measure extraction performance
            memory_before = self.measure_memory_usage()
            start_time = time.time()
            
            extractor = DataExtractor()
            extracted_df = extractor.extract_csv_data(temp_file)
            record_count = extracted_df.count()
            
            end_time = time.time()
            memory_after = self.measure_memory_usage()
            
            extraction_time = end_time - start_time
            records_per_second = record_count / extraction_time if extraction_time > 0 else 0
            memory_used = memory_after["rss_mb"] - memory_before["rss_mb"]
            
            results["dataset_size"].append(size)
            results["extraction_time"].append(extraction_time)
            results["records_per_second"].append(records_per_second)
            results["memory_usage_mb"].append(memory_used)
            
            print(f"⏱️ Extraction time: {extraction_time:.2f}s")
            print(f"🚀 Records/sec: {records_per_second:,.0f}")
            print(f"💾 Memory used: {memory_used:.1f} MB")
            
            # Cleanup
            extractor.close_session()
            import os
            os.unlink(temp_file)
        
        return results
    
    def benchmark_transformation(self, dataset_sizes: List[int]) -> Dict[str, List]:
        """Benchmark data transformation performance"""
        print("\n🔄 BENCHMARKING DATA TRANSFORMATION")
        print("=" * 50)
        
        results = {
            "dataset_size": [],
            "transformation_time": [],
            "records_per_second": [],
            "memory_usage_mb": [],
            "data_quality_improvement": []
        }
        
        for size in dataset_sizes:
            print(f"\n📈 Testing with {size:,} records...")
            
            # Create test dataset
            test_df = self.create_test_dataset(size)
            original_count = test_df.count()
            
            # Measure transformation performance
            memory_before = self.measure_memory_usage()
            start_time = time.time()
            
            transformer = DataTransformer()
            transformed_df = transformer.transform_data(test_df)
            final_count = transformed_df.count()
            
            end_time = time.time()
            memory_after = self.measure_memory_usage()
            
            transformation_time = end_time - start_time
            records_per_second = final_count / transformation_time if transformation_time > 0 else 0
            memory_used = memory_after["rss_mb"] - memory_before["rss_mb"]
            quality_improvement = ((original_count - final_count) / original_count * 100) if original_count > 0 else 0
            
            results["dataset_size"].append(size)
            results["transformation_time"].append(transformation_time)
            results["records_per_second"].append(records_per_second)
            results["memory_usage_mb"].append(memory_used)
            results["data_quality_improvement"].append(quality_improvement)
            
            print(f"⏱️ Transformation time: {transformation_time:.2f}s")
            print(f"🚀 Records/sec: {records_per_second:,.0f}")
            print(f"💾 Memory used: {memory_used:.1f} MB")
            print(f"📊 Quality improvement: {quality_improvement:.1f}%")
        
        return results
    
    def benchmark_loading(self, dataset_sizes: List[int]) -> Dict[str, List]:
        """Benchmark data loading performance"""
        print("\n💾 BENCHMARKING DATA LOADING")
        print("=" * 50)
        
        results = {
            "dataset_size": [],
            "loading_time": [],
            "records_per_second": [],
            "memory_usage_mb": []
        }
        
        for size in dataset_sizes:
            print(f"\n📈 Testing with {size:,} records...")
            
            # Create and transform test dataset
            test_df = self.create_test_dataset(size)
            transformer = DataTransformer()
            transformed_df = transformer.transform_data(test_df)
            record_count = transformed_df.count()
            
            # Measure loading performance
            import tempfile
            with tempfile.TemporaryDirectory() as temp_dir:
                memory_before = self.measure_memory_usage()
                start_time = time.time()
                
                loader = DataLoader()
                loader.load_to_csv(transformed_df, temp_dir)
                
                end_time = time.time()
                memory_after = self.measure_memory_usage()
                
                loading_time = end_time - start_time
                records_per_second = record_count / loading_time if loading_time > 0 else 0
                memory_used = memory_after["rss_mb"] - memory_before["rss_mb"]
                
                results["dataset_size"].append(size)
                results["loading_time"].append(loading_time)
                results["records_per_second"].append(records_per_second)
                results["memory_usage_mb"].append(memory_used)
                
                print(f"⏱️ Loading time: {loading_time:.2f}s")
                print(f"🚀 Records/sec: {records_per_second:,.0f}")
                print(f"💾 Memory used: {memory_used:.1f} MB")
        
        return results
    
    def run_full_benchmark(self, dataset_sizes: List[int] = None) -> Dict:
        """Run complete benchmark suite"""
        if dataset_sizes is None:
            dataset_sizes = [1000, 5000, 10000, 25000, 50000]
        
        print("🚀 OPERATE OPTIMA PERFORMANCE BENCHMARK")
        print("=" * 60)
        print(f"📊 Testing with dataset sizes: {dataset_sizes}")
        
        if not SPARK_AVAILABLE:
            print("❌ Cannot run benchmarks without PySpark")
            return {}
        
        self.setup_spark()
        
        try:
            benchmark_results = {
                "extraction": self.benchmark_extraction(dataset_sizes),
                "transformation": self.benchmark_transformation(dataset_sizes),
                "loading": self.benchmark_loading(dataset_sizes),
                "system_info": self.get_system_info()
            }
            
            self.results = benchmark_results
            self.generate_report()
            self.save_results()
            
            return benchmark_results
            
        finally:
            if self.spark:
                self.spark.stop()
    
    def get_system_info(self) -> Dict:
        """Get system information for benchmarking context"""
        import platform
        
        return {
            "cpu_count": psutil.cpu_count(),
            "cpu_count_logical": psutil.cpu_count(logical=True),
            "memory_total_gb": psutil.virtual_memory().total / 1024**3,
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "spark_version": "3.5.0" if SPARK_AVAILABLE else "Not Available"
        }
    
    def generate_report(self):
        """Generate performance report"""
        print("\n📋 PERFORMANCE BENCHMARK REPORT")
        print("=" * 60)
        
        if not self.results:
            print("❌ No benchmark results available")
            return
        
        # System information
        sys_info = self.results["system_info"]
        print(f"🖥️ System: {sys_info['platform']}")
        print(f"🔧 CPU Cores: {sys_info['cpu_count']} physical, {sys_info['cpu_count_logical']} logical")
        print(f"💾 RAM: {sys_info['memory_total_gb']:.1f} GB")
        print(f"🐍 Python: {sys_info['python_version']}")
        print(f"⚡ Spark: {sys_info['spark_version']}")
        
        # Performance summary
        for phase, data in self.results.items():
            if phase == "system_info":
                continue
                
            print(f"\n🔥 {phase.upper()} PERFORMANCE:")
            if data["records_per_second"]:
                max_rps = max(data["records_per_second"])
                avg_rps = sum(data["records_per_second"]) / len(data["records_per_second"])
                print(f"   Max throughput: {max_rps:,.0f} records/second")
                print(f"   Avg throughput: {avg_rps:,.0f} records/second")
                
                max_time = max(data[f"{phase[:-2] if phase.endswith('ion') else phase}_time"])
                print(f"   Max processing time: {max_time:.2f} seconds")
    
    def create_visualizations(self):
        """Create performance visualization charts"""
        if not self.results:
            print("❌ No benchmark results to visualize")
            return
        
        print("📊 Creating performance visualizations...")
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Operate Optima ETL Pipeline Performance Benchmarks', fontsize=16)
        
        phases = ["extraction", "transformation", "loading"]
        colors = ["#667eea", "#764ba2", "#f093fb"]
        
        # Throughput comparison
        ax1 = axes[0, 0]
        for i, phase in enumerate(phases):
            if phase in self.results:
                data = self.results[phase]
                ax1.plot(data["dataset_size"], data["records_per_second"], 
                        marker='o', label=phase.capitalize(), color=colors[i], linewidth=2)
        
        ax1.set_xlabel("Dataset Size")
        ax1.set_ylabel("Records/Second")
        ax1.set_title("Processing Throughput")
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Processing time comparison
        ax2 = axes[0, 1]
        for i, phase in enumerate(phases):
            if phase in self.results:
                data = self.results[phase]
                time_key = f"{phase[:-2] if phase.endswith('ion') else phase}_time"
                ax2.plot(data["dataset_size"], data[time_key], 
                        marker='s', label=phase.capitalize(), color=colors[i], linewidth=2)
        
        ax2.set_xlabel("Dataset Size")
        ax2.set_ylabel("Processing Time (seconds)")
        ax2.set_title("Processing Time")
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Memory usage
        ax3 = axes[1, 0]
        for i, phase in enumerate(phases):
            if phase in self.results:
                data = self.results[phase]
                ax3.plot(data["dataset_size"], data["memory_usage_mb"], 
                        marker='^', label=phase.capitalize(), color=colors[i], linewidth=2)
        
        ax3.set_xlabel("Dataset Size")
        ax3.set_ylabel("Memory Usage (MB)")
        ax3.set_title("Memory Consumption")
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Scalability (records/second vs dataset size)
        ax4 = axes[1, 1]
        if "transformation" in self.results:
            data = self.results["transformation"]
            ax4.scatter(data["dataset_size"], data["records_per_second"], 
                       color="#667eea", s=50, alpha=0.7)
            
            # Add trend line
            z = np.polyfit(data["dataset_size"], data["records_per_second"], 1)
            p = np.poly1d(z)
            ax4.plot(data["dataset_size"], p(data["dataset_size"]), 
                    "--", color="#764ba2", alpha=0.8)
        
        ax4.set_xlabel("Dataset Size")
        ax4.set_ylabel("Records/Second")
        ax4.set_title("Scalability Analysis")
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig("benchmark_results.png", dpi=300, bbox_inches='tight')
        print("✅ Visualizations saved as 'benchmark_results.png'")
    
    def save_results(self, filename: str = "benchmark_results.json"):
        """Save benchmark results to JSON file"""
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"💾 Results saved to {filename}")


def main():
    """Main benchmark execution"""
    benchmark = PerformanceBenchmark()
    
    # Run benchmarks with different dataset sizes
    dataset_sizes = [1000, 5000, 10000, 25000]  # Adjust based on your system
    
    try:
        results = benchmark.run_full_benchmark(dataset_sizes)
        
        # Create visualizations if matplotlib and numpy are available
        try:
            import numpy as np
            benchmark.create_visualizations()
        except ImportError:
            print("⚠️ Install matplotlib and numpy for visualizations")
        
        print("\n🎉 Benchmark completed successfully!")
        
    except Exception as e:
        print(f"❌ Benchmark failed: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
