💼 Project Name: Operate Optima – Spark-Based ETL Optimization
Build a high-performance ETL pipeline using Apache Spark & SQL to clean and process business transaction data, with real benchmarks showing reduced processing time. The pipeline will:

Extract raw data from CSV files

Clean and transform it using PySpark

Load the cleaned output into a processed directory

Log performance metrics for optimization

📦 Dataset Source (Kaggle - Real Business Data)
Kaggle Dataset Name: Customer Sales Data
Direct CSV Link (once downloaded):

ruby
Copy
Edit
https://www.kaggle.com/datasets/kyanyoga/sample-sales-data/download?datasetVersionNumber=1
Or use this file from the zip:

SampleSalesData.csv

🧠 Features to Implement
Extract

Load large CSVs using PySpark

Infer schema automatically

Transform

Clean null values, fix column types

Create new columns like total_revenue = quantity * price

Filter out rows with low/zero quantity or price

Load

Save cleaned data to /data/processed/ as CSV

Ensure header and overwrite enabled

Benchmark

Print ETL duration using time.time()

Log row counts before/after cleaning

🛠️ Folder Structure
r
Copy
Edit
operate-optima/
│
├── data/
│   ├── raw/               <- Place downloaded Kaggle CSV here
│   └── processed/         <- Final cleaned data goes here
│
├── src/
│   ├── extract.py         <- PySpark reader
│   ├── transform.py       <- Cleansing logic
│   ├── load.py            <- Writer to CSV
│   └── config.py          <- File path variables
│
├── pipeline.py            <- Main script to run the pipeline
├── requirements.txt       <- Dependencies
└── README.md              <- Documentation
🔧 Setup Instructions
bash
Copy
Edit
# Step 1: Create virtual environment (optional)
python -m venv venv
source venv/bin/activate  # Or venv\Scripts\activate on Windows

# Step 2: Install dependencies
pip install pyspark pandas

# Step 3: Download dataset from Kaggle and unzip to `data/raw/`
# File should be located at: data/raw/SampleSalesData.csv

# Step 4: Run the pipeline
python pipeline.py
📌 Dataset Columns (from SampleSalesData.csv)
cs
Copy
Edit
OrderDate, Region, Rep, Item, Units, Unit Cost, Total
1/6/19, East, Jones, Pencil, 95, 1.99, 189.05
⚡ Example Transformations (in transform.py)
Rename columns to snake_case

Convert Unit Cost and Total to float

Filter rows with Units < 10

Add profit_margin = total - (units * unit_cost)

⏱️ Benchmark Example (Console Output)
yaml
Copy
Edit
🔹 Extracting...
🔹 Transforming...
🔹 Loading...
✅ ETL Completed in 12.35 seconds.
Before: 5000 rows → After cleaning: 4312 rows
📎 Extra Kaggle Dataset Options (for scaling later)
Sales Transactions Dataset Weekly – 8000 rows

Online Retail Dataset – 500K rows

Superstore Sales Data – classic benchmark dataset