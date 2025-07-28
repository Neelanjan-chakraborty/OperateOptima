# Operate Optima - Production Dockerfile
# Multi-stage build for optimized Apache Spark ETL Pipeline

# Build stage
FROM python:3.9-slim as builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Download and install Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin:/opt/spark/sbin \
    PYSPARK_PYTHON=python3

# Production stage
FROM python:3.9-slim as production

# Copy system dependencies from builder
COPY --from=builder /usr/lib/jvm/java-11-openjdk-amd64 /usr/lib/jvm/java-11-openjdk-amd64
COPY --from=builder /opt/spark /opt/spark

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin:/opt/spark/sbin \
    PYSPARK_PYTHON=python3 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create application user
RUN groupadd -r sparkuser && useradd -r -g sparkuser sparkuser

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY pipeline.py .
COPY data/ ./data/

# Create necessary directories and set permissions
RUN mkdir -p /app/logs /app/data/processed \
    && chown -R sparkuser:sparkuser /app

# Switch to non-root user
USER sparkuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import pyspark; print('Spark OK')" || exit 1

# Expose ports (if needed for Spark UI)
EXPOSE 4040 4041

# Default command
CMD ["python", "pipeline.py"]

# Labels for metadata
LABEL maintainer="your.email@example.com" \
      version="1.0.0" \
      description="Operate Optima - Enterprise Spark ETL Pipeline" \
      spark.version="3.5.0" \
      python.version="3.9"
