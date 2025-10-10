FROM apache/airflow:latest

# Switch to root to install Java
USER root

# Install Java 17
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to airflow user
USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    mysql-connector-python \
    nbconvert \
    jupyter \
    requests \
    pyspark \
    delta-spark

# Pre-download Spark JARs to avoid runtime download issues
RUN python -c "from pyspark.sql import SparkSession; \
    spark = SparkSession.builder \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.13:4.0.0,com.mysql:mysql-connector-j:8.0.33') \
    .getOrCreate(); \
    spark.stop()" || true
