# docker/Dockerfile
FROM bitnami/spark:latest

# Add Iceberg runtime as env variable (Spark will auto-load)
ENV SPARK_JARS_PACKAGES=org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0

# Copy your PySpark scripts into the container
COPY ./src /app/src

# Set working directory
WORKDIR /app/src
