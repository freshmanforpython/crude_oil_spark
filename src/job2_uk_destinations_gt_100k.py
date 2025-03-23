from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("UKDestinationsOver100k") \
    .getOrCreate()

# Set input and output paths
input_path = "./data/data.csv"
output_dir = "./output/uk_destinations_gt_100k"

# Create output directory if not exists
os.makedirs(output_dir, exist_ok=True)

# Load CSV data
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Filter rows where origin == "UK"
df_uk = df.filter(col("originName") == "United Kingdom").filter(col("originTypeName") == "Country")

# Group by destination, sum quantity
uk_dest_total = (df_uk.groupBy("destinationName")
                 .agg(spark_sum("quantity").alias("total_quantity"))
                 .filter(col("total_quantity") > 100000)
                 .orderBy(col("total_quantity").desc()))

# Show result
uk_dest_total.show()

# Save result to CSV (as single file)
uk_dest_total.coalesce(1).write.mode("overwrite").csv(output_dir, header=True)

spark.stop()
