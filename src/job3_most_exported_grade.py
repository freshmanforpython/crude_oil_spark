from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, row_number
from pyspark.sql.window import Window
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MostExportedGradePerYearOrigin") \
    .getOrCreate()


# Load Data
df = spark.read.csv("./data/data.csv", header=True, inferSchema=True)

# Output Data
output_dir = "./output/most_exported_grade_per_year_origin"

# Create the directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)


# Group by year, origin, grade, then sum quantity
grouped_df = (df.groupBy("year", "originName", "gradeName")
                .agg(spark_sum("quantity").alias("total_quantity")))

# Define window partitioned by (year, origin), ordered by total_quantity desc
window_spec = Window.partitionBy("year", "originName").orderBy(col("total_quantity").desc())

# Assign row_number to each row in partition
ranked_df = grouped_df.withColumn("rank", row_number().over(window_spec))

# Filter to get only the top 1 grade per (year, origin)
top_grade_df = ranked_df.filter(col("rank") == 1).select("year", "originName", "gradeName", "total_quantity")

# Show result
top_grade_df.show()

# Save result to CSV (as single file)
top_grade_df.coalesce(1).write.mode("overwrite").csv(output_dir, header=True)

spark.stop()
