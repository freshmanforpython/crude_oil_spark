from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import os

# Initialize SparkSession with Iceberg support
spark = SparkSession.builder \
    .appName("Top5DestinationsAlbania") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "./warehouse") \
    .getOrCreate()

# Load Data
df = spark.read.csv("/data/data.csv", header=True, inferSchema=True)


# Filter: Origin = Albania
df_albania = df.filter(col("originName") == "Albania").filter(col("originTypeName") == "Country")

# Group by destination, sum quantity
top5_dest = (df_albania.groupBy("destinationName")
             .agg(spark_sum("quantity").alias("total_quantity"))
             .orderBy(col("total_quantity").desc())
             .limit(5))

# Show result
top5_dest.show()

# Write to Iceberg table (local catalog)
top5_dest.writeTo("local.db.top5_albania").using("iceberg").createOrReplace()
print("Warehouse path:", spark.conf.get("spark.sql.catalog.local.warehouse"))

spark.stop()
