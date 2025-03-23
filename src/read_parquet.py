from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ReadParquet") \
    .getOrCreate()

# Read Parquet file (path inside Docker)
df = spark.read.parquet("/Users/sixup/Downloads/crude_oil_spark/src/warehouse/db/top5_albania/data/00000-14-d754d12b-892c-495f-a389-c30e02909393-00001.parquet")

# Show first few rows
df.show()

# Print schema
df.printSchema()

spark.stop()
