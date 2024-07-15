from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

minio_bucket = "my-first-bucket"

# Create the SparkSession builder for Iceberg with Minio configuration
iceberg_builder = SparkSession.builder \
    .appName("iceberg-concurrent-write-isolation-test") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-hive-runtime:1.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", f"s3a://{minio_bucket}/iceberg_data/") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .enableHiveSupport()

# Build the SparkSession for Iceberg
iceberg_spark = iceberg_builder.getOrCreate()

# Iceberg table location in Minio
iceberg_table_location = f"s3a://{minio_bucket}/iceberg_data/tabla1"

# Read data from the Iceberg table
iceberg_df = iceberg_spark.read.format("iceberg").load(f"{iceberg_table_location}/iceberg_table_name")

# Show the dataframe schema and some sample data
print("**************************")
print("This the Dataframe schema ")
print("**************************")
iceberg_df.printSchema()

print("**************************")
print("******Dataframe Data******")
print("**************************")
iceberg_df.show()
