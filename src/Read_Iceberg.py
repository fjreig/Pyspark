from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

# Create the SparkSession builder for Iceberg with Minio configuration
iceberg_builder = SparkSession.builder \
    .appName("iceberg-concurrent-write-isolation-test") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-hive-runtime:1.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", f"s3a://{minio_bucket}/iceberg_data/") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ['minio_access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ['minio_secret_key']) \
    .config("spark.hadoop.fs.s3a.endpoint", os.environ['minio_url']) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .enableHiveSupport()

# Build the SparkSession for Iceberg
spark = iceberg_builder.getOrCreate()

# Read data from the Iceberg table
df = spark.read.format("iceberg").load("s3a://monitorizacion/iceberg_data/tablas/prueba1")
df.printSchema()
df.show()
