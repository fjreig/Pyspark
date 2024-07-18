from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s")

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Pipeline PostgreSQL to Iceberg")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-hive-runtime:1.5.0",
        ) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", os.environ['minio_bucket']) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['minio_access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['minio_secret_key']) \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ['minio_url']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    )
    logging.info("Spark session created successfully")
    return (spark)

def Obtener_df(spark_session):
    try:
        query = """SELECT * FROM public.fv_apv_emi """
        df = spark_session.read \
            .format("jdbc") \
            .option("url", os.environ['url_postgres']) \
            .option("user", os.environ['user_postgres']) \
            .option("password", os.environ['password_postgres']) \
            .option("query", query) \
            .load()
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")
        raise
    return (df)

def Guardar_df(df, nombre_tabla):
    df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable(nombre_tabla)

def main():
    spark = create_spark_session()
    df = Obtener_df(spark)
    df.show()
    Guardar_df(df, "procesados.fv_apv_emi")

if __name__ == "__main__":
    main()
