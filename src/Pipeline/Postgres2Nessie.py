from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s")

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Pipeline PostgreSQL to Nessie")
          #packages
        .config("spark.jars.packages","org.postgresql:postgresql:42.7.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8",)
          #SQL Extensions
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
          #Configuring Catalog
        .config('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.nessie.uri', os.environ["url_Nessie"])
        .config('spark.sql.catalog.nessie.ref', 'main')
        .config('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .config('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .config('spark.sql.catalog.nessie.s3.endpoint', os.environ["minio_url"]) #Si no funciona, indicar la IP del contenedor minio
        .config('spark.sql.catalog.nessie.warehouse', os.environ["minio_bucket"])
        .config('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .config('spark.hadoop.fs.s3a.access.key', os.environ["minio_access_key"])
        .config('spark.hadoop.fs.s3a.secret.key', os.environ["minio_secret_key"])
        .getOrCreate()
    )
    logging.info("Spark session created successfully")
    return (spark)

def Obtener_df(spark_session, tabla):
    try:
        url = os.environ["url_postgres"]
        properties = {
            "user": os.environ["user_postgres"],
            "password": os.environ["password_postgres"],
            "driver": "org.postgresql.Driver"
        }
        df = spark_session.read.jdbc(url, tabla, properties=properties)
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")
        raise
    return (df)

def Guardar_df(df, nombre_tabla):
    nombre_tabla = "nessie." + nombre_tabla
    df.writeTo(nombre_tabla).createOrReplace()

def main():
    spark = create_spark_session()
    df = Obtener_df(spark, "fv_pavasal_vara_historico")
    df.show()
    Guardar_df(df, "Tabla_FV")

if __name__ == "__main__":
    main()
