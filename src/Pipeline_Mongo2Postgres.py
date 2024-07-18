from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Pipeline PostgreSQL to Kafka")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.2,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        ) \
        .config("spark.mongodb.read.connection.uri", mongo_url) \
        .config("spark.mongodb.write.connection.uri", mongo_url) \
        .getOrCreate()
    )
    logging.info("Spark session created successfully")
    return (spark)

def Obtener_DF_nuevos(spark_session, tabla):
    try:
        df = spark_session.read \
            .format("mongodb") \
            .option("database", "Info") \
            .option("collection", tabla) \
            .load()
        df.createOrReplaceTempView("Consulta1")
        df.cache()
        df = spark_session.sql(f"""SELECT fecha, precio FROM Consulta1 where year(fecha)>= 2024 """)
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")
        raise
    return (df)

def Guardar(df, nombre_tabla):
    properties = {
        "user": "",
        "password": "",
        "driver": "org.postgresql.Driver"
    }
    url_write = "jdbc:postgresql://"
    table_name_write1 = "public." + nombre_tabla
    df.write.jdbc(url_write, table_name_write1, mode="overwrite", properties=properties)

def main():
    spark = create_spark_session()
    df = Obtener_DF_nuevos(spark, "OMIE")
    df.show()
    Guardar(df, "omie")

if __name__ == "__main__":
    main()
