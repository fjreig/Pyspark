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
            "org.postgresql:postgresql:42.7.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .getOrCreate()
    )
    logging.info("Spark session created successfully")
    return (spark)

def Obtener_df(spark_session, tabla):
    try:
        query = f"""SELECT * FROM public.{tabla} """
        df = spark_session.read \
            .format("jdbc") \
            .option("url", os.environ['url_postgres'] + "/Monitorizacion") \
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
    properties = {
        "user":  os.environ['user_postgres_local'],
        "password":  os.environ['password_postgres_local'],
        "driver": "org.postgresql.Driver"
    }
    url_write = os.environ['url_postgres_local'] + "/Monitorizacion"
    table_name_write1 = "public." + nombre_tabla
    df.write.jdbc(url_write, table_name_write1, mode="overwrite", properties=properties)

def main():
    spark = create_spark_session()
    df = Obtener_df(spark, "pabat_aarr")
    df.show()
    Guardar_df(df, "FV_Pavasal_Vara_AARR")

if __name__ == "__main__":
    main()
