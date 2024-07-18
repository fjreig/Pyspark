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

def Guardar_df(df, topic):
    df = df.select(to_json(struct("Fecha")).alias("key"), to_json(struct("Fecha","radiacion","temp_amb","temp_panel")).alias("value"))
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.environ['kafka_broker']) \
        .option("topic", topic) \
        .save()
    return (df)

def main():
    spark = create_spark_session()
    df = Obtener_df(spark)
    df.show()
    df_final = Guardar_df(df,  "emi")
    df_final.show()

if __name__ == "__main__":
    main()
