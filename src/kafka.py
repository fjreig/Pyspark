from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
import os

## Create the SparkSession builder
spark = SparkSession \
    .builder \
    .appName("Kafka") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.memory", "3g") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.7.1") \
    .getOrCreate()

def main():
    print("SCALA VERSION: ", spark.sparkContext._gateway.jvm.scala.util.Properties.versionString())

    df = spark.read.parquet('/opt/spark-apps/data/tabla1.parquet')

    df.show()

    df = df.select(to_json(struct("Fecha","Inversor")).alias("key"), to_json(struct("Fecha","Inversor","EA","Is1","Is2","Is3","Is4","Is5","Is6","Is7","Is8","Is9","Is10","Is11","Is12","Is13","Is14")).alias("value"))

    df.show()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.environ['kafka_broker']) \
    .option("topic", "tabla1") \
    .save()

if __name__ == "__main__":
    main()
