from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import concat,concat_ws,to_timestamp
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import os

## Create the SparkSession builder
spark = SparkSession.builder \
    .appName("Postgres") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

## Postgres Config
properties = {
    "user": os.environ['user_postgres'],
    "password": os.environ['password_postgres'],
    "driver": "org.postgresql.Driver"
}

url_write = os.environ['url_postgres'] + "/Monitorizacion"
table_name_write1 = "public.prueba1"

df1 = spark.read.parquet('/opt/spark-apps/data/tabla1.parquet')

df1 = df1.withColumn("fecha",to_timestamp("fecha"))

## Guardar en Postgres
df1.write.jdbc(url_write, table_name_write1, mode="overwrite", properties=properties)
