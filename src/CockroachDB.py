from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import concat,concat_ws,to_timestamp
from pyspark.sql import functions as F
import os

## Create the SparkSession builder
spark = SparkSession.builder \
    .appName("Postgres") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

## CockroachDB Config
properties = {
    "user": os.environ['user_CockroachDB'],
    "password": os.environ['password_CockroachDB'],
    "driver": "org.postgresql.Driver"
}

url_read = os.environ['url_CockroachDB']
table_name_read = "public.fv_pavasal_vara_historico"

df = spark.read.jdbc(url_read, table_name_read, properties=properties)

df.show()
