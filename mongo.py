from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import concat,concat_ws,to_timestamp
from pyspark.sql import functions as F
import os

## Create the SparkSession builder
spark = SparkSession \
    .builder \
    .appName("Mongo") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.memory", "3g") \
    .config("spark.mongodb.read.connection.uri", os.environ['mongo_url']) \
    .config("spark.mongodb.write.connection.uri", os.environ['mongo_url']) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.3.0') \
    .getOrCreate()

def ConsultarDF():
    df = spark.read.format("mongodb") \
        .option("database", "monitorizacion") \
        .option("collection", "fv_apv") \
        .load()
    
    #df.printSchema()

    df.createOrReplaceTempView("Consulta1")
    df.cache()

    query = spark.sql(f"""
        SELECT date(Fecha) as Fecha, hour(Fecha) as Hora, CONCAT(date(Fecha), ' ', hour(Fecha), ':00:00') as Fecha2, inversor as Inversor,
        round(max(ea_diaria)-min(ea_diaria),1) as EA,
        round(avg(is1),1) as Is1, round(avg(is2),1) as Is2, round(avg(is3),1) as Is3, round(avg(is4),1) as Is4, round(avg(is5),1) as Is5,
        round(avg(is6),1) as Is6, round(avg(is7),1) as Is7, round(avg(is8),1) as Is8, round(avg(is9),1) as Is9, round(avg(is10),1) as Is10,
        round(avg(is11),1) as Is11, round(avg(is12),1) as Is12, round(avg(is13),1) as Is13, round(avg(is14),1) as Is14, round(avg(is15),1) as Is15,
        round(avg(is16),1) as Is16, round(avg(is17),1) as Is17, round(avg(is18),1) as Is18
        FROM Consulta1 
        where month(Fecha) = 5
        group by date(Fecha), hour(Fecha), inversor
        order by date(Fecha), hour(Fecha), inversor
        """)
    
    query = query.drop('Fecha', 'Hora')
    query = query.withColumn("Fecha2",to_timestamp("Fecha2"))
    query = query.withColumnRenamed("Fecha2", "Fecha")
    
    return(query)

def main():
    print("SCALA VERSION: ", spark.sparkContext._gateway.jvm.scala.util.Properties.versionString())
    df = ConsultarDF()
    df.show()

    df.write.parquet ("/opt/spark-apps/data/tabla1.parquet")

if __name__ == "__main__":
    main()
