from pyspark.sql import SparkSession
import os
spark = SparkSession.builder \
    .appName("Postgres") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.memory", "3g") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

def main():
    #print("SCALA VERSION: ", spark.sparkContext._gateway.jvm.scala.util.Properties.versionString())
    url_read = os.environ['url_postgres'] + "/Monitorizacion"
    
    query = """SELECT Fecha, Cuadro as Inversor,
        i1 as Is1, i2 as Is2, i3 as Is3, i4 as Is4, i5 as Is5, i6 as Is6,
        i7 as Is7, i8 as Is8, I9 as Is9, I10 as Is10, I11 as Is11, I12 as Is12,
        i13 as Is13, i14 as Is14, I15 as Is15, I16 as Is16, I17 as Is17, I18 as Is18
        FROM public.fv_pavasal_cheste_strings
        where extract(year from fecha) = 2024
        order by Fecha, Cuadro"""
    
    df = spark.read \
        .format("jdbc") \
        .option("url", url_read) \
        .option("user", os.environ['user_postgres']) \
        .option("password", os.environ['password_postgres']) \
        .option("query", query) \
        .load()
    
    df = df.withColumn("Fecha",to_timestamp("Fecha"))
    df.show()

if __name__ == "__main__":
    main()
