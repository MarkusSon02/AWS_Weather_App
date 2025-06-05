from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession (this replaces the older SparkContext + SQLContext setup)
spark = SparkSession.builder \
    .appName("transform") \
    .getOrCreate()

df = spark.read.json("Edmonton_weather_2025-01-01.json")
df = df.selectExpr('location.*', 'historical')

df = df.withColumn("filename", input_file_name())
df = df.withColumn("raw_filename", element_at(split(col("filename"), "/"), -1))
df = df.withColumn("raw_date", element_at(split(col("raw_filename"), "_"), -1))
df = df.withColumn("date", element_at(split(col("raw_date"), "\\."), 1))

df.select("filename", "raw_filename", "raw_date", "date").show(truncate=False)
df.printSchema()


df = df.selectExpr('location.*', 'historical.' + df['date'])
