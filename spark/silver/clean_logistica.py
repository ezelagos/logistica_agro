from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("silver_logistica").getOrCreate()

df = spark.read.option("header", True).csv("data/bronze/logistica.csv")

df_clean = (
    df
    .dropna()
    .withColumn("cantidad", col("cantidad").cast("int"))
)

df_clean.write.mode("overwrite").parquet("data/silver/logistica")
