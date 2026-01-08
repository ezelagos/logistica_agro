import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

DOMAIN = "semillero"
BRONZE_BASE = "data/bronze"
SILVER_BASE = "data/silver"

def main(execution_date: str):
    spark = SparkSession.builder.appName(f"silver-{DOMAIN}").getOrCreate()

    bronze_path = f"{BRONZE_BASE}/{DOMAIN}/date={execution_date}"
    silver_path = f"{SILVER_BASE}/{DOMAIN}/date={execution_date}"

    df = spark.read.parquet(bronze_path)

    df_silver = df.select(
        "event_id",
        col("event_timestamp").cast("timestamp").alias("event_ts"),
        "evento",
        "cultivo",
        "cantidad_bolsas",
        "origen",
        "destino",
        "lote_id"
    )

    df_silver.write.mode("overwrite").parquet(silver_path)
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1])
