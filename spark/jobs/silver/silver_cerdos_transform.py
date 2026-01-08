import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

DOMAIN = "cerdos"

BRONZE_BASE = "data/bronze"
SILVER_BASE = "data/silver"

def main(execution_date: str):

    spark = SparkSession.builder.appName(f"silver-{DOMAIN}").getOrCreate()

    bronze_path = f"{BRONZE_BASE}/{DOMAIN}/date={execution_date}"
    silver_path = f"{SILVER_BASE}/{DOMAIN}/date={execution_date}"

    df = spark.read.parquet(bronze_path)

    required_cols = {"peso_promedio_kg", "cantidad_animales"}
    if not required_cols.issubset(df.columns):
        raise RuntimeError("Contrato BRONZE inválido para cerdos")

    df_silver = (
        df
        .withColumn("event_ts", to_timestamp(col("event_timestamp")))
        .filter(col("peso_promedio_kg") > 0)
        .filter(col("cantidad_animales") > 0)
        .select(
            "event_id",
            "lote_id",
            "event_ts",
            "evento",
            "granja",
            "cantidad_animales",
            "peso_promedio_kg",
            "destino"
        )
    )

    if df_silver.count() == 0:
        raise RuntimeError("SILVER cerdos vacío")

    df_silver.write.mode("overwrite").parquet(silver_path)

    print(f"[OK] SILVER cerdos generado → {silver_path}")
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1])
