import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_timestamp, row_number
)

DOMAIN = "camiones"

BRONZE_BASE = "data/bronze"
SILVER_BASE = "data/silver"

VALID_EVENTOS = [
    "VIAJE_INICIADO",
    "CARGA",
    "DESCARGA",
    "VIAJE_FINALIZADO"
]

def main(execution_date: str):
    spark = (
        SparkSession.builder
        .appName(f"silver-{DOMAIN}")
        .getOrCreate()
    )

    bronze_path = f"{BRONZE_BASE}/{DOMAIN}/date={execution_date}"
    silver_path = f"{SILVER_BASE}/{DOMAIN}/date={execution_date}"

    df = spark.read.parquet(bronze_path)

    df_typed = (
        df
        .withColumn("event_ts", to_timestamp(col("event_timestamp")))
        .filter(col("event_ts").isNotNull())
        .filter(col("evento").isin(VALID_EVENTOS))
        .filter(col("trip_id").isNotNull())
    )

    # DEDUPE fuerte
    w = Window.partitionBy(
        "trip_id", "evento", "event_ts"
    ).orderBy(col("created_at").desc())

    df_dedup = (
        df_typed
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    total_rows = df.count()
    silver_rows = df_dedup.count()

    print("===================================")
    print("SILVER TRANSFORM — CAMIONES")
    print("-----------------------------------")
    print(f"Filas BRONZE: {total_rows}")
    print(f"Filas SILVER: {silver_rows}")
    print("===================================")

    if silver_rows == 0:
        raise RuntimeError("❌ SILVER camiones quedó vacía")

    (
        df_dedup
        .write
        .mode("overwrite")
        .parquet(silver_path)
    )

    print(f"[OK] SILVER generado → {silver_path}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit silver_camiones_transform.py YYYY-MM-DD")

    main(sys.argv[1])
