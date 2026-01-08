# spark/jobs/bronze/bronze_cerdos_ingest.py

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

DOMAIN = "cerdos"

RAW_BASE = os.getenv("RAW_BASE", "data/raw")
BRONZE_BASE = os.getenv("BRONZE_BASE", "data/bronze")

def main(execution_date: str):

    spark = (
        SparkSession.builder
        .appName(f"bronze-{DOMAIN}")
        .getOrCreate()
    )

    raw_path = f"{RAW_BASE}/{DOMAIN}/date={execution_date}"
    bronze_path = f"{BRONZE_BASE}/{DOMAIN}/date={execution_date}"

    # 1️⃣ LEER RAW (CSV)
    df_raw = (
        spark.read
        .option("header", "true")
        .csv(raw_path)
    )

    if df_raw.count() == 0:
        raise RuntimeError("RAW cerdos vacío — no se puede generar BRONZE")

    # 2️⃣ Normalización mínima (BRONZE = datos crudos confiables)
    df_bronze = (
        df_raw
        .withColumn("event_id", col("event_id"))
        .withColumn("event_timestamp", col("event_timestamp"))
        .withColumn("evento", col("evento"))
        .withColumn("lote_id", col("lote_id"))
        .withColumn("granja", col("granja"))
        .withColumn("cantidad_animales", col("cantidad_animales"))
        .withColumn("peso_promedio_kg", col("peso_promedio_kg"))
        .withColumn("destino", col("destino"))
    )

    # 3️⃣ ESCRIBIR BRONZE (PARQUET)
    (
        df_bronze
        .write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    print(f"[OK] BRONZE CERDOS generado → {bronze_path}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("Debe indicarse execution_date (YYYY-MM-DD)")
    main(sys.argv[1])
