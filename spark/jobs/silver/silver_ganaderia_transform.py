# spark/jobs/silver/silver_ganaderia_transform.py

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

DOMAIN = "ganaderia"

BRONZE_BASE = "data/bronze"
SILVER_BASE = "data/silver"

VALID_EVENTOS = ["NACIMIENTO", "TRASLADO", "VENTA", "FAENA", "MUERTE"]

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("unidad_negocio", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("tipo_movimiento", StringType(), True),
    StructField("animal_id", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("peso_kg", IntegerType(), True),
    StructField("origen", StringType(), True),
    StructField("destino", StringType(), True),
    StructField("establecimiento", StringType(), True),
    StructField("created_at", StringType(), True),
])

def main(execution_date: str):

    spark = (
        SparkSession.builder
        .appName(f"silver-{DOMAIN}")
        .getOrCreate()
    )

    bronze_path = f"{BRONZE_BASE}/{DOMAIN}/date={execution_date}"
    silver_path = f"{SILVER_BASE}/{DOMAIN}/date={execution_date}"

    df_bronze = (
        spark.read
        .schema(BRONZE_SCHEMA)
        .parquet(bronze_path)
    )

    total_rows = df_bronze.count()
    if total_rows == 0:
        raise RuntimeError("❌ BRONZE vacío — no se genera SILVER")

    df_silver = (
        df_bronze
        .withColumn("event_ts", to_timestamp(col("event_timestamp")))
        .filter(col("event_id").isNotNull())
        .filter(col("animal_id").isNotNull())
        .filter(col("tipo_movimiento").isin(VALID_EVENTOS))
        .filter(col("event_ts").isNotNull())
        .select(
            "event_id",
            "animal_id",
            "event_ts",
            "tipo_movimiento",
            "categoria",
            "peso_kg",
            "origen",
            "destino",
            "establecimiento"
        )
    )

    valid_rows = df_silver.count()
    if valid_rows == 0:
        raise RuntimeError("❌ SILVER sin registros válidos")

    (
        df_silver
        .write
        .mode("overwrite")
        .parquet(silver_path)
    )

    print(f"[OK] SILVER CREADO — {DOMAIN} → {silver_path}")
    print(f"BRONZE: {total_rows} | SILVER: {valid_rows}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit silver_ganaderia_transform.py YYYY-MM-DD")

    main(sys.argv[1])
