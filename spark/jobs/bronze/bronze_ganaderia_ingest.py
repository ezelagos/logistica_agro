import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

# ------------------------
# Configuración
# ------------------------

DOMAIN = "ganaderia"

RAW_BASE = "data/raw"
BRONZE_BASE = "data/bronze"

VALID_EVENT_TYPES = ["NACIMIENTO", "TRASLADO", "VENTA", "FAENA", "MUERTE"]
VALID_CATEGORIES = ["Ternero", "Novillo", "Vaquillona", "Vaca", "Toro"]

# ------------------------
# Schema explícito
# ------------------------

RAW_SCHEMA = StructType([
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

# ------------------------
# Main
# ------------------------

def main(execution_date: str):

    spark = (
        SparkSession.builder
        .appName(f"bronze-{DOMAIN}")
        .getOrCreate()
    )

    raw_path = f"{RAW_BASE}/{DOMAIN}/date={execution_date}"
    bronze_path = f"{BRONZE_BASE}/{DOMAIN}/date={execution_date}"

    # ------------------------
    # Leer metadata
    # ------------------------

    with open(f"{raw_path}/_metadata.json") as f:
        metadata = json.load(f)

    assert metadata["format"] == "csv", "Formato RAW inválido"

    # ------------------------
    # Leer RAW
    # ------------------------

    df_raw = (
        spark.read
        .schema(RAW_SCHEMA)
        .option("header", True)
        .csv(f"{raw_path}/data.csv")
    )

    total_rows = df_raw.count()

    # ------------------------
    # Validaciones duras BRONZE
    # ------------------------

    df_valid = (
        df_raw
        .withColumn(
            "event_timestamp_parsed",
            to_timestamp(col("event_timestamp"))
        )
        .filter(col("event_id").isNotNull())
        .filter(col("animal_id").isNotNull() & (col("animal_id") != ""))
        .filter(col("tipo_movimiento").isin(VALID_EVENT_TYPES))
        .filter(col("categoria").isin(VALID_CATEGORIES))
        .filter(col("peso_kg").isNotNull() & (col("peso_kg") > 0))
        .filter(col("event_timestamp_parsed").isNotNull())
        .drop("event_timestamp_parsed")
    )

    valid_rows = df_valid.count()
    rejected_rows = total_rows - valid_rows

    # ------------------------
    # Log empresarial
    # ------------------------

    print("===================================")
    print(f"BRONZE INGEST — {DOMAIN}")
    print(f"Fecha ejecución: {execution_date}")
    print("-----------------------------------")
    print(f"Filas RAW leídas:  {total_rows}")
    print(f"Filas válidas:     {valid_rows}")
    print(f"Filas descartadas: {rejected_rows}")
    print("===================================")

    if valid_rows == 0:
        print("⚠️ BRONZE sin registros válidos — se genera partición vacía por contrato")

    # ------------------------
    # Escritura BRONZE
    # ------------------------

    (
        df_valid
        .write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    print(f"[OK] BRONZE generado → {bronze_path}")

    spark.stop()

# ------------------------
# Entry
# ------------------------

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit bronze_ganaderia_ingest.py YYYY-MM-DD")

    main(sys.argv[1])
