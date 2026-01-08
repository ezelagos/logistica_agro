import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

# ------------------------
# Config
# ------------------------

DOMAIN = "cereales"

RAW_BASE = "data/raw"
BRONZE_BASE = "data/bronze"

VALID_TIPOS = ["INGRESO", "EGRESO"]
VALID_CEREALES = ["Soja", "Maíz", "Trigo", "Girasol"]

# ------------------------
# Schema RAW
# ------------------------

RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("unidad_negocio", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("tipo_movimiento", StringType(), True),
    StructField("cereal", StringType(), True),
    StructField("peso_kg", IntegerType(), True),
    StructField("origen", StringType(), True),
    StructField("destino", StringType(), True),
    StructField("planta_acopio", StringType(), True),
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
    # Metadata
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
    # Validaciones BRONZE
    # ------------------------

    df_valid = (
        df_raw
        .withColumn("event_ts", to_timestamp(col("event_timestamp")))
        .filter(col("event_id").isNotNull())
        .filter(col("tipo_movimiento").isin(VALID_TIPOS))
        .filter(col("cereal").isin(VALID_CEREALES))
        .filter(col("peso_kg").isNotNull())
        .filter(col("planta_acopio").isNotNull())
        .filter(col("origen").isNotNull())
        .filter(col("destino").isNotNull())
        .filter(col("event_ts").isNotNull())
        .drop("event_ts")
    )

    valid_rows = df_valid.count()
    rejected_rows = total_rows - valid_rows

    print("===================================")
    print(f"BRONZE INGEST — {DOMAIN}")
    print(f"Fecha ejecución: {execution_date}")
    print("-----------------------------------")
    print(f"Filas RAW leídas:  {total_rows}")
    print(f"Filas válidas:     {valid_rows}")
    print(f"Filas descartadas: {rejected_rows}")
    print("===================================")

    if valid_rows == 0:
        print("⚠️ BRONZE cereales vacío — se genera partición vacía por contrato")

    (
        df_valid
        .write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    print(f"[OK] BRONZE generado → {bronze_path}")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit bronze_cereales_ingest.py YYYY-MM-DD")

    main(sys.argv[1])
