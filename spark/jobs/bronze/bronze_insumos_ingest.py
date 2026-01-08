import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, lit, try_to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

# ------------------------
# Configuración dominio
# ------------------------

DOMAIN = "insumos"

RAW_BASE = "data/raw"
BRONZE_BASE = "data/bronze"

TIPOS_VALIDOS = ["compra", "transferencia", "venta", "consumo"]
CATEGORIAS_VALIDAS = ["fertilizante", "agroquímico", "semilla"]
UNIDADES_VALIDAS = ["kg", "lt", "bolsas"]

# ------------------------
# Schema RAW (contrato real)
# ------------------------

RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("unidad_negocio", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("tipo_movimiento", StringType(), True),
    StructField("insumo", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("unidad_medida", StringType(), True),
    StructField("origen", StringType(), True),
    StructField("destino", StringType(), True),
    StructField("sucursal", StringType(), True),
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
    # Leer metadata RAW
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
    # Normalización + parsing tolerante
    # ------------------------

    df_norm = (
        df_raw
        .withColumn("tipo_movimiento", lower(trim(col("tipo_movimiento"))))
        .withColumn("categoria", lower(trim(col("categoria"))))
        .withColumn("unidad_medida", lower(trim(col("unidad_medida"))))
        .withColumn(
            "event_ts",
            try_to_timestamp(col("event_timestamp"))
        )
        .withColumn(
            "created_at_ts",
            try_to_timestamp(col("created_at"))
        )
        .withColumn("execution_date", lit(execution_date))
    )

    # ------------------------
    # Reglas duras BRONZE
    # ------------------------

    valid_condition = (
        col("event_id").isNotNull() &
        col("event_ts").isNotNull() &
        col("created_at_ts").isNotNull() &
        (col("unidad_negocio") == "insumos") &
        (col("tipo_evento") == "insumo_movement") &
        col("tipo_movimiento").isin(TIPOS_VALIDOS) &
        col("categoria").isin(CATEGORIAS_VALIDAS) &
        col("unidad_medida").isin(UNIDADES_VALIDAS) &
        col("cantidad").isNotNull() &
        (col("cantidad") > 0)
    )

    df_valid = df_norm.filter(valid_condition)
    valid_rows = df_valid.count()
    rejected_rows = total_rows - valid_rows

    # ------------------------
    # Auditoría empresarial
    # ------------------------

    print("===================================")
    print(f"BRONZE INGEST — {DOMAIN}")
    print(f"Fecha ejecución: {execution_date}")
    print("-----------------------------------")
    print(f"Filas RAW:        {total_rows}")
    print(f"Filas válidas:    {valid_rows}")
    print(f"Filas descartadas:{rejected_rows}")
    print("===================================")

    # ------------------------
    # Escritura BRONZE (contrato firme)
    # ------------------------

    (
        df_valid
        .write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    if valid_rows == 0:
        print("⚠️ BRONZE sin registros válidos — partición vacía generada")
    else:
        print(f"[OK] BRONZE generado → {bronze_path}")

    spark.stop()

# ------------------------
# Entry
# ------------------------

if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise ValueError(
            "Uso: spark-submit bronze_insumos_ingest.py YYYY-MM-DD"
        )

    main(sys.argv[1])
