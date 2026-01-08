# spark/jobs/silver/silver_insumos_transform.py
# CORREGIDO — alinea contrato con BRONZE (lowercase + timestamps ya parseables)

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

DOMAIN = "insumos"

BRONZE_BASE = "data/bronze"
SILVER_BASE = "data/silver"

# BRONZE guarda tipo_movimiento en lowercase
VALID_TIPOS = ["compra", "transferencia", "venta", "consumo"]

BRONZE_SCHEMA = StructType([
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

    if df_bronze.count() == 0:
        raise RuntimeError("❌ BRONZE vacío")

    df_silver = (
        df_bronze
        .filter(col("event_id").isNotNull())
        .filter(col("tipo_movimiento").isin(VALID_TIPOS))
        .filter(col("cantidad").isNotNull() & (col("cantidad") > 0))
        .withColumn("tipo_movimiento", upper(col("tipo_movimiento")))
        .select(
            col("event_id"),
            col("event_timestamp").cast("timestamp").alias("event_ts"),
            col("tipo_movimiento"),
            col("insumo"),
            col("categoria"),
            col("cantidad"),
            col("unidad_medida"),
            col("origen"),
            col("destino"),
            col("sucursal")
        )
    )

    if df_silver.count() == 0:
        raise RuntimeError("❌ SILVER sin registros válidos")

    (
        df_silver
        .write
        .mode("overwrite")
        .parquet(silver_path)
    )

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit silver_insumos_transform.py YYYY-MM-DD")

    main(sys.argv[1])
