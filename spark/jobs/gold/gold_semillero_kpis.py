import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as _sum,
    count,
    year,
    month,
    coalesce
)

# ------------------------------------------------------------------------------
# Spark session
# ------------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("gold_semillero_kpis")
    .getOrCreate()
)

# ------------------------------------------------------------------------------
# Logger
# ------------------------------------------------------------------------------
logger = logging.getLogger("gold_semillero")
logger.setLevel(logging.INFO)

fh = logging.FileHandler("logs/gold/semillero.log")
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)
fh.setFormatter(formatter)
logger.addHandler(fh)

logger.info("INICIO GOLD SEMILLERO")

# ------------------------------------------------------------------------------
# Input SILVER
# ------------------------------------------------------------------------------
silver_path = "data/silver/semillero"

df = spark.read.parquet(silver_path)

# ------------------------------------------------------------------------------
# Validación mínima de schema
# ------------------------------------------------------------------------------
required_cols = {
    "event_id",
    "event_ts",
    "evento",
    "cultivo",
    "cantidad_bolsas",
    "origen",
    "destino",
    "lote_id"
}

missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Columnas faltantes en SILVER semillero: {missing}")

# ------------------------------------------------------------------------------
# Enriquecimientos mínimos
# ------------------------------------------------------------------------------
df_base = (
    df
    .withColumn("sucursal", coalesce(col("destino"), col("origen")))
    .withColumn("anio", year(col("event_ts")))
    .withColumn("mes", month(col("event_ts")))
)

# ------------------------------------------------------------------------------
# KPI GENERAL
# ------------------------------------------------------------------------------
kpi_general = (
    df_base
    .groupBy()
    .agg(
        count("*").alias("eventos_totales"),
        _sum("cantidad_bolsas").alias("bolsas_totales")
    )
)

# ------------------------------------------------------------------------------
# KPI POR EVENTO
# ------------------------------------------------------------------------------
kpi_por_evento = (
    df_base
    .groupBy("evento")
    .agg(
        count("*").alias("eventos"),
        _sum("cantidad_bolsas").alias("bolsas")
    )
)

# ------------------------------------------------------------------------------
# KPI POR CULTIVO
# ------------------------------------------------------------------------------
kpi_por_cultivo = (
    df_base
    .groupBy("cultivo")
    .agg(
        count("*").alias("eventos"),
        _sum("cantidad_bolsas").alias("bolsas")
    )
)

# ------------------------------------------------------------------------------
# KPI POR SUCURSAL
# ------------------------------------------------------------------------------
kpi_por_sucursal = (
    df_base
    .groupBy("sucursal")
    .agg(
        count("*").alias("eventos"),
        _sum("cantidad_bolsas").alias("bolsas")
    )
)

# ------------------------------------------------------------------------------
# KPI TEMPORAL
# ------------------------------------------------------------------------------
kpi_temporal = (
    df_base
    .groupBy("anio", "mes")
    .agg(
        count("*").alias("eventos"),
        _sum("cantidad_bolsas").alias("bolsas")
    )
)

# ------------------------------------------------------------------------------
# Escritura GOLD
# ------------------------------------------------------------------------------
output_base = "data/gold/semillero"

kpi_general.write.mode("overwrite").parquet(f"{output_base}/kpi_general")
kpi_por_evento.write.mode("overwrite").parquet(f"{output_base}/kpi_por_evento")
kpi_por_cultivo.write.mode("overwrite").parquet(f"{output_base}/kpi_por_cultivo")
kpi_por_sucursal.write.mode("overwrite").parquet(f"{output_base}/kpi_por_sucursal")
kpi_temporal.write.mode("overwrite").parquet(f"{output_base}/kpi_temporal")

logger.info("                           ")
logger.info("GOLD SEMILLERO FINALIZADO OK")
logger.info("                           ")

spark.stop()
