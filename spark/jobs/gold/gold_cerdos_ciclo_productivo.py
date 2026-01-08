import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    min as min_,
    max as max_,
    datediff,
    count,
    avg
)

# =========================
# LOGGER
# =========================
def setup_logger(domain: str):
    os.makedirs("logs/gold", exist_ok=True)

    logger = logging.getLogger(domain)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(f"logs/gold/{domain}.log")
    fh.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger


DOMAIN = "cerdos"
GOLD_NAME = "ciclo_productivo"


def main(execution_date: str):
    logger = setup_logger(f"{DOMAIN}_{GOLD_NAME}")
    logger.info("INICIO GOLD ciclo_productivo")

    spark = SparkSession.builder.appName(f"gold-{DOMAIN}-{GOLD_NAME}").getOrCreate()

    silver_path = f"data/silver/{DOMAIN}/date={execution_date}"
    gold_path = f"data/gold/{DOMAIN}/date={execution_date}/{GOLD_NAME}"

    logger.info(f"Leyendo SILVER desde {silver_path}")
    df = spark.read.parquet(silver_path)

    required_cols = [
        "lote_id",
        "evento",
        "event_ts",
        "granja",
        "cantidad_animales"
    ]

    for c in required_cols:
        if c not in df.columns:
            raise RuntimeError(f"Columna faltante en SILVER: {c}")

    # =========================
    # CICLO POR LOTE
    # =========================
    ciclo_lote = (
        df
        .filter(col("evento").isin("NACIMIENTO", "VENTA"))
        .groupBy("lote_id")
        .agg(
            min_(col("event_ts")).alias("inicio_ciclo"),
            max_(col("event_ts")).alias("fin_ciclo"),
            datediff(max_(col("event_ts")), min_(col("event_ts"))).alias("dias_ciclo"),
            max_(col("cantidad_animales")).alias("animales_lote")
        )
        .filter(col("dias_ciclo").isNotNull())
        .filter(col("dias_ciclo") >= 0)
    )

    if ciclo_lote.count() == 0:
        raise RuntimeError("No se pudieron calcular ciclos productivos")

    # =========================
    # KPIs GENERALES
    # =========================
    kpis_ciclo = (
        ciclo_lote
        .agg(
            count("*").alias("lotes_analizados"),
            avg("dias_ciclo").alias("dias_promedio_ciclo"),
            avg("animales_lote").alias("animales_promedio_lote")
        )
    )

    # =========================
    # WRITE GOLD
    # =========================
    ciclo_lote.write.mode("overwrite").parquet(f"{gold_path}/por_lote")
    kpis_ciclo.write.mode("overwrite").parquet(f"{gold_path}/resumen")

    logger.info("GOLD ciclo_productivo FINALIZADO OK")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit gold_cerdos_ciclo_productivo.py YYYY-MM-DD")

    main(sys.argv[1])
