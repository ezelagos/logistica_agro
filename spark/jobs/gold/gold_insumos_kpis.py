# spark/jobs/gold/gold_insumos_kpis.py

import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    count,
    countDistinct,
    to_date
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


DOMAIN = "insumos"


def main(execution_date: str):
    logger = setup_logger(DOMAIN)
    logger.info("INICIO GOLD INSUMOS")

    spark = SparkSession.builder.appName(f"gold-{DOMAIN}").getOrCreate()

    silver_path = f"data/silver/{DOMAIN}/date={execution_date}"
    gold_path = f"data/gold/{DOMAIN}/date={execution_date}"

    logger.info(f"Leyendo SILVER desde {silver_path}")
    df = spark.read.parquet(silver_path)

    if df.count() == 0:
        logger.error("SILVER vacío — abortando GOLD")
        raise RuntimeError("SILVER vacío")

    # =========================
    # NORMALIZACIÓN
    # =========================
    df = (
        df
        .withColumn("fecha", to_date(col("event_ts")))
        .filter(col("cantidad").isNotNull() & (col("cantidad") > 0))
    )

    # =========================
    # KPI GENERAL
    # =========================
    kpi_general = df.agg(
        count("*").alias("total_eventos"),
        countDistinct("insumo").alias("insumos_distintos"),
        sum("cantidad").alias("cantidad_total")
    )

    # =========================
    # KPI POR CATEGORIA
    # =========================
    kpi_categoria = (
        df.groupBy("categoria")
        .agg(
            count("*").alias("eventos"),
            sum("cantidad").alias("cantidad_total")
        )
        .orderBy(col("cantidad_total").desc())
    )

    # =========================
    # KPI POR INSUMO
    # =========================
    kpi_insumo = (
        df.groupBy("insumo", "unidad_medida")
        .agg(
            count("*").alias("eventos"),
            sum("cantidad").alias("cantidad_total")
        )
        .orderBy(col("cantidad_total").desc())
    )

    # =========================
    # KPI POR TIPO MOVIMIENTO
    # =========================
    kpi_movimiento = (
        df.groupBy("tipo_movimiento")
        .agg(
            count("*").alias("eventos"),
            sum("cantidad").alias("cantidad_total")
        )
    )

    # =========================
    # KPI POR SUCURSAL
    # =========================
    kpi_sucursal = (
        df.groupBy("sucursal")
        .agg(
            count("*").alias("eventos"),
            sum("cantidad").alias("cantidad_total")
        )
        .orderBy(col("cantidad_total").desc())
    )

    # =========================
    # KPI ORIGEN / DESTINO
    # =========================
    kpi_origen = (
        df.groupBy("origen")
        .agg(
            count("*").alias("eventos"),
            sum("cantidad").alias("cantidad_total")
        )
    )

    kpi_destino = (
        df.groupBy("destino")
        .agg(
            count("*").alias("eventos"),
            sum("cantidad").alias("cantidad_total")
        )
    )

    # =========================
    # KPI TEMPORAL
    # =========================
    kpi_diario = (
        df.groupBy("fecha")
        .agg(
            count("*").alias("eventos"),
            sum("cantidad").alias("cantidad_total")
        )
        .orderBy("fecha")
    )

    # =========================
    # WRITE GOLD
    # =========================
    kpi_general.write.mode("overwrite").parquet(f"{gold_path}/kpi_general")
    kpi_categoria.write.mode("overwrite").parquet(f"{gold_path}/kpi_categoria")
    kpi_insumo.write.mode("overwrite").parquet(f"{gold_path}/kpi_insumo")
    kpi_movimiento.write.mode("overwrite").parquet(f"{gold_path}/kpi_movimiento")
    kpi_sucursal.write.mode("overwrite").parquet(f"{gold_path}/kpi_sucursal")
    kpi_origen.write.mode("overwrite").parquet(f"{gold_path}/kpi_origen")
    kpi_destino.write.mode("overwrite").parquet(f"{gold_path}/kpi_destino")
    kpi_diario.write.mode("overwrite").parquet(f"{gold_path}/kpi_diario")

    logger.info("                           ")
    logger.info("GOLD INSUMOS FINALIZADO OK")
    logger.info("                           ")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit gold_insumos_kpis.py YYYY-MM-DD")

    main(sys.argv[1])
