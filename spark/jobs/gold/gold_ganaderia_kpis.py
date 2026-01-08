import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg

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


DOMAIN = "ganaderia"


def main(execution_date: str):
    logger = setup_logger("ganaderia_kpis")
    logger.info("INICIO GOLD GANADERIA")

    spark = SparkSession.builder.appName(f"gold-{DOMAIN}").getOrCreate()

    silver_path = f"data/silver/{DOMAIN}/date={execution_date}"
    gold_path = f"data/gold/{DOMAIN}/date={execution_date}"

    logger.info(f"Leyendo SILVER desde {silver_path}")
    df = spark.read.parquet(silver_path)

    if df.count() == 0:
        raise RuntimeError("SILVER GANADERIA vacío — no se puede generar GOLD")

    # =========================
    # NORMALIZACIÓN
    # =========================
    df = (
        df
        .withColumn("peso_kg", col("peso_kg").cast("double"))
        .withColumn("toneladas", col("peso_kg") / 1000)
        .filter(col("peso_kg").isNotNull())
    )

    # =========================
    # KPI GENERAL
    # =========================
    kpis_generales = df.agg(
        count("*").alias("total_eventos"),
        count("animal_id").alias("total_animales"),
        sum("peso_kg").alias("kg_totales"),
        sum("toneladas").alias("toneladas_totales"),
        avg("peso_kg").alias("kg_promedio_evento")
    )

    # =========================
    # KPI POR MOVIMIENTO
    # =========================
    kpis_movimiento = (
        df.groupBy("tipo_movimiento")
        .agg(
            count("*").alias("eventos"),
            sum("peso_kg").alias("kg_totales"),
            avg("peso_kg").alias("kg_promedio")
        )
    )

    # =========================
    # KPI POR CATEGORIA
    # =========================
    kpis_categoria = (
        df.groupBy("categoria")
        .agg(
            count("animal_id").alias("animales"),
            avg("peso_kg").alias("peso_promedio_kg"),
            sum("peso_kg").alias("kg_totales")
        )
    )

    # =========================
    # KPI ORIGEN / DESTINO
    # =========================
    kpis_origen = (
        df.groupBy("origen")
        .agg(
            count("*").alias("eventos"),
            sum("toneladas").alias("toneladas_totales")
        )
    )

    kpis_destino = (
        df.groupBy("destino")
        .agg(
            count("*").alias("eventos"),
            sum("toneladas").alias("toneladas_totales")
        )
    )

    # =========================
    # KPI ESTABLECIMIENTO
    # =========================
    kpis_establecimiento = (
        df.groupBy("establecimiento")
        .agg(
            count("animal_id").alias("animales"),
            avg("peso_kg").alias("peso_promedio_kg"),
            sum("toneladas").alias("toneladas_totales")
        )
    )

    # =========================
    # WRITE GOLD
    # =========================
    kpis_generales.write.mode("overwrite").parquet(f"{gold_path}/kpis_generales")
    kpis_movimiento.write.mode("overwrite").parquet(f"{gold_path}/kpis_movimiento")
    kpis_categoria.write.mode("overwrite").parquet(f"{gold_path}/kpis_categoria")
    kpis_origen.write.mode("overwrite").parquet(f"{gold_path}/kpis_origen")
    kpis_destino.write.mode("overwrite").parquet(f"{gold_path}/kpis_destino")
    kpis_establecimiento.write.mode("overwrite").parquet(f"{gold_path}/kpis_establecimiento")

    logger.info("                           ")
    logger.info("GOLD GANADERIA FINALIZADO OK")
    logger.info("                           ")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit gold_ganaderia_kpis.py YYYY-MM-DD")

    main(sys.argv[1])
