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


DOMAIN = "cerdos"


def main(execution_date: str):
    logger = setup_logger(DOMAIN)
    logger.info("INICIO GOLD CERDOS")

    spark = SparkSession.builder.appName(f"gold-{DOMAIN}").getOrCreate()

    silver_path = f"data/silver/{DOMAIN}/date={execution_date}"
    gold_base = f"data/gold/{DOMAIN}/date={execution_date}"

    df = spark.read.parquet(silver_path)

    if df.count() == 0:
        raise RuntimeError("SILVER vacío — contrato violado")

    df = (
        df
        .withColumn("peso_kg", col("peso_promedio_kg").cast("double"))
        .withColumn("toneladas", col("peso_kg") / 1000)
    )

    # =========================
    # KPIS PRODUCCION
    # =========================
    kpis_produccion = df.agg(
        count("*").alias("total_eventos"),
        sum("cantidad_animales").alias("animales_totales"),
        sum("peso_kg").alias("kg_totales"),
        sum("toneladas").alias("toneladas_totales"),
        avg("peso_kg").alias("kg_promedio_evento")
    )

    # =========================
    # KPIS POR EVENTO
    # =========================
    kpis_por_evento = (
        df.groupBy("evento")
        .agg(
            count("*").alias("eventos"),
            sum("cantidad_animales").alias("animales"),
            sum("toneladas").alias("toneladas")
        )
    )

    # =========================
    # KPIS POR GRANJA
    # =========================
    kpis_por_granja = (
        df.groupBy("granja")
        .agg(
            count("*").alias("eventos"),
            sum("cantidad_animales").alias("animales"),
            sum("toneladas").alias("toneladas")
        )
    )

    # =========================
    # INSIGHTS
    # =========================
    insights = (
        df.groupBy("granja")
        .agg(
            avg("peso_kg").alias("peso_promedio_kg"),
            avg("cantidad_animales").alias("animales_promedio")
        )
    )

    # =========================
    # WRITE GOLD (PATHS EXPLÍCITOS)
    # =========================
    kpis_produccion.write.mode("overwrite").parquet(f"{gold_base}/kpis_produccion")
    kpis_por_evento.write.mode("overwrite").parquet(f"{gold_base}/kpis_por_evento")
    kpis_por_granja.write.mode("overwrite").parquet(f"{gold_base}/kpis_por_granja")
    insights.write.mode("overwrite").parquet(f"{gold_base}/insights")

    logger.info("                           ")
    logger.info("GOLD CERDOS FINALIZADO OK")
    logger.info("                           ")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit gold_cerdos_kpis.py YYYY-MM-DD")

    main(sys.argv[1])
