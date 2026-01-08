import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, col

# =========================
# LOGGER LOCAL
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


DOMAIN = "cereales"


def main(execution_date: str):
    logger = setup_logger(DOMAIN)
    logger.info("INICIO GOLD cereales")

    spark = SparkSession.builder.appName(f"gold-{DOMAIN}").getOrCreate()

    silver_path = f"data/silver/{DOMAIN}/date={execution_date}"
    gold_path = f"data/gold/{DOMAIN}/date={execution_date}"

    logger.info(f"Leyendo SILVER desde {silver_path}")
    df = spark.read.parquet(silver_path)

    total = df.count()
    logger.info(f"Filas leídas desde SILVER: {total}")

    if total == 0:
        logger.error("SILVER vacío — abortando GOLD")
        raise RuntimeError("SILVER vacío")

    # =========================
    # NORMALIZACIÓN DE UNIDAD
    # =========================
    logger.info("Convirtiendo peso_kg → toneladas")
    df = df.withColumn("toneladas", col("peso_kg") / 1000)

    # =========================
    # KPIs GENERALES
    # =========================
    logger.info("Calculando KPIs generales")
    resumen = df.agg(
        count("*").alias("total_movimientos"),
        sum("toneladas").alias("toneladas_totales"),
        avg("toneladas").alias("toneladas_promedio")
    )

    # =========================
    # KPIs POR CEREAL
    # =========================
    logger.info("Calculando KPIs por cereal")
    por_cereal = (
        df.groupBy("cereal")
        .agg(
            count("*").alias("movimientos"),
            sum("toneladas").alias("toneladas_totales")
        )
    )

    # =========================
    # KPIs POR MOVIMIENTO
    # =========================
    logger.info("Calculando KPIs por tipo de movimiento")
    por_movimiento = (
        df.groupBy("tipo_movimiento")
        .agg(
            count("*").alias("cantidad_eventos"),
            sum("toneladas").alias("toneladas_totales")
        )
    )

    # =========================
    # ESCRITURA GOLD
    # =========================
    logger.info("Escribiendo GOLD resumen_general")
    resumen.write.mode("overwrite").parquet(f"{gold_path}/resumen_general")

    logger.info("Escribiendo GOLD por_cereal")
    por_cereal.write.mode("overwrite").parquet(f"{gold_path}/por_cereal")

    logger.info("Escribiendo GOLD por_movimiento")
    por_movimiento.write.mode("overwrite").parquet(f"{gold_path}/por_movimiento")

    logger.info("                           ")
    logger.info("GOLD CEREALES FINALIZADO OK")
    logger.info("                           ")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit gold_cereales_kpis.py YYYY-MM-DD")

    main(sys.argv[1])
