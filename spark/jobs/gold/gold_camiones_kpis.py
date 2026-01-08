import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum

# =========================
# LOGGER LOCAL (spark-safe)
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


DOMAIN = "camiones"


def main(execution_date: str):
    logger = setup_logger(DOMAIN)
    logger.info("INICIO GOLD camiones")

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

    logger.info("Calculando KPIs")

    resumen = df.agg(
        count("*").alias("total_eventos"),
        sum("peso_kg").alias("peso_total_kg_transportado")
    )

    por_evento = df.groupBy("evento").count()

    logger.info("Escribiendo GOLD resumen_general")
    resumen.write.mode("overwrite").parquet(
        f"{gold_path}/resumen_general"
    )

    logger.info("Escribiendo GOLD por_evento")
    por_evento.write.mode("overwrite").parquet(
        f"{gold_path}/por_evento"
    )

    logger.info("                           ")
    logger.info("GOLD camiones FINALIZADO OK")
    logger.info("                           ")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Uso: spark-submit gold_camiones_kpis.py YYYY-MM-DD")

    main(sys.argv[1])
