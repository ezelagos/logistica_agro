import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

DOMAIN = "cerdos"

def setup_logger(domain: str):
    os.makedirs("logs/gold", exist_ok=True)
    logger = logging.getLogger(domain + "_mortandad")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(f"logs/gold/{domain}_mortandad.log")
    fh.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger


def main(execution_date: str):
    logger = setup_logger(DOMAIN)
    logger.info("INICIO GOLD mortandad")

    spark = SparkSession.builder.appName("gold-cerdos-mortandad").getOrCreate()

    silver_path = f"data/silver/{DOMAIN}/date={execution_date}"
    gold_path = f"data/gold/{DOMAIN}/date={execution_date}/mortandad"

    df = spark.read.parquet(silver_path)

    df_mort = (
        df.filter(col("evento") == "SANIDAD")
        .groupBy("lote_id", "granja")
        .agg(
            _sum("cantidad_animales").alias("animales_afectados")
        )
    )

    if df_mort.count() == 0:
        raise RuntimeError("mortandad vacía")

    df_mort.write.mode("overwrite").parquet(gold_path)

    logger.info("                           ")
    logger.info("GOLD CERDOS MORTANDAD FINALIZADO OK")
    logger.info("                           ")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1])
