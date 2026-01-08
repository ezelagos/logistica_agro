import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg

DOMAIN = "veterinaria"

def main(execution_date: str):
    spark = SparkSession.builder.appName(f"gold-{DOMAIN}").getOrCreate()

    df = spark.read.parquet(f"data/silver/{DOMAIN}/date={execution_date}")

    resumen = df.agg(
        count("*").alias("tratamientos_totales"),
        avg("dose_ml").alias("dosis_promedio_ml")
    )

    por_medicamento = df.groupBy("medicine").count()

    resumen.write.mode("overwrite").parquet(f"data/gold/{DOMAIN}/date={execution_date}/resumen_general")
    por_medicamento.write.mode("overwrite").parquet(f"data/gold/{DOMAIN}/date={execution_date}/por_entidad")

    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1])
