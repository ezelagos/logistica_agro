import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, upper, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

DOMAIN = "semillero"
RAW_BASE = "data/raw"
BRONZE_BASE = "data/bronze"

VALID_EVENTOS = ["PRODUCCION", "TRATAMIENTO", "EMBOLSADO", "VENTA", "TRANSFERENCIA"]
VALID_CULTIVOS = ["SOJA", "MAÍZ", "TRIGO", "GIRASOL"]

RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("lote_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("unidad_negocio", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("evento", StringType(), True),
    StructField("cultivo", StringType(), True),
    StructField("cantidad_bolsas", IntegerType(), True),
    StructField("origen", StringType(), True),
    StructField("destino", StringType(), True),
    StructField("created_at", StringType(), True),
])

def main(execution_date: str):
    spark = SparkSession.builder.appName(f"bronze-{DOMAIN}").getOrCreate()

    raw_path = f"{RAW_BASE}/{DOMAIN}/date={execution_date}"
    bronze_path = f"{BRONZE_BASE}/{DOMAIN}/date={execution_date}"

    with open(f"{raw_path}/_metadata.json") as f:
        metadata = json.load(f)
    assert metadata["format"] == "csv"

    df = (
        spark.read.schema(RAW_SCHEMA).option("header", True)
        .csv(f"{raw_path}/data.csv")
        .withColumn("event_ts", to_timestamp(col("event_timestamp")))
        .withColumn("evento", upper(trim(col("evento"))))
        .withColumn("cultivo", upper(trim(col("cultivo"))))
    )

    df_valid = (
        df.filter(col("event_id").isNotNull())
          .filter(col("lote_id").isNotNull())
          .filter(col("evento").isin(VALID_EVENTOS))
          .filter(col("cultivo").isin(VALID_CULTIVOS))
          .filter(col("cantidad_bolsas") > 0)
          .filter(col("event_ts").isNotNull())
          .drop("event_ts")
    )

    df_valid.write.mode("overwrite").parquet(bronze_path)
    spark.stop()

if __name__ == "__main__":
    main(sys.argv[1])
