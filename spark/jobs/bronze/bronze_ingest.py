import sys
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, input_file_name

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def load_metadata(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Metadata no encontrada: {path}")
    with open(path) as f:
        return json.load(f)

def validate_metadata(meta: dict):
    required = {"domain", "execution_date", "rows", "format", "schema_version"}
    missing = required - set(meta.keys())
    if missing:
        raise ValueError(f"Metadata inválida. Faltan campos: {missing}")

    if meta["format"] not in {"csv", "json"}:
        raise ValueError(f"Formato no soportado: {meta['format']}")

# --------------------------------------------------
# Main
# --------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise ValueError(
            "Uso: spark-submit bronze_ingest.py <domain> YYYY-MM-DD"
        )

    domain = sys.argv[1]
    execution_date = sys.argv[2]

    raw_base = f"data/raw/{domain}/date={execution_date}"
    metadata_path = f"{raw_base}/_metadata.json"
    bronze_path = f"data/bronze/{domain}/date={execution_date}"

    # --------------------------------------------------
    # Metadata
    # --------------------------------------------------
    metadata = load_metadata(metadata_path)
    validate_metadata(metadata)

    file_format = metadata["format"]

    if file_format == "csv":
        data_path = f"{raw_base}/data.csv"
    else:  # json
        data_path = f"{raw_base}/data.json"

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Archivo de datos no encontrado: {data_path}")

    # --------------------------------------------------
    # Spark session
    # --------------------------------------------------
    spark = (
        SparkSession.builder
        .appName(f"bronze-{domain}")
        .getOrCreate()
    )

    # --------------------------------------------------
    # Read RAW
    # --------------------------------------------------
    if file_format == "csv":
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(data_path)
        )
    else:  # json
        df = (
            spark.read
            .option("inferSchema", True)
            .json(data_path)
        )

    # --------------------------------------------------
    # Enriquecimiento técnico (bronze real)
    # --------------------------------------------------
    df_bronze = (
        df
        .withColumn("source_domain", lit(domain))
        .withColumn("ingestion_date", lit(execution_date))
        .withColumn("source_file", input_file_name())
        .withColumn("schema_version", lit(metadata["schema_version"]))
    )

    # --------------------------------------------------
    # Escritura BRONZE (idempotente)
    # --------------------------------------------------
    (
        df_bronze
        .write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    count = df_bronze.count()
    expected = metadata["rows"]

    if count != expected:
        print(
            f"[WARN] Filas escritas ({count}) "
            f"!= metadata.rows ({expected})"
        )

    print(f"[OK] BRONZE generado → {bronze_path}")
    print(f"[OK] Filas: {count}")

    spark.stop()
