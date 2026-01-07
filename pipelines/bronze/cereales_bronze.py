import pandas as pd
from pipelines.bronze.metadata import build_ingestion_metadata
from generators.base.logger import setup_logger

logger = setup_logger("bronze.cereales", "bronze_cereales.log")

def process_cereales_raw(input_path, output_path):
    df = pd.read_csv(input_path)
    logger.info(f"Registros RAW recibidos: {len(df)}")

    records = []
    for _, row in df.iterrows():
        record = row.to_dict()
        metadata = build_ingestion_metadata(
            source="cereales",
            pipeline="bronze_cereales",
            record=record
        )
        records.append({**record, **metadata})

    bronze_df = pd.DataFrame(records)
    bronze_df.to_parquet(output_path, index=False)

    logger.info(f"Bronze generado en {output_path} con {len(bronze_df)} registros")
