#base_raw_writer.py
import os
import json
from datetime import datetime

def persist_raw(
    df,
    domain: str,
    execution_date: str,
    generator_name: str,
    file_format: str = "csv"
) -> None:
    base_path = f"data/raw/{domain}/date={execution_date}"
    os.makedirs(base_path, exist_ok=True)

    if file_format == "csv":
        data_path = f"{base_path}/data.csv"
        df.to_csv(data_path, index=False)
    elif file_format == "json":
        data_path = f"{base_path}/data.json"
        df.to_json(data_path, orient="records", lines=True)
    else:
        raise ValueError(f"Formato no soportado: {file_format}")

    metadata = {
        "domain": domain,
        "execution_date": execution_date,
        "rows": int(len(df)),
        "generator": generator_name,
        "generated_at": datetime.utcnow().isoformat(),
        "schema_version": "v1",
        "format": file_format
    }

    with open(f"{base_path}/_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
