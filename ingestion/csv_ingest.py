import pandas as pd
from pathlib import Path

def ingest_csv(input_path: str, outhput_path: str):
    df = pd.read_csv(input_path)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    ingest_csv(
        "data/raw/logistica.csv",
        "data/bronze/logistica.csv"
    )