#generate_branch_offices
import pandas as pd
import os

BRANCH_OFFICES = [
    ("BR-25MAY", "25 de Mayo", "Buenos Aires"),
    ("BR-9JUL", "9 de Julio", "Buenos Aires"),
    ("BR-AZUL", "Azul", "Buenos Aires"),
    ("BR-BBCA", "Bahía Blanca", "Buenos Aires"),
    ("BR-BOL", "Bolívar", "Buenos Aires"),
    ("BR-CATR", "Catriló", "La Pampa"),
    ("BR-CABA", "Ciudad Autónoma de Buenos Aires", "Buenos Aires"),
    ("BR-GPICO", "General Pico", "La Pampa"),
    ("BR-JUN", "Junín", "Buenos Aires"),
    ("BR-LFLO", "Las Flores", "Buenos Aires"),
    ("BR-PERG", "Pergamino", "Buenos Aires"),
    ("BR-TRE", "Trenque Lauquen", "Buenos Aires"),
    ("BR-SR", "Santa Rosa", "La Pampa"),
    ("BR-TAND", "Tandil", "Buenos Aires"),
    ("BR-VMACK", "Vicuña Mackenna", "Córdoba"),
]

def generate_branch_offices():
    rows = []
    for branch_id, city, province in BRANCH_OFFICES:
        rows.append({
            "branch_id": branch_id,
            "city": city,
            "province": province,
            "branch_type": "commercial"
        })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    df = generate_branch_offices()
    out_dir = "data/silver/core"
    os.makedirs(out_dir, exist_ok=True)

    path = f"{out_dir}/branch_offices.parquet"
    df.to_parquet(path, index=False)

    print(f"✅ Branch offices generadas: {len(df)} → {path}")
