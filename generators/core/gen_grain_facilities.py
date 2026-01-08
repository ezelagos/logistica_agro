#gen_grain_facilities.py
import pandas as pd
import os

GRAIN_FACILITIES = [
    ("GF-9JUL", "9 de Julio", "Buenos Aires"),
    ("GF-ANG", "Anguil", "La Pampa"),
    ("GF-CSEC", "Cañada Seca", "Buenos Aires"),
    ("GF-CATR", "Catriló", "La Pampa"),
    ("GF-CEB", "Ceballos", "La Pampa"),
    ("GF-CBAR", "Colonia Barón", "La Pampa"),
    ("GF-DAI", "Daireaux", "Buenos Aires"),
    ("GF-GMOR", "González Moreno", "Buenos Aires"),
    ("GF-GUAT", "Guatraché", "La Pampa"),
    ("GF-JJP", "Juan José Paso", "Buenos Aires"),
    ("GF-LZAN", "La Zanja", "Buenos Aires"),
    ("GF-LON", "Lonquimay", "La Pampa"),
    ("GF-PEL", "Pellegrini", "Buenos Aires"),
    ("GF-SAL", "Salazar", "Buenos Aires"),
    ("GF-SR", "Santa Rosa", "La Pampa"),
    ("GF-TLQ", "Trenque Lauquen", "Buenos Aires"),
    ("GF-URI", "Uriburu", "La Pampa"),
    ("GF-VMZ", "Villa Maza", "Buenos Aires"),
    ("GF-VOL", "Volta", "Buenos Aires"),
]

def generate_grain_facilities():
    rows = []
    for facility_id, city, province in GRAIN_FACILITIES:
        rows.append({
            "facility_id": facility_id,
            "city": city,
            "province": province,
            "facility_type": "grain_storage"
        })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    df = generate_grain_facilities()
    out_dir = "data/silver/core"
    os.makedirs(out_dir, exist_ok=True)

    path = f"{out_dir}/grain_facilities.parquet"
    df.to_parquet(path, index=False)

    print(f"✅ Grain facilities generadas: {len(df)} → {path}")
