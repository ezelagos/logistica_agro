import uuid
import random
import logging
import argparse
from datetime import datetime
import pandas as pd
from faker import Faker

from generators.common.base_raw_writer import persist_raw

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | insumos | %(message)s"
)
logger = logging.getLogger(__name__)

faker = Faker("es_AR")

INSUMOS = [
    ("Urea", "Fertilizante", "kg"),
    ("MAP", "Fertilizante", "kg"),
    ("Glifosato", "Agroquímico", "lt"),
    ("2,4-D", "Agroquímico", "lt"),
    ("Maíz híbrido", "Semilla", "bolsas"),
    ("Soja RR", "Semilla", "bolsas")
]

SUCURSALES = [
    "9 de Julio",
    "Trenque Lauquen",
    "Santa Rosa",
    "Pergamino",
    "Bahía Blanca"
]

PROVEEDORES = [
    "Proveedor Nacional",
    "Importador Regional",
    "Fabricante Directo"
]

def generate_insumos_events(n_events: int) -> pd.DataFrame:
    events = []

    for _ in range(n_events):
        event_time = faker.date_time_this_year()
        tipo = random.choices(
            ["COMPRA", "TRANSFERENCIA", "VENTA", "CONSUMO"],
            weights=[0.3, 0.2, 0.25, 0.25]
        )[0]

        insumo, categoria, unidad = random.choice(INSUMOS)
        sucursal = random.choice(SUCURSALES)

        if tipo == "COMPRA":
            origen = random.choice(PROVEEDORES)
            destino = sucursal
            cantidad = random.randint(1000, 25000)
        elif tipo == "TRANSFERENCIA":
            origen = random.choice(
                [s for s in SUCURSALES if s != sucursal]
            )
            destino = sucursal
            cantidad = random.randint(500, 8000)
        elif tipo == "CONSUMO":
            origen = sucursal
            destino = "Ganadería"
            cantidad = random.randint(100, 3000)
        else:
            origen = sucursal
            destino = f"Productor {faker.last_name()}"
            cantidad = random.randint(200, 5000)

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": event_time.isoformat(),
            "unidad_negocio": "insumos",
            "tipo_evento": "insumo_movement",
            "tipo_movimiento": tipo,
            "insumo": insumo,
            "categoria": categoria,
            "cantidad": cantidad,
            "unidad_medida": unidad,
            "origen": origen,
            "destino": destino,
            "sucursal": sucursal,
            "created_at": datetime.utcnow().isoformat()
        })

    return pd.DataFrame(events)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--events", type=int, default=400)
    args = parser.parse_args()

    logger.info("Generando RAW Insumos")
    df = generate_insumos_events(args.events)

    persist_raw(
        df=df,
        domain="insumos",
        execution_date=args.date,
        generator_name="gen_insumos_events.py"
    )

    logger.info("RAW Insumos generado OK")
