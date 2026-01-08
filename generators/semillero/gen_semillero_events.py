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
    format="%(asctime)s | %(levelname)s | semillero | %(message)s"
)
logger = logging.getLogger(__name__)

faker = Faker("es_AR")

CULTIVOS = ["Soja", "Maíz", "Trigo", "Girasol"]
EVENTOS = ["PRODUCCION", "TRATAMIENTO", "EMBOLSADO", "VENTA", "TRANSFERENCIA"]
SUCURSALES = [
    "9 de Julio",
    "Trenque Lauquen",
    "Pergamino",
    "Santa Rosa",
    "Bahía Blanca"
]

def generate_semillero_events(n_lotes: int) -> pd.DataFrame:
    events = []

    for _ in range(n_lotes):
        lote_id = str(uuid.uuid4())
        cultivo = random.choice(CULTIVOS)
        sucursal = random.choice(SUCURSALES)
        bolsas = random.randint(80, 600)
        base_time = faker.date_time_this_year()

        for evento in EVENTOS:
            events.append({
                "event_id": str(uuid.uuid4()),
                "lote_id": lote_id,
                "event_timestamp": base_time.isoformat(),
                "unidad_negocio": "semillero",
                "tipo_evento": "semillero_event",
                "evento": evento,
                "cultivo": cultivo,
                "cantidad_bolsas": bolsas if evento != "VENTA"
                    else random.randint(20, bolsas),
                "origen": sucursal,
                "destino": (
                    f"Productor {faker.last_name()}"
                    if evento == "VENTA" else sucursal
                ),
                "created_at": datetime.utcnow().isoformat()
            })

    return pd.DataFrame(events)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--lotes", type=int, default=120)
    args = parser.parse_args()

    logger.info("Generando RAW Semillero")
    df = generate_semillero_events(args.lotes)

    persist_raw(
        df=df,
        domain="semillero",
        execution_date=args.date,
        generator_name="gen_semillero_events.py"
    )

    logger.info("RAW Semillero generado OK")
