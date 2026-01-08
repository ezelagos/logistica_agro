import uuid
import random
import logging
import argparse
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

from generators.common.base_raw_writer import persist_raw

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | siembras | %(message)s"
)
logger = logging.getLogger(__name__)

faker = Faker("es_AR")

CULTIVOS = ["Soja", "Maíz", "Trigo", "Girasol"]
EVENT_TYPES = [
    "PREPARACION_SUELO",
    "SIEMBRA",
    "FERTILIZACION",
    "PULVERIZACION",
    "COSECHA"
]

def generate_siembras_events(n_lotes: int) -> pd.DataFrame:
    events = []

    for _ in range(n_lotes):
        lote_id = str(uuid.uuid4())
        campo = f"Campo {faker.last_name()}"
        cultivo = random.choice(CULTIVOS)
        superficie_ha = random.randint(30, 350)

        start_time = faker.date_time_this_year()
        timestamps = [
            start_time,
            start_time + timedelta(days=7),
            start_time + timedelta(days=30),
            start_time + timedelta(days=60),
            start_time + timedelta(days=120),
        ]

        for event_type, ts in zip(EVENT_TYPES, timestamps):
            events.append({
                "event_id": str(uuid.uuid4()),
                "lote_id": lote_id,
                "event_timestamp": ts.isoformat(),
                "unidad_negocio": "siembras",
                "tipo_evento": "siembra_event",
                "evento": event_type,
                "campo": campo,
                "cultivo": cultivo,
                "superficie_ha": superficie_ha,
                "rinde_qq_ha": random.randint(25, 55)
                    if event_type == "COSECHA" else None,
                "created_at": datetime.utcnow().isoformat()
            })

    return pd.DataFrame(events)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--lotes", type=int, default=140)
    args = parser.parse_args()

    logger.info("Generando RAW Siembras")
    df = generate_siembras_events(args.lotes)

    persist_raw(
        df=df,
        domain="siembras",
        execution_date=args.date,
        generator_name="gen_siembras_events.py"
    )

    logger.info("RAW Siembras generado OK")
