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
    format="%(asctime)s | %(levelname)s | cerdos | %(message)s"
)
logger = logging.getLogger(__name__)

faker = Faker("es_AR")

GRANJAS = [
    "Granja Norte",
    "Granja Centro",
    "Granja Sur",
    "Granja La Esperanza",
    "Granja El Molino"
]

EVENT_TYPES = [
    "NACIMIENTO",
    "ALIMENTACION",
    "SANIDAD",
    "TRASLADO",
    "VENTA"
]

def generate_cerdos_events(n_lotes: int) -> pd.DataFrame:
    events = []

    for _ in range(n_lotes):
        lote_id = str(uuid.uuid4())
        granja = random.choice(GRANJAS)
        cantidad_animales = random.randint(30, 200)

        start_time = faker.date_time_this_year()
        timestamps = [
            start_time,
            start_time + timedelta(days=15),
            start_time + timedelta(days=45),
            start_time + timedelta(days=90),
            start_time + timedelta(days=120),
        ]

        for event_type, ts in zip(EVENT_TYPES, timestamps):
            events.append({
                "event_id": str(uuid.uuid4()),
                "lote_id": lote_id,
                "event_timestamp": ts.isoformat(),
                "unidad_negocio": "cerdos",
                "tipo_evento": "cerdo_event",
                "evento": event_type,
                "granja": granja,
                "cantidad_animales": cantidad_animales,
                "peso_promedio_kg": random.randint(6, 120)
                    if event_type in ["TRASLADO", "VENTA"] else None,
                "destino": f"Frigorifico {faker.city()}"
                    if event_type == "VENTA" else None,
                "created_at": datetime.utcnow().isoformat()
            })

    return pd.DataFrame(events)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--lotes", type=int, default=120)
    args = parser.parse_args()

    logger.info("Generando RAW Cerdos")
    df = generate_cerdos_events(args.lotes)

    persist_raw(
        df=df,
        domain="cerdos",
        execution_date=args.date,
        generator_name="gen_cerdos_events.py"
    )

    logger.info("RAW Cerdos generado OK")
