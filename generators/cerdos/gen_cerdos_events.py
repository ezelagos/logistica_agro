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

        peso_actual = random.randint(1, 3)  # peso inicial realista

        timeline = [
            ("NACIMIENTO", 0, (1, 3)),
            ("ALIMENTACION", 15, (10, 40)),
            ("SANIDAD", 45, (30, 70)),
            ("TRASLADO", 90, (70, 100)),
            ("VENTA", 120, (90, 130)),
        ]

        for evento, days, peso_range in timeline:
            peso_actual = random.randint(*peso_range)

            events.append({
                "event_id": str(uuid.uuid4()),
                "lote_id": lote_id,
                "event_timestamp": (start_time + timedelta(days=days)).isoformat(),
                "unidad_negocio": "cerdos",
                "tipo_evento": "cerdo_event",
                "evento": evento,
                "granja": granja,
                "cantidad_animales": cantidad_animales,
                "peso_promedio_kg": peso_actual,
                "destino": f"Frigorifico {faker.city()}" if evento == "VENTA" else None,
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
