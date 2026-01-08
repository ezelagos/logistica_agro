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
    format="%(asctime)s | %(levelname)s | cereales | %(message)s"
)
logger = logging.getLogger(__name__)

faker = Faker("es_AR")

CEREALES = ["Soja", "Maíz", "Trigo", "Girasol"]

ACOPIOS = [
    "9 de Julio", "Anguil", "Cañada Seca", "Catriló",
    "González Moreno", "Trenque Lauquen",
    "Santa Rosa", "Uriburu", "Volta"
]

DESTINOS_EGRESO = [
    "Puerto Rosario",
    "Puerto Bahía Blanca",
    "Molino local",
    "Fábrica balanceado"
]

def generate_cereales_events(n_events: int) -> pd.DataFrame:
    events = []

    for _ in range(n_events):
        event_time = faker.date_time_this_year()
        tipo = random.choice(["INGRESO", "EGRESO"])
        cereal = random.choice(CEREALES)
        planta = random.choice(ACOPIOS)

        if tipo == "INGRESO":
            origen = f"Campo {faker.last_name()}"
            destino = planta
            peso = random.randint(25000, 32000)
        else:
            origen = planta
            destino = random.choice(DESTINOS_EGRESO)
            peso = random.randint(26000, 34000)

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": event_time.isoformat(),
            "unidad_negocio": "cereales",
            "tipo_evento": "cereal_movement",
            "tipo_movimiento": tipo,
            "cereal": cereal,
            "peso_kg": peso,
            "origen": origen,
            "destino": destino,
            "planta_acopio": planta,
            "created_at": datetime.utcnow().isoformat()
        })

    return pd.DataFrame(events)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--events", type=int, default=300)
    args = parser.parse_args()

    logger.info("Generando RAW Cereales")
    df = generate_cereales_events(args.events)

    persist_raw(
    df=df,
    domain="cereales",
    execution_date=args.date,
    generator_name="gen_cereales_events.py",
    file_format="csv"
    )
    
    logger.info("RAW Cereales generado OK")
