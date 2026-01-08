import uuid
import random
import logging
import argparse
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

from generators.common.base_raw_writer import persist_raw

# --------------------------------------------------
# Logger
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | camiones | %(message)s"
)
logger = logging.getLogger(__name__)

faker = Faker("es_AR")

SUCURSALES = [
    "9 de Julio", "Trenque Lauquen", "Santa Rosa",
    "Pergamino", "Bahía Blanca", "Junín", "Pehuajó"
]

ACOPIOS = [
    "9 de Julio", "Catriló", "Daireaux",
    "González Moreno", "Uriburu", "Villa Maza", "Volta"
]

EVENT_TYPES = ["VIAJE_INICIADO", "CARGA", "DESCARGA", "VIAJE_FINALIZADO"]

def generate_camiones_events(n_trips: int) -> pd.DataFrame:
    events = []

    for _ in range(n_trips):
        trip_id = str(uuid.uuid4())
        patente = faker.license_plate()
        peso_kg = random.randint(28000, 30000)

        origen = random.choice(SUCURSALES + ACOPIOS)
        destino = random.choice([x for x in SUCURSALES + ACOPIOS if x != origen])

        start_time = faker.date_time_this_year()
        timestamps = [
            start_time,
            start_time + timedelta(minutes=60),
            start_time + timedelta(minutes=180),
            start_time + timedelta(minutes=420),
        ]

        for event_type, ts in zip(EVENT_TYPES, timestamps):
            events.append({
                "event_id": str(uuid.uuid4()),
                "trip_id": trip_id,
                "event_timestamp": ts.isoformat(),
                "unidad_negocio": "camiones",
                "tipo_evento": "camion_event",
                "evento": event_type,
                "patente": patente,
                "origen": origen,
                "destino": destino,
                "peso_kg": peso_kg if event_type in ["CARGA", "DESCARGA"] else None,
                "created_at": datetime.utcnow().isoformat()
            })

    return pd.DataFrame(events)

# --------------------------------------------------
# Main
# --------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--records", type=int, default=120)
    args = parser.parse_args()

    logger.info("Generando RAW Camiones")
    df = generate_camiones_events(args.records)

    persist_raw(
        df=df,
        domain="camiones",
        execution_date=args.date,
        generator_name="gen_camiones_events.py"
    )

    logger.info("RAW Camiones generado OK")
