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
    format="%(asctime)s | %(levelname)s | ganaderia | %(message)s"
)
logger = logging.getLogger(__name__)

faker = Faker("es_AR")

CATEGORIAS = ["Ternero", "Novillo", "Vaquillona", "Vaca", "Toro"]

ESTABLECIMIENTOS = [
    "Campo Norte",
    "Campo Sur",
    "Feedlot Central",
    "Recría Oeste",
    "Engorde Este"
]

DESTINOS_COMERCIALES = [
    "Frigorífico Regional",
    "Mercado Concentrador",
    "Cliente Directo"
]

TIPOS_EVENTO = ["NACIMIENTO", "TRASLADO", "VENTA", "FAENA", "MUERTE"]

def generate_ganaderia_events(n_events: int) -> pd.DataFrame:
    events = []

    for _ in range(n_events):
        event_time = faker.date_time_this_year()
        tipo = random.choice(TIPOS_EVENTO)
        categoria = random.choice(CATEGORIAS)

        animal_id = f"AR-{random.randint(100000, 999999)}"
        establecimiento = random.choice(ESTABLECIMIENTOS)

        if tipo == "NACIMIENTO":
            origen = destino = establecimiento
            peso = random.randint(30, 45)
        elif tipo == "TRASLADO":
            origen = random.choice(
                [e for e in ESTABLECIMIENTOS if e != establecimiento]
            )
            destino = establecimiento
            peso = random.randint(120, 300)
        elif tipo in ["VENTA", "FAENA"]:
            origen = establecimiento
            destino = random.choice(DESTINOS_COMERCIALES)
            peso = random.randint(380, 520)
        else:
            origen = establecimiento
            destino = "Baja Sanitaria"
            peso = random.randint(50, 450)

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": event_time.isoformat(),
            "unidad_negocio": "ganaderia",
            "tipo_evento": "ganaderia_event",
            "tipo_movimiento": tipo,
            "animal_id": animal_id,
            "categoria": categoria,
            "peso_kg": peso,
            "origen": origen,
            "destino": destino,
            "establecimiento": establecimiento,
            "created_at": datetime.utcnow().isoformat()
        })

    return pd.DataFrame(events)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--events", type=int, default=350)
    args = parser.parse_args()

    logger.info("Generando RAW Ganadería")
    df = generate_ganaderia_events(args.events)

    persist_raw(
        df=df,
        domain="ganaderia",
        execution_date=args.date,
        generator_name="gen_ganaderia_events.py"
    )

    logger.info("RAW Ganadería generado OK")
