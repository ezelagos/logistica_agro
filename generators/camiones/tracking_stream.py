import time
import json
import random
from datetime import datetime
from faker import Faker
from pathlib import Path

from generators.base.event import build_event
from generators.base.logger import setup_logger

logger = setup_logger("camiones.streaming", "camiones_streaming.log")

faker = Faker("es_AR")

OUTPUT_PATH = Path("data/raw/streaming/camiones")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

def generar_evento_gps():
    return {
        "patente": faker.license_plate(),
        "lat": faker.latitude(),
        "lon": faker.longitude(),
        "velocidad_kmh": random.randint(40, 100),
        "rumbo": random.choice(["N", "S", "E", "O"])
    }

def run_stream(intervalo_segundos=2):
    logger.info("Iniciando stream de GPS de camiones")

    while True:
        payload = generar_evento_gps()

        evento = build_event(
            event_type="GPS_CAMION",
            unidad_negocio="Camiones",
            payload=payload
        )

        archivo = OUTPUT_PATH / f"{evento['event_id']}.json"
        with open(archivo, "w") as f:
            json.dump(evento, f)

        logger.info(
            f"Evento GPS generado | Patente={payload['patente']} "
            f"Velocidad={payload['velocidad_kmh']} km/h"
        )

        time.sleep(intervalo_segundos)

if __name__ == "__main__":
    run_stream()
