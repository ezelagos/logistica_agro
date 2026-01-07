import random
from faker import Faker
from generators.base.event import build_event
from generators.base.logger import setup_logger

logger = setup_logger("cereales.viajes", "cereales_viajes.log")

faker = Faker("es_AR")

def generar_ingresos(cantidad, silos):
    eventos = []
    logger.info(f"Generando {cantidad} ingresos")

    for _ in range(cantidad):
        silo = random.choice(silos)
        espacio = silo["capacidad_max"] - silo["stock_kg"]

        if espacio <= 0:
            logger.warning(f"Silo lleno: {silo['silo_id']}")
            continue

        peso = min(random.randint(28_000, 30_000), espacio)
        silo["stock_kg"] += peso

        evento = build_event(
            event_type="INGRESO_SILO",
            unidad_negocio="Cereales",
            payload={
                "silo_id": silo["silo_id"],
                "cereal": silo["cereal"],
                "peso_kg": peso,
                "origen": f"Campo {faker.last_name()}",
                "patente": faker.license_plate()
            }
        )

        eventos.append(evento)

    logger.info(f"Ingresos generados: {len(eventos)}")
    return eventos
