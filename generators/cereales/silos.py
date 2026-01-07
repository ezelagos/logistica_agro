import random
from faker import Faker
from generators.base.logger import setup_logger

logger = setup_logger("cereales.silos", "cereales_silos.log")

class GeneradorSilos:
    def __init__(self):
        self.faker = Faker("es_AR")
        self.acopios = [
            "9 de Julio", "Anguil", "Cañada Seca", "Catrilo",
            "Santa Rosa", "Trenque Lauquen"
        ]
        self.cereales = ["Soja", "Maiz", "Trigo", "Girasol"]

    def generar(self):
        silos = []

        logger.info("Inicializando silos")
        for loc in self.acopios:
            for cereal in self.cereales:
                humedad = round(random.uniform(10, 16), 1)
                tipo = "E" if humedad <= 13.5 else "L"

                silo = {
                    "silo_id": f"SILO_{loc}_{cereal}_{tipo}",
                    "ubicacion": loc,
                    "cereal": cereal,
                    "capacidad_max": 200_000,
                    "stock_kg": random.randint(20_000, 150_000),
                    "humedad": humedad,
                    "tipo": tipo
                }

                silos.append(silo)

        logger.info(f"Silos generados: {len(silos)}")
        return silos
