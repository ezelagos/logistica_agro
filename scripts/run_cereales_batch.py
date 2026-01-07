from generators.cereales.silos import GeneradorSilos
from generators.cereales.viajes import generar_ingresos
from ingestion.batch.bronze_writer import write_events

silos = GeneradorSilos().generar()
eventos = generar_ingresos(50, silos)

write_events(eventos, "cereales")
