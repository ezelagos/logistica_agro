import json
from pathlib import Path
from generators.base.logger import setup_logger

logger = setup_logger("bronze.writer", "bronze_writer.log")

def write_events(events, unidad):
    base = Path(f"data/raw/{unidad.lower()}")
    base.mkdir(parents=True, exist_ok=True)

    for e in events:
        file = base / f"{e['event_id']}.json"
        with open(file, "w") as f:
            json.dump(e, f)

    logger.info(f"Eventos escritos: {len(events)} en {base}")
