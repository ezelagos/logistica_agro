from datetime import datetime
from typing import Dict, Any
import uuid

def build_event(
    event_type: str,
    unidad_negocio: str,
    payload: Dict[str, Any]
) -> Dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "unidad_negocio": unidad_negocio,
        "event_time": datetime.utcnow().isoformat(),
        "processing_time": datetime.utcnow().isoformat(),
        "payload": payload
    }
