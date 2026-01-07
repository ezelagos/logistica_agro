from datetime import datetime
import uuid
import hashlib

def build_ingestion_metadata(source: str, pipeline: str, record: dict):
    record_str = str(sorted(record.items()))
    record_hash = hashlib.md5(record_str.encode()).hexdigest()

    return {
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "source_system": source,
        "pipeline_id": pipeline,
        "batch_id": str(uuid.uuid4()),
        "record_hash": record_hash
    }
