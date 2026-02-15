import time
import json
from datetime import datetime

def log_event(event_type, data):
    log_record = {
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": event_type,
        "data": data
    }

    with open("genai/rag_logs.jsonl", "a") as f:
        f.write(json.dumps(log_record) + "\n")

def track_latency(start_time):
    return round(time.time() - start_time, 4)

