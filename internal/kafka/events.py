from dataclasses import dataclass
import json
from typing import Any, Dict, Optional  

@dataclass
class KafkaEvent:
   job_id: str
   payload: Dict[str, Any]
   max_retries: int
   created_at: str

   def to_json(self) -> str:
     return json.dumps({
        "event_type": "JobCreated",
        "job_id": self.job_id,
        "payload": self.payload,
        "max_retries": self.max_retries,
        "created_at": self.created_at,
    })