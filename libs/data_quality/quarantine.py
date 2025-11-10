from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from libs.connectors.base import DataFrameConversions


def quarantine_records(dataset, *, context, reason: str, bucket: str = "exceptions") -> Path:
    df = DataFrameConversions.ensure_pandas(dataset)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = context.data_root / bucket / f"quarantine_{timestamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: List[Dict[str, Any]] = df.to_dict(orient="records")
    record = {"reason": reason, "records": payload, "run_id": context.run_id}
    path.write_text(json.dumps(record, indent=2))
    context.logger.error("Quarantined %s records to %s", len(payload), path)
    return path
