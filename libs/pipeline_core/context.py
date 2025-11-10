from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Optional

try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:  # pragma: no cover - pyspark optional
    SparkSession = None  # type: ignore


class MetricsEmitter:
    """Lightweight metrics emitter compatible with Prometheus-style counters."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}

    def incr(self, name: str, value: float = 1.0) -> None:
        self.counters[name] = self.counters.get(name, 0.0) + value

    def gauge(self, name: str, value: float) -> None:
        self.counters[name] = value


@dataclass
class PipelineContext:
    run_id: str
    app_name: str
    data_root: Path
    config_root: Path
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("pipeline"))
    metrics: MetricsEmitter = field(default_factory=MetricsEmitter)
    spark_builder: Optional[Callable[[], "SparkSession"]] = None
    lookup_cache: Dict[str, dict] = field(default_factory=dict)

    _spark: Optional["SparkSession"] = field(init=False, default=None, repr=False)

    def ensure_spark(self) -> Optional["SparkSession"]:
        if self._spark is not None:
            return self._spark
        if self.spark_builder is not None:
            self._spark = self.spark_builder()
        elif SparkSession is not None:
            self._spark = (
                SparkSession.builder.appName(self.app_name)
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .getOrCreate()
            )
        return self._spark

    def lookup(self, reference: str) -> dict:
        if reference in self.lookup_cache:
            return self.lookup_cache[reference]
        lookup_path = self.config_root / "common" / "lookups" / f"{reference}.json"
        if not lookup_path.exists():
            raise FileNotFoundError(f"Lookup reference {reference} not found at {lookup_path}")
        payload = json.loads(lookup_path.read_text())
        self.lookup_cache[reference] = payload
        return payload

    def write_dataset(self, relative_path: str, content: str) -> None:
        target_path = self.data_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(content)
