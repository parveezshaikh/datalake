from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    from pyspark.sql import DataFrame as SparkDataFrame
except ModuleNotFoundError:  # pragma: no cover
    SparkDataFrame = None  # type: ignore


Dataset = object


class BaseConnector:
    def __init__(self, *, options: dict, context, logger: Optional[logging.Logger] = None) -> None:
        self.options = options
        self.context = context
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def _resolve_path(self, raw_path: str) -> Path:
        candidate = Path(raw_path)
        if candidate.is_absolute():
            return candidate
        base = Path.cwd()
        resolved = base / raw_path
        if resolved.exists() or resolved.parent.exists():
            return resolved
        return (self.context.data_root / raw_path).resolve()


class SourceConnector(BaseConnector):
    def load(self) -> Dataset:  # pragma: no cover - interface
        raise NotImplementedError


class TargetConnector(BaseConnector):
    def save(self, dataset: Dataset) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class DataFrameConversions:
    @staticmethod
    def ensure_pandas(dataset: Dataset) -> pd.DataFrame:
        if isinstance(dataset, pd.DataFrame):
            return dataset
        if SparkDataFrame is not None and isinstance(dataset, SparkDataFrame):
            return dataset.toPandas()
        raise TypeError("Unsupported dataset type for conversion")
