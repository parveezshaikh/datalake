from __future__ import annotations

from typing import Dict, Type

from .base import SourceConnector, TargetConnector
from .csv_connector import CSVSourceConnector, CSVTargetConnector
from .database_connector import DatabaseSourceConnector, DatabaseTargetConnector
from .hive_connector import HiveTargetConnector

SOURCE_CONNECTORS: Dict[str, Type[SourceConnector]] = {
    "csv": CSVSourceConnector,
    "database": DatabaseSourceConnector,
}

TARGET_CONNECTORS: Dict[str, Type[TargetConnector]] = {
    "csv": CSVTargetConnector,
    "database": DatabaseTargetConnector,
    "hive": HiveTargetConnector,
}


def build_source(conn_type: str, *, options: dict, context) -> SourceConnector:
    if conn_type not in SOURCE_CONNECTORS:
        raise KeyError(f"Source connector '{conn_type}' not registered")
    return SOURCE_CONNECTORS[conn_type](options=options, context=context)


def build_target(conn_type: str, *, options: dict, context) -> TargetConnector:
    if conn_type not in TARGET_CONNECTORS:
        raise KeyError(f"Target connector '{conn_type}' not registered")
    return TARGET_CONNECTORS[conn_type](options=options, context=context)
