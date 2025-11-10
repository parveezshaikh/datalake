from __future__ import annotations

import pandas as pd

from libs.logging_utils import log_exception

from .base import Dataset, SourceConnector, TargetConnector


class CSVSourceConnector(SourceConnector):
    def load(self) -> Dataset:
        path = self._resolve_path(self.options["path"])
        header = self.options.get("header", "true").lower() == "true"
        delimiter = self.options.get("delimiter", ",")
        spark = self.context.ensure_spark()
        try:
            if spark is not None:
                df = (
                    spark.read.option("header", header)
                    .option("delimiter", delimiter)
                    .csv(str(path))
                )
                return df
            return pd.read_csv(path, header=0 if header else None, delimiter=delimiter)
        except Exception as exc:
            log_exception(self.logger, f"Failed to load CSV source at {path}", exc)
            raise


class CSVTargetConnector(TargetConnector):
    def save(self, dataset: Dataset) -> None:
        path = self._resolve_path(self.options["path"])
        mode = self.options.get("mode", "overwrite")
        compression = self.options.get("compression")
        path.parent.mkdir(parents=True, exist_ok=True)
        spark = self.context.ensure_spark()
        try:
            if spark is not None and hasattr(dataset, "write"):
                writer = dataset.write.mode(mode)
                if compression:
                    writer = writer.option("compression", compression)
                writer.csv(str(path))
                return
            if isinstance(dataset, pd.DataFrame):
                dataset.to_csv(path, index=False, compression=compression or None)
                return
        except Exception as exc:
            log_exception(self.logger, f"Failed to write CSV target at {path}", exc)
            raise
        raise TypeError("Unsupported dataset type for CSV target")
