from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

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
        header = self.options.get("header", "true").lower() != "false"
        path.parent.mkdir(parents=True, exist_ok=True)
        spark = self.context.ensure_spark()
        try:
            if spark is not None and hasattr(dataset, "write"):
                writer = dataset.coalesce(1).write.mode(mode).option("header", header)
                if compression:
                    writer = writer.option("compression", compression)
                if path.suffix and mode != "append":
                    temp_dir = path.parent / f".spark_tmp_{path.stem}_{uuid4().hex}"
                    if temp_dir.exists():
                        shutil.rmtree(temp_dir)
                    writer.csv(str(temp_dir))
                    self._promote_spark_single_file(temp_dir, path)
                else:
                    writer.csv(str(path))
                return
            if isinstance(dataset, pd.DataFrame):
                dataset.to_csv(path, index=False, header=header, compression=compression or None)
                return
        except Exception as exc:
            log_exception(self.logger, f"Failed to write CSV target at {path}", exc)
            raise
        raise TypeError("Unsupported dataset type for CSV target")

    def _promote_spark_single_file(self, temp_dir: Path, final_path: Path) -> None:
        candidates = sorted(temp_dir.glob("part-*"))
        if not candidates:
            raise FileNotFoundError(f"No CSV output produced under {temp_dir}")
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if final_path.exists():
            if final_path.is_file():
                final_path.unlink()
            else:
                shutil.rmtree(final_path)
        shutil.move(str(candidates[0]), str(final_path))
        for leftover in temp_dir.iterdir():
            if leftover.is_dir():
                shutil.rmtree(leftover)
            else:
                leftover.unlink()
        temp_dir.rmdir()
