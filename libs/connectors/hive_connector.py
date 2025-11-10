from __future__ import annotations

from pathlib import Path

import pandas as pd

from libs.logging_utils import log_exception

from .base import Dataset, TargetConnector


class HiveTargetConnector(TargetConnector):
    def save(self, dataset: Dataset) -> None:
        table_name = self.options["table"]
        mode = self.options.get("mode", "append")
        relative_path = Path("hive") / table_name.replace(".", "/")
        target_path = self.context.data_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        spark = self.context.ensure_spark()
        try:
            if spark is not None and hasattr(dataset, "write"):
                writer = dataset.write.mode(mode).format(self.options.get("format", "parquet"))
                if partition := self.options.get("partitionBy"):
                    writer = writer.partitionBy(*[col.strip() for col in partition.split(",")])
                writer.save(str(target_path))
                return
            if isinstance(dataset, pd.DataFrame):
                dataset.to_parquet(target_path.with_suffix(".parquet"), index=False)
                return
        except Exception as exc:
            log_exception(self.logger, f"Failed to write Hive target {table_name}", exc)
            raise
        raise TypeError("Unsupported dataset type for Hive target")
