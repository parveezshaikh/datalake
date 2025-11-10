from __future__ import annotations

import pandas as pd

from .base import Dataset, SourceConnector, TargetConnector


class DatabaseSourceConnector(SourceConnector):
    def load(self) -> Dataset:
        spark = self.context.ensure_spark()
        mock_path = self.options.get("mockDataPath")
        if mock_path:
            resolved = self._resolve_path(mock_path)
            return pd.read_parquet(resolved) if resolved.suffix == ".parquet" else pd.read_csv(resolved)
        if spark is None:
            raise RuntimeError("Spark session required for JDBC reads when no mockDataPath supplied")
        reader = spark.read.format("jdbc")
        reader = reader.option("url", self.options["jdbcUrl"])
        if "table" in self.options:
            reader = reader.option("dbtable", self.options["table"])
        if "sql" in self.options:
            reader = reader.option("query", self.options["sql"])
        if "user" in self.options:
            reader = reader.option("user", self.options["user"])
        if "password" in self.options:
            reader = reader.option("password", self.options["password"])
        fetch_size = self.options.get("fetchSize")
        if fetch_size:
            reader = reader.option("fetchsize", fetch_size)
        return reader.load()


class DatabaseTargetConnector(TargetConnector):
    def save(self, dataset: Dataset) -> None:
        spark = self.context.ensure_spark()
        if spark is None:
            # Persist locally when Spark/JDBC is unavailable
            fallback_path = self.context.data_root / "_mock_exports" / f"{self.options.get('table', 'export')}.csv"
            fallback_path.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(dataset, pd.DataFrame):
                dataset.to_csv(fallback_path, index=False)
            else:
                dataset.toPandas().to_csv(fallback_path, index=False)
            self.logger.warning("Spark not available; wrote dataset to %s instead of JDBC target", fallback_path)
            return
        writer = dataset.write.mode(self.options.get("mode", "append")).format("jdbc")
        writer = writer.option("url", self.options["jdbcUrl"])
        writer = writer.option("dbtable", self.options["table"])
        if "user" in self.options:
            writer = writer.option("user", self.options["user"])
        if "password" in self.options:
            writer = writer.option("password", self.options["password"])
        if "isolationLevel" in self.options:
            writer = writer.option("isolationLevel", self.options["isolationLevel"])
        writer.save()
