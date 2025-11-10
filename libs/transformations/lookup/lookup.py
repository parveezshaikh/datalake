from __future__ import annotations

import pandas as pd

from libs.transformations.base import BaseTransformation
from libs.transformations import utils

try:  # pragma: no cover - optional dependency
    from pyspark.sql import functions as F
except ModuleNotFoundError:  # pragma: no cover
    F = None  # type: ignore


class LookupTransformation(BaseTransformation):
    def apply(self, datasets, context) -> None:
        source_id = self.config.options["source"]
        reference = self.config.options["reference"]
        join_columns = [col.strip() for col in self.config.options.get("joinColumns", "id").split(",") if col.strip()]
        join_type = self.config.options.get("joinType", "left")
        output_fields = [col.strip() for col in self.config.options.get("outputFields", "").split(",") if col.strip()]
        dataset = self.resolve_dataset(datasets, source_id)
        if reference in datasets:
            lookup_df = datasets[reference]
        else:
            lookup_payload = context.lookup(reference)
            spark = context.ensure_spark()
            if spark is not None:
                lookup_df = spark.createDataFrame(pd.DataFrame(lookup_payload))
            else:
                lookup_df = pd.DataFrame(lookup_payload)
        if utils.is_spark(dataset):
            left = dataset.alias("src")
            right = lookup_df.alias("lkp")
            condition = None
            for column in join_columns:
                expr = F.col(f"src.{column}") == F.col(f"lkp.{column}")
                condition = expr if condition is None else condition & expr
            joined = left.join(right, condition, join_type)
            base_cols = [F.col(f"src.{col}").alias(col) for col in dataset.columns]
            lookup_cols = [F.col(f"lkp.{field}").alias(field) for field in output_fields if field in lookup_df.columns]
            joined = joined.select(*(base_cols + lookup_cols))
        else:
            joined = utils.lookup(dataset, lookup_df, join_columns=join_columns, how=join_type)
            if output_fields:
                allowed = set(dataset.columns).union(output_fields)
                joined = joined[[col for col in joined.columns if col in allowed]]
        target_id = self.config.options.get("output", source_id)
        self.store_dataset(datasets, target_id, joined)
