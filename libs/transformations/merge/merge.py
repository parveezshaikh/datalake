from __future__ import annotations

import pandas as pd

from libs.transformations.base import BaseTransformation
from libs.transformations import utils


class MergeTransformation(BaseTransformation):
    def apply(self, datasets, context) -> None:
        source_id = self.config.options["source"]
        target_dataset_id = self.config.options.get("target", source_id)
        keys = [col.strip() for col in self.config.options.get("keys", "").split(",") if col.strip()]
        source_df = self.resolve_dataset(datasets, source_id)
        target_df = self.resolve_dataset(datasets, target_dataset_id) if target_dataset_id in datasets else None
        if target_df is None:
            merged = source_df
        elif utils.is_spark(source_df):
            merged = source_df.unionByName(target_df, allowMissingColumns=True)
            if keys:
                merged = merged.dropDuplicates(keys)
        else:
            source_pd = source_df if isinstance(source_df, pd.DataFrame) else pd.DataFrame(source_df)
            target_pd = target_df if isinstance(target_df, pd.DataFrame) else pd.DataFrame(target_df)
            merged = pd.concat([target_pd, source_pd], ignore_index=True)
            if keys:
                merged = merged.drop_duplicates(subset=keys, keep="last")
        output_id = self.config.options.get("output", target_dataset_id)
        self.store_dataset(datasets, output_id, merged)
