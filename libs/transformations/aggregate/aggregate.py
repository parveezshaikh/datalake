from __future__ import annotations

from libs.transformations.base import BaseTransformation
from libs.transformations import utils


class AggregateTransformation(BaseTransformation):
    def apply(self, datasets, context) -> None:
        source_id = self.config.options["source"]
        dataset = self.resolve_dataset(datasets, source_id)
        group_by = [col.strip() for col in self.config.options.get("groupBy", "").split(",") if col.strip()]
        metrics = [metric.strip() for metric in self.config.options.get("metrics", "").split(",") if metric.strip()]
        aggregated = utils.aggregate(dataset, group_by, metrics)
        target_id = self.config.options.get("output", source_id)
        self.store_dataset(datasets, target_id, aggregated)
