from __future__ import annotations

from libs.transformations.base import BaseTransformation
from libs.transformations import utils


class MaskTransformation(BaseTransformation):
    def apply(self, datasets, context) -> None:
        source_id = self.config.options["source"]
        dataset = self.resolve_dataset(datasets, source_id)
        columns = [col.strip() for col in self.config.options.get("columns", "").split(",") if col.strip()]
        strategy = self.config.options.get("strategy", "hash")
        masked = utils.mask(dataset, columns, strategy)
        target_id = self.config.options.get("output", source_id)
        self.store_dataset(datasets, target_id, masked)
