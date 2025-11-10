from __future__ import annotations

from libs.transformations.base import BaseTransformation
from libs.transformations import utils


class DeduplicateTransformation(BaseTransformation):
    def apply(self, datasets, context) -> None:
        source_id = self.config.options["source"]
        keys = [key.strip() for key in self.config.options.get("keys", "").split(",") if key.strip()]
        keep = self.config.options.get("keep", "first")
        order_by = [col.strip() for col in self.config.options.get("orderBy", "").split(",") if col.strip()]
        dataset = self.resolve_dataset(datasets, source_id)
        deduped = utils.deduplicate(dataset, keys, keep=keep, order_by=order_by or None)
        target_id = self.config.options.get("output", source_id)
        self.store_dataset(datasets, target_id, deduped)
        context.logger.debug("Deduplicated %s -> %s", source_id, target_id)
