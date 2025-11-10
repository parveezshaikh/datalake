from __future__ import annotations

from libs.transformations.base import BaseTransformation
from libs.transformations import utils


class SortTransformation(BaseTransformation):
    def apply(self, datasets, context) -> None:
        source_id = self.config.options["source"]
        dataset = self.resolve_dataset(datasets, source_id)
        columns = [col.strip() for col in self.config.options.get("orderBy", "").split(",") if col.strip()]
        direction = self.config.options.get("direction", "asc")
        sorted_df = utils.sort(dataset, columns, ascending=direction.lower() == "asc")
        target_id = self.config.options.get("output", source_id)
        self.store_dataset(datasets, target_id, sorted_df)
