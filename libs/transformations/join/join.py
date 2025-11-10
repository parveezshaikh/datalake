from __future__ import annotations

from libs.transformations.base import BaseTransformation
from libs.transformations import utils


class JoinTransformation(BaseTransformation):
    def apply(self, datasets, context) -> None:
        left_id = self.config.options["left"]
        right_id = self.config.options["right"]
        how = self.config.options.get("type", "inner")
        condition = self.config.options.get("condition")
        left_df = self.resolve_dataset(datasets, left_id)
        right_df = self.resolve_dataset(datasets, right_id)
        joined = utils.join(left_df, right_df, condition=condition, how=how)
        target_id = self.config.options.get("output", left_id)
        self.store_dataset(datasets, target_id, joined)
