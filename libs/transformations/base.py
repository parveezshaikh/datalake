from __future__ import annotations

from typing import Dict

from libs.pipeline_core.step_registry import TransformationStrategy


class BaseTransformation(TransformationStrategy):
    def resolve_dataset(self, datasets: Dict[str, object], dataset_id: str):
        if dataset_id not in datasets:
            raise KeyError(f"Dataset '{dataset_id}' is not available for transformation '{self.config.name}'")
        return datasets[dataset_id]

    def store_dataset(self, datasets: Dict[str, object], dataset_id: str, dataset) -> None:
        datasets[dataset_id] = dataset

    def output_dataset_id(self) -> str:
        return self.config.options.get("output", self.config.options.get("source"))
