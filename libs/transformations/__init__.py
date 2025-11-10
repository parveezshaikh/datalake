from __future__ import annotations

from libs.pipeline_core.step_registry import StepRegistry

from .aggregate.aggregate import AggregateTransformation
from .deduplicate.deduplicate import DeduplicateTransformation
from .join.join import JoinTransformation
from .lookup.lookup import LookupTransformation
from .mask.mask import MaskTransformation
from .merge.merge import MergeTransformation
from .sort.sort import SortTransformation


def register_default_transformations(registry: StepRegistry) -> None:
    registry.register("deduplicate", DeduplicateTransformation)
    registry.register("sort", SortTransformation)
    registry.register("join", JoinTransformation)
    registry.register("lookup", LookupTransformation)
    registry.register("mask", MaskTransformation)
    registry.register("aggregate", AggregateTransformation)
    registry.register("merge", MergeTransformation)
