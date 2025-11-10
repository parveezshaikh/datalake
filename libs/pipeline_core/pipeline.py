from __future__ import annotations

import logging
from typing import Dict

from libs.connectors import build_source, build_target
from libs.pipeline_core.models import PipelineConfig, SourceConfig, TargetConfig, TransformationConfig
from libs.pipeline_core.step_registry import StepRegistry


class SparkPipeline:
    def __init__(self, config: PipelineConfig, context, step_registry: StepRegistry) -> None:
        self.config = config
        self.context = context
        self.step_registry = step_registry
        self.logger = logging.getLogger(f"pipeline.{config.pipeline_id}")
        self.datasets: Dict[str, object] = {}

    def run(self) -> Dict[str, object]:
        self.logger.info("Starting pipeline %s run_id=%s", self.config.pipeline_id, self.context.run_id)
        self._load_sources()
        self._apply_transformations()
        self._write_targets()
        self.logger.info("Completed pipeline %s", self.config.pipeline_id)
        return self.datasets

    def _load_sources(self) -> None:
        for source in self.config.sources:
            self.logger.info("Loading source %s (%s)", source.id, source.type)
            connector = build_source(source.type, options=source.options, context=self.context)
            dataset = connector.load()
            self.datasets[source.id] = dataset
            self.context.metrics.incr("pipeline_source_loaded")

    def _apply_transformations(self) -> None:
        for transformation_cfg in self.config.transformations:
            strategy = self.step_registry.build(transformation_cfg)
            self.logger.info("Applying transformation %s", transformation_cfg.name)
            try:
                strategy.apply(self.datasets, self.context)
                self.context.metrics.incr(f"pipeline_transform_{transformation_cfg.name}")
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.exception("Transformation %s failed: %s", transformation_cfg.name, exc)
                if self.config.error_policy.on_step_error == "halt":
                    raise
                if self.config.error_policy.on_step_error == "quarantine":
                    try:
                        from libs.data_quality import quarantine_records  # local import avoid cycle

                        dataset_id = transformation_cfg.options.get("source")
                        if dataset_id and dataset_id in self.datasets:
                            quarantine_records(
                                self.datasets[dataset_id],
                                context=self.context,
                                reason=f"transformation_failure:{transformation_cfg.name}",
                            )
                    except Exception:  # pragma: no cover
                        self.logger.exception("Failed to quarantine records for %s", transformation_cfg.name)

    def _write_targets(self) -> None:
        if not self.config.targets:
            self.logger.warning("Pipeline %s has no targets", self.config.pipeline_id)
            return
        for target in self.config.targets:
            dataset_id = target.options.get("source") or target.options.get("dataset")
            dataset = self.datasets.get(dataset_id) if dataset_id else next(iter(self.datasets.values()))
            if dataset is None:
                raise KeyError(f"Dataset '{dataset_id}' unavailable for target {target.type}")
            connector = build_target(target.type, options=target.options, context=self.context)
            connector.save(dataset)
            self.context.metrics.incr("pipeline_targets_written")
