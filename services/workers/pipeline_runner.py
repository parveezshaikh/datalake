from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from libs.pipeline_core import (
    PipelineContext,
    SparkPipeline,
    StepRegistry,
    load_pipeline_config,
)
from libs.transformations import register_default_transformations


class PipelineRunner:
    def __init__(self, *, base_path: Path | None = None, spark_builder=None) -> None:
        self.base_path = base_path or Path.cwd()
        self.spark_builder = spark_builder
        self.logger = logging.getLogger("pipeline_runner")

    def _build_context(self, pipeline_config, *, overrides: Optional[Dict[str, str]] = None) -> PipelineContext:
        run_id = overrides.get("run_id") if overrides else None
        run_id = run_id or datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%S")
        app_name = pipeline_config.metadata.app_name
        data_root = self.base_path / "data"
        config_root = self.base_path / "config"
        context = PipelineContext(
            run_id=run_id,
            app_name=app_name,
            data_root=data_root,
            config_root=config_root,
            spark_builder=self.spark_builder,
        )
        return context

    def run_pipeline(self, pipeline_path: Path, *, overrides: Optional[Dict[str, str]] = None) -> Dict[str, object]:
        pipeline_config = load_pipeline_config(pipeline_path)
        context = self._build_context(pipeline_config, overrides=overrides)
        registry = StepRegistry()
        register_default_transformations(registry)
        pipeline = SparkPipeline(pipeline_config, context, registry)
        pipeline.run()
        return {
            "pipeline_id": pipeline_config.pipeline_id,
            "metrics": context.metrics.counters,
            "run_id": context.run_id,
        }

    def run_by_id(self, *, app: str, layer: str, pipeline_id: str, overrides: Optional[Dict[str, str]] = None) -> Dict[str, object]:
        pipeline_path = self.base_path / "config" / "applications" / app / layer / "pipelines" / f"{pipeline_id}.xml"
        if not pipeline_path.exists():
            raise FileNotFoundError(f"Pipeline definition {pipeline_path} not found")
        return self.run_pipeline(pipeline_path, overrides=overrides)
