from __future__ import annotations

import json
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
        result = {
            "pipeline_id": pipeline_config.pipeline_id,
            "metrics": context.metrics.counters,
            "run_id": context.run_id,
        }
        self._write_run_log(pipeline_config, context, result)
        return result

    def run_by_id(self, *, app: str, layer: str, pipeline_id: str, overrides: Optional[Dict[str, str]] = None) -> Dict[str, object]:
        pipeline_path = self.base_path / "config" / "applications" / app / layer / "pipelines" / f"{pipeline_id}.xml"
        if not pipeline_path.exists():
            raise FileNotFoundError(f"Pipeline definition {pipeline_path} not found")
        return self.run_pipeline(pipeline_path, overrides=overrides)

    def _write_run_log(self, pipeline_config, context, result) -> None:
        logs_root = (
            self.base_path
            / "logs"
            / "applications"
            / pipeline_config.metadata.app_name
            / pipeline_config.layer
        )
        logs_root.mkdir(parents=True, exist_ok=True)
        log_entry = {
            "run_id": context.run_id,
            "pipeline_id": pipeline_config.pipeline_id,
            "app_name": pipeline_config.metadata.app_name,
            "layer": pipeline_config.layer,
            "version": pipeline_config.version,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "metrics": result.get("metrics", {}),
            "config_path": str(pipeline_config.path or ""),
        }
        log_path = logs_root / f"{pipeline_config.pipeline_id}_{context.run_id}.log"
        log_path.write_text(json.dumps(log_entry, indent=2))
        self.logger.info("Pipeline log recorded at %s", log_path)
