from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field

from services.workers.pipeline_runner import PipelineRunner

app = FastAPI(title="Data Lake Orchestrator", version="0.1.0")
logger = logging.getLogger("orchestrator")
runner = PipelineRunner(base_path=Path(__file__).resolve().parents[2])


class PipelineRunRequest(BaseModel):
    app: str = Field(..., description="Application name (e.g., sample_app)")
    layer: str = Field(..., description="Data lifecycle layer (staging/standardization/service)")
    pipeline_id: str = Field(..., description="Pipeline identifier (matches XML filename)")
    parameters: dict = Field(default_factory=dict)


class PipelineRunResponse(BaseModel):
    run_id: str
    status: str
    metrics: dict


@app.get("/healthz")
def health() -> dict:
    return {"status": "ok"}


@app.get("/pipelines")
def list_pipelines(app_name: str, layer: str) -> List[str]:
    base = Path(__file__).resolve().parents[2] / "config" / "applications" / app_name / layer / "pipelines"
    if not base.exists():
        raise HTTPException(status_code=404, detail=f"No pipelines found for {app_name}/{layer}")
    return sorted(path.stem for path in base.glob("*.xml"))


def _run_pipeline_sync(payload: PipelineRunRequest) -> PipelineRunResponse:
    try:
        result = runner.run_by_id(app=payload.app, layer=payload.layer, pipeline_id=payload.pipeline_id, overrides=payload.parameters)
    except FileNotFoundError as exc:  # pragma: no cover - FastAPI level
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PipelineRunResponse(run_id=result["run_id"], status="completed", metrics=result["metrics"])


@app.post("/pipelines/run", response_model=PipelineRunResponse)
def trigger_pipeline(payload: PipelineRunRequest, background_tasks: BackgroundTasks | None = None):
    if background_tasks is None:
        return _run_pipeline_sync(payload)

    response = PipelineRunResponse(run_id="pending", status="accepted", metrics={})

    def _background_job():
        logger.info("Starting async run for %s", payload.pipeline_id)
        _run_pipeline_sync(payload)

    background_tasks.add_task(_background_job)
    return response
