from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field

from libs.logging_utils import configure_logging, log_exception
from services.workers.pipeline_runner import PipelineRunner

configure_logging()

app = FastAPI(title="Data Lake Orchestrator", version="0.1.0")
logger = logging.getLogger("orchestrator")
BASE_PATH = Path(__file__).resolve().parents[2]
runner = PipelineRunner(base_path=BASE_PATH)


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
    try:
        base = BASE_PATH / "config" / "applications" / app_name / layer / "pipelines"
        if not base.exists():
            raise HTTPException(status_code=404, detail=f"No pipelines found for {app_name}/{layer}")
        return sorted(path.stem for path in base.glob("*.xml"))
    except HTTPException:
        raise
    except Exception as exc:
        log_exception(logger, "Failed to list pipelines", exc)
        raise HTTPException(status_code=500, detail="Unable to list pipelines") from exc


def _run_pipeline_sync(payload: PipelineRunRequest) -> PipelineRunResponse:
    try:
        result = runner.run_by_id(app=payload.app, layer=payload.layer, pipeline_id=payload.pipeline_id, overrides=payload.parameters)
    except FileNotFoundError as exc:  # pragma: no cover - FastAPI level
        log_exception(logger, "Pipeline run failed - definition missing", exc)
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        log_exception(logger, "Pipeline execution failed", exc)
        raise HTTPException(status_code=500, detail="Pipeline execution failed") from exc
    return PipelineRunResponse(run_id=result["run_id"], status="completed", metrics=result["metrics"])


@app.post("/pipelines/run", response_model=PipelineRunResponse)
def trigger_pipeline(payload: PipelineRunRequest, background_tasks: BackgroundTasks | None = None):
    if background_tasks is None:
        return _run_pipeline_sync(payload)

    response = PipelineRunResponse(run_id="pending", status="accepted", metrics={})

    def _background_job():
        logger.info("Starting async run for %s", payload.pipeline_id)
        try:
            _run_pipeline_sync(payload)
        except HTTPException:
            raise
        except Exception as exc:
            log_exception(logger, "Background run crashed", exc)

    background_tasks.add_task(_background_job)
    return response
