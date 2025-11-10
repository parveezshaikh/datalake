from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from services.workers.pipeline_runner import PipelineRunner

BASE_PATH = Path(__file__).resolve().parents[2]
CONFIG_ROOT = (BASE_PATH / "config").resolve()
LOGS_ROOT = (BASE_PATH / "logs").resolve()
STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(title="Self-Service Portal", version="0.1.0")
runner = PipelineRunner(base_path=BASE_PATH)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class PipelinePayload(BaseModel):
    path: str = Field(..., description="Path relative to config root")
    content: str = Field(..., description="Full pipeline XML contents")


class NewPipelineRequest(BaseModel):
    app: str = Field(..., description="Application name (applications/<app>)")
    layer: str = Field(..., pattern="^(staging|standardization|service)$", description="Data layer")
    pipeline_id: str = Field(..., description="Pipeline identifier (filename without .xml)")


class PipelineRunPayload(BaseModel):
    path: str = Field(..., description="Pipeline path relative to config root")
    parameters: Dict[str, str] = Field(default_factory=dict, description="Optional override parameters")


DEFAULT_PIPELINE_TEMPLATE = """<pipeline id="{pipeline_id}" version="1.0" layer="{layer}">
  <metadata>
    <appName>{app}</appName>
    <sla>PT1H</sla>
    <schedule>0 * * * *</schedule>
  </metadata>
  <sources>
    <csv id="{pipeline_id}_source" path="data/{app}/{layer}/{pipeline_id}.csv" header="true" delimiter="," inferSchema="true" />
  </sources>
  <transformations>
    <!-- add transformations here -->
  </transformations>
  <targets>
    <csv source="{pipeline_id}_source" path="data/applications/{app}/{layer}/{pipeline_id}.csv" mode="overwrite" />
  </targets>
</pipeline>
"""


TRANSFORMATIONS_METADATA = [
    {
        "name": "deduplicate",
        "description": "Remove duplicate rows based on one or more key columns.",
        "attributes": [
            {"name": "source", "required": True, "description": "Dataset id to deduplicate."},
            {"name": "keys", "required": True, "description": "Comma separated list of key columns."},
            {"name": "keep", "required": False, "description": "first | last | latest (default first)."},
            {"name": "orderBy", "required": False, "description": "Columns used when keep=latest."},
            {"name": "output", "required": False, "description": "Dataset id to store the deduplicated result."},
        ],
    },
    {
        "name": "sort",
        "description": "Sort dataset by one or more columns.",
        "attributes": [
            {"name": "source", "required": True, "description": "Dataset id to sort."},
            {"name": "orderBy", "required": True, "description": "Comma separated columns to order by."},
            {"name": "direction", "required": False, "description": "asc | desc (default asc)."},
            {"name": "output", "required": False, "description": "Dataset id for sorted output."},
        ],
    },
    {
        "name": "join",
        "description": "Join two datasets using a SQL style condition.",
        "attributes": [
            {"name": "left", "required": True, "description": "Left dataset id."},
            {"name": "right", "required": True, "description": "Right dataset id."},
            {"name": "type", "required": False, "description": "inner | left | right | full (default inner)."},
            {"name": "condition", "required": True, "description": "Join expression such as left.col = right.col."},
            {"name": "output", "required": False, "description": "Dataset id for the join result (defaults to left)."},
        ],
    },
    {
        "name": "lookup",
        "description": "Enrich a dataset with lookup table values.",
        "attributes": [
            {"name": "source", "required": True, "description": "Dataset id to enrich."},
            {"name": "reference", "required": True, "description": "Lookup reference id or dataset."},
            {"name": "joinColumns", "required": True, "description": "Columns used to match source and lookup."},
            {"name": "joinType", "required": False, "description": "left | inner (default left)."},
            {"name": "outputFields", "required": False, "description": "Lookup columns to retain in the result."},
            {"name": "output", "required": False, "description": "Dataset id for enriched output."},
        ],
    },
    {
        "name": "mask",
        "description": "Apply masking strategy to sensitive columns.",
        "attributes": [
            {"name": "source", "required": True, "description": "Dataset id containing sensitive data."},
            {"name": "columns", "required": True, "description": "Comma separated columns to mask."},
            {"name": "strategy", "required": False, "description": "hash | tokenize | redact (default hash)."},
            {"name": "output", "required": False, "description": "Dataset id with masked data."},
        ],
    },
    {
        "name": "aggregate",
        "description": "Group and aggregate rows to compute metrics.",
        "attributes": [
            {"name": "source", "required": True, "description": "Dataset id to aggregate."},
            {"name": "groupBy", "required": True, "description": "Comma separated grouping columns."},
            {"name": "metrics", "required": True, "description": "Expressions like count:*, sum:amount."},
            {"name": "output", "required": False, "description": "Dataset id for aggregated result."},
        ],
    },
    {
        "name": "merge",
        "description": "Union two datasets and optionally deduplicate on keys.",
        "attributes": [
            {"name": "source", "required": True, "description": "New dataset to merge."},
            {"name": "target", "required": False, "description": "Existing dataset id to merge into."},
            {"name": "keys", "required": False, "description": "Columns used to drop duplicates after merge."},
            {"name": "output", "required": False, "description": "Dataset id for merged result (defaults to target)."},
        ],
    },
]


@app.get("/", response_class=HTMLResponse)
async def root_page() -> HTMLResponse:
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="UI assets missing")
    return HTMLResponse(index_path.read_text())


@app.get("/api/pipelines/tree")
async def list_pipeline_tree() -> dict:
    return {
        "root": "config",
        "nodes": _build_tree(CONFIG_ROOT),
    }


@app.get("/api/pipeline")
async def get_pipeline(path: str = Query(..., description="Path relative to config root")) -> dict:
    target = _resolve_config_path(path)
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail=f"Pipeline {path} not found")
    return {"path": path, "content": target.read_text()}


@app.put("/api/pipeline")
async def save_pipeline(payload: PipelinePayload) -> dict:
    target = _resolve_config_path(payload.path)
    if not target.parent.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(payload.content)
    return {"status": "saved", "path": payload.path}


@app.post("/api/pipeline/new")
async def create_pipeline(payload: NewPipelineRequest) -> dict:
    relative_path = f"applications/{payload.app}/{payload.layer}/pipelines/{payload.pipeline_id}.xml"
    target = _resolve_config_path(relative_path)
    if target.exists():
        raise HTTPException(status_code=409, detail="Pipeline already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    template = DEFAULT_PIPELINE_TEMPLATE.format(
        pipeline_id=payload.pipeline_id,
        app=payload.app,
        layer=payload.layer,
    )
    target.write_text(template)
    return {"path": relative_path, "content": template}


@app.post("/api/pipeline/run")
async def run_pipeline(payload: PipelineRunPayload) -> dict:
    config_path = _resolve_config_path(payload.path)
    identifiers = _extract_pipeline_identifiers(config_path)
    if identifiers is None:
        raise HTTPException(status_code=400, detail="Unsupported pipeline path structure")
    app_name, layer, pipeline_id = identifiers
    result = runner.run_by_id(app=app_name, layer=layer, pipeline_id=pipeline_id, overrides=payload.parameters or None)
    log_entry = _read_log(app_name, layer, pipeline_id, result["run_id"])
    return {
        "status": "completed",
        "run_id": result["run_id"],
        "metrics": result["metrics"],
        "log": log_entry,
    }


@app.get("/api/pipeline/logs")
async def list_pipeline_logs(path: str = Query(..., description="Pipeline path relative to config root"), limit: int = 5) -> dict:
    config_path = _resolve_config_path(path)
    identifiers = _extract_pipeline_identifiers(config_path)
    if identifiers is None:
        raise HTTPException(status_code=400, detail="Unsupported pipeline path structure")
    app_name, layer, pipeline_id = identifiers
    entries = _list_logs(app_name, layer, pipeline_id, limit=limit)
    return {"logs": entries}


@app.get("/api/pipeline/log")
async def get_pipeline_log(
    path: str = Query(..., description="Pipeline path relative to config root"),
    run_id: str = Query(..., description="Pipeline run identifier"),
) -> dict:
    config_path = _resolve_config_path(path)
    identifiers = _extract_pipeline_identifiers(config_path)
    if identifiers is None:
        raise HTTPException(status_code=400, detail="Unsupported pipeline path structure")
    app_name, layer, pipeline_id = identifiers
    entry = _read_log(app_name, layer, pipeline_id, run_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="Log not found")
    return {"log": entry}


@app.get("/api/transformations")
async def list_transformations() -> List[dict]:
    return TRANSFORMATIONS_METADATA


def _build_tree(root: Path) -> List[dict]:
    def walker(path: Path) -> List[dict]:
        entries: List[dict] = []
        for child in sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name.lower())):
            if child.name.startswith("."):
                continue
            rel_path = child.relative_to(CONFIG_ROOT).as_posix()
            if child.is_dir():
                entries.append(
                    {
                        "name": child.name,
                        "path": rel_path,
                        "type": "directory",
                        "children": walker(child),
                    }
                )
            elif child.suffix.lower() == ".xml":
                entries.append(
                    {
                        "name": child.name,
                        "path": rel_path,
                        "type": "file",
                    }
                )
        return entries

    return walker(root)


def _resolve_config_path(relative_path: str) -> Path:
    if not relative_path:
        raise HTTPException(status_code=400, detail="path is required")
    candidate = (CONFIG_ROOT / relative_path).resolve()
    try:
        candidate.relative_to(CONFIG_ROOT)
    except ValueError as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail="Path escapes config root") from exc
    return candidate


def _extract_pipeline_identifiers(config_path: Path) -> Optional[tuple[str, str, str]]:
    try:
        rel_parts = config_path.relative_to(CONFIG_ROOT).parts
    except ValueError:
        return None
    if len(rel_parts) < 5 or rel_parts[0] != "applications" or rel_parts[3] != "pipelines":
        return None
    app_name = rel_parts[1]
    layer = rel_parts[2]
    pipeline_file = rel_parts[4]
    pipeline_id = Path(pipeline_file).stem
    return app_name, layer, pipeline_id


def _list_logs(app_name: str, layer: str, pipeline_id: str, *, limit: int = 5) -> List[dict]:
    log_dir = LOGS_ROOT / "applications" / app_name / layer
    if not log_dir.exists():
        return []
    entries = []
    for log_file in sorted(log_dir.glob(f"{pipeline_id}_*.log"), key=lambda path: path.stat().st_mtime, reverse=True):
        entry = _load_log_file(log_file)
        if entry:
            entries.append(
                {
                    "run_id": entry.get("run_id"),
                    "executed_at": entry.get("executed_at"),
                    "path": log_file.relative_to(LOGS_ROOT).as_posix(),
                }
            )
        if len(entries) >= limit:
            break
    return entries


def _read_log(app_name: str, layer: str, pipeline_id: str, run_id: str) -> Optional[dict]:
    log_file = LOGS_ROOT / "applications" / app_name / layer / f"{pipeline_id}_{run_id}.log"
    if not log_file.exists():
        return None
    return _load_log_file(log_file)


def _load_log_file(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("services.self_service.main:app", host="0.0.0.0", port=8081, reload=True)
