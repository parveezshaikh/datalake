from __future__ import annotations

from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE_PATH = Path(__file__).resolve().parents[2]
CONFIG_ROOT = (BASE_PATH / "config").resolve()
STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(title="Self-Service Portal", version="0.1.0")

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class PipelinePayload(BaseModel):
    path: str = Field(..., description="Path relative to config root")
    content: str = Field(..., description="Full pipeline XML contents")


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


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("services.self_service.main:app", host="0.0.0.0", port=8081, reload=True)
