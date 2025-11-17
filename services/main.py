from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from glob import glob
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from libs.logging_utils import configure_logging, log_exception
from services.workers.pipeline_runner import PipelineRunner

BASE_PATH = Path(__file__).resolve().parents[1]
CONFIG_ROOT = (BASE_PATH / "config").resolve()
DATA_ROOT = (BASE_PATH / "data").resolve()
LOGS_ROOT = (BASE_PATH / "logs").resolve()
SELF_STATIC_DIR = Path(__file__).resolve().parent / "self_service" / "static"
DASHBOARD_STATIC_DIR = Path(__file__).resolve().parent / "dashboard" / "static"
REMOTE_PREFIXES = ("s3://", "gs://", "abfs://", "adl://", "jdbc:", "http://", "https://")

configure_logging()

app = FastAPI(title="Self-Service Portal", version="0.1.0")
logger = logging.getLogger("self_service")
runner = PipelineRunner(base_path=BASE_PATH)

if SELF_STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=SELF_STATIC_DIR), name="static")


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


class PipelineValidatePayload(BaseModel):
    path: str = Field(..., description="Pipeline path relative to config root")


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
    return RedirectResponse(url="/self")


@app.get("/self", response_class=HTMLResponse)
async def self_service_portal() -> HTMLResponse:
    return _serve_static_page("selfservice.html", base_dir=SELF_STATIC_DIR)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_portal() -> HTMLResponse:
    return _serve_static_page("dashboard.html", base_dir=DASHBOARD_STATIC_DIR)


@app.get("/api/pipelines/tree")
async def list_pipeline_tree() -> dict:
    try:
        return {
            "root": "config",
            "nodes": _build_tree(CONFIG_ROOT),
        }
    except Exception as exc:
        log_exception(logger, "Failed to build pipeline tree", exc)
        raise HTTPException(status_code=500, detail="Unable to read pipeline tree") from exc


@app.get("/api/pipeline")
async def get_pipeline(path: str = Query(..., description="Path relative to config root")) -> dict:
    target = _resolve_config_path(path)
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail=f"Pipeline {path} not found")
    try:
        return {"path": path, "content": target.read_text()}
    except OSError as exc:
        log_exception(logger, "Unable to read pipeline file", exc)
        raise HTTPException(status_code=500, detail="Unable to read pipeline file") from exc


@app.put("/api/pipeline")
async def save_pipeline(payload: PipelinePayload) -> dict:
    try:
        target = _resolve_config_path(payload.path)
        if not target.parent.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload.content)
        logger.info("Saved pipeline %s", payload.path)
        return {"status": "saved", "path": payload.path}
    except HTTPException:
        raise
    except Exception as exc:
        log_exception(logger, "Failed to save pipeline", exc)
        raise HTTPException(status_code=500, detail="Unable to save pipeline") from exc


@app.post("/api/pipeline/validate")
async def validate_pipeline(payload: PipelineValidatePayload) -> dict:
    config_path = _resolve_config_path(payload.path)
    result = _validate_pipeline_file(config_path)
    logger.info("Validation requested for %s valid=%s", payload.path, result["valid"])
    return result


@app.post("/api/pipeline/new")
async def create_pipeline(payload: NewPipelineRequest) -> dict:
    try:
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
        logger.info("Created pipeline %s", relative_path)
        return {"path": relative_path, "content": template}
    except HTTPException:
        raise
    except Exception as exc:
        log_exception(logger, "Failed to create pipeline", exc)
        raise HTTPException(status_code=500, detail="Unable to create pipeline") from exc


@app.post("/api/pipeline/run")
async def run_pipeline(payload: PipelineRunPayload) -> dict:
    config_path = _resolve_config_path(payload.path)
    identifiers = _extract_pipeline_identifiers(config_path)
    if identifiers is None:
        raise HTTPException(status_code=400, detail="Unsupported pipeline path structure")
    validation = _validate_pipeline_file(config_path)
    if not validation["valid"]:
        raise HTTPException(status_code=400, detail=validation)
    app_name, layer, pipeline_id = identifiers
    try:
        result = runner.run_by_id(app=app_name, layer=layer, pipeline_id=pipeline_id, overrides=payload.parameters or None)
    except Exception as exc:
        log_exception(logger, "Pipeline execution failed via portal", exc)
        raise HTTPException(status_code=500, detail="Pipeline execution failed") from exc
    log_entry = _read_log(app_name, layer, pipeline_id, result["run_id"])
    return {
        "status": "completed",
        "run_id": result["run_id"],
        "metrics": result["metrics"],
        "log": log_entry,
        "warnings": validation.get("warnings", []),
    }


@app.get("/api/pipeline/logs")
async def list_pipeline_logs(path: str = Query(..., description="Pipeline path relative to config root"), limit: int = 5) -> dict:
    config_path = _resolve_config_path(path)
    identifiers = _extract_pipeline_identifiers(config_path)
    if identifiers is None:
        raise HTTPException(status_code=400, detail="Unsupported pipeline path structure")
    app_name, layer, pipeline_id = identifiers
    try:
        entries = _list_logs(app_name, layer, pipeline_id, limit=limit)
        return {"logs": entries}
    except Exception as exc:
        log_exception(logger, "Failed to list pipeline logs", exc)
        raise HTTPException(status_code=500, detail="Unable to list logs") from exc


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
    try:
        entry = _read_log(app_name, layer, pipeline_id, run_id)
    except Exception as exc:
        log_exception(logger, "Failed to read pipeline log", exc)
        raise HTTPException(status_code=500, detail="Unable to read log") from exc
    if entry is None:
        raise HTTPException(status_code=404, detail="Log not found")
    return {"log": entry}


@app.get("/api/transformations")
async def list_transformations() -> List[dict]:
    return TRANSFORMATIONS_METADATA


@app.get("/api/dashboard/config-summary")
async def get_dashboard_config_summary() -> dict:
    try:
        summary = _build_dashboard_configuration_summary()
        return summary
    except Exception as exc:
        log_exception(logger, "Failed to load dashboard configuration summary", exc)
        raise HTTPException(status_code=500, detail="Unable to load configuration summary") from exc


@app.get("/api/dashboard/operations")
async def get_dashboard_operations() -> dict:
    try:
        runs = _collect_pipeline_run_history()
        return {"runs": runs, "generated_at": datetime.now(timezone.utc).isoformat()}
    except Exception as exc:
        log_exception(logger, "Failed to load operations dashboard data", exc)
        raise HTTPException(status_code=500, detail="Unable to load operations data") from exc


def _serve_static_page(filename: str, *, base_dir: Path) -> HTMLResponse:
    asset_path = base_dir / filename
    if not asset_path.exists():
        raise HTTPException(status_code=404, detail=f"{filename} asset missing")
    return HTMLResponse(asset_path.read_text())


def _validate_pipeline_file(config_path: Path) -> dict:
    errors: List[str] = []
    warnings: List[str] = []
    if not config_path.exists():
        errors.append(f"Pipeline file {config_path.name} does not exist")
        return {"valid": False, "errors": errors, "warnings": warnings}
    try:
        contents = config_path.read_text()
    except OSError as exc:
        log_exception(logger, "Failed to read pipeline during validation", exc)
        errors.append(f"Unable to read pipeline: {exc}")
        return {"valid": False, "errors": errors, "warnings": warnings}
    if not contents.strip():
        errors.append("Pipeline file is empty")
        return {"valid": False, "errors": errors, "warnings": warnings}
    try:
        root = ET.fromstring(contents)
    except ET.ParseError as exc:
        errors.append(f"XML parse error: {exc}")
        return {"valid": False, "errors": errors, "warnings": warnings}
    if root.tag != "pipeline":
        errors.append("Root element must be <pipeline>")
        return {"valid": False, "errors": errors, "warnings": warnings}
    pipeline_id = root.get("id")
    layer = root.get("layer")
    if not pipeline_id:
        errors.append("Pipeline attribute 'id' is required")
    if not layer:
        errors.append("Pipeline attribute 'layer' is required")
    metadata = root.find("metadata")
    if metadata is None:
        errors.append("<metadata> block is required with <appName>")
        app_name = None
    else:
        app_name = (metadata.findtext("appName") or "").strip()
        if not app_name:
            errors.append("<appName> must be provided inside <metadata>")
    sources = root.find("sources")
    csv_sources = sources.findall("csv") if sources is not None else []
    if sources is None or not csv_sources:
        errors.append("At least one <csv> source must be defined")
    for csv_node in csv_sources:
        source_id = csv_node.get("id") or "<unknown>"
        path_value = csv_node.get("path")
        if not path_value:
            errors.append(f"Source '{source_id}' is missing required 'path' attribute")
            continue
        if _is_remote_path(path_value):
            warnings.append(f"Skipping remote source path validation for {path_value}")
        else:
            path_errors, path_warnings = _validate_data_path(path_value)
            errors.extend(path_errors)
            warnings.extend(path_warnings)
        schema_ref = csv_node.find("schemaRef")
        if schema_ref is not None and schema_ref.text:
            schema_errors = _validate_schema_reference(schema_ref.text.strip())
            errors.extend(schema_errors)
    targets = root.find("targets")
    if targets is None or not list(targets):
        errors.append("At least one <target> is required")
    for lookup_node in root.findall(".//lookup"):
        reference = lookup_node.get("reference")
        if reference:
            lookup_path = CONFIG_ROOT / "common" / "lookups" / f"{reference}.json"
            if not lookup_path.exists():
                warnings.append(f"Lookup reference '{reference}' was not found under common/lookups")
    logger.debug(
        "Validation finished for %s (errors=%d warnings=%d)",
        config_path,
        len(errors),
        len(warnings),
    )
    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}


def _validate_data_path(raw_path: str) -> tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    matches = _resolve_data_matches(raw_path)
    if not matches:
        errors.append(f"Data file or folder '{raw_path}' does not exist")
        return errors, warnings
    empty_files = [path for path in matches if path.is_file() and path.stat().st_size == 0]
    if empty_files and len(empty_files) == len(matches):
        errors.append(f"Data file(s) at '{raw_path}' contain no data")
    elif empty_files:
        warnings.append(
            "Some files at '{path}' are empty: {files}".format(
                path=raw_path,
                files=", ".join(str(path) for path in empty_files[:3]),
            )
        )
    return errors, warnings


def _validate_schema_reference(reference_path: str) -> List[str]:
    candidate = (BASE_PATH / reference_path).resolve()
    try:
        candidate.relative_to(BASE_PATH)
    except ValueError:
        return [f"Schema reference '{reference_path}' escapes repository"]
    if not candidate.exists():
        return [f"Schema reference '{reference_path}' not found"]
    return []


def _resolve_data_matches(raw_path: str) -> List[Path]:
    has_glob = any(ch in raw_path for ch in "*?[")
    candidates: List[str]
    path_obj = Path(raw_path)
    if path_obj.is_absolute():
        candidates = [raw_path]
    else:
        candidates = [
            str((BASE_PATH / raw_path)),
            str((DATA_ROOT / raw_path)),
        ]
    resolved: List[Path] = []
    for candidate in candidates:
        if has_glob:
            resolved.extend(Path(match) for match in glob(candidate))
        else:
            path_candidate = Path(candidate)
            if path_candidate.exists():
                resolved.append(path_candidate)
    unique: List[Path] = []
    seen = set()
    for match in resolved:
        match_resolved = match.resolve()
        if match_resolved in seen:
            continue
        seen.add(match_resolved)
        unique.append(match_resolved)
    return unique


def _is_remote_path(raw_path: str) -> bool:
    return raw_path.startswith(REMOTE_PREFIXES)


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


def _build_dashboard_configuration_summary() -> dict:
    summaries: List[dict] = []
    applications_root = CONFIG_ROOT / "applications"
    if not applications_root.exists():
        return {"applications": [], "layers": [], "summaries": [], "generated_at": datetime.now(timezone.utc).isoformat()}
    layers: set[str] = set()
    applications: set[str] = set()
    for app_dir in _iter_child_directories(applications_root):
        app_name = app_dir.name
        applications.add(app_name)
        for layer_dir in _iter_child_directories(app_dir):
            layer_name = layer_dir.name
            layers.add(layer_name)
            pipelines = _collect_xml_ids(layer_dir / "pipelines")
            jobs = _collect_xml_ids(layer_dir / "jobs")
            data_files = _count_data_files(app_name, layer_name)
            summaries.append(
                {
                    "app": app_name,
                    "layer": layer_name,
                    "pipelines": pipelines,
                    "jobs": jobs,
                    "data_file_count": data_files,
                }
            )
    summaries.sort(key=lambda item: (item["app"], item["layer"]))
    return {
        "applications": sorted(applications),
        "layers": sorted(layers),
        "summaries": summaries,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _iter_child_directories(parent: Path) -> List[Path]:
    if not parent.exists():
        return []
    entries: List[Path] = []
    for child in sorted(parent.iterdir(), key=lambda p: p.name.lower()):
        if child.is_dir() and not child.name.startswith((".", "_")):
            entries.append(child)
    return entries


def _collect_xml_ids(path: Path) -> List[str]:
    if not path.exists():
        return []
    return sorted(child.stem for child in path.glob("*.xml"))


def _count_data_files(app_name: str, layer_name: str) -> int:
    layer_root = DATA_ROOT / "applications" / app_name / layer_name
    if not layer_root.exists():
        return 0
    count = 0
    for candidate in layer_root.rglob("*"):
        if candidate.is_file():
            count += 1
    return count


def _collect_pipeline_run_history() -> List[dict]:
    logs_root = LOGS_ROOT / "applications"
    if not logs_root.exists():
        return []
    runs: List[dict] = []
    for app_dir in _iter_child_directories(logs_root):
        app_name = app_dir.name
        for layer_dir in _iter_child_directories(app_dir):
            layer_name = layer_dir.name
            for log_file in sorted(layer_dir.glob("*.log")):
                payload = _load_log_file(log_file)
                if not payload:
                    continue
                metrics = payload.get("metrics") or {}
                duration_ms = payload.get("duration_ms")
                duration_minutes = None
                if isinstance(duration_ms, (int, float)):
                    duration_minutes = round(duration_ms / 60000, 2)
                rows_processed = _derive_row_count(metrics)
                runs.append(
                    {
                        "run_id": payload.get("run_id"),
                        "pipeline_id": payload.get("pipeline_id"),
                        "app_name": payload.get("app_name") or app_name,
                        "layer": payload.get("layer") or layer_name,
                        "executed_at": payload.get("executed_at"),
                        "rows_processed": rows_processed,
                        "status": payload.get("status", "completed"),
                        "duration_minutes": duration_minutes,
                    }
                )
    runs.sort(key=lambda item: item.get("executed_at") or "", reverse=True)
    return runs


def _derive_row_count(metrics: Dict[str, object]) -> Optional[int]:
    if not metrics:
        return None
    preferred_keys = ["rows_processed", "records_processed", "rows_written", "records_written"]
    for key in preferred_keys:
        value = metrics.get(key)
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
    total = 0.0
    for key, value in metrics.items():
        if "row" not in key.lower() and "record" not in key.lower():
            continue
        if isinstance(value, (int, float)):
            total += float(value)
        elif isinstance(value, str):
            try:
                total += float(value)
            except ValueError:
                continue
    if total > 0:
        return int(total)
    return None


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("services.main:app", host="0.0.0.0", port=8081, reload=True)
