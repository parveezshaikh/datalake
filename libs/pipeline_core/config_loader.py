from __future__ import annotations

from pathlib import Path
from typing import List

try:
    from lxml import etree
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    from xml.etree import ElementTree as etree  # type: ignore

from .models import (
    ErrorPolicy,
    JobPipelineConfig,
    PipelineConfig,
    PipelineMetadata,
    PipelineRef,
    SourceConfig,
    TargetConfig,
    TransformationConfig,
)


def _element_options(element) -> dict:
    opts = {**element.attrib}
    for child in element:
        # Nested tags become options using tag name as key
        text = (child.text or "").strip()
        if text:
            opts[child.tag] = text
    return opts


def _iter_children(parent, tag: str):
    node = parent.find(tag)
    return [] if node is None else list(node)


def _parse_sources(root) -> List[SourceConfig]:
    sources = []
    for source in _iter_children(root, "sources"):
        source_id = source.attrib.get("id") or source.attrib.get("name")
        if not source_id:
            raise ValueError("Each source requires an 'id' or 'name' attribute")
        sources.append(SourceConfig(id=source_id, type=source.tag, options=_element_options(source)))
    return sources


def _parse_transformations(root) -> List[TransformationConfig]:
    return [
        TransformationConfig(name=element.tag, options=_element_options(element))
        for element in _iter_children(root, "transformations")
    ]


def _parse_targets(root) -> List[TargetConfig]:
    return [TargetConfig(type=element.tag, options=_element_options(element)) for element in _iter_children(root, "targets")]


def _parse_error_policy(root) -> ErrorPolicy:
    policy_element = root.find("errorPolicy")
    if policy_element is None:
        return ErrorPolicy()
    options = _element_options(policy_element)
    return ErrorPolicy(
        on_record_error=options.get("onRecordError", "quarantine"),
        on_step_error=options.get("onStepError", "halt"),
        quarantine_topic=options.get("quarantineTopic"),
        remediation_sla=options.get("remediationSla"),
    )


def _safe_findtext(element, tag: str, default: str | None = None) -> str | None:
    if element is None:
        return default
    text = element.findtext(tag)
    return text if text is not None else default


def load_pipeline_config(path: Path) -> PipelineConfig:
    tree = etree.parse(str(path))
    root = tree.getroot()
    metadata_element = root.find("metadata")
    metadata = PipelineMetadata(
        app_name=_safe_findtext(metadata_element, "appName", "unknown"),
        sla=_safe_findtext(metadata_element, "sla"),
        schedule=_safe_findtext(metadata_element, "schedule"),
    )
    config = PipelineConfig(
        pipeline_id=root.attrib["id"],
        version=root.attrib.get("version", "1.0"),
        layer=root.attrib.get("layer", "staging"),
        metadata=metadata,
        sources=_parse_sources(root),
        transformations=_parse_transformations(root),
        targets=_parse_targets(root),
        error_policy=_parse_error_policy(root),
        path=Path(path),
    )
    return config


def load_job_pipeline_config(path: Path) -> JobPipelineConfig:
    tree = etree.parse(str(path))
    root = tree.getroot()
    pipeline_refs: List[PipelineRef] = []
    for ref in _iter_children(root, "pipelines"):
        if ref.tag != "pipelineRef":
            continue
        pipeline_refs.append(
            PipelineRef(
                pipeline_id=ref.attrib["id"],
                depends_on=ref.attrib.get("dependsOn"),
                on_failure=ref.attrib.get("onFailure", "retry"),
                max_retries=int(ref.attrib.get("maxRetries", 0)),
            )
        )
    notifications = {}
    for node in _iter_children(root, "notifications"):
        notifications.setdefault(node.tag, []).append({**node.attrib})
    return JobPipelineConfig(
        job_id=root.attrib["id"],
        version=root.attrib.get("version", "1.0"),
        pipelines=pipeline_refs,
        notifications=notifications,
        path=Path(path),
    )
