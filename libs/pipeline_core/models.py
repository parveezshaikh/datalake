from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class PipelineMetadata:
    app_name: str
    sla: Optional[str] = None
    schedule: Optional[str] = None


@dataclass
class SourceConfig:
    id: str
    type: str
    options: Dict[str, str] = field(default_factory=dict)


@dataclass
class TransformationConfig:
    name: str
    options: Dict[str, str] = field(default_factory=dict)


@dataclass
class TargetConfig:
    type: str
    options: Dict[str, str] = field(default_factory=dict)


@dataclass
class ErrorPolicy:
    on_record_error: str = "quarantine"
    on_step_error: str = "halt"
    quarantine_topic: Optional[str] = None
    remediation_sla: Optional[str] = None


@dataclass
class PipelineConfig:
    pipeline_id: str
    version: str
    layer: str
    metadata: PipelineMetadata
    sources: List[SourceConfig]
    transformations: List[TransformationConfig]
    targets: List[TargetConfig]
    error_policy: ErrorPolicy
    path: Optional[Path] = None


@dataclass
class PipelineRef:
    pipeline_id: str
    depends_on: Optional[str] = None
    on_failure: str = "retry"
    max_retries: int = 0


@dataclass
class JobPipelineConfig:
    job_id: str
    version: str
    pipelines: List[PipelineRef]
    notifications: Dict[str, List[Dict[str, str]]] = field(default_factory=dict)
    path: Optional[Path] = None
