from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

import pandas as pd

from libs.connectors.base import DataFrameConversions


@dataclass
class RuleResult:
    rule: str
    passed: bool
    message: str = ""


class DataQualityRule:
    rule_id: str

    def validate(self, dataset: pd.DataFrame) -> RuleResult:  # pragma: no cover - interface
        raise NotImplementedError


class NotNullRule(DataQualityRule):
    def __init__(self, rule_id: str, columns: Sequence[str]) -> None:
        self.rule_id = rule_id
        self.columns = columns

    def validate(self, dataset: pd.DataFrame) -> RuleResult:
        missing = {col: dataset[col].isna().sum() for col in self.columns}
        failed = {col: count for col, count in missing.items() if count > 0}
        passed = not failed
        message = "" if passed else f"Null values detected: {failed}"
        return RuleResult(self.rule_id, passed, message)


class UniquenessRule(DataQualityRule):
    def __init__(self, rule_id: str, columns: Sequence[str]) -> None:
        self.rule_id = rule_id
        self.columns = columns

    def validate(self, dataset: pd.DataFrame) -> RuleResult:
        duplicates = dataset.duplicated(subset=list(self.columns)).sum()
        passed = duplicates == 0
        message = "" if passed else f"Duplicate rows detected: {duplicates}"
        return RuleResult(self.rule_id, passed, message)


class RangeRule(DataQualityRule):
    def __init__(self, rule_id: str, column: str, *, min_value=None, max_value=None) -> None:
        self.rule_id = rule_id
        self.column = column
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, dataset: pd.DataFrame) -> RuleResult:
        series = dataset[self.column]
        failures = []
        if self.min_value is not None:
            failures.append((series < self.min_value).sum())
        if self.max_value is not None:
            failures.append((series > self.max_value).sum())
        failure_count = sum(failures)
        passed = failure_count == 0
        message = "" if passed else f"Range violations detected: {failure_count} rows"
        return RuleResult(self.rule_id, passed, message)


class RulesEngine:
    def __init__(self, rules: Sequence[DataQualityRule]) -> None:
        self.rules = list(rules)

    def evaluate(self, dataset) -> List[RuleResult]:
        df = DataFrameConversions.ensure_pandas(dataset)
        return [rule.validate(df) for rule in self.rules]
