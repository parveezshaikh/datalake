from __future__ import annotations

import hashlib
from typing import Sequence

import pandas as pd

try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import functions as F
    from pyspark.sql import Window
except ModuleNotFoundError:  # pragma: no cover - pyspark optional
    SparkDataFrame = None  # type: ignore
    F = None  # type: ignore
    Window = None  # type: ignore


Dataset = object


def is_spark(dataset: Dataset) -> bool:
    return SparkDataFrame is not None and isinstance(dataset, SparkDataFrame)


def ensure_columns_present(dataset: Dataset, columns: Sequence[str]) -> None:
    missing = [col for col in columns if col not in dataset.columns]
    if missing:
        raise KeyError(f"Columns {missing} missing from dataset")


def deduplicate(dataset: Dataset, keys: Sequence[str], keep: str = "first", order_by: Sequence[str] | None = None):
    if is_spark(dataset):
        if keep in {"first", "last"} or order_by is None or F is None or Window is None:
            return dataset.dropDuplicates(list(keys))
        direction = "desc" if keep == "latest" else "asc"
        window = Window.partitionBy(*keys).orderBy(*[F.col(col).desc() if direction == "desc" else F.col(col).asc() for col in order_by])
        return dataset.withColumn("__row_number", F.row_number().over(window)).filter(F.col("__row_number") == 1).drop("__row_number")
    if not isinstance(dataset, pd.DataFrame):
        raise TypeError("Unsupported dataset type for deduplicate")
    subset = list(keys)
    if keep == "last":
        dataset = dataset.iloc[::-1]
    elif keep == "latest" and order_by:
        dataset = dataset.sort_values(list(order_by), ascending=False)
    return dataset.drop_duplicates(subset=subset, keep="first")


def sort(dataset: Dataset, columns: Sequence[str], ascending: bool = True):
    if is_spark(dataset):
        order = [col.strip() for col in columns]
        if not ascending:
            order = [F.col(col).desc() for col in order]
        else:
            order = [F.col(col).asc() for col in order]
        return dataset.sort(*order)
    if isinstance(dataset, pd.DataFrame):
        return dataset.sort_values(list(columns), ascending=ascending)
    raise TypeError("Unsupported dataset type for sort")


def join(left, right, condition: str, how: str = "inner"):
    if is_spark(left):
        return left.join(right, on=condition, how=how)
    if isinstance(left, pd.DataFrame) and isinstance(right, pd.DataFrame):
        left_alias, right_alias = condition.split("=")
        left_key = left_alias.split(".")[-1].strip()
        right_key = right_alias.split(".")[-1].strip()
        return left.merge(right, left_on=left_key, right_on=right_key, how=how)
    raise TypeError("Unsupported dataset type for join")


def lookup(dataset, lookup_df, join_columns: Sequence[str], how: str = "left"):
    if is_spark(dataset):
        conditions = [dataset[col] == lookup_df[col] for col in join_columns]
        condition = conditions[0]
        for expr in conditions[1:]:
            condition = condition & expr
        return dataset.join(lookup_df, condition, how)
    if isinstance(dataset, pd.DataFrame) and isinstance(lookup_df, pd.DataFrame):
        return dataset.merge(lookup_df, on=list(join_columns), how=how)
    raise TypeError("Unsupported dataset type for lookup")


def mask(dataset, columns: Sequence[str], strategy: str = "hash"):
    if is_spark(dataset):
        if strategy in {"hash", "tokenize", "redact"}:
            exprs = []
            for col in dataset.columns:
                if col in columns:
                    if strategy == "redact":
                        exprs.append(F.lit("***").alias(col))
                    else:
                        exprs.append(F.sha2(F.col(col).cast("string"), 256).alias(col))
                else:
                    exprs.append(F.col(col))
            return dataset.select(*exprs)
        raise ValueError(f"Unsupported masking strategy {strategy}")
    if isinstance(dataset, pd.DataFrame):
        df = dataset.copy()
        for col in columns:
            if col not in df.columns:
                continue
            if strategy == "redact":
                df[col] = "***"
            else:
                df[col] = df[col].astype(str).apply(lambda value: hashlib.sha256(value.encode()).hexdigest())
        return df
    raise TypeError("Unsupported dataset type for mask")


def aggregate(dataset, group_by: Sequence[str], metrics: Sequence[str]):
    parsed_metrics = []
    for metric in metrics:
        func, column = metric.split(":") if ":" in metric else (metric, "*")
        alias = f"{func}_{column.replace('*', 'rows')}"
        parsed_metrics.append((func, column, alias))
    if is_spark(dataset):
        aggregations = []
        for func, column, alias in parsed_metrics:
            if column == "*" and func == "count":
                aggregations.append(F.count("*").alias(alias))
            else:
                aggregations.append(getattr(F, func)(F.col(column)).alias(alias))
        return dataset.groupBy(*group_by).agg(*aggregations)
    if isinstance(dataset, pd.DataFrame):
        grouped = dataset.groupby(list(group_by), dropna=False)
        series_collection = []
        for func, column, alias in parsed_metrics:
            if column == "*" and func == "count":
                series = grouped.size().rename(alias)
            else:
                series = getattr(grouped[column], func)().rename(alias)
            series_collection.append(series)
        result = pd.concat(series_collection, axis=1).reset_index()
        return result
    raise TypeError("Unsupported dataset type for aggregate")
