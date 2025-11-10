from __future__ import annotations

from typing import Callable, Dict, Type

from .models import TransformationConfig


class TransformationStrategy:
    """Base protocol for transformation strategies."""

    def __init__(self, config: TransformationConfig) -> None:
        self.config = config

    def apply(self, datasets: dict, context) -> None:  # pragma: no cover - interface
        raise NotImplementedError


StrategyFactory = Callable[[TransformationConfig], TransformationStrategy]


class StepRegistry:
    def __init__(self) -> None:
        self._strategies: Dict[str, StrategyFactory] = {}

    def register(self, name: str, strategy_cls: Type[TransformationStrategy]) -> None:
        self._strategies[name] = lambda config: strategy_cls(config)

    def build(self, config: TransformationConfig) -> TransformationStrategy:
        factory = self._strategies.get(config.name)
        if factory is None:
            raise KeyError(f"Transformation '{config.name}' is not registered")
        return factory(config)

    def available(self) -> Dict[str, StrategyFactory]:
        return dict(self._strategies)
