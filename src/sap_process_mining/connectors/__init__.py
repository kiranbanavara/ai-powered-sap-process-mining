"""Pluggable SAP data-source connectors.

Each connector exposes `extract_o2c()` → EventLog. Downstream code never branches on source.
"""

from .base import BaseConnector
from .synthetic import SyntheticConnector
from .s4hana import S4HanaConnector
from .ecc import EccConnector

_REGISTRY: dict[str, type[BaseConnector]] = {
    "synthetic": SyntheticConnector,
    "s4hana": S4HanaConnector,
    "ecc": EccConnector,
}


def get_connector(kind: str, **kwargs) -> BaseConnector:
    try:
        cls = _REGISTRY[kind]
    except KeyError:
        raise ValueError(f"Unknown connector '{kind}'. Available: {sorted(_REGISTRY)}") from None
    return cls(**kwargs)


__all__ = [
    "BaseConnector",
    "SyntheticConnector",
    "S4HanaConnector",
    "EccConnector",
    "get_connector",
]
