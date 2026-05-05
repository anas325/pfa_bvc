"""Per-domain harvest configurations.

Each module exposes `CONFIG: DomainConfig`. The runner discovers them by name.
"""
from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..cdx_common import DomainConfig

REGISTRY = [
    "lematin",
    "leconomiste",
    "medias24",
    "lavieeco",
    "challenge",
    "finances_news",
    "leseco",
    "hespress_fr",
]


def load(name: str) -> "DomainConfig":
    if name not in REGISTRY:
        raise KeyError(f"unknown domain '{name}'. known: {REGISTRY}")
    mod = import_module(f"cdx_harvester.domains.{name}")
    return mod.CONFIG


def load_all() -> list["DomainConfig"]:
    return [load(n) for n in REGISTRY]
