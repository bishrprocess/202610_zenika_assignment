"""
Config loader for project
Reads ``config/config.yaml``, to resolve all paths relative to repo root.

Example Usage:
    from config import load_config

    config = load_config()
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "config.yaml"


class ConfigError(Exception):
    """Raised when the config is invalid. Config related error."""


@dataclass(frozen=True)
class Config:
    """
    Wrapper around parsed YAML config file.
    """

    raw: dict[str, Any]
    root: Path = REPO_ROOT

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]


    def get(self, dotted_key: str, default: Any = None) -> Any:
        """
        Getting a nested value from config file.
        """
        node: Any = self.raw
        for part in dotted_key.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node


    def require(self, dotted_key: str) -> Any:
        """
        Get but raises an error if key is not found.
        """
        sentinel = object()
        value = self.get(dotted_key, sentinel)
        if value is sentinel:
            raise ConfigError("Missing required config key: {key}".format(key=dotted_key))
        return value


    def path(self, name:str, create: bool = False) -> Path:
        """
        Resolve a ``paths.<name>`` relative to ``REPO_ROOT``.
        """
        rel = self.require("paths.{name}".format(name=name))
        resolved = (self.root / rel).resolve()
        if create:
            resolved.mkdir(parents=True, exist_ok=True)
        return resolved


    def output_file(self, path_name: str, filename_key: str) -> Path:
        """
        Build an output file using the configured ``outputs.format`` as the extension.
        """
        ext = self.require("outputs.format")
        name = self.require("outputs.{name}".format(name=filename_key))
        return self.path(path_name, create=True) / "{name}.{ext}".format(name=name, ext=ext)


def load_config(config_path: str = None) -> Config:
    """
    Load and validate YAML config file.
    """
    path = Path(
        config_path or DEFAULT_CONFIG_PATH
    )
    if not path.is_file():
        raise ConfigError("Config file not found: {path}".format(path=path))

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ConfigError("Config file not a dict, empty or not a map: {path}".format(path=path))

    cfg = Config(raw=data)
    _validate(cfg)
    return cfg


def _validate(cfg: Config) -> None:
    """
    Quick fail in case there are missing sections or bad values according to tests in case of corruption.
    """
    for section in ("source", "paths", "scope", "reference", "outputs"):
        cfg.require(section)

    if cfg.require("scope.start_year_month") > cfg.require("scope.end_year_month"):
        raise ConfigError("scope.start_year_month must not be after scope.end_year_month")

    if cfg.require("outputs.format") not in {"csv", "parquet"}:
        raise ConfigError("outputs.format must be 'csv' or 'parquet'")


