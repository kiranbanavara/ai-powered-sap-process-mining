"""Configuration schema (Pydantic).

One YAML file drives the whole run. Environment variables referenced as `${VAR}` in YAML
strings are expanded at load time so secrets never live in the file.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field


class SyntheticConnectorConfig(BaseModel):
    kind: Literal["synthetic"] = "synthetic"
    seed: int = 42
    n_cases: int = 800
    days_back: int = 60


class S4HanaConnectorConfig(BaseModel):
    kind: Literal["s4hana"] = "s4hana"
    base_url: str
    user: str | None = None
    password: str | None = None
    oauth_token: str | None = None
    verify_ssl: bool = True
    timeout: float = 60.0
    page_size: int = 500


class EccConnectorConfig(BaseModel):
    kind: Literal["ecc"] = "ecc"
    mode: Literal["sql", "rfc"] = "sql"
    sqlalchemy_url: str | None = None
    rfc_config: dict | None = None


ConnectorConfig = SyntheticConnectorConfig | S4HanaConnectorConfig | EccConnectorConfig


class LLMConfig(BaseModel):
    provider: Literal["anthropic", "openai", "gemini"]
    model: str | None = None
    api_key: str | None = None  # if unset, provider reads from env
    base_url: str | None = None  # for OpenAI-compatible endpoints


class RunConfig(BaseModel):
    process: Literal["o2c", "p2p"] = "o2c"
    window_days: int = 30
    # Scope filters — only the ones relevant to the selected process are used.
    sales_orgs: list[str] | None = None
    purchasing_orgs: list[str] | None = None
    company_codes: list[str] | None = None


class OutputConfig(BaseModel):
    directory: str = "reports"
    filename_template: str = "o2c-{date}.md"


class AppConfig(BaseModel):
    connector: ConnectorConfig = Field(..., discriminator="kind")
    llm: LLMConfig
    run: RunConfig = Field(default_factory=RunConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)


_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _expand_env(obj):
    if isinstance(obj, str):
        def replace(match: re.Match) -> str:
            name = match.group(1)
            return os.environ.get(name, "")
        return _ENV_PATTERN.sub(replace, obj)
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(v) for v in obj]
    return obj


def load_config(path: str | Path) -> AppConfig:
    p = Path(path)
    raw = yaml.safe_load(p.read_text())
    expanded = _expand_env(raw)
    return AppConfig.model_validate(expanded)
