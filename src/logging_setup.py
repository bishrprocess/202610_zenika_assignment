"""
Logging setup to ensure centralized logging settings.
"""
from __future__ import annotations

import logging

from src.config import Config


def setup_logging(
        cfg: Config | None = None,
) -> None:
    """
    Configure root logging. Level comes from project.log_level (Default to INFO)
    :param cfg: Config object to extract relevant variables
    :return: None
    """
    level_name = str(cfg.get("project.log_level", "INFO")) if cfg else "INFO"
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(name)s: %(message)s",
        force=True,
    )

