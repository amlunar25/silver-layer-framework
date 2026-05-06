from typing import Any, Dict

import yaml

from silver_framework.logger import get_logger

_log = get_logger("config_loader")


def load_config(config_path: str) -> Dict[str, Any]:
    """Load and return a YAML config file as a dictionary."""
    _log.info("Loading config from '%s'", config_path)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    _log.info("Config loaded for entity '%s'", config.get("entity", "unknown"))
    return config
