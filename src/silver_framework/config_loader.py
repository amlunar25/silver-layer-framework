from typing import Any, Dict
import yaml


def load_config(config_path: str) -> Dict[str, Any]:
    """Load and return a YAML config file as a dictionary."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
