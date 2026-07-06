from typing import Any, Dict, List, Optional

import yaml

from silver_framework.logger import get_logger

_log = get_logger("config_loader")

# Maps the new-format extraction.mode values to the internal contract that the
# rest of the framework understands (full_scan | incremental).
_MODE_MAP = {
    "full_refresh": "full_scan",
    "full_scan": "full_scan",
    "incremental": "incremental",
}


def load_config(config_path: str) -> Dict[str, Any]:
    """Load a YAML config file and return it in the flat internal contract.

    Two formats are supported:

    * New nested "source config" format (identified by a top-level ``source``
      key). It is normalised into the flat contract via
      :func:`_normalize_source_config`.
    * Legacy flat entity format — returned unchanged for backward compatibility.
    """
    _log.info("Loading config from '%s'", config_path)
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    if "source" in raw:
        config = _normalize_source_config(raw)
        _log.info(
            "Normalised source config for entity '%s' (source=%s, table=%s)",
            config["entity"], raw["source"].get("name"), raw["source"].get("table"),
        )
        return config

    _log.info("Config loaded for entity '%s'", raw.get("entity", "unknown"))
    return raw


def _join_fqn(parts: List[Optional[str]]) -> str:
    """Join catalog/schema/table parts into a dotted name, skipping empty parts.

    This lets both three-part (``catalog.schema.table``) and two-part
    (``schema.table``, used by local runs) names work from the same config.
    """
    return ".".join(p for p in parts if p)


def _bronze_to_silver(part: Optional[str]) -> str:
    """Derive the silver counterpart of a bronze catalog/schema part.

    Swaps the substring ``bronze`` for ``silver`` (case-insensitive prefix
    convention). Empty parts stay empty.
    """
    if not part:
        return ""
    return part.replace("bronze", "silver").replace("Bronze", "Silver")


def _normalize_source_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map the nested source-config format to the flat internal contract.

    Silver and audit table names are derived from ``bronze_table`` by swapping
    the ``bronze`` prefix for ``silver`` and appending fixed audit table names.
    """
    source = raw["source"]
    bronze = raw["bronze_table"]

    entity = f"{source['name']}_{source['table']}"

    catalog = bronze.get("catalog") or ""
    schema  = bronze.get("schema") or ""
    table   = bronze.get("table_name") or source["table"]

    silver_catalog = _bronze_to_silver(catalog)
    silver_schema  = _bronze_to_silver(schema)

    bronze_table = _join_fqn([catalog, schema, table])
    silver_table = _join_fqn([silver_catalog, silver_schema, table])
    audit_table  = _join_fqn([silver_catalog, silver_schema, "audit_log"])
    ingestion_audit_table = _join_fqn([silver_catalog, silver_schema, "ingestion_audit_log"])

    # Extraction: map mode vocabulary and pull the filter column from watermark.
    raw_extraction = raw.get("extraction", {}) or {}
    mode = _MODE_MAP.get(raw_extraction.get("mode", "full_scan"), "full_scan")
    watermark = raw_extraction.get("watermark", {}) or {}
    filter_column = watermark.get("column", "process_date")

    # Schema: flatten columns and lowercase types to match schema_enforcement._TYPE_MAP.
    columns = (raw.get("schema", {}) or {}).get("columns", []) or []
    schema_list = [
        {"name": col["name"], "type": str(col["type"]).lower()}
        for col in columns
    ]

    config: Dict[str, Any] = {
        "entity": entity,
        "bronze_table": bronze_table,
        "silver_table": silver_table,
        "audit_table": audit_table,
        "ingestion_audit_table": ingestion_audit_table,
        "extraction": {
            "mode": mode,
            "filter_column": filter_column,
        },
        "schema": schema_list,
        "primary_keys": bronze.get("primary_keys", []),
    }

    # Silver-layer sections are optional extensions of the source config; pass
    # them through untouched when present so downstream modules keep their
    # existing contract (and defaults when absent).
    for key in ("schema_enforcement", "deduplication", "soft_delete",
                "dq_checks", "transformations"):
        if key in raw:
            config[key] = raw[key]

    return config
