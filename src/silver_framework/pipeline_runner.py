from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from silver_framework.audit_logger import log_dq_audit, log_ingestion_audit
from silver_framework.bronze_connector import read_bronze
from silver_framework.config_loader import load_config
from silver_framework.dq_framework import apply_dq_checks
from silver_framework.logger import get_logger
from silver_framework.schema_enforcement import enforce_schema
from silver_framework.silver_connector import (
    apply_soft_delete,
    deduplicate,
    ensure_silver_table,
    get_merge_metrics,
    upsert_to_silver,
)
from silver_framework.custom_transformations import apply_custom_transformations
from silver_framework.transformations import apply_transformations


def _resolve_incremental_start_date(
    spark: SparkSession,
    config: Dict[str, Any],
    log: Any,
) -> Optional[str]:
    """Return MAX(filter_column) from the silver table as a YYYY-MM-DD string.

    Returns None when the table does not exist or is empty, signalling that the
    caller should fall back to a full scan.
    """
    silver_table  = config["silver_table"]
    filter_column = config.get("extraction", {}).get("filter_column", "process_date")

    if not spark.catalog.tableExists(silver_table):
        log.info("Silver table '%s' does not exist — cannot derive start_date", silver_table)
        return None

    row = spark.sql(f"SELECT MAX({filter_column}) AS max_date FROM {silver_table}").collect()[0]
    if row["max_date"] is None:
        log.info("Silver table '%s' is empty — cannot derive start_date", silver_table)
        return None

    start_date = str(row["max_date"])[:10]   # normalise date/timestamp → YYYY-MM-DD
    log.info("Auto-detected start_date='%s' from MAX(%s) in '%s'", start_date, filter_column, silver_table)
    return start_date


def _cache(df: DataFrame, use_cache: bool) -> DataFrame:
    """Cache df only when use_cache is True (not supported on Serverless)."""
    if use_cache:
        df.cache()
    return df


def _unpersist(df: Optional[DataFrame], use_cache: bool) -> None:
    if use_cache and df is not None:
        try:
            df.unpersist()
        except Exception:
            pass


def run_entity(
    spark: SparkSession,
    config_path: str,
    full_scan: Optional[bool] = None,
    extraction_start_date: Optional[str] = None,
    extraction_end_date: Optional[str] = None,
    use_cache: bool = True,
) -> Dict[str, Any]:
    """Run the full Bronze → Silver pipeline for a single entity.

    All table references (bronze_table, silver_table, audit_table,
    ingestion_audit_table) are read directly from the YAML config.

    full_scan controls the extraction mode:
      - None  → use extraction.mode from the YAML config (default)
      - True  → force a full scan regardless of the YAML setting
      - False → force incremental regardless of the YAML setting

    Set use_cache=False on Databricks Serverless, which does not support
    DataFrame.cache() / persist().

    Returns a result dict with keys: entity, status, input_count, valid_count,
    invalid_count, error.
    """
    config = load_config(config_path)

    # Resolve extraction mode: explicit runtime value overrides the YAML default
    if full_scan is None:
        full_scan = config.get("extraction", {}).get("mode", "full_scan") == "full_scan"
        log_source = "config"
    else:
        log_source = "override"

    entity: str = config["entity"]
    log = get_logger(entity)
    log.info("Extraction mode: full_scan=%s (source=%s)", full_scan, log_source)

    # For incremental mode with no explicit start_date, derive it from the silver table.
    # Falls back to a full scan when the silver table is missing or empty.
    if not full_scan and extraction_start_date is None:
        extraction_start_date = _resolve_incremental_start_date(spark, config, log)
        if extraction_start_date is None:
            log.warning("No start_date available — falling back to full scan for entity '%s'", entity)
            full_scan = True

    if not use_cache:
        log.info("Caching disabled (Serverless mode)")

    result: Dict[str, Any] = {
        "entity": entity,
        "status": "SUCCESS",
        "input_count": 0,
        "valid_count": 0,
        "invalid_count": 0,
        "error": None,
    }

    raw_df = None
    transformed_df = None

    try:
        # ── Stage 1: Read Bronze ─────────────────────────────────────────────
        log.info("Stage 1/8 — Reading Bronze: '%s'", config["bronze_table"])
        raw_df = read_bronze(spark, config, full_scan, extraction_start_date, extraction_end_date)
        _cache(raw_df, use_cache)
        input_count = raw_df.count()
        result["input_count"] = input_count
        log.info("Bronze records: %d", input_count)

        # ── Stage 2: Transform ───────────────────────────────────────────────
        log.info("Stage 2/8 — Transforming (base: normalise columns, trim strings)")
        transformed_df = apply_transformations(raw_df)
        _unpersist(raw_df, use_cache)
        raw_df = None

        log.info("Stage 2/8 — Transforming (custom: YAML-driven column transformations)")
        transformed_df = apply_custom_transformations(transformed_df, config)
        _cache(transformed_df, use_cache)

        # ── Stage 3: Data Quality ────────────────────────────────────────────
        log.info("Stage 3/8 — Running DQ checks")
        valid_df, invalid_df = apply_dq_checks(
            transformed_df, config.get("dq_checks", []), config["primary_keys"]
        )
        _unpersist(transformed_df, use_cache)
        transformed_df = None
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        result["valid_count"] = valid_count
        result["invalid_count"] = invalid_count
        log.info("DQ result — valid: %d, quarantine: %d", valid_count, invalid_count)
        if invalid_count > 0:
            log.warning("%d records quarantined for entity '%s'", invalid_count, entity)

        # ── Stage 4: Schema Enforcement ──────────────────────────────────────
        log.info("Stage 4/8 — Enforcing schema")
        enforced_df = enforce_schema(valid_df, config["schema"])

        # ── Stage 5: Deduplicate ─────────────────────────────────────────────
        log.info("Stage 5/8 — Deduplicating")
        deduped_df = deduplicate(enforced_df, config)

        # ── Stage 6: Soft Delete ─────────────────────────────────────────────
        log.info("Stage 6/8 — Applying soft delete filter")
        deduped_count = deduped_df.count()
        final_df = apply_soft_delete(deduped_df, config)
        ingested_count = final_df.count()
        deleted_count = deduped_count - ingested_count
        log.info("Soft delete removed %d records", deleted_count)

        # ── Stage 7: Ensure Silver Table Exists ──────────────────────────────
        log.info("Stage 7/8 — Ensuring silver table exists: '%s'", config["silver_table"])
        ensure_silver_table(spark, config)

        # ── Stage 8: Upsert ──────────────────────────────────────────────────
        log.info("Stage 8/8 — Upserting to Silver: '%s'", config["silver_table"])
        upsert_to_silver(spark, final_df, config)
        log.info("Upserted %d records to '%s'", ingested_count, config["silver_table"])

        # ── Audit ────────────────────────────────────────────────────────────
        log_dq_audit(spark, config, input_count, valid_count, invalid_count, "SUCCESS")

        merge_metrics = get_merge_metrics(spark, config["silver_table"])
        log_ingestion_audit(
            spark, config,
            ingested_records=ingested_count,
            inserted_records=merge_metrics["inserted"],
            updated_records=merge_metrics["updated"],
            deleted_records=deleted_count,
            status="SUCCESS",
        )
        log.info("Entity '%s' pipeline completed successfully", entity)

    except Exception as exc:
        log.error("Pipeline failed for entity '%s': %s", entity, exc, exc_info=True)
        result["status"] = "FAILED"
        result["error"] = str(exc)
        try:
            log_dq_audit(spark, config, result["input_count"], result["valid_count"], result["invalid_count"], "FAILED")
        except Exception as audit_exc:
            log.error("Could not write FAILED DQ audit record: %s", audit_exc)

    finally:
        _unpersist(raw_df, use_cache)
        _unpersist(transformed_df, use_cache)

    return result


def run_entities_parallel(
    spark: SparkSession,
    config_paths: List[str],
    full_scan: Optional[bool] = None,
    extraction_start_date: Optional[str] = None,
    extraction_end_date: Optional[str] = None,
    max_workers: int = 4,
    use_cache: bool = True,
) -> List[Dict[str, Any]]:
    """Run multiple entity pipelines concurrently.

    full_scan applies to all entities:
      - None  → each entity uses its own extraction.mode from YAML (recommended)
      - True  → force full scan for all entities
      - False → force incremental for all entities

    For per-entity scan overrides, call run_entity directly for each entity
    and manage concurrency in the caller.

    Set use_cache=False on Databricks Serverless, which does not support
    DataFrame.cache() / persist().

    One entity's failure never blocks the others. Returns a list of result
    dicts (one per entity) after all have finished.
    """
    log = get_logger("pipeline_runner")
    effective_workers = min(max_workers, len(config_paths))
    log.info(
        "Starting parallel run — %d entities, %d workers, use_cache=%s: %s",
        len(config_paths), effective_workers, use_cache, config_paths,
    )

    results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        futures = {
            executor.submit(
                run_entity, spark, path, full_scan,
                extraction_start_date, extraction_end_date, use_cache,
            ): path
            for path in config_paths
        }

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                path = futures[future]
                log.error("Unhandled exception for config '%s': %s", path, exc, exc_info=True)
                result = {"entity": path, "status": "FAILED", "error": str(exc),
                          "input_count": 0, "valid_count": 0, "invalid_count": 0}
            results.append(result)

            if result["status"] == "SUCCESS":
                log.info("Entity '%s' — SUCCESS", result["entity"])
            else:
                log.error("Entity '%s' — FAILED: %s", result["entity"], result.get("error"))

    succeeded = [r for r in results if r["status"] == "SUCCESS"]
    failed    = [r for r in results if r["status"] == "FAILED"]
    log.info(
        "Pipeline complete — %d succeeded, %d failed",
        len(succeeded), len(failed),
    )

    return results
