from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from silver_framework.audit_logger import log_audit
from silver_framework.bronze_connector import read_bronze
from silver_framework.config_loader import load_config
from silver_framework.dq_framework import apply_dq_checks
from silver_framework.logger import get_logger
from silver_framework.schema_enforcement import enforce_schema
from silver_framework.silver_connector import apply_soft_delete, deduplicate, ensure_silver_table, upsert_to_silver
from silver_framework.transformations import apply_transformations


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
    full_scan: bool = True,
    extraction_start_date: Optional[str] = None,
    extraction_end_date: Optional[str] = None,
    use_cache: bool = True,
) -> Dict[str, Any]:
    """Run the full Bronze → Silver pipeline for a single entity.

    All table references (bronze_table, silver_table, audit_table) are read
    directly from the YAML config at config_path — no overrides needed.

    Set use_cache=False on Databricks Serverless, which does not support
    DataFrame.cache() / persist().

    Returns a result dict with keys: entity, status, input_count, valid_count,
    invalid_count, error.
    """
    config = load_config(config_path)
    entity: str = config["entity"]
    log = get_logger(entity)

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
        log.info("Stage 2/8 — Transforming")
        transformed_df = apply_transformations(raw_df)
        _unpersist(raw_df, use_cache)
        raw_df = None
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
        final_df = apply_soft_delete(deduped_df, config)

        # ── Stage 7: Ensure Silver Table Exists ──────────────────────────────
        log.info("Stage 7/8 — Ensuring silver table exists: '%s'", config["silver_table"])
        ensure_silver_table(spark, config)

        # ── Stage 8: Upsert ──────────────────────────────────────────────────
        log.info("Stage 8/8 — Upserting to Silver: '%s'", config["silver_table"])
        upsert_to_silver(spark, final_df, config)
        log.info("Upserted %d records to '%s'", final_df.count(), config["silver_table"])

        # ── Audit ────────────────────────────────────────────────────────────
        log_audit(spark, config, input_count, valid_count, invalid_count, "SUCCESS")
        log.info("Entity '%s' pipeline completed successfully", entity)

    except Exception as exc:
        log.error("Pipeline failed for entity '%s': %s", entity, exc, exc_info=True)
        result["status"] = "FAILED"
        result["error"] = str(exc)
        try:
            log_audit(spark, config, result["input_count"], result["valid_count"], result["invalid_count"], "FAILED")
        except Exception as audit_exc:
            log.error("Could not write FAILED audit record: %s", audit_exc)

    finally:
        _unpersist(raw_df, use_cache)
        _unpersist(transformed_df, use_cache)

    return result


def run_entities_parallel(
    spark: SparkSession,
    config_paths: List[str],
    full_scan: bool = True,
    extraction_start_date: Optional[str] = None,
    extraction_end_date: Optional[str] = None,
    max_workers: int = 4,
    use_cache: bool = True,
) -> List[Dict[str, Any]]:
    """Run multiple entity pipelines concurrently with the same scan settings.

    For per-entity scan settings (mixed full/incremental), call run_entity
    directly for each entity and manage concurrency in the caller.

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
