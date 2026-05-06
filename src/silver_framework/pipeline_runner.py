from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

from silver_framework.audit_logger import log_audit
from silver_framework.bronze_connector import read_bronze
from silver_framework.config_loader import load_config
from silver_framework.dq_framework import apply_dq_checks
from silver_framework.logger import get_logger
from silver_framework.schema_enforcement import enforce_schema
from silver_framework.silver_connector import apply_soft_delete, deduplicate, upsert_to_silver
from silver_framework.transformations import apply_transformations


def run_entity(
    spark: SparkSession,
    config_path: str,
    full_scan: bool = True,
    extraction_start_date: Optional[str] = None,
    extraction_end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the full Bronze → Silver pipeline for a single entity.

    Returns a result dict with keys: entity, status, input_count, valid_count,
    invalid_count, error.
    """
    config = load_config(config_path)
    entity: str = config["entity"]
    log = get_logger(entity)

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
        log.info("Stage 1/7 — Reading Bronze: '%s'", config["bronze_table"])
        raw_df = read_bronze(spark, config, full_scan, extraction_start_date, extraction_end_date)
        # Cache before count so the scan is reused by apply_transformations
        raw_df.cache()
        input_count = raw_df.count()
        result["input_count"] = input_count
        log.info("Bronze records: %d", input_count)

        # ── Stage 2: Transform ───────────────────────────────────────────────
        log.info("Stage 2/7 — Transforming")
        transformed_df = apply_transformations(raw_df)
        raw_df.unpersist()
        raw_df = None
        # Cache before DQ — multiple checks each re-scan this DataFrame
        transformed_df.cache()

        # ── Stage 3: Data Quality ────────────────────────────────────────────
        log.info("Stage 3/7 — Running DQ checks")
        valid_df, invalid_df = apply_dq_checks(
            transformed_df, config.get("dq_checks", []), config["primary_keys"]
        )
        transformed_df.unpersist()
        transformed_df = None
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        result["valid_count"] = valid_count
        result["invalid_count"] = invalid_count
        log.info("DQ result — valid: %d, quarantine: %d", valid_count, invalid_count)
        if invalid_count > 0:
            log.warning("%d records quarantined for entity '%s'", invalid_count, entity)

        # ── Stage 4: Schema Enforcement ──────────────────────────────────────
        log.info("Stage 4/7 — Enforcing schema")
        enforced_df = enforce_schema(valid_df, config["schema"])

        # ── Stage 5: Deduplicate ─────────────────────────────────────────────
        log.info("Stage 5/7 — Deduplicating")
        deduped_df = deduplicate(enforced_df, config)

        # ── Stage 6: Soft Delete ─────────────────────────────────────────────
        log.info("Stage 6/7 — Applying soft delete filter")
        final_df = apply_soft_delete(deduped_df, config)

        # ── Stage 7: Upsert ──────────────────────────────────────────────────
        log.info("Stage 7/7 — Upserting to Silver: '%s'", config["silver_table"])
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
        if raw_df is not None:
            try:
                raw_df.unpersist()
            except Exception:
                pass
        if transformed_df is not None:
            try:
                transformed_df.unpersist()
            except Exception:
                pass

    return result


def run_entities_parallel(
    spark: SparkSession,
    config_paths: List[str],
    full_scan: bool = True,
    extraction_start_date: Optional[str] = None,
    extraction_end_date: Optional[str] = None,
    max_workers: int = 4,
) -> List[Dict[str, Any]]:
    """Run multiple entity pipelines concurrently.

    One entity's failure never blocks the others. Returns a list of result
    dicts (one per entity) after all have finished.
    """
    log = get_logger("pipeline_runner")
    effective_workers = min(max_workers, len(config_paths))
    log.info(
        "Starting parallel run — %d entities, %d workers: %s",
        len(config_paths), effective_workers, config_paths,
    )

    results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        futures = {
            executor.submit(
                run_entity, spark, path, full_scan, extraction_start_date, extraction_end_date
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
    failed = [r for r in results if r["status"] == "FAILED"]
    log.info(
        "Pipeline complete — %d succeeded, %d failed",
        len(succeeded), len(failed),
    )

    return results
