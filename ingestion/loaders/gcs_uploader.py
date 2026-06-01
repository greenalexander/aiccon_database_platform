"""
ingestion/loaders/gcs_uploader.py

Handles writing files to Google Cloud Storage.

GCS is the intermediate storage layer between pipeline stages. Raw parquet
files land in raw/{domain}/, processed parquet files in processed/{domain}/,
and the pipeline log in database/. BigQuery is loaded separately by
database/build_db.py.

This module provides:
- A function to upload a raw parquet to the raw/ layer
- A function to upload a processed parquet to the processed/ layer
- A function to write the pipeline log
- A helper to check that the GCS bucket is reachable and writable
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import storage
from google.cloud.exceptions import NotFound

from ingestion.loaders.base_loader import get_logger, load_config


# ── Internal helpers ──────────────────────────────────────────────────────────

def _get_client() -> storage.Client:
    """Return a GCS client. Credentials are resolved from the environment
    (GOOGLE_APPLICATION_CREDENTIALS in .env for local runs; Workload Identity
    or a secret-injected key in GitHub Actions)."""
    return storage.Client()


def _blob_path(config: dict, *parts: str) -> str:
    """
    Build a GCS object path from config prefix and path parts.

    Example:
        _blob_path(config, "raw", "social_economy", "file.parquet")
        → "aiccon-data/raw/social_economy/file.parquet"
    """
    prefix = config["gcs"]["prefix"].rstrip("/")
    return "/".join([prefix, *parts])


# ── Health check ──────────────────────────────────────────────────────────────

def check_gcs_available(
    config: dict,
    logger: logging.Logger | None = None,
) -> bool:
    """
    Verify that the GCS bucket exists and the service account can write to it.

    Call this at the start of a pipeline run to fail fast if credentials
    are missing or the bucket name in config is wrong.
    """
    lg = logger or get_logger("gcs_uploader")
    bucket_name: str = config["gcs"]["bucket"]
    client = _get_client()

    try:
        bucket = client.bucket(bucket_name)
        test_blob = bucket.blob(_blob_path(config, "_connection_test", "write_test.json"))
        test_blob.upload_from_string(json.dumps({"status": "ok"}))
        test_blob.delete()
    except NotFound:
        lg.error(
            f"GCS bucket not found: {bucket_name}\n"
            "Check that GCS_BUCKET in .env matches the bucket you created in GCP."
        )
        return False
    except Exception as e:
        lg.error(f"GCS bucket exists but write test failed: {e}")
        return False

    lg.info(f"GCS bucket reachable and writable: {bucket_name}")
    return True


# ── Raw layer ─────────────────────────────────────────────────────────────────

def upload_raw(
    local_path: Path,
    domain: str,
    config: dict,
    logger: logging.Logger | None = None,
) -> str:
    """
    Upload a raw parquet file to GCS raw/{domain}/.

    Raw files use date-stamped names (written by save_raw_parquet in
    base_loader.py) so they accumulate rather than overwrite. This function
    uploads without renaming.

    Returns the full GCS URI (gs://bucket/path) of the uploaded object.
    """
    lg = logger or get_logger("gcs_uploader")
    bucket_name: str = config["gcs"]["bucket"]
    client = _get_client()

    blob_name = _blob_path(config, "raw", domain, local_path.name)
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(str(local_path))

    uri = f"gs://{bucket_name}/{blob_name}"
    lg.info(f"Uploaded raw file → {uri}")
    return uri


# ── Processed layer ───────────────────────────────────────────────────────────

def upload_processed(
    local_path: Path,
    domain: str,
    config: dict,
    logger: logging.Logger | None = None,
    dest_filename: str = "",
) -> str:
    """
    Upload a processed parquet file to GCS processed/{domain}/.

    Processed files are deterministic outputs — if a file with the same name
    already exists in GCS it is overwritten.

    dest_filename sets the uploaded object name. Always pass a dated name
    (e.g. social_economy_2026-06.parquet) so GCS filenames are predictable
    and _find_latest_processed() can sort them chronologically.

    Returns the full GCS URI of the uploaded object.
    """
    if not dest_filename:
        raise ValueError(
            "dest_filename is required. Pass a dated name like "
            f"'{domain}_YYYY-MM.parquet' to ensure predictable GCS filenames."
        )
    lg = logger or get_logger("gcs_uploader")
    bucket_name: str = config["gcs"]["bucket"]
    client = _get_client()

    filename = dest_filename
    blob_name = _blob_path(config, "processed", domain, filename)
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(str(local_path))

    uri = f"gs://{bucket_name}/{blob_name}"
    lg.info(f"Uploaded processed file → {uri}")
    return uri


def upload_processed_batch(
    local_paths: list[Path],
    domain: str,
    config: dict,
    logger: logging.Logger | None = None,
) -> list[str]:
    """Upload multiple processed parquet files for the same domain."""
    return [
        upload_processed(p, domain, config, logger)
        for p in local_paths
    ]


# ── Pipeline log ──────────────────────────────────────────────────────────────

def write_pipeline_log(
    config: dict,
    run_summary: dict,
    logger: logging.Logger | None = None,
) -> str:
    """
    Write a JSON log of the pipeline run to GCS database/pipeline_log.json.

    The log is overwritten on each run — it reflects the most recent run only.
    Individual parquet manifests serve as the audit trail for historical runs.

    run_summary should contain at minimum:
        {
            "started_at": "2024-11-01T08:00:00+00:00",
            "finished_at": "2024-11-01T08:12:34+00:00",
            "domains_run": ["social_economy"],
            "files_written": [...],
            "errors": [],
        }

    Returns the full GCS URI of the log object.
    """
    lg = logger or get_logger("gcs_uploader")
    bucket_name: str = config["gcs"]["bucket"]
    client = _get_client()

    run_summary["log_written_at"] = datetime.now(timezone.utc).isoformat()

    blob_name = _blob_path(config, "database", "pipeline_log.json")
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_string(
        json.dumps(run_summary, indent=2, ensure_ascii=False, default=str),
        content_type="application/json",
    )

    uri = f"gs://{bucket_name}/{blob_name}"
    lg.info(f"Pipeline log written → {uri}")
    return uri


# ── Convenience: check then upload ───────────────────────────────────────────

def safe_upload_processed(
    local_paths: list[Path],
    domain: str,
    config: dict | None = None,
    logger: logging.Logger | None = None,
) -> list[str]:
    """
    Check GCS is reachable, then upload a batch of processed files.

    This is the function most pipeline scripts will call directly.
    Raises RuntimeError if the bucket is not reachable.
    """
    lg = logger or get_logger("gcs_uploader")
    cfg = config or load_config()

    if not check_gcs_available(cfg, lg):
        raise RuntimeError(
            "Cannot reach GCS bucket. "
            "Check that GOOGLE_APPLICATION_CREDENTIALS and GCS_BUCKET in .env are correct."
        )

    return upload_processed_batch(local_paths, domain, cfg, lg)