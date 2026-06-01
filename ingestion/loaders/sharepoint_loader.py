"""
ingestion/loaders/sharepoint_uploader.py

Handles writing files to the SharePoint synced local folder.

Because SharePoint is mounted as a synced drive (OneDrive sync client),
"uploading" is just copying files to the right local path — the sync
client handles the actual transfer to SharePoint in the background.

This module provides:
- A function to copy a processed parquet to the processed/ layer
- A function to copy the built .duckdb to the database/ layer
- A function to write the pipeline log
- A helper to check that the sync root exists and is reachable
"""

from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

from ingestion.loaders.base_loader import (
    get_logger,
    load_config,
    processed_path,
    database_path,
)


# ── Health check ──────────────────────────────────────────────────────────────

def check_sharepoint_available(config: dict, logger: logging.Logger | None = None) -> bool:
    """
    Verify that the SharePoint synced folder exists and is writable.

    Call this at the start of a pipeline run to fail fast if the sync
    client is not running or the folder has moved.
    """
    lg = logger or get_logger("sharepoint_uploader")
    root: Path = config["sharepoint_root"]

    if not root.exists():
        lg.error(
            f"SharePoint root not found: {root}\n"
            "Make sure OneDrive sync is running and SHAREPOINT_ROOT in .env is correct."
        )
        return False

    # Write a small test file to confirm write access
    test_file = root / ".aiccon_write_test"
    try:
        test_file.write_text("ok")
        test_file.unlink()
    except OSError as e:
        lg.error(f"SharePoint root exists but is not writable: {e}")
        return False

    lg.info(f"SharePoint root reachable and writable: {root}")
    return True


# ── Processed layer ───────────────────────────────────────────────────────────

def upload_processed(
    local_path: Path,
    domain: str,
    config: dict,
    logger: logging.Logger | None = None,
) -> Path:
    """
    Copy a processed parquet file into the SharePoint processed/{domain}/ folder.

    The destination filename is preserved. If a file with the same name already
    exists, it is overwritten (processed files are deterministic outputs).

    Returns the destination path.
    """
    lg = logger or get_logger("sharepoint_uploader")
    dest_dir = processed_path(config, domain)
    dest = dest_dir / local_path.name

    shutil.copy2(local_path, dest)
    lg.info(f"Uploaded processed file → {dest}")
    return dest


def upload_processed_batch(
    local_paths: list[Path],
    domain: str,
    config: dict,
    logger: logging.Logger | None = None,
) -> list[Path]:
    """Upload multiple processed parquet files for the same domain."""
    return [
        upload_processed(p, domain, config, logger)
        for p in local_paths
    ]


# ── Raw layer ─────────────────────────────────────────────────────────────────

def upload_raw(
    local_path: Path,
    domain: str,
    config: dict,
    logger: logging.Logger | None = None,
) -> Path:
    """
    Copy a raw parquet file into the SharePoint raw/{domain}/ folder.

    Raw files use date-stamped names (written by save_raw_parquet) so they
    accumulate rather than overwrite. This function just copies — it does
    not rename or modify the file.

    Returns the destination path.
    """
    lg = logger or get_logger("sharepoint_uploader")
    root = config["sharepoint_root"]
    raw_base = config["sharepoint"]["raw_dir"]
    dest_dir = root / raw_base / domain
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / local_path.name

    shutil.copy2(local_path, dest)
    lg.info(f"Uploaded raw file → {dest}")
    return dest


# ── Database layer ────────────────────────────────────────────────────────────

def upload_database(
    local_duckdb_path: Path,
    config: dict,
    logger: logging.Logger | None = None,
) -> Path:
    """
    Copy the built .duckdb file into the SharePoint database/ folder.

    This overwrites the previous version. PowerBI connects to this file,
    so it always reflects the most recent pipeline run.

    Returns the destination path.
    """
    lg = logger or get_logger("sharepoint_uploader")
    dest_dir = database_path(config)
    dest = dest_dir / config["sharepoint"]["database_filename"]

    shutil.copy2(local_duckdb_path, dest)
    lg.info(f"Uploaded database → {dest}")
    return dest


# ── Pipeline log ──────────────────────────────────────────────────────────────

def write_pipeline_log(
    config: dict,
    run_summary: dict,
    logger: logging.Logger | None = None,
) -> Path:
    """
    Write a JSON log of the pipeline run to the SharePoint database/ folder.

    The log is overwritten on each run — it reflects the most recent run only.
    For history, each parquet file's individual manifest serves as the audit trail.

    run_summary should contain at minimum:
        {
            "started_at": "2024-11-01T08:00:00+00:00",
            "finished_at": "2024-11-01T08:12:34+00:00",
            "domains_run": ["social_economy"],
            "files_written": [...],
            "errors": [],
        }
    """
    lg = logger or get_logger("sharepoint_uploader")
    dest_dir = database_path(config)
    log_path = dest_dir / config["sharepoint"]["log_filename"]

    run_summary["log_written_at"] = datetime.now(timezone.utc).isoformat()

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(run_summary, f, indent=2, ensure_ascii=False, default=str)

    lg.info(f"Pipeline log written → {log_path}")
    return log_path


# ── Convenience: check then upload ───────────────────────────────────────────

def safe_upload_processed(
    local_paths: list[Path],
    domain: str,
    config: dict | None = None,
    logger: logging.Logger | None = None,
) -> list[Path]:
    """
    Check SharePoint is available, then upload a batch of processed files.

    This is the function most pipeline scripts will call directly.
    Raises RuntimeError if SharePoint is not reachable.
    """
    lg = logger or get_logger("sharepoint_uploader")
    cfg = config or load_config()

    if not check_sharepoint_available(cfg, lg):
        raise RuntimeError(
            "Cannot reach SharePoint synced folder. "
            "Check that OneDrive sync is running and SHAREPOINT_ROOT is correct."
        )

    return upload_processed_batch(local_paths, domain, cfg, lg)