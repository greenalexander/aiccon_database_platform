"""
processing/pipeline.py

Orchestrates the processing stage of the aiccon-data pipeline.

For each active domain it:
    1. Loads raw parquet files from the SharePoint raw layer
    2. Harmonises geography, legal form, and NACE codes
    3. Merges sources with priority resolution
    4. Writes a single processed parquet to the SharePoint processed layer

Currently implemented domains:
    - social_economy  ← active
    - labour          ← active

Adding a new domain later:
    1. Write processing/integrate/merge_{domain}.py following the same
       pattern as merge_social_economy.py / merge_labour.py
    2. Import its merge function here and add it to DOMAIN_PROCESSORS below
    3. Enable the domain in config/settings.yaml

Run the full processing stage:
    python -m processing.pipeline

Run a single domain only:
    python -m processing.pipeline --domain social_economy
    python -m processing.pipeline --domain labour
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.loaders.base_loader import get_logger, load_config, processed_path
from ingestion.loaders.sharepoint_loader import (
    check_sharepoint_available,
    upload_processed,
    write_pipeline_log,
)
from processing.integrate.merge_social_economy import merge_social_economy
from processing.integrate.merge_labour import merge_labour

logger = get_logger("processing.pipeline")


# ── Domain processor registry ─────────────────────────────────────────────────
# Maps domain name → merge function
# Each merge function signature: (config: dict) -> pd.DataFrame
#
# To add a new domain, import its merge function and add it here.
# No other changes needed in this file.

DOMAIN_PROCESSORS = {
    "social_economy": merge_social_economy,
    "labour":         merge_labour,
    # "welfare":        merge_welfare,       # add when ready
    # "immigration":    merge_immigration,   # add when ready
    # "poverty":        merge_poverty,       # add when ready
    # "sdg":            merge_sdg,           # add when ready
    # "civil_society":  merge_civil_society, # add when ready
    # "housing":        merge_housing,       # add when ready
}


# ── Core processing function ──────────────────────────────────────────────────

def run_domain(domain: str, config: dict) -> dict:
    """
    Run the full processing pipeline for a single domain.

    Parameters
    ----------
    domain : str
        Domain name, e.g. "social_economy".
    config : dict
        Loaded configuration from settings.yaml + .env.

    Returns
    -------
    dict
        Summary dict with keys: domain, status, rows, output_path, error.
    """
    summary = {
        "domain": domain,
        "status": "not_started",
        "rows": 0,
        "output_path": None,
        "error": None,
    }

    # Check domain is registered
    if domain not in DOMAIN_PROCESSORS:
        msg = (
            f"Domain '{domain}' has no registered processor. "
            f"Available: {list(DOMAIN_PROCESSORS.keys())}"
        )
        logger.error(msg)
        summary["status"] = "error"
        summary["error"] = msg
        return summary

    # Check domain is enabled in settings.yaml
    domain_config = config.get("domains", {}).get(domain, {})
    if not domain_config.get("enabled", False):
        logger.info(f"Domain '{domain}' is disabled in settings.yaml — skipping.")
        summary["status"] = "skipped"
        return summary

    logger.info(f"{'─' * 50}")
    logger.info(f"Processing domain: {domain}")
    logger.info(f"{'─' * 50}")

    try:
        merge_fn = DOMAIN_PROCESSORS[domain]
        df = merge_fn(config)

        if df.empty:
            logger.warning(f"{domain}: merge returned empty DataFrame.")
            summary["status"] = "empty"
            return summary

        summary["rows"] = len(df)
        logger.info(f"{domain}: merge produced {len(df):,} rows.")

        # Write to a temp file, then upload to SharePoint processed layer
        # Using a temp file avoids writing a partial file to SharePoint
        # if something goes wrong mid-write.
        with tempfile.NamedTemporaryFile(
            suffix=".parquet", delete=False, prefix=f"{domain}_"
        ) as tmp:
            tmp_path = Path(tmp.name)

        df.to_parquet(tmp_path, index=False, engine="pyarrow")
        logger.info(f"{domain}: written to temp file ({tmp_path.stat().st_size / 1024:.1f} KB)")

        dest = upload_processed(tmp_path, domain, config, logger)
        tmp_path.unlink()

        summary["status"] = "success"
        summary["output_path"] = str(dest)
        logger.info(f"{domain}: uploaded to {dest}")

    except Exception as e:
        logger.error(f"{domain}: processing failed — {e}", exc_info=True)
        summary["status"] = "error"
        summary["error"] = str(e)
        # Clean up temp file if it exists
        try:
            if "tmp_path" in locals():
                tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

    return summary


def run_processing(
    domains: list[str] | None = None,
    config: dict | None = None,
) -> list[dict]:
    """
    Run processing for all specified domains (or all active domains if none given).

    Parameters
    ----------
    domains : list[str], optional
        Specific domains to process. If None, processes all domains that are
        both registered in DOMAIN_PROCESSORS and enabled in settings.yaml.
    config : dict, optional
        Pre-loaded config. Loaded from disk if not provided.

    Returns
    -------
    list[dict]
        List of per-domain summary dicts from run_domain().
    """
    cfg = config or load_config()

    # Check SharePoint is reachable before doing any work
    if not check_sharepoint_available(cfg, logger):
        raise RuntimeError(
            "SharePoint synced folder is not reachable. "
            "Check that OneDrive sync is running and SHAREPOINT_ROOT is set correctly."
        )

    # Determine which domains to run
    if domains:
        to_run = domains
    else:
        # All domains that are registered AND enabled in config
        to_run = [
            d for d in DOMAIN_PROCESSORS
            if cfg.get("domains", {}).get(d, {}).get("enabled", False)
        ]

    if not to_run:
        logger.warning(
            "No domains to process. Either pass --domain explicitly or "
            "enable at least one domain in config/settings.yaml."
        )
        return []

    logger.info(f"Processing stage starting. Domains: {to_run}")
    started_at = datetime.now(timezone.utc)

    summaries = []
    for domain in to_run:
        summary = run_domain(domain, cfg)
        summaries.append(summary)

    finished_at = datetime.now(timezone.utc)
    elapsed = (finished_at - started_at).total_seconds()

    # Print summary table
    logger.info(f"\n{'═' * 50}")
    logger.info("Processing stage complete")
    logger.info(f"{'═' * 50}")
    for s in summaries:
        rows_str = f"{s['rows']:,} rows" if s["rows"] else ""
        err_str  = f" — {s['error']}" if s["error"] else ""
        logger.info(f"  {s['domain']:<20} {s['status']:<10} {rows_str}{err_str}")
    logger.info(f"Elapsed: {elapsed:.1f}s")

    # Write pipeline log to SharePoint database folder
    try:
        write_pipeline_log(
            cfg,
            run_summary={
                "stage": "processing",
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "elapsed_seconds": elapsed,
                "domains_run": to_run,
                "results": summaries,
            },
        )
    except Exception as e:
        logger.warning(f"Could not write pipeline log: {e}")

    return summaries


# ── CLI entry point ───────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the aiccon-data processing stage."
    )
    parser.add_argument(
        "--domain",
        type=str,
        default=None,
        help=(
            "Process a single domain only (e.g. --domain social_economy). "
            "If omitted, all enabled domains in settings.yaml are processed."
        ),
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to settings.yaml (default: auto-detected from project root).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    config = load_config(settings_path=args.config)
    domains = [args.domain] if args.domain else None

    summaries = run_processing(domains=domains, config=config)

    # Exit with error code if any domain failed
    any_failed = any(s["status"] == "error" for s in summaries)
    sys.exit(1 if any_failed else 0)