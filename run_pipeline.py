"""
run_pipeline.py

Single entry point for the full aiccon-data pipeline.

── What the three stages do ──────────────────────────────────────────────────

  ingest   Calls the API fetcher scripts for each domain. Downloads fresh
           data from Eurostat and ISTAT and writes raw parquet files to
           SharePoint/aiccon-data/raw/{domain}/. Each file is date-stamped
           so you accumulate monthly snapshots of the source data.

  process  Reads the raw parquet files, harmonises geography (NUTS codes),
           legal forms, and NACE labels, merges sources with priority rules,
           and writes a single cleaned parquet to
           SharePoint/aiccon-data/processed/{domain}/. This is the step
           that uses the mapping tables in processing/mappings/.

  database Reads the processed parquet files and builds the DuckDB star
           schema: creates dimension and fact tables, resolves surrogate
           keys, creates the analytical views, and uploads the finished
           aiccon.duckdb to SharePoint/aiccon-data/database/. This is
           the file PowerBI connects to.

── When to run each stage ────────────────────────────────────────────────────

  Monthly update (normal case — run this once a month):
      python run_pipeline.py
      Runs all three stages for all enabled domains.

  After fixing a mapping table (e.g. adding rows to nuts_istat.csv)
  without needing new data from the APIs:
      python run_pipeline.py --stage process
      python run_pipeline.py --stage database
      Reuses the existing raw parquet files. No API calls made.

  After fixing a bug in a merge script without changing mappings or data:
      python run_pipeline.py --stage database
      Just rebuilds the DuckDB file from existing processed parquet.
      Takes seconds rather than minutes.

  When building and testing a new domain (e.g. immigration):
      python run_pipeline.py --domain immigration
      Runs all three stages for immigration only, leaving existing
      social_economy and labour data untouched in SharePoint and in the database.

  Debugging a specific stage for a specific domain:
      python run_pipeline.py --stage ingest --domain social_economy

── How to debug failures ─────────────────────────────────────────────────────

  The exit code will be 1 if anything failed, but for diagnosis:

  1. Read the terminal output — ERROR lines include the full Python
     traceback. This is usually enough to identify the problem.

  2. Check pipeline_log.json in your SharePoint database/ folder.
     It is a plain JSON file you can open in any text editor. It contains
     a structured summary of every stage and domain — status, row counts,
     error messages — from the most recent run.

  3. Run the failing stage in isolation to reproduce with less noise:
         python run_pipeline.py --stage process --domain social_economy

  4. Run the individual script directly for maximum detail:
         python -m ingestion.api_sources.social_economy.istat
         python -m processing.integrate.merge_social_economy
         python -m database.build_db --domain social_economy
         python -m database.tests.test_integrity

── Adding a new domain ───────────────────────────────────────────────────────

  Only ONE change is needed in this file: add the new loader classes to
  DOMAIN_INGESTION_CLASSES in run_ingest() below.
  See the commented immigration example there.

  The other files you update when adding a domain are:
      ingestion/api_sources/{domain}/eurostat.py  ← new fetcher
      ingestion/api_sources/{domain}/istat.py     ← new fetcher
      processing/integrate/merge_{domain}.py      ← new merge script
      processing/pipeline.py                      ← add to DOMAIN_PROCESSORS
      database/build_db.py                        ← add to DOMAIN_LOADERS
      database/schema/fact_tables.sql             ← fill in stub table
      database/schema/views.sql                   ← add domain views
      database/tests/test_integrity.py            ← fill in stub check
      config/settings.yaml                        ← set enabled: true
      processing/mappings/domain_sources.csv      ← add source rows
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ingestion.loaders.base_loader import get_logger, load_config
from ingestion.loaders.sharepoint_loader import (
    check_sharepoint_available,
    write_pipeline_log,
)

logger = get_logger("run_pipeline")

# Valid stages in execution order
STAGES = ["ingest", "process", "database"]


# ── Stage runners ─────────────────────────────────────────────────────────────

def run_ingest(domains: list[str] | None, config: dict) -> dict:
    """
    Run the ingestion stage — fetch from APIs and write raw parquet files.

    ── How to add a new domain ───────────────────────────────────────────────
    Step 1: import the new loader classes at the top of this function,
            following the pattern of the existing social_economy imports.

    Step 2: add the domain to DOMAIN_INGESTION_CLASSES, mapping the domain
            name to a list of loader classes to run for that domain.
            Each loader class must be a subclass of BaseLoader with a
            SOURCE_ID and DOMAIN attribute and a fetch() method.

    Example — adding immigration:

        from ingestion.api_sources.immigration.eurostat import EurostatImmigrationLoader
        from ingestion.api_sources.immigration.istat   import IstatImmigrationLoader

        DOMAIN_INGESTION_CLASSES = {
            "social_economy": [EurostatSocialEconomyLoader, IstatSocialEconomyLoader],
            "immigration":    [EurostatImmigrationLoader, IstatImmigrationLoader],
        }

    Some domains may only have one source (e.g. only ISTAT, no Eurostat):

        "welfare": [IstatWelfareLoader],

    Some may have three or more if you add manual-source loaders later:

        "social_economy": [EurostatSocialEconomyLoader,
                           IstatSocialEconomyLoader,
                           RuntsLoader],        # manual source loader
    """
    # ── Active loader imports ─────────────────────────────────────────────────
    from ingestion.api_sources.social_economy.eurostat import EurostatSocialEconomyLoader
    from ingestion.api_sources.social_economy.istat   import IstatSocialEconomyLoader
    from ingestion.api_sources.labour.eurostat_labour         import EurostatLabourLoader
    from ingestion.api_sources.labour.istat_labour            import IstatLabourLoader

    # ── Domain registry ───────────────────────────────────────────────────────
    # Maps domain name → list of loader classes to run for that domain.
    # Add new domains here following the pattern above.
    DOMAIN_INGESTION_CLASSES: dict[str, list] = {
        "social_economy": [EurostatSocialEconomyLoader, IstatSocialEconomyLoader],
        "labour":         [EurostatLabourLoader,  IstatLabourLoader],

        # Uncomment and complete when building each domain:
        # "immigration": [EurostatImmigrationLoader, IstatImmigrationLoader],
        # "welfare":     [EurostatWelfareLoader, IstatWelfareLoader],
        # "poverty":     [EurostatPovertyLoader, IstatPovertyLoader],
        # "sdg":         [EurostatSdgLoader, IstatSdgLoader],
    }

    active_domains = _resolve_domains(domains, config, DOMAIN_INGESTION_CLASSES)
    if not active_domains:
        return {"status": "skipped", "reason": "no active domains"}

    started_at = datetime.now(timezone.utc)
    results = {}

    for domain in active_domains:
        logger.info(f"{'─' * 50}")
        logger.info(f"Ingesting domain: {domain}")
        logger.info(f"{'─' * 50}")
        domain_results = []

        for LoaderClass in DOMAIN_INGESTION_CLASSES[domain]:
            loader_name = LoaderClass.__name__
            try:
                loader = LoaderClass(config=config)
                written = loader.run()
                domain_results.append({
                    "loader":  loader_name,
                    "status":  "success",
                    "files":   [str(p) for p in written],
                    "n_files": len(written),
                })
                logger.info(f"  {loader_name}: {len(written)} file(s) written")
            except Exception as e:
                logger.error(f"  {loader_name}: FAILED — {e}", exc_info=True)
                domain_results.append({
                    "loader": loader_name,
                    "status": "error",
                    "error":  str(e),
                })

        results[domain] = domain_results

    return {
        "status":     "complete",
        "started_at": started_at.isoformat(),
        "results":    results,
    }


def run_process(domains: list[str] | None, config: dict) -> dict:
    """
    Run the processing stage via processing/pipeline.py.

    Reads raw parquet from SharePoint, harmonises and merges,
    writes processed parquet back to SharePoint.
    No API calls are made in this stage.
    """
    from processing.pipeline import run_processing

    started_at = datetime.now(timezone.utc)
    summaries  = run_processing(domains=domains, config=config)

    any_error = any(s["status"] == "error" for s in summaries)
    return {
        "status":     "error" if any_error else "complete",
        "started_at": started_at.isoformat(),
        "results":    summaries,
    }


def run_database(domains: list[str] | None, config: dict) -> dict:
    """
    Run the database build stage via database/build_db.py.

    Reads processed parquet from SharePoint, builds the DuckDB star schema,
    and uploads aiccon.duckdb to SharePoint. No API calls or processing.
    This is the fastest stage — usually takes under a minute.
    """
    from database.build_db import build_database

    started_at = datetime.now(timezone.utc)
    try:
        dest = build_database(domains=domains, config=config)
        return {
            "status":      "complete",
            "started_at":  started_at.isoformat(),
            "output_path": str(dest),
        }
    except Exception as e:
        logger.error(f"Database build failed: {e}", exc_info=True)
        return {
            "status":     "error",
            "started_at": started_at.isoformat(),
            "error":      str(e),
        }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _resolve_domains(
    domains: list[str] | None,
    config: dict,
    registered: dict,
) -> list[str]:
    """
    Return the list of domains to run.

    If --domain was passed explicitly, use that.
    Otherwise use all domains that are both registered in the given
    dict and enabled: true in settings.yaml.
    """
    if domains:
        return [d for d in domains if d in registered]
    return [
        d for d in registered
        if config.get("domains", {}).get(d, {}).get("enabled", False)
    ]


def _print_summary(stage_results: dict[str, dict], elapsed: float) -> None:
    """Print a final summary table of all stages."""
    logger.info(f"\n{'═' * 60}")
    logger.info("Pipeline run complete")
    logger.info(f"{'═' * 60}")
    for stage, result in stage_results.items():
        status = result.get("status", "skipped")
        icon   = "✓" if status == "complete" else ("✗" if status == "error" else "–")
        logger.info(f"  {icon} {stage:<12} {status}")
    logger.info(f"Total elapsed: {elapsed:.1f}s")
    logger.info(f"{'═' * 60}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_pipeline(
    stages:  list[str] | None = None,
    domains: list[str] | None = None,
    config:  dict | None = None,
) -> bool:
    """
    Run the full pipeline (or a subset of stages/domains).

    Returns True if all stages completed without errors, False otherwise.
    """
    cfg           = config or load_config()
    stages_to_run = stages or STAGES
    started_at    = datetime.now(timezone.utc)
    stage_results = {}

    if not check_sharepoint_available(cfg, logger):
        logger.error(
            "SharePoint synced folder is not reachable.\n"
            "Check that OneDrive sync is running and SHAREPOINT_ROOT is set in .env"
        )
        return False

    logger.info(f"{'═' * 60}")
    logger.info("aiccon-data pipeline starting")
    logger.info(f"Stages : {stages_to_run}")
    logger.info(f"Domains: {domains or 'all enabled'}")
    logger.info(f"{'═' * 60}")

    stage_runners = {
        "ingest":   run_ingest,
        "process":  run_process,
        "database": run_database,
    }

    all_ok = True
    for stage in stages_to_run:
        if stage not in stage_runners:
            logger.error(f"Unknown stage '{stage}'. Valid stages: {STAGES}")
            all_ok = False
            continue

        logger.info(f"\n{'━' * 60}")
        logger.info(f"STAGE: {stage.upper()}")
        logger.info(f"{'━' * 60}")

        result = stage_runners[stage](domains, cfg)
        stage_results[stage] = result

        if result.get("status") == "error":
            all_ok = False
            logger.error(
                f"Stage '{stage}' encountered errors. "
                "Subsequent stages will still run — check logs above for details."
            )

    finished_at = datetime.now(timezone.utc)
    elapsed     = (finished_at - started_at).total_seconds()

    _print_summary(stage_results, elapsed)

    try:
        write_pipeline_log(cfg, run_summary={
            "started_at":      started_at.isoformat(),
            "finished_at":     finished_at.isoformat(),
            "elapsed_seconds": elapsed,
            "stages_run":      stages_to_run,
            "domains":         domains or "all enabled",
            "all_ok":          all_ok,
            "stage_results":   stage_results,
        })
    except Exception as e:
        logger.warning(f"Could not write pipeline log: {e}")

    return all_ok


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the aiccon-data pipeline.\n\n"
            "Examples:\n"
            "  python run_pipeline.py                           # full monthly run\n"
            "  python run_pipeline.py --stage ingest            # fetch from APIs only\n"
            "  python run_pipeline.py --stage process           # re-process existing raw files\n"
            "  python run_pipeline.py --stage database          # rebuild DuckDB only\n"
            "  python run_pipeline.py --domain social_economy   # one domain, all stages\n"
            "  python run_pipeline.py --stage process --domain social_economy"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--stage",
        type=str,
        choices=STAGES,
        default=None,
        help="Run a single stage only. Omit to run all three stages in order.",
    )
    parser.add_argument(
        "--domain",
        type=str,
        default=None,
        help="Run a single domain only. Omit to run all enabled domains.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to settings.yaml (default: auto-detected from project root).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args    = parse_args()
    config  = load_config(settings_path=args.config)
    stages  = [args.stage]  if args.stage  else None
    domains = [args.domain] if args.domain else None

    success = run_pipeline(stages=stages, domains=domains, config=config)
    sys.exit(0 if success else 1)