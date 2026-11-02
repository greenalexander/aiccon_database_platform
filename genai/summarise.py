"""
genai/summarise.py

Entry point for the monthly digest:
    facts (genai/facts.py) -> opening paragraph (genai/overview.py: Gemini, checked,
    template fallback) -> Italian markdown (genai/render.py) -> GCS.

Writes, under the bucket prefix:
    summaries/YYYY-MM.json   audit record: facts, every Gemini attempt, settings
    summaries/YYYY-MM.md     the digest

Exits with 1 only if the facts cannot be computed or the files cannot be written.
Gemini problems never fail the run: the template paragraph is used instead.
Logs carry statuses, model names and token counts, never prompts or Gemini output,
because GitHub Actions logs are public for this repository.

Run from the repository root:
    python -m genai.summarise --month 2026-11 --no-upload    # writes to scratch/
    python -m genai.summarise --month 2026-11                # writes to GCS
    python -m genai.summarise --local-copy digest            # GCS, plus digest/digest_YYYY-MM.md
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logger = logging.getLogger("genai.summarise")


def upload(config: dict, name: str, text: str, content_type: str) -> str:
    from google.cloud import storage

    bucket = config["gcs"]["bucket"]
    blob_name = f"{config['gcs']['prefix'].strip('/')}/summaries/{name}"
    storage.Client().bucket(bucket).blob(blob_name).upload_from_string(
        text.encode("utf-8"), content_type=content_type)
    return f"gs://{bucket}/{blob_name}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build and store the monthly digest.")
    parser.add_argument("--month", default=datetime.now(timezone.utc).strftime("%Y-%m"),
                        help="Target month, YYYY-MM (default: current UTC month).")
    parser.add_argument("--no-upload", action="store_true",
                        help="Write to --out-dir instead of GCS.")
    parser.add_argument("--out-dir", default="scratch", help="Used with --no-upload.")
    parser.add_argument("--local-copy", default=None, metavar="DIR",
                        help="Also write digest_YYYY-MM.md to DIR (used by the workflow "
                             "to publish the digest as a GitHub release).")
    parser.add_argument("--no-llm", action="store_true",
                        help="Skip Gemini and use the template paragraph.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    for noisy in ("httpx", "google_genai"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    import gcsfs
    from genai.facts import MONTH_RE, build_facts
    from genai.overview import DEFAULTS, PROMPT_VERSION, write_overview
    from genai.render import load_labels, overview_counts, render_markdown, template_overview
    from ingestion.loaders.base_loader import load_config

    if not MONTH_RE.match(args.month):
        parser.error("--month must look like 2026-11")

    config = load_config()                       # also loads config/.env locally
    try:
        facts = build_facts(gcsfs.GCSFileSystem(), config, args.month)
    except Exception:  # noqa: BLE001
        logger.exception("Could not compute the facts for %s", args.month)
        return 1
    logger.info("Facts for %s: %s", args.month, facts["status_counts"])

    labels = load_labels()
    settings = {**DEFAULTS, **(config.get("genai") or {})}
    if args.no_llm:
        overview = {"text": template_overview(overview_counts(facts)), "source": "template",
                    "model": None, "prompt_version": PROMPT_VERSION, "attempts": [],
                    "reason": "--no-llm"}
    else:
        overview = write_overview(facts, labels, settings)

    logger.info("Opening paragraph: %s%s%s", overview["source"],
                f" ({overview['model']})" if overview["model"] else "",
                f", reason: {overview['reason']}" if overview["reason"] else "")
    for a in overview["attempts"]:
        logger.info("  attempt %s: usage=%s, problems=%d, error=%s", a["model"],
                    a.get("usage"), len(a.get("problems", [])),
                    a["error"].split(":")[0] if "error" in a else "none")

    digest = render_markdown(facts, labels, overview=overview["text"])
    audit = json.dumps({"written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "facts": facts, "overview": overview, "genai_settings": settings},
                       indent=2, ensure_ascii=False, default=str)

    try:
        if args.no_upload:
            out = Path(args.out_dir)
            out.mkdir(parents=True, exist_ok=True)
            (out / f"digest_{args.month}.json").write_text(audit, encoding="utf-8")
            (out / f"digest_{args.month}.md").write_text(digest, encoding="utf-8")
            logger.info("Digest written to %s", out.resolve())
        else:
            # Audit record first, so a digest never exists without its audit trail.
            for name, text, ctype in ((f"{args.month}.json", audit, "application/json"),
                                      (f"{args.month}.md", digest, "text/markdown; charset=utf-8")):
                logger.info("Written %s", upload(config, name, text, ctype))
        if args.local_copy:
            copy_dir = Path(args.local_copy)
            copy_dir.mkdir(parents=True, exist_ok=True)
            (copy_dir / f"digest_{args.month}.md").write_text(digest, encoding="utf-8")
    except Exception:  # noqa: BLE001
        logger.exception("Could not write the digest")
        return 1

    # On GitHub Actions, also show the digest on the run's Summary page.
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("\n" + digest + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())