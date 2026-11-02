"""
genai/facts.py

Computes the facts behind the monthly release digest: which datasets were
refreshed, which gained new periods, which were not updated and which are
missing. For each domain it compares the processed parquet of the target month
with the most recent earlier one, using metadata only (dataset codes, periods,
row counts, extraction times). Values are never read.

Everything the digest states comes from the dictionary built here. The language
model only writes prose around it.

Run from the repository root:
    python -m genai.facts                       # current UTC month
    python -m genai.facts --month 2026-10       # any month with processed files
    python -m genai.facts --out facts.json      # write to a file instead of stdout
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

SCHEMA_VERSION = 1

# Domain -> module whose ISTAT_DATASET_META and EUROSTAT_DATASET_META list the
# datasets the domain is configured to contain. Add new domains here.
DOMAIN_MODULES = {
    "social_economy": "processing.integrate.merge_social_economy",
    "labour": "processing.integrate.merge_labour",
}

MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
FILE_MONTH_RE = re.compile(r"_(\d{4}-\d{2})\.parquet$")
FREQUENCY_PATTERNS = {            # inferred from the period label itself
    "M": re.compile(r"^\d{4}-\d{2}$"),
    "Q": re.compile(r"^\d{4}-Q[1-4]$"),
    "A": re.compile(r"^\d{4}$"),
}
METADATA_COLUMNS = ("dataset_code", "extracted_at", "period_label", "time")


# ── Pure functions (no I/O) ───────────────────────────────────────────────────

def period_frequency(period: str) -> str:
    """'2026-09' -> 'M', '2026-Q2' -> 'Q', '2025' -> 'A', anything else -> '?'."""
    for freq, pattern in FREQUENCY_PATTERNS.items():
        if pattern.match(period):
            return freq
    return "?"


def group_periods(periods: set[str]) -> dict[str, list[str]]:
    """Group period labels by frequency, each list sorted chronologically."""
    grouped: dict[str, list[str]] = {}
    for p in periods:
        grouped.setdefault(period_frequency(p), []).append(p)
    return {f: sorted(v) for f, v in sorted(grouped.items())}


def summarise_file(df: pd.DataFrame) -> dict[str, dict]:
    """dataset_code -> {rows, periods (set), extracted_at (UTC Timestamp)}."""
    period = (df["period_label"].astype("string") if "period_label" in df.columns
              else pd.Series(pd.NA, index=df.index, dtype="string"))
    if "time" in df.columns:
        period = period.fillna(df["time"].astype("string"))
    work = pd.DataFrame({
        "dataset_code": df["dataset_code"].astype("string"),
        "period": period.str.strip(),
        "extracted_at": pd.to_datetime(df["extracted_at"], utc=True, format="ISO8601"),
    })
    out = {}
    for code, g in work.groupby("dataset_code"):
        out[str(code)] = {
            "rows": int(len(g)),
            "periods": set(g["period"].dropna().unique()),
            "extracted_at": g["extracted_at"].max(),
        }
    return out


def compare_dataset(code: str, expected: bool, prev: dict | None, curr: dict | None,
                    has_previous_file: bool) -> dict:
    """Facts for one dataset. prev and curr are entries from summarise_file()."""
    rec: dict = {"dataset_code": code, "expected": expected}

    if curr is None:
        rec["status"] = "missing"
        rec["last_extracted_on"] = (prev["extracted_at"].date().isoformat()
                                    if prev is not None else None)
        return rec

    latest = {f: v[-1] for f, v in group_periods(curr["periods"]).items()}
    rec.update(rows=curr["rows"], latest_period=latest,
               extracted_on=curr["extracted_at"].date().isoformat())

    if not has_previous_file:
        rec["status"] = "baseline"
    elif prev is None:
        rec["status"] = "new_dataset"
    else:
        rec["status"] = ("updated" if curr["extracted_at"] > prev["extracted_at"]
                         else "not_updated")
        rec["rows_change"] = curr["rows"] - prev["rows"]
        rec["new_periods"] = group_periods(curr["periods"] - prev["periods"])
        rec["removed_periods"] = group_periods(prev["periods"] - curr["periods"])
    return rec


# ── I/O ───────────────────────────────────────────────────────────────────────

def expected_datasets(domain: str) -> set[str]:
    module = importlib.import_module(DOMAIN_MODULES[domain])
    return {*module.ISTAT_DATASET_META, *module.EUROSTAT_DATASET_META}


def list_processed(fs, config: dict, domain: str) -> dict[str, str]:
    """Month (YYYY-MM) -> GCS path of that month's processed parquet."""
    prefix = (f"{config['gcs']['bucket']}/{config['gcs']['prefix'].strip('/')}"
              f"/processed/{domain}/")
    try:
        paths = fs.ls(prefix, refresh=True)
    except FileNotFoundError:
        return {}
    months = {}
    for p in paths:
        m = FILE_MONTH_RE.search(p)
        if m:
            months[m.group(1)] = p
    return months


def read_metadata(fs, path: str) -> pd.DataFrame:
    """Read only the metadata columns of a processed parquet."""
    with fs.open(path, "rb") as f:
        pf = pq.ParquetFile(f)
        cols = [c for c in METADATA_COLUMNS if c in pf.schema_arrow.names]
        return pf.read(columns=cols).to_pandas()


def build_facts(fs, config: dict, month: str) -> dict:
    domains = [d for d in DOMAIN_MODULES
               if config.get("domains", {}).get(d, {}).get("enabled", False)]
    domain_facts = []

    for domain in domains:
        expected = expected_datasets(domain)
        files = list_processed(fs, config, domain)
        earlier = sorted(m for m in files if m < month)
        prev_month = earlier[-1] if earlier else None
        rec = {"domain": domain, "previous_month": prev_month,
               "current_file": files.get(month), "previous_file": files.get(prev_month)}

        if month not in files:
            rec["status"] = "not_processed"
            rec["datasets"] = [{"dataset_code": c, "expected": True, "status": "not_processed"}
                               for c in sorted(expected)]
            domain_facts.append(rec)
            continue

        curr = summarise_file(read_metadata(fs, files[month]))
        prev = summarise_file(read_metadata(fs, files[prev_month])) if prev_month else {}
        rec["status"] = "processed"
        rec["datasets"] = [
            compare_dataset(code, code in expected, prev.get(code), curr.get(code),
                            has_previous_file=prev_month is not None)
            for code in sorted(expected | curr.keys() | prev.keys())
        ]
        domain_facts.append(rec)

    status_counts: dict[str, int] = {}
    for d in domain_facts:
        for ds in d["datasets"]:
            status_counts[ds["status"]] = status_counts.get(ds["status"], 0) + 1

    return {
        "schema_version": SCHEMA_VERSION,
        "month": month,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "commit_sha": os.environ.get("GITHUB_SHA"),
        "status_counts": dict(sorted(status_counts.items())),
        "datasets_with_new_periods": sum(
            1 for d in domain_facts for ds in d["datasets"] if ds.get("new_periods")),
        "domains": domain_facts,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Compute the facts for the monthly digest.")
    parser.add_argument("--month", default=datetime.now(timezone.utc).strftime("%Y-%m"),
                        help="Target month, YYYY-MM (default: current UTC month).")
    parser.add_argument("--out", default=None, help="Write JSON here instead of stdout.")
    args = parser.parse_args()
    if not MONTH_RE.match(args.month):
        parser.error("--month must look like 2026-11")

    import gcsfs
    from ingestion.loaders.base_loader import load_config

    config = load_config()          # loads config/.env locally, before the GCS client
    facts = build_facts(gcsfs.GCSFileSystem(), config, args.month)
    text = json.dumps(facts, indent=2, ensure_ascii=False)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"Facts written to {args.out}: {facts['status_counts']}")
    else:
        print(text)


if __name__ == "__main__":
    main()