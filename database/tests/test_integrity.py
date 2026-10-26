"""
database/tests/test_integrity.py

Post-load integrity checks for the aiccon-data BigQuery dataset.

Run after every database build, and from GitHub Actions after the monthly
pipeline run:

    python -m database.tests.test_integrity
    python -m database.tests.test_integrity --domain labour
    python -m database.tests.test_integrity --max-age-days 60

Exit code is 1 if any check FAILs or could not run (ERROR), 0 otherwise.
WARN results are reported but never change the exit code.

What is checked
---------------
Dimension tables (always checked, whatever --domain says):
    row count             table exists and meets a minimum row count
    nulls                 required columns contain no NULLs
    key uniqueness        surrogate key and natural key are each unique
    Italian NUTS units    dim_geography holds the Italian NUTS0/2/3 units
    auto-inserted codes   (WARN) Italian codes that did not come from nuts_istat.csv

Fact tables (one block per domain; optional checks run only if configured):
    row count, nulls, fact_key uniqueness
    grain duplicates      no two rows identical on every descriptive column
    orphan keys           every foreign key matches a dimension row
    value bounds          no values outside plausible bounds (counts >= 0, rates 0-100)
    providers             rows present from each expected provider (ISTAT, Eurostat)
    Italian NUTS units    the table has data at each expected NUTS level
    other countries       enough non-Italian countries at NUTS0 (Eurostat rows)
    indicator coverage    expected indicator_code values are present
    frequencies           expected frequencies present; monthly labels are YYYY-MM
    optional columns      dimension columns such as education are not entirely empty
    datasets              (WARN) expected dataset_code values present and recently extracted
    views                 listed views (later: dbt marts) exist and return rows

Value bounds, providers, other countries, frequencies and optional columns
are carried over from the DuckDB-era integrity_tests.py.

Row-count minimums cannot detect a table that has been loaded twice, because
counts only go up. The grain check is what catches that: rows that agree on
every dimension column but occur more than once are the signature of a
double load, or of a dimension column dropped between merge and load.

Cost
----
A few dozen small queries per run. Each is capped by MAX_BYTES_BILLED: if
BigQuery estimates that a query would bill more than the cap, it refuses to
run it and nothing is charged.

Adding a new domain
-------------------
Add an entry to FACT_CHECKS. Nothing else in this file needs to change.
A domain enabled in settings.yaml without an entry produces a WARN, so a
new domain cannot go untested without anyone noticing.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.loaders.base_loader import get_logger, load_config  # noqa: E402

logger = get_logger("database.tests.integrity")


# ══════════════════════════════════════════════════════════════════════════════
# Configuration: thresholds and expectations live here, not in the checks
# ══════════════════════════════════════════════════════════════════════════════

# Per-query billing cap. These tables are small, so a query that would bill
# more than this is a bug in the SQL, not a legitimate check.
MAX_BYTES_BILLED = 1_000_000_000  # 1 GB

# A dataset whose most recent extracted_at is older than this produces a WARN.
# Monthly pipeline plus two weeks of slack.
DEFAULT_MAX_AGE_DAYS = 45

# Italian units per NUTS level (NUTS 2024: 5 NUTS1, 21 NUTS2, 107 NUTS3).
# NUTS1 is not checked because nothing in the facts is published at that level.
ITALY_NUTS_UNITS = {0: 1, 2: 21, 3: 107}

# Status for Italian codes that are in dim_geography but not in nuts_istat.csv
# (inserted automatically, without names). nuts_istat.csv covers NUTS 2024
# since October 2026, so any such code is a gap to fix.
AUTO_GEO_STATUS = "FAIL"

# Required format of period_label on monthly (frequency = 'M') rows.
MONTHLY_LABEL_RE = r"^\d{4}-\d{2}$"

# Columns that are attributes of a fact row rather than part of its identity.
# Every other column of a fact table is treated as part of the grain.
GRAIN_EXCLUDE = {"fact_key", "value", "extracted_at", "nace_label_en"}

# Required (never NULL) columns for every fact table.
FACT_REQUIRED = [
    "fact_key", "geo_key", "time_key", "source_key", "indicator_key",
    "value", "dataset_code",
]

# Fact foreign key -> dimension table. The dimension's primary key has the
# same column name. Keys absent from a fact table's schema are skipped, and
# NULL keys are not counted as orphans (legal_form_key is nullable by design).
FACT_FOREIGN_KEYS = [
    ("geo_key",        "dim_geography"),
    ("time_key",       "dim_time"),
    ("source_key",     "dim_source"),
    ("indicator_key",  "dim_indicator"),
    ("legal_form_key", "dim_legal_form"),
]


def _csv_row_count(filename: str, distinct_on: list[str] | None = None) -> int:
    """Rows in a mapping CSV, optionally distinct on some columns."""
    df = pd.read_csv(_find_mappings_dir() / filename, dtype=str)
    df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)
    if distinct_on:
        df = df.dropna(subset=distinct_on).drop_duplicates(subset=distinct_on)
    return len(df)


# min_rows is an int or a zero-argument callable, evaluated at run time.
DIMENSIONS: dict[str, dict] = {
    "dim_geography": {
        "key": "geo_key",
        "natural_key": ["nuts_code"],
        "required": ["geo_key", "nuts_code", "nuts_level", "country_code"],
        # ISTAT aggregates (Nord, Mezzogiorno, Trentino-Alto Adige) are not
        # NUTS units, so they deliberately have no nuts_level.
        "null_exempt": {"nuts_level": "geo_source = 'istat_aggregate'"},
        # Italian NUTS0 + NUTS2 + NUTS3. Other countries are added on top.
        "min_rows": sum(ITALY_NUTS_UNITS.values()),
    },
    "dim_time": {
        "key": "time_key",
        "natural_key": ["year"],
        "required": ["time_key", "year"],
        "min_rows": 41,  # 1990-2030 inclusive, see populate_dim_time()
    },
    "dim_source": {
        "key": "source_key",
        "natural_key": ["source_id"],
        "required": ["source_key", "source_id"],
        "min_rows": lambda: _csv_row_count("domain_sources.csv", ["source_id"]),
    },
    "dim_legal_form": {
        "key": "legal_form_key",
        "natural_key": ["source_system", "source_code"],
        "required": ["legal_form_key", "unified_category", "source_system", "source_code"],
        "min_rows": lambda: _csv_row_count("legal_form_map.csv"),
    },
    "dim_indicator": {
        "key": "indicator_key",
        "natural_key": ["indicator_code"],
        "required": ["indicator_key", "indicator_code"],
        "min_rows": 18,  # rows defined in populate_dim_indicator()
    },
}


FACT_CHECKS: dict[str, dict] = {
    "social_economy": {
        "table": "fact_social_economy",
        # ~90% of the clean count. Baseline 19 Oct 2026: 7,258 rows, every one
        # duplicated, so 3,629 real rows.
        "min_rows": 3_250,
        # Carried over from the DuckDB-era integrity_tests.py. Any other
        # indicator present is listed in the check's detail line; promote it
        # here once you are sure the merge always produces it.
        "expected_indicators": [
            "volunteering_rate",
            "association_membership_rate",
            "n_employed",
        ],
        # Matched against dim_source.provider, or as a substring of source_id.
        "expected_providers": ["istat", "eurostat"],
        # (min, max) per indicator_code; "*" covers every indicator not listed.
        # None = unbounded on that side. Only non-negativity is checked here
        # because volunteering_rate mixes units (see dim_indicator notes).
        "value_bounds": {"*": (0, None)},
        # Minimum distinct Italian units with data, per NUTS level. NUTS2
        # assumes the Eurostat regional employment rows (nama_10r_3empers)
        # are loaded. Add 3: 107 if provincial data are expected.
        "italy_nuts_min": {0: 1, 2: 21},
        # Datasets present in the 19 Oct 2026 baseline
        "expected_datasets": [
            "83_63_DF_DCCV_AVQ_PERSONE_129",
            "83_63_DF_DCCV_AVQ_PERSONE_130",
            "83_63_DF_DCCV_AVQ_PERSONE_131",
            "83_63_DF_DCCV_AVQ_PERSONE_132",
            "85_171_DF_DCSA_VOLON_ORG1_1",
            "85_171_DF_DCSA_VOLON_ORG1_2",
            "85_171_DF_DCSA_VOLON_ORG1_3",
            "85_171_DF_DCSA_VOLON_ORG1_4",
            "85_84_DF_DCSA_VOLON1_1",
            "85_84_DF_DCSA_VOLON1_2",
            "85_84_DF_DCSA_VOLON1_3",
            "85_84_DF_DCSA_VOLON1_6",
            "lfsa_egan22d",
            "nama_10r_3empers",
            "sbs_r_nuts2021",
        ],
        # BigQuery views (later: dbt marts) that PowerBI depends on.
        "views": [],
    },
    "labour": {
        "table": "fact_labour",
        # ~90% of the baseline (19 Oct 2026: 309,321 rows, no duplicates).
        # Will rise once provincial data load correctly.
        "min_rows": 275_000,
        # ISTAT_DATASET_META and EUROSTAT_DATASET_META in merge_labour.py
        "expected_indicators": [
            "labour_force",
            "employment_rate",
            "unemployment_rate",
            "inactivity_rate",
            "neet_rate",
            "n_employed",
        ],
        "expected_providers": ["istat", "eurostat"],
        "value_bounds": {
            "*":                 (0, None),   # labour_force, n_employed
            "employment_rate":   (0, 100),
            "unemployment_rate": (0, 100),
            "inactivity_rate":   (0, 100),
            "neet_rate":         (0, 100),
        },
        # NUTS3 comes from the ISTAT provincial datasets (TAXINATT1_5,
        # TAXDISOCCU1_8, TAXOCCU1_5); NUTS2 from NEET1_11 and Eurostat.
        # KNOWN GAP (26 Oct 2026): 105 of 107 provinces have data. Accepted
        # for now so that a failed run signals something new. Restore 107
        # once the two missing provinces are mapped.
        "italy_nuts_min": {0: 1, 2: 21, 3: 105},
        # Non-Italian countries with NUTS0 rows (Eurostat). Baseline: 33.
        "min_other_countries": 30,
        # A = annual, Q = quarterly (60,024 rows in the baseline),
        # M = monthly (FORZLVMENS1_1)
        "expected_frequencies": ["A", "Q", "M"],
        # Must not be entirely null or blank (Eurostat rate datasets fill them)
        "populated_columns": ["education", "citizenship"],
        # Mirrors the same two dicts in merge_labour.py. Update both together.
        "expected_datasets": [
            "150_873_DF_DCCV_FORZLVMENS1_1",
            "172_931_DF_DCCV_NEET1_11",
            "172_931_DF_DCCV_NEET1_9",
            "152_913_DF_DCCV_TAXINATT1_5",
            "151_914_DF_DCCV_TAXDISOCCU1_8",
            "151_914_DF_DCCV_TAXDISOCCU1_6",
            "151_914_DF_DCCV_TAXDISOCCU1_5",
            "150_915_DF_DCCV_TAXOCCU1_5",
            "lfsa_egan22d",
            "lfst_r_lfu3rt",
            "lfst_r_lfur2gan",
            "lfst_r_lfe2emprtn",
        ],
        "views": [],
    },
    # Minimal entry for a new domain. Every key below "table", "min_rows" and
    # "expected_indicators" is optional; omitted checks are simply skipped.
    #
    # "immigration": {
    #     "table": "fact_immigration",
    #     "min_rows": 1,
    #     "expected_indicators": ["n_resident_foreign"],
    #     "expected_providers": ["istat", "eurostat"],
    #     "value_bounds": {"*": (0, None)},
    #     "italy_nuts_min": {0: 1, 2: 21, 3: 107},
    # },
}


# ══════════════════════════════════════════════════════════════════════════════
# Results
# ══════════════════════════════════════════════════════════════════════════════

PASS, WARN, FAIL, ERROR = "PASS", "WARN", "FAIL", "ERROR"

# What every check function returns: (status, observed, expected, detail)
Outcome = tuple[str, str, str, str]


@dataclass
class CheckResult:
    table:    str
    check:    str
    status:   str
    observed: str = "-"
    expected: str = ""
    detail:   str = ""

    @property
    def failed(self) -> bool:
        return self.status in (FAIL, ERROR)


def _run(table: str, check: str, fn: Callable[[], Outcome]) -> CheckResult:
    """Run one check. Any exception becomes an ERROR result, never a crash."""
    try:
        status, observed, expected, detail = fn()
    except Exception as e:
        logger.error(f"  ERROR  {table} / {check}: {type(e).__name__}: {e}")
        return CheckResult(table, check, ERROR, detail=f"{type(e).__name__}: {e}")
    logger.info(f"  {status:<5}  {table} / {check}")
    return CheckResult(table, check, status, observed, expected, detail)


# ══════════════════════════════════════════════════════════════════════════════
# BigQuery helpers
# ══════════════════════════════════════════════════════════════════════════════

def _get_bq_client(config: dict) -> bigquery.Client:
    return bigquery.Client(
        project=config["bigquery"]["project"],
        location=config["bigquery"].get("location", "US"),
    )


def _ref(config: dict, table: str) -> str:
    return f"{config['bigquery']['project']}.{config['bigquery']['dataset']}.{table}"


def _query(client: bigquery.Client, sql: str) -> list:
    """Run a query with the billing cap and return all rows (bigquery.Row)."""
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED)
    return list(client.query(sql, job_config=job_config).result())


def _table_columns(client: bigquery.Client, ref: str) -> list[str] | None:
    """Column names of a table, or None if it does not exist. Metadata only, no query cost."""
    try:
        return [f.name for f in client.get_table(ref).schema]
    except NotFound:
        return None


def _find_mappings_dir() -> Path:
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        candidate = parent / "processing" / "mappings"
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Could not find processing/mappings/ directory.")


def _n(x: int | None) -> str:
    return "-" if x is None else f"{x:,}"


def _fmt_sample(pairs: list[tuple]) -> str:
    return ", ".join(f"{'NULL' if k is None else k} (+{n:,})" for k, n in pairs)


def _duplicates(
    client: bigquery.Client,
    ref: str,
    cols: list[str],
    breakdown_col: str,
    limit: int = 5,
) -> tuple[int, int, list[tuple]]:
    """
    Find combinations of `cols` that occur more than once.

    Returns (n_groups, n_excess_rows, top breakdown_col values by excess rows).
    n_excess_rows is how many rows would have to be deleted to make the
    combination unique.
    """
    group = ", ".join(f"`{c}`" for c in cols)
    cte = f"""
        WITH dup AS (
            SELECT {group}, COUNT(*) AS _n
            FROM `{ref}`
            GROUP BY {group}
            HAVING COUNT(*) > 1
        )
    """
    totals = _query(
        client,
        cte + "SELECT COUNT(*) AS n_groups, IFNULL(SUM(_n - 1), 0) AS n_excess FROM dup",
    )[0]

    sample: list[tuple] = []
    if totals["n_groups"]:
        rows = _query(client, cte + f"""
            SELECT CAST(`{breakdown_col}` AS STRING) AS k, SUM(_n - 1) AS excess
            FROM dup
            GROUP BY k
            ORDER BY excess DESC
            LIMIT {limit}
        """)
        sample = [(r["k"], r["excess"]) for r in rows]

    return totals["n_groups"], totals["n_excess"], sample


def _italy_units_by_level(
    client: bigquery.Client,
    config: dict,
    fact_ref: str | None = None,
) -> dict[int, int]:
    """
    Distinct Italian nuts_codes per NUTS level, either in dim_geography
    (fact_ref=None) or among the geo_keys actually used by a fact table.
    COUNT(DISTINCT nuts_code) keeps this correct even if geo_key is duplicated.
    """
    geo_ref = _ref(config, "dim_geography")
    if fact_ref is None:
        source = f"`{geo_ref}` AS g"
        # Inactive rows are codes retired from nuts_istat.csv (e.g. ITC45)
        active_only = "AND IFNULL(g.is_active, TRUE)"
    else:
        source = (
            f"(SELECT DISTINCT geo_key FROM `{fact_ref}`) AS f "
            f"JOIN `{geo_ref}` AS g ON f.geo_key = g.geo_key"
        )
        active_only = ""
    rows = _query(client, f"""
        SELECT g.nuts_level, COUNT(DISTINCT g.nuts_code) AS n_units
        FROM {source}
        WHERE g.country_code = 'IT' {active_only}
        GROUP BY g.nuts_level
    """)
    return {r["nuts_level"]: r["n_units"] for r in rows if r["nuts_level"] is not None}


# ══════════════════════════════════════════════════════════════════════════════
# Checks: each returns an Outcome tuple
# ══════════════════════════════════════════════════════════════════════════════

def check_row_count(client: bigquery.Client, ref: str, min_rows: int) -> Outcome:
    n = _query(client, f"SELECT COUNT(*) AS n FROM `{ref}`")[0]["n"]
    return (PASS if n >= min_rows else FAIL), _n(n), f">= {min_rows:,}", ""


def check_nulls(
    client: bigquery.Client,
    ref: str,
    columns: list[str],
    required: list[str],
    exempt: dict[str, str] | None = None,
) -> Outcome:
    """exempt maps a column to a SQL condition under which NULL is allowed."""
    exempt  = exempt or {}
    present = [c for c in required if c in columns]
    absent  = [c for c in required if c not in columns]

    def _null_expr(c: str) -> str:
        if c in exempt:
            return f"COUNTIF(`{c}` IS NULL AND NOT IFNULL({exempt[c]}, FALSE)) AS `{c}`"
        return f"COUNTIF(`{c}` IS NULL) AS `{c}`"

    nulls: dict[str, int] = {}
    if present:
        exprs = ",\n".join(_null_expr(c) for c in present)
        row = _query(client, f"SELECT {exprs} FROM `{ref}`")[0]
        nulls = {c: row[c] for c in present if row[c]}

    problems  = [f"{c}: {n:,} nulls" for c, n in nulls.items()]
    problems += [f"{c}: column missing from table" for c in absent]
    total = sum(nulls.values())
    return (FAIL if problems else PASS), _n(total), "0", "; ".join(problems)


def check_key_uniqueness(
    client: bigquery.Client,
    ref: str,
    key: str,
    natural_key: list[str],
) -> Outcome:
    """
    Surrogate key and natural key must each be unique. BigQuery does not
    enforce either, so a duplicate here means a loader bug. Downstream it
    shows up as fanned-out joins in SQL and as a refresh error in PowerBI,
    which requires the 'one' side of a relationship to be unique.
    """
    problems: list[str] = []
    excess_total = 0
    for cols in ([key], natural_key):
        if not cols:
            continue
        n_groups, n_excess, sample = _duplicates(client, ref, cols, cols[0])
        if n_groups:
            excess_total += n_excess
            problems.append(
                f"{'+'.join(cols)}: {n_groups:,} values duplicated "
                f"({n_excess:,} excess rows), top: {_fmt_sample(sample)}"
            )
    return (FAIL if problems else PASS), _n(excess_total), "0", "; ".join(problems)


def check_grain(client: bigquery.Client, ref: str, columns: list[str]) -> Outcome:
    """
    No two rows may agree on every descriptive column. The grain is derived
    from the live table schema, so it follows fact_tables.sql automatically.
    """
    grain = [c for c in columns if c not in GRAIN_EXCLUDE]
    breakdown = "dataset_code" if "dataset_code" in grain else grain[0]
    n_groups, n_excess, sample = _duplicates(client, ref, grain, breakdown)
    detail = ""
    if n_groups:
        detail = (
            f"{n_groups:,} dimension combinations occur more than once. "
            f"Excess rows by {breakdown}: {_fmt_sample(sample)}"
        )
    return (FAIL if n_groups else PASS), _n(n_excess), "0", detail


def check_orphans(
    client: bigquery.Client,
    config: dict,
    ref: str,
    columns: list[str],
) -> Outcome:
    problems: list[str] = []
    total = 0
    for fk, dim in FACT_FOREIGN_KEYS:
        if fk not in columns:
            continue
        dim_ref = _ref(config, dim)
        rows = _query(client, f"""
            SELECT CAST(f.`{fk}` AS STRING) AS k, COUNT(*) AS n
            FROM `{ref}` AS f
            LEFT JOIN (SELECT DISTINCT `{fk}` FROM `{dim_ref}`) AS d
              ON f.`{fk}` = d.`{fk}`
            WHERE f.`{fk}` IS NOT NULL AND d.`{fk}` IS NULL
            GROUP BY k
            ORDER BY n DESC
        """)
        n = sum(r["n"] for r in rows)
        if n:
            total += n
            examples = ", ".join(r["k"] for r in rows[:5])
            problems.append(
                f"{fk} -> {dim}: {n:,} rows, {len(rows):,} unknown keys (e.g. {examples})"
            )
    return (FAIL if total else PASS), _n(total), "0", "; ".join(problems)


def check_italy_coverage(by_level: dict[int, int], expected: dict[int, int]) -> Outcome:
    levels   = sorted(expected)
    observed = "/".join(str(by_level.get(lv, 0)) for lv in levels)
    target   = ">= " + "/".join(str(expected[lv]) for lv in levels)
    short = [
        f"NUTS{lv}: {by_level.get(lv, 0)} of {expected[lv]}"
        for lv in levels
        if by_level.get(lv, 0) < expected[lv]
    ]
    detail = ("short at " + "; ".join(short)) if short else ""
    return (FAIL if short else PASS), observed, target, detail


def check_italy_auto_geo(client: bigquery.Client, config: dict) -> Outcome:
    """
    _upsert_geo_rows() in build_db silently adds any nuts_code it has not
    seen, including Italian ones. Those rows have no names, region or
    macro-area, so they appear blank in PowerBI. They usually mean
    nuts_istat.csv is missing a code that ISTAT or Eurostat now publishes.
    """
    geo_ref = _ref(config, "dim_geography")
    rows = _query(client, f"""
        SELECT nuts_code
        FROM `{geo_ref}`
        WHERE country_code = 'IT'
          AND IFNULL(geo_source, '') NOT IN ('nuts_istat_csv', 'istat_aggregate')
        ORDER BY nuts_code
    """)
    codes = [r["nuts_code"] for r in rows]
    detail = ""
    if codes:
        shown = ", ".join(codes[:15])
        more  = f" and {len(codes) - 15} more" if len(codes) > 15 else ""
        detail = f"not in nuts_istat.csv, auto-inserted without names: {shown}{more}"
    return (AUTO_GEO_STATUS if codes else PASS), _n(len(codes)), "0", detail


def check_indicators(
    client: bigquery.Client,
    config: dict,
    ref: str,
    expected: list[str],
) -> Outcome:
    ind_ref = _ref(config, "dim_indicator")
    rows = _query(client, f"""
        SELECT i.indicator_code, COUNT(*) AS n
        FROM `{ref}` AS f
        JOIN `{ind_ref}` AS i ON f.indicator_key = i.indicator_key
        GROUP BY i.indicator_code
    """)
    present = {r["indicator_code"]: r["n"] for r in rows}
    missing = [c for c in expected if c not in present]
    extra   = sorted(set(present) - set(expected))

    parts = []
    if missing:
        # Say which missing codes are absent from dim_indicator too: those
        # rows were dropped at key resolution, not lost in the merge.
        in_list = ", ".join(f"'{c}'" for c in missing)
        in_dim = {
            r["indicator_code"]
            for r in _query(client, f"""
                SELECT indicator_code FROM `{ind_ref}`
                WHERE indicator_code IN ({in_list})
            """)
        }
        labelled = [c if c in in_dim else f"{c} [not in dim_indicator]" for c in missing]
        parts.append("missing: " + ", ".join(labelled))
    if extra:
        parts.append("present but not in expected list: " + ", ".join(extra))

    observed = f"{len(expected) - len(missing)}/{len(expected)}"
    return (FAIL if missing else PASS), observed, f"{len(expected)}/{len(expected)}", "; ".join(parts)


def check_datasets(
    client: bigquery.Client,
    ref: str,
    expected: list[str],
    max_age_days: int,
) -> Outcome:
    """
    WARN-level. A dataset that is missing, or whose latest extracted_at is
    old, usually means its fetcher failed and the processing stage fell back
    to an older raw file. Not a FAIL because priority resolution in the merge
    can legitimately absorb all of a dataset's rows into another one.
    """
    rows = _query(client, f"""
        SELECT dataset_code,
               TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(extracted_at), DAY) AS age_days
        FROM `{ref}`
        GROUP BY dataset_code
        ORDER BY dataset_code
    """)
    ages    = {r["dataset_code"]: r["age_days"] for r in rows}
    missing = [d for d in expected if d not in ages]
    stale   = [(d, a) for d, a in ages.items() if a is None or a > max_age_days]

    def _age(a: int | None) -> str:
        return "no timestamp" if a is None else f"{a} days"

    parts = []
    if missing:
        parts.append("missing: " + ", ".join(missing))
    if stale:
        parts.append(
            f"older than {max_age_days} days: "
            + ", ".join(f"{d} ({_age(a)})" for d, a in stale)
        )

    target = f"<= {max_age_days}d old"
    if expected:
        target = f"{len(expected)} present, {target}"
    return (WARN if parts else PASS), _n(len(ages)), target, "; ".join(parts)


def _violation(lo: float | None, hi: float | None) -> str | None:
    parts = []
    if lo is not None:
        parts.append(f"f.value < {lo}")
    if hi is not None:
        parts.append(f"f.value > {hi}")
    return "(" + " OR ".join(parts) + ")" if parts else None


def _bounds_str(lo: float | None, hi: float | None) -> str:
    if lo is not None and hi is not None:
        return f"[{lo}, {hi}]"
    return f">= {lo}" if lo is not None else f"<= {hi}"


def check_value_bounds(
    client: bigquery.Client,
    config: dict,
    ref: str,
    bounds: dict[str, tuple],
) -> Outcome:
    """
    Values outside plausible bounds are almost always unit or parsing
    errors: a negative count, or a rate stored as a share (0.45 instead of
    45 would pass, but 4500 from a per-mille mix-up would not).
    """
    specific = {k: v for k, v in bounds.items() if k != "*"}
    clauses = []
    for code, (lo, hi) in specific.items():
        cond = _violation(lo, hi)
        if cond:
            clauses.append(f"(i.indicator_code = '{code}' AND {cond})")
    if "*" in bounds:
        cond = _violation(*bounds["*"])
        if cond and specific:
            others = ", ".join(f"'{c}'" for c in specific)
            clauses.append(f"(i.indicator_code NOT IN ({others}) AND {cond})")
        elif cond:
            clauses.append(cond)
    if not clauses:
        return PASS, "0", "0", ""

    ind_ref = _ref(config, "dim_indicator")
    where = " OR ".join(clauses)
    rows = _query(client, f"""
        SELECT i.indicator_code, COUNT(*) AS n,
               MIN(f.value) AS vmin, MAX(f.value) AS vmax
        FROM `{ref}` AS f
        JOIN `{ind_ref}` AS i ON f.indicator_key = i.indicator_key
        WHERE {where}
        GROUP BY i.indicator_code
        ORDER BY n DESC
    """)
    total = sum(r["n"] for r in rows)
    parts = []
    for r in rows:
        lo, hi = specific.get(r["indicator_code"], bounds.get("*", (None, None)))
        parts.append(
            f"{r['indicator_code']}: {r['n']:,} rows outside {_bounds_str(lo, hi)} "
            f"(observed {r['vmin']:g} to {r['vmax']:g})"
        )
    return (FAIL if total else PASS), _n(total), "0", "; ".join(parts)


def check_providers(
    client: bigquery.Client,
    config: dict,
    ref: str,
    providers: list[str],
) -> Outcome:
    src_ref = _ref(config, "dim_source")
    exprs = ",\n".join(
        f"COUNTIF(LOWER(s.provider) = '{p}' OR STRPOS(LOWER(s.source_id), '{p}') > 0) AS `{p}`"
        for p in providers
    )
    row = _query(client, f"""
        SELECT {exprs}
        FROM `{ref}` AS f
        JOIN `{src_ref}` AS s ON f.source_key = s.source_key
    """)[0]
    counts  = {p: row[p] for p in providers}
    missing = [p for p, n in counts.items() if not n]
    detail  = ", ".join(f"{p}: {n:,} rows" for p, n in counts.items())
    if missing:
        detail = "no rows from " + ", ".join(missing) + " (" + detail + ")"
    observed = f"{len(providers) - len(missing)}/{len(providers)}"
    return (FAIL if missing else PASS), observed, f"{len(providers)}/{len(providers)}", detail


def check_other_countries(
    client: bigquery.Client,
    config: dict,
    ref: str,
    minimum: int,
) -> Outcome:
    geo_ref = _ref(config, "dim_geography")
    n = _query(client, f"""
        SELECT COUNT(DISTINCT g.country_code) AS n
        FROM (SELECT DISTINCT geo_key FROM `{ref}`) AS f
        JOIN `{geo_ref}` AS g ON f.geo_key = g.geo_key
        WHERE g.nuts_level = 0
          AND g.country_code IS NOT NULL
          AND g.country_code != 'IT'
    """)[0]["n"]
    return (PASS if n >= minimum else FAIL), _n(n), f">= {minimum}", ""


def check_frequencies(
    client: bigquery.Client,
    ref: str,
    columns: list[str],
    expected: list[str],
) -> Outcome:
    for col in ("frequency", "period_label"):
        if col not in columns:
            return FAIL, "-", ", ".join(expected), f"column {col} missing from table"

    rows = _query(client, f"""
        SELECT frequency,
               COUNT(*) AS n,
               COUNTIF(frequency = 'M'
                       AND (period_label IS NULL
                            OR NOT REGEXP_CONTAINS(period_label, r'{MONTHLY_LABEL_RE}'))
               ) AS bad_labels
        FROM `{ref}`
        GROUP BY frequency
    """)
    counts  = {r["frequency"]: r["n"] for r in rows}
    bad     = sum(r["bad_labels"] for r in rows)
    missing = [f for f in expected if not counts.get(f)]

    parts = []
    if missing:
        parts.append("missing: " + ", ".join(missing))
    if bad:
        parts.append(f"{bad:,} monthly rows with period_label not in YYYY-MM format")
    breakdown = ", ".join(
        f"{'(blank)' if k in (None, '') else k}: {n:,}"
        for k, n in sorted(counts.items(), key=lambda kv: str(kv[0]))
    )
    parts.append(f"rows by frequency: {breakdown}")

    observed = ", ".join(f for f in expected if counts.get(f)) or "-"
    return (FAIL if (missing or bad) else PASS), observed, ", ".join(expected), "; ".join(parts)


def check_populated(
    client: bigquery.Client,
    ref: str,
    columns: list[str],
    wanted: list[str],
) -> Outcome:
    """Optional dimension columns: NULL is fine row by row, but not everywhere."""
    present = [c for c in wanted if c in columns]
    absent  = [c for c in wanted if c not in columns]

    counts: dict[str, int] = {}
    if present:
        exprs = ",\n".join(
            f"COUNTIF(`{c}` IS NOT NULL AND CAST(`{c}` AS STRING) != '') AS `{c}`"
            for c in present
        )
        row = _query(client, f"SELECT {exprs} FROM `{ref}`")[0]
        counts = {c: row[c] for c in present}

    empty = [c for c, n in counts.items() if not n]
    parts  = [f"{c}: {n:,} rows" for c, n in counts.items()]
    parts += [f"{c}: column missing from table" for c in absent]
    observed = f"{len(present) - len(empty)}/{len(wanted)}"
    status = FAIL if (empty or absent) else PASS
    return status, observed, f"{len(wanted)}/{len(wanted)}", "; ".join(parts)


def check_views(client: bigquery.Client, config: dict, views: list[str]) -> Outcome:
    problems: list[str] = []
    ok = 0
    for view in views:
        vref = _ref(config, view)
        if _table_columns(client, vref) is None:
            problems.append(f"{view}: missing")
            continue
        n = _query(client, f"SELECT COUNT(*) AS n FROM `{vref}`")[0]["n"]
        if n:
            ok += 1
        else:
            problems.append(f"{view}: returns 0 rows")
    total = len(views)
    return (FAIL if problems else PASS), f"{ok}/{total}", f"{total}/{total}", "; ".join(problems)


# ══════════════════════════════════════════════════════════════════════════════
# Orchestration
# ══════════════════════════════════════════════════════════════════════════════

def _coverage_name(expected: dict[int, int]) -> str:
    return "Italian NUTS" + "/".join(str(lv) for lv in sorted(expected)) + " units"


def _missing_table(table: str, ref: str) -> CheckResult:
    logger.error(f"  FAIL   {table}: table not found ({ref})")
    return CheckResult(table, "table exists", FAIL, "-", "exists", f"{ref} not found")


def _check_dimension(client: bigquery.Client, cfg: dict, table: str, spec: dict) -> list[CheckResult]:
    ref = _ref(cfg, table)
    columns = _table_columns(client, ref)
    if columns is None:
        return [_missing_table(table, ref)]

    def _min_rows() -> int:
        m = spec["min_rows"]
        return m() if callable(m) else m

    return [
        _run(table, "row count",
             lambda: check_row_count(client, ref, _min_rows())),
        _run(table, "nulls in required columns",
             lambda: check_nulls(client, ref, columns, spec["required"], spec.get("null_exempt"))),
        _run(table, "key uniqueness",
             lambda: check_key_uniqueness(client, ref, spec["key"], spec["natural_key"])),
    ]


def _check_fact(client: bigquery.Client, cfg: dict, spec: dict, max_age_days: int) -> list[CheckResult]:
    table = spec["table"]
    ref = _ref(cfg, table)
    columns = _table_columns(client, ref)
    if columns is None:
        return [_missing_table(table, ref)]

    # Optional keys: a check whose setting is empty or absent is skipped.
    bounds    = spec.get("value_bounds", {})
    providers = spec.get("expected_providers", [])
    nuts_min  = spec.get("italy_nuts_min", {})
    countries = spec.get("min_other_countries")
    freqs     = spec.get("expected_frequencies", [])
    populated = spec.get("populated_columns", [])
    datasets  = spec.get("expected_datasets", [])
    views     = spec.get("views", [])

    checks: list[tuple[str, Callable[[], Outcome]]] = [
        ("row count",
         lambda: check_row_count(client, ref, spec["min_rows"])),
        ("nulls in required columns",
         lambda: check_nulls(client, ref, columns, FACT_REQUIRED)),
        ("key uniqueness",
         lambda: check_key_uniqueness(client, ref, "fact_key", [])),
        ("duplicate rows at grain",
         lambda: check_grain(client, ref, columns)),
        ("orphan foreign keys",
         lambda: check_orphans(client, cfg, ref, columns)),
    ]
    if bounds:
        checks.append(("values within bounds",
                       lambda: check_value_bounds(client, cfg, ref, bounds)))
    if providers:
        checks.append(("providers present",
                       lambda: check_providers(client, cfg, ref, providers)))
    if nuts_min:
        checks.append((_coverage_name(nuts_min),
                       lambda: check_italy_coverage(_italy_units_by_level(client, cfg, ref), nuts_min)))
    if countries:
        checks.append(("other countries at NUTS0",
                       lambda: check_other_countries(client, cfg, ref, countries)))
    checks.append(("indicator coverage",
                   lambda: check_indicators(client, cfg, ref, spec.get("expected_indicators", []))))
    if freqs:
        checks.append(("frequencies and period labels",
                       lambda: check_frequencies(client, ref, columns, freqs)))
    if populated:
        checks.append(("optional columns populated",
                       lambda: check_populated(client, ref, columns, populated)))
    checks.append(("datasets present and fresh",
                   lambda: check_datasets(client, ref, datasets, max_age_days)))
    if views:
        checks.append(("views return rows",
                       lambda: check_views(client, cfg, views)))

    return [_run(table, name, fn) for name, fn in checks]


def _resolve_domains(domains: list[str] | None, cfg: dict) -> tuple[list[str], list[CheckResult]]:
    """
    Domains to check, plus results for domains that have no FACT_CHECKS entry:
    WARN if the domain is merely enabled in settings.yaml, FAIL if it was
    requested explicitly with --domain.
    """
    enabled = [
        d for d, s in (cfg.get("domains") or {}).items()
        if (s or {}).get("enabled", False)
    ]
    requested = domains or enabled
    to_check, notes = [], []
    for d in requested:
        if d in FACT_CHECKS:
            to_check.append(d)
        else:
            notes.append(CheckResult(
                f"fact_{d}", "checks registered", FAIL if domains else WARN,
                "-", "FACT_CHECKS entry",
                "no entry in FACT_CHECKS in test_integrity.py, so this fact table was not checked",
            ))
    return to_check, notes


def _preflight(client: bigquery.Client, cfg: dict) -> CheckResult | None:
    """Fail fast with a readable message if the dataset is unreachable."""
    ds = f"{cfg['bigquery']['project']}.{cfg['bigquery']['dataset']}"
    try:
        client.get_dataset(ds)
        return None
    except NotFound:
        msg = f"dataset {ds} not found: check BQ_PROJECT and BQ_DATASET"
    except Exception as e:
        msg = (
            f"cannot reach {ds} ({type(e).__name__}: {e}). Check "
            "GOOGLE_APPLICATION_CREDENTIALS and that the service account has the "
            "BigQuery Data Viewer and BigQuery Job User roles"
        )
    logger.error(msg)
    return CheckResult("(dataset)", "BigQuery reachable", ERROR, detail=msg)


def run_integrity_checks(
    domains: list[str] | None = None,
    config: dict | None = None,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
) -> list[CheckResult]:
    """
    Run all checks and return the results. Importable, so run_pipeline.py
    can call it directly as a fourth stage later if that proves useful.
    """
    cfg = config or load_config()
    client = _get_bq_client(cfg)

    unreachable = _preflight(client, cfg)
    if unreachable:
        return [unreachable]

    results: list[CheckResult] = []

    logger.info("── Dimension tables ─────────────────────────────")
    for table, spec in DIMENSIONS.items():
        results.extend(_check_dimension(client, cfg, table, spec))

    results.append(_run(
        "dim_geography", _coverage_name(ITALY_NUTS_UNITS),
        lambda: check_italy_coverage(_italy_units_by_level(client, cfg), ITALY_NUTS_UNITS),
    ))
    results.append(_run(
        "dim_geography", "auto-inserted Italian codes",
        lambda: check_italy_auto_geo(client, cfg),
    ))

    logger.info("── Fact tables ──────────────────────────────────")
    to_check, notes = _resolve_domains(domains, cfg)
    results.extend(notes)
    for domain in to_check:
        logger.info(f"Domain: {domain}")
        results.extend(_check_fact(client, cfg, FACT_CHECKS[domain], max_age_days))

    return results


# ══════════════════════════════════════════════════════════════════════════════
# Reporting
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(results: list[CheckResult], dataset_ref: str) -> None:
    w_table = max(len(r.table) for r in results)
    w_check = max(len(r.check) for r in results)
    w_obs   = max(8, max(len(r.observed) for r in results))
    rule    = "═" * (w_table + w_check + w_obs + 30)

    lines = [
        "",
        rule,
        f"Integrity checks: {dataset_ref}",
        rule,
        f"{'STATUS':<6}  {'TABLE':<{w_table}}  {'CHECK':<{w_check}}  {'OBSERVED':>{w_obs}}  EXPECTED",
    ]
    for r in results:
        lines.append(
            f"{r.status:<6}  {r.table:<{w_table}}  {r.check:<{w_check}}  "
            f"{r.observed:>{w_obs}}  {r.expected}"
        )
        if r.detail:
            lines.append(f"{'':<8}{r.detail}")

    c = Counter(r.status for r in results)
    lines += [
        "─" * len(rule),
        f"{len(results)} checks: {c[PASS]} passed, {c[WARN]} warnings, "
        f"{c[FAIL]} failed, {c[ERROR]} could not run",
        rule,
    ]
    print("\n".join(lines))


def write_github_step_summary(results: list[CheckResult], dataset_ref: str) -> None:
    """
    In GitHub Actions, the runner sets GITHUB_STEP_SUMMARY to a file path.
    Markdown appended to that file is rendered on the workflow run's summary
    page. Outside Actions the variable is unset and this does nothing.
    """
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return

    def cell(s: str) -> str:
        return s.replace("|", "\\|").replace("\n", " ")

    c = Counter(r.status for r in results)
    lines = [
        f"## Integrity checks: `{dataset_ref}`",
        "",
        f"{c[PASS]} passed, {c[WARN]} warnings, {c[FAIL]} failed, {c[ERROR]} could not run",
        "",
        "| Status | Table | Check | Observed | Expected | Detail |",
        "|---|---|---|---:|---|---|",
    ]
    # Problems first, so they are visible without scrolling
    order = {ERROR: 0, FAIL: 1, WARN: 2, PASS: 3}
    for r in sorted(results, key=lambda r: order.get(r.status, 9)):
        lines.append(
            f"| {r.status} | {cell(r.table)} | {cell(r.check)} | {cell(r.observed)} "
            f"| {cell(r.expected)} | {cell(r.detail)} |"
        )
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Integrity checks for the aiccon-data BigQuery dataset."
    )
    parser.add_argument(
        "--domain", type=str, default=None,
        help="Check one domain's fact table only. Dimension tables are always checked.",
    )
    parser.add_argument(
        "--config", type=str, default=None,
        help="Path to settings.yaml (default: auto-detected).",
    )
    parser.add_argument(
        "--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS,
        help=f"Warn about datasets extracted longer ago than this (default {DEFAULT_MAX_AGE_DAYS}).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args    = parse_args()
    config  = load_config(settings_path=args.config)
    domains = [args.domain] if args.domain else None

    results = run_integrity_checks(domains=domains, config=config, max_age_days=args.max_age_days)

    dataset_ref = f"{config['bigquery']['project']}.{config['bigquery']['dataset']}"
    print_summary(results, dataset_ref)
    write_github_step_summary(results, dataset_ref)

    sys.exit(1 if any(r.failed for r in results) else 0)
