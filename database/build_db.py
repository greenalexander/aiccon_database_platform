"""
database/build_db.py

Builds the aiccon.duckdb file from processed parquet files on SharePoint.

What this script does, in order:
    1. Checks SharePoint is reachable
    2. Creates a fresh DuckDB database in a temp location
    3. Executes dimensions.sql  — creates shared dimension tables
    4. Executes fact_tables.sql — creates domain fact tables
    5. Populates dimension tables from mapping CSVs and processed parquet
    6. Loads each active domain's processed parquet into its fact table
    7. Executes views.sql       — creates analytical views
    8. Runs integrity checks    — fails loudly if something is wrong
    9. Copies the finished .duckdb to SharePoint database/ folder
   10. Writes pipeline log

Adding a new domain:
    1. Add the domain name to ACTIVE_DOMAINS below
    2. Add a loader function following the pattern of _load_social_economy()
    3. Register it in DOMAIN_LOADERS
    SQL schema changes go in fact_tables.sql and views.sql, not here.

Run:
    python -m database.build_db
    python -m database.build_db --domain social_economy   # single domain only
    python -m database.build_db --domain labour           # single domain only
"""

from __future__ import annotations

import argparse
import sys
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.loaders.base_loader import get_logger, load_config, processed_path
from ingestion.loaders.sharepoint_loader import (
    check_sharepoint_available,
    upload_database,
    write_pipeline_log,
)

logger = get_logger("database.build_db")

# ── Paths ─────────────────────────────────────────────────────────────────────

HERE = Path(__file__).resolve().parent
SCHEMA_DIR = HERE / "schema"

DIMENSIONS_SQL  = SCHEMA_DIR / "dimensions.sql"
FACT_TABLES_SQL = SCHEMA_DIR / "fact_tables.sql"
VIEWS_SQL       = SCHEMA_DIR / "views.sql"

# ── Domain registry ───────────────────────────────────────────────────────────
# Maps domain name → loader function.
# Add new domains here when ready — nothing else in this file needs changing.
# Loader function signature: (con: duckdb.DuckDBPyConnection, config: dict) -> int
# Return value: number of rows loaded into the fact table.
#
# Registered domains:
#   social_economy  ← active
#   labour          ← active

DOMAIN_LOADERS: dict[str, callable] = {}   # populated below via decorator


def domain_loader(name: str):
    """Decorator to register a domain loader function."""
    def decorator(fn):
        DOMAIN_LOADERS[name] = fn
        return fn
    return decorator


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_mappings_dir() -> Path:
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        candidate = parent / "processing" / "mappings"
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Could not find processing/mappings/ directory.")


def _find_latest_processed(processed_dir: Path, domain: str) -> Path | None:
    """
    Find the most recently written processed parquet for a domain.
    Files are named {domain}_{random_suffix}.parquet (via tempfile).
    Sorts by file modification time — do NOT sort alphabetically since
    the random suffix has no chronological meaning.
    """
    matches = sorted(
        processed_dir.glob(f"{domain}_*.parquet"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not matches:
        logger.warning(f"No processed parquet found for domain '{domain}' in {processed_dir}")
        return None
    if len(matches) > 1:
        logger.info(
            f"Multiple processed files for '{domain}' — "
            f"using most recent by mtime: {matches[0].name} "
            f"(others: {[m.name for m in matches[1:]]})"
        )
    return matches[0]


def _execute_sql_file(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Execute a SQL file, logging each statement's outcome."""
    logger.info(f"Executing {path.name}")
    sql = path.read_text(encoding="utf-8")
    # Use .execute() instead of .executescript()
    con.execute(sql)
    logger.info(f"  {path.name} executed successfully")


def _next_surrogate_key(con: duckdb.DuckDBPyConnection, table: str, key_col: str) -> int:
    """Return the next available surrogate key for a table."""
    result = con.execute(f"SELECT COALESCE(MAX({key_col}), 0) + 1 FROM {table}").fetchone()
    return result[0]


# ── Dimension population ──────────────────────────────────────────────────────

def populate_dim_geography(con: duckdb.DuckDBPyConnection, mappings_dir: Path) -> int:
    """
    Populate dim_geography from nuts_istat.csv.

    For non-Italian countries (from Eurostat), geography rows are added
    dynamically when fact tables are loaded — see _upsert_geo_from_parquet().
    This function handles the Italian territorial hierarchy.
    """
    path = mappings_dir / "nuts_istat.csv"
    df = pd.read_csv(path, dtype=str)
    df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)

    # Map CSV columns to dim_geography columns
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "nuts_code":       r.get("nuts_code")    or None,
            "nuts_level":      int(r["nuts_level"])  if pd.notna(r.get("nuts_level")) else None,
            "nuts_name_it":    r.get("nuts_name_it") or None,
            "nuts_name_en":    r.get("nuts_name_en") or None,
            "istat_code":      r.get("istat_code")   or None,
            "country_code":    "IT",
            "region_name":     r.get("region_name")  or None,
            "macro_area":      r.get("macro_area")   or None,
            "municipality_type": None,
            "geo_source":      "nuts_istat_csv",
            "is_active":       True,
        })

    geo_df = pd.DataFrame(rows)

    con.execute("""
        INSERT OR IGNORE INTO dim_geography
            (nuts_code, nuts_level, nuts_name_it, nuts_name_en, istat_code,
             country_code, region_name, macro_area, municipality_type,
             geo_source, is_active)
        SELECT
            nuts_code, nuts_level, nuts_name_it, nuts_name_en, istat_code,
            country_code, region_name, macro_area, municipality_type,
            geo_source, is_active
        FROM geo_df
    """)

    n = con.execute("SELECT COUNT(*) FROM dim_geography").fetchone()[0]
    logger.info(f"  dim_geography: {n} rows")
    return n


def populate_dim_time(con: duckdb.DuckDBPyConnection, year_range: tuple[int, int] = (1990, 2030)) -> int:
    """
    Populate dim_time with annual rows covering the full year range.
    Extend the range if your sources go further back or forward.
    """
    rows = []
    for year in range(year_range[0], year_range[1] + 1):
        rows.append({
            "year":             year,
            "period_type":      "A",
            "period_label":     str(year),
            "reference_period": str(year),
        })

    time_df = pd.DataFrame(rows)
    con.execute("""
        INSERT OR IGNORE INTO dim_time (year, period_type, period_label, reference_period)
        SELECT year, period_type, period_label, reference_period
        FROM time_df
    """)

    n = con.execute("SELECT COUNT(*) FROM dim_time").fetchone()[0]
    logger.info(f"  dim_time: {n} rows ({year_range[0]}–{year_range[1]})")
    return n


def populate_dim_source(con: duckdb.DuckDBPyConnection, mappings_dir: Path) -> int:
    """Populate dim_source from domain_sources.csv."""
    path = mappings_dir / "domain_sources.csv"
    df = pd.read_csv(path, dtype=str)
    df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)
    df["priority"] = pd.to_numeric(df["priority"], errors="coerce")

    # Derive provider from source_id prefix if not already in CSV
    if "provider" not in df.columns:
        df["provider"] = df["source_id"].str.split("_").str[0]

    con.execute("""
        INSERT OR IGNORE INTO dim_source
            (source_id, provider, source_name, source_name_it, domain, access_type,
             priority, update_frequency, territorial_levels, temporal_coverage, notes)
        SELECT
            source_id, provider, source_name, source_name_it, domain, access_type,
            priority, update_frequency, territorial_levels, temporal_coverage, notes
        FROM df
    """)

    n = con.execute("SELECT COUNT(*) FROM dim_source").fetchone()[0]
    logger.info(f"  dim_source: {n} rows")
    return n


def populate_dim_legal_form(con: duckdb.DuckDBPyConnection, mappings_dir: Path) -> int:
    """Populate dim_legal_form from legal_form_map.csv."""
    path = mappings_dir / "legal_form_map.csv"
    df = pd.read_csv(path, dtype=str)
    df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)

    con.execute("""
        INSERT OR IGNORE INTO dim_legal_form
            (unified_category, unified_category_en, nace_primary,
             ets_classification, source_system, source_code, source_label_it)
        SELECT
            unified_category, unified_category_en, nace_primary,
            ets_classification, source_system, source_code, source_label_it
        FROM df
    """)

    n = con.execute("SELECT COUNT(*) FROM dim_legal_form").fetchone()[0]
    logger.info(f"  dim_legal_form: {n} rows")
    return n


def populate_dimensions(con: duckdb.DuckDBPyConnection, config: dict) -> None:
    """Populate all shared dimension tables. Called once before any domain loads."""
    mappings_dir = _find_mappings_dir()
    logger.info("Populating dimension tables")
    populate_dim_geography(con, mappings_dir)
    populate_dim_time(con)
    populate_dim_source(con, mappings_dir)
    populate_dim_legal_form(con, mappings_dir)
    # dim_indicator is pre-populated by the INSERT OR IGNORE block in dimensions.sql,
    # which covers social_economy, labour, and placeholder rows for future domains.
    n = con.execute("SELECT COUNT(*) FROM dim_indicator").fetchone()[0]
    logger.info(f"  dim_indicator: {n} rows (from dimensions.sql)")


# ── Dimension key lookups ─────────────────────────────────────────────────────

def _build_geo_lookup(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Return a dict mapping nuts_code → geo_key for all rows in dim_geography."""
    rows = con.execute("SELECT nuts_code, geo_key FROM dim_geography WHERE nuts_code IS NOT NULL").fetchall()
    return {r[0]: r[1] for r in rows}


def _build_time_lookup(con: duckdb.DuckDBPyConnection) -> dict[int, int]:
    """Return a dict mapping year (int) → time_key."""
    rows = con.execute("SELECT year, time_key FROM dim_time").fetchall()
    return {r[0]: r[1] for r in rows}


def _build_source_lookup(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Return a dict mapping source_id → source_key."""
    rows = con.execute("SELECT source_id, source_key FROM dim_source").fetchall()
    return {r[0]: r[1] for r in rows}


def _build_indicator_lookup(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Return a dict mapping indicator_code → indicator_key."""
    rows = con.execute("SELECT indicator_code, indicator_key FROM dim_indicator").fetchall()
    return {r[0]: r[1] for r in rows}


def _build_legal_form_lookup(con: duckdb.DuckDBPyConnection) -> dict[tuple, int]:
    """Return a dict mapping (unified_category, source_system) → legal_form_key."""
    rows = con.execute(
        "SELECT unified_category, source_system, legal_form_key FROM dim_legal_form"
    ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def _upsert_geo_rows(
    con: duckdb.DuckDBPyConnection,
    nuts_codes: list[str],
    geo_lookup: dict[str, int],
) -> dict[str, int]:
    """
    Insert any nuts_codes not yet in dim_geography (e.g. non-Italian EU countries
    from Eurostat) and return the updated lookup dict.
    """
    new_codes = [c for c in nuts_codes if c and c not in geo_lookup]
    if not new_codes:
        return geo_lookup

    new_rows = []
    for code in set(new_codes):
        if not code or len(code) < 2:
            continue
        new_rows.append({
            "nuts_code":    code,
            "nuts_level":   len(code) - 2 if len(code) <= 5 else None,
            "nuts_name_it": None,
            "nuts_name_en": None,
            "istat_code":   None,
            "country_code": code[:2],
            "region_name":  None,
            "macro_area":   None,
            "municipality_type": None,
            "geo_source":   "eurostat_auto",
            "is_active":    True,
        })

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        con.execute("""
            INSERT OR IGNORE INTO dim_geography
                (nuts_code, nuts_level, nuts_name_it, nuts_name_en, istat_code,
                 country_code, region_name, macro_area, municipality_type,
                 geo_source, is_active)
            SELECT
                nuts_code, nuts_level, nuts_name_it, nuts_name_en, istat_code,
                country_code, region_name, macro_area, municipality_type,
                geo_source, is_active
            FROM new_df
        """)
        logger.info(f"  Auto-inserted {len(new_rows)} new geo rows from Eurostat data")
        # Rebuild lookup
        return _build_geo_lookup(con)

    return geo_lookup

def _resolve_keys(df: pd.DataFrame, con: duckdb.DuckDBPyConnection, domain: str, **kwargs) -> pd.DataFrame:
    """
    Replace natural key columns with surrogate integer keys.

    Rows where a required key cannot be resolved (unknown geo, time, source,
    or indicator) are dropped with a warning. legal_form_key is nullable —
    null values are expected and kept (most rows have no legal form).
    """
    if df.empty:
        return df

    # 1. GEOGRAPHY
    # The geo filter has already been applied in the domain loader before this
    # call, so here we just map whatever nuts_codes remain.
    geo_map = dict(con.execute("SELECT nuts_code, geo_key FROM dim_geography").fetchall())
    df["geo_key"] = df["nuts_code"].map(geo_map)
    missing_geo = df["geo_key"].isna().sum()
    if missing_geo:
        bad = df.loc[df["geo_key"].isna(), "nuts_code"].unique()[:10]
        logger.warning(f"  {missing_geo} rows dropped: nuts_code not in dim_geography: {list(bad)}")
    df = df.dropna(subset=["geo_key"])
    df["geo_key"] = df["geo_key"].astype(int)

    # 2. TIME
    # The parquet may store time as:
    #   - integer/float year: 2023 or 2023.0  (annual datasets)
    #   - 'YYYY-MM' string: '2015-01'          (FORZLVMENS1_1 monthly)
    #   - 'YYYY-QN' string: '2023-Q1'          (quarterly datasets)
    #   - pd.NA / NaN                           (should not occur but guard anyway)
    # dim_time is annual, so we always resolve to the year only.
    # Extract the first 4-digit sequence from any string representation.
    time_map = dict(con.execute("SELECT year, time_key FROM dim_time").fetchall())
    df["_year_int"] = (
        df["time"].astype(str)                         # '2023', '2023.0', '2015-01', '<NA>'
                  .str.extract(r"(\d{4})", expand=False)  # always picks up the year
                  .pipe(pd.to_numeric, errors="coerce")
                  .astype("Int64")
    )
    df["time_key"] = df["_year_int"].map(time_map)
    missing_time = df["time_key"].isna().sum()
    if missing_time:
        bad = df.loc[df["time_key"].isna(), "time"].unique()[:10]
        logger.warning(f"  {missing_time} rows dropped: year not in dim_time: {list(bad)}")
    df = df.dropna(subset=["time_key"])
    df["time_key"] = df["time_key"].astype(int)
    df = df.drop(columns=["_year_int"])

    # 3. SOURCE
    # The parquet has two relevant columns:
    #   source_id   — coarse label ('istat' or 'eurostat')
    #   dataset_code — granular dataset code (e.g. '85_84_DF_DCSA_VOLON1_1')
    # dim_source is keyed on source_id which holds values like
    # 'istat_85_84_DF_DCSA_VOLON1_1', i.e. "{source_id}_{dataset_code}".
    # Build the compound key to match dim_source.source_id.
    src_map = dict(con.execute("SELECT source_id, source_key FROM dim_source").fetchall())
    df["_source_compound"] = df["source_id"].str.strip() + "_" + df["dataset_code"].str.strip()
    df["source_key"] = df["_source_compound"].map(src_map)
    missing_src = df["source_key"].isna().sum()
    if missing_src:
        bad = df.loc[df["source_key"].isna(), "_source_compound"].unique()[:10]
        logger.warning(
            f"  {missing_src} rows dropped: compound source_id not in dim_source: {list(bad)}. "
            "Add missing entries to domain_sources.csv."
        )
    df = df.dropna(subset=["source_key"])
    df["source_key"] = df["source_key"].astype(int)
    df = df.drop(columns=["_source_compound"])

    # 4. INDICATOR
    ind_map = dict(con.execute("SELECT indicator_code, indicator_key FROM dim_indicator").fetchall())
    df["indicator_key"] = df["indicator_code"].map(ind_map)
    missing_ind = df["indicator_key"].isna().sum()
    if missing_ind:
        bad = df.loc[df["indicator_key"].isna(), "indicator_code"].unique()[:10]
        logger.warning(
            f"  {missing_ind} rows dropped: indicator_code not in dim_indicator: {list(bad)}. "
            "Add these to the INSERT OR IGNORE block in dimensions.sql."
        )
    df = df.dropna(subset=["indicator_key"])
    df["indicator_key"] = df["indicator_key"].astype(int)

    # 5. LEGAL FORM (nullable — null is expected for most rows; do NOT drop on null)
    if "legal_form_unified" in df.columns:
        lf_map = dict(con.execute("SELECT unified_category, legal_form_key FROM dim_legal_form").fetchall())
        df["legal_form_key"] = df["legal_form_unified"].map(lf_map)
    else:
        df["legal_form_key"] = None

    return df



# ── Domain loaders ────────────────────────────────────────────────────────────

@domain_loader("social_economy")
def _load_social_economy(con: duckdb.DuckDBPyConnection, config: dict) -> int:
    """
    Load the social economy processed parquet into fact_social_economy.

    Adding a new domain:
        Copy this function, replace 'social_economy' with new domain name,
        update the fact table name and the list of columns being selected,
        and register it with @domain_loader('your_domain_name').
    """
    proc_dir = processed_path(config, "social_economy")
    path = _find_latest_processed(proc_dir, "social_economy")
    if path is None:
        logger.warning("social_economy: no processed parquet found — skipping.")
        return 0

    logger.info(f"  Loading {path.name}")
    df = pd.read_parquet(path)
    logger.info(f"  Read {len(df):,} rows from processed parquet")

    # Drop QA-only columns before loading
    qa_cols = ["geo_unmatched", "legal_form_unmatched"]
    df = df.drop(columns=[c for c in qa_cols if c in df.columns])

    # ── Geo filter FIRST, before any upsert or lookup ──
    if "nuts_code" in df.columns:
        mask_it      = df["nuts_code"].str.startswith("IT", na=False)
        mask_country = df["nuts_code"].str.len() == 2
        before = len(df)
        df = df[mask_it | mask_country].copy()
        logger.info(f"  Filtered out {before - len(df)} granular non-IT rows.")

    # Auto-insert any Eurostat geo codes not yet in dim_geography
    geo_lookup = _build_geo_lookup(con)
    if "nuts_code" in df.columns:
        geo_lookup = _upsert_geo_rows(con, df["nuts_code"].dropna().unique().tolist(), geo_lookup)


    # Resolve surrogate keys
    df = _resolve_keys(
        df=df,
        con=con,
        domain="social_economy",
    )

    
    if df.empty:
        logger.warning("social_economy: all rows dropped during key resolution.")
        return 0

    # Assign surrogate fact_key
    start_key = _next_surrogate_key(con, "fact_social_economy", "fact_key")
    df["fact_key"] = range(start_key, start_key + len(df))

    # Select only columns that exist in the fact table
    fact_columns = [
        "fact_key", "geo_key", "time_key", "source_key", "indicator_key",
        "value", "unit",
        "legal_form_key", "ets_classification",
        "nace_code", "nace_label_en",
        "gender", "age_group",
        "volunteering_form", "activity_type", "years_active",
        "education", "labour_status", "household_size", "econ_resources",
        "municipality_type",
        "org_sector", "org_type", "motivation", "personal_impact", "multi_membership",
        "association_type",
        "dataset_code", "extracted_at",
    ]
    available = [c for c in fact_columns if c in df.columns]
    df = df[available]

    con.execute("INSERT INTO fact_social_economy SELECT * FROM df")
    n = con.execute("SELECT COUNT(*) FROM fact_social_economy").fetchone()[0]
    logger.info(f"  fact_social_economy: {n:,} rows total")
    return n


@domain_loader("labour")
def _load_labour(con: duckdb.DuckDBPyConnection, config: dict) -> int:
    """
    Load the labour processed parquet into fact_labour.

    Geography strategy mirrors the merge script:
      - All NUTS0 national totals (2-char codes) for every country.
      - Italian sub-national codes (starting 'IT') for NUTS2/NUTS3 detail.
      - All other sub-national non-Italian rows are dropped — they were already
        filtered in the Eurostat fetcher, but we guard here for safety.

    The monthly labour force series (FORZLVMENS1_1) uses frequency='M' and
    stores period_label as 'YYYY-MM'. time_key resolves to the annual row for
    the year — this is intentional (see fact_labour design notes).
    """
    proc_dir = processed_path(config, "labour")
    path = _find_latest_processed(proc_dir, "labour")
    if path is None:
        logger.warning("labour: no processed parquet found — skipping.")
        return 0

    logger.info(f"  Loading {path.name}")
    df = pd.read_parquet(path)
    logger.info(f"  Read {len(df):,} rows from processed parquet")

    # Drop QA-only columns
    qa_cols = ["geo_unmatched", "legal_form_unmatched"]
    df = df.drop(columns=[c for c in qa_cols if c in df.columns])

    # ── Geo filter ────────────────────────────────────────────────────────
    # Keep: all NUTS0 (len==2) + all Italian codes (starts with 'IT').
    # The Eurostat fetcher already applied filter_geo(), but we re-check here
    # in case the parquet was produced by an older version.
    if "nuts_code" in df.columns:
        mask = (
            df["nuts_code"].str.len().eq(2) |
            df["nuts_code"].str.startswith("IT", na=False)
        )
        before = len(df)
        df = df[mask].copy()
        dropped = before - len(df)
        if dropped:
            logger.info(f"  Geo filter: dropped {dropped:,} non-IT sub-national rows.")

    # ── Auto-insert unknown geo codes ─────────────────────────────────────
    # Covers EU country codes (DE, FR, ES…) not in the Italian NUTS CSV.
    geo_lookup = _build_geo_lookup(con)
    if "nuts_code" in df.columns:
        geo_lookup = _upsert_geo_rows(
            con, df["nuts_code"].dropna().unique().tolist(), geo_lookup
        )

    # ── Resolve surrogate keys ────────────────────────────────────────────
    # Pre-process the time column before key resolution:
    # Monthly rows store time as 'YYYY-MM' — extract the year so the lookup
    # against dim_time (which is annual) succeeds.
    if "time" in df.columns:
        df["time"] = (
            df["time"].astype(str)
                      .str.extract(r"^(\d{4})", expand=False)   # keep only the year part
        )

    df = _resolve_keys(df=df, con=con, domain="labour")
    if df.empty:
        logger.warning("labour: all rows dropped during key resolution.")
        return 0

    # ── Assign surrogate fact_key ─────────────────────────────────────────
    start_key = _next_surrogate_key(con, "fact_labour", "fact_key")
    df["fact_key"] = range(start_key, start_key + len(df))

    # ── Select and align fact table columns ───────────────────────────────
    # The full ordered list must match fact_labour's column definition exactly.
    # Columns absent from the parquet are added as None so the INSERT always
    # supplies all 19 columns — DuckDB's INSERT INTO requires an exact match
    # when using SELECT * rather than an explicit column list.
    fact_columns = [
        "fact_key", "geo_key", "time_key", "source_key", "indicator_key",
        "value", "unit",
        "frequency", "period_label",
        "gender", "age_group",
        "education", "citizenship", "adjustment", "unemployment_duration",
        "nace_code", "nace_label_en",
        "dataset_code", "extracted_at",
    ]
    for col in fact_columns:
        if col not in df.columns:
            df[col] = None
    df = df[fact_columns]

    con.execute("INSERT INTO fact_labour SELECT * FROM df")
    n = con.execute("SELECT COUNT(*) FROM fact_labour").fetchone()[0]
    logger.info(f"  fact_labour: {n:,} rows total")
    return n


# ── When adding a new domain, copy this template: ─────────────────────────────
#
# @domain_loader("immigration")
# def _load_immigration(con: duckdb.DuckDBPyConnection, config: dict) -> int:
#     proc_dir = processed_path(config, "immigration")
#     path = _find_latest_processed(proc_dir, "immigration")
#     if path is None:
#         logger.warning("immigration: no processed parquet found — skipping.")
#         return 0
#
#     logger.info(f"  Loading {path.name}")
#     df = pd.read_parquet(path)
#     qa_cols = ["geo_unmatched", "legal_form_unmatched"]
#     df = df.drop(columns=[c for c in qa_cols if c in df.columns])
#
#     geo_lookup = _build_geo_lookup(con)
#     if "nuts_code" in df.columns:
#         geo_lookup = _upsert_geo_rows(con, df["nuts_code"].dropna().unique().tolist(), geo_lookup)
#
#     df = _resolve_keys(
#         df,
#         geo_lookup=geo_lookup,
#         time_lookup=_build_time_lookup(con),
#         source_lookup=_build_source_lookup(con),
#         indicator_lookup=_build_indicator_lookup(con),
#         legal_form_lookup=_build_legal_form_lookup(con),
#     )
#     if df.empty:
#         return 0
#
#     start_key = _next_surrogate_key(con, "fact_immigration", "fact_key")
#     df["fact_key"] = range(start_key, start_key + len(df))
#
#     fact_columns = [
#         "fact_key", "geo_key", "time_key", "source_key", "indicator_key",
#         "value", "unit",
#         "nationality", "permit_type", "migration_flow",   # domain-specific
#         "gender", "age_group",
#         "dataset_code", "extracted_at",
#     ]
#     available = [c for c in fact_columns if c in df.columns]
#     df = df[available]
#
#     con.execute("INSERT INTO fact_immigration SELECT * FROM df")
#     n = con.execute("SELECT COUNT(*) FROM fact_immigration").fetchone()[0]
#     logger.info(f"  fact_immigration: {n:,} rows total")
#     return n


# ── Main build function ───────────────────────────────────────────────────────

def build_database(
    domains: list[str] | None = None,
    config: dict | None = None,
) -> Path:
    """
    Build the complete aiccon.duckdb file and upload it to SharePoint.

    Parameters
    ----------
    domains : list[str], optional
        Specific domains to load. If None, loads all domains that are both
        registered in DOMAIN_LOADERS and enabled in settings.yaml.
    config : dict, optional
        Pre-loaded config. Loaded from disk if not provided.

    Returns
    -------
    Path
        Path to the uploaded .duckdb file on SharePoint.
    """
    cfg = config or load_config()

    if not check_sharepoint_available(cfg, logger):
        raise RuntimeError("SharePoint synced folder is not reachable.")

    # Determine which domains to load
    if domains:
        to_load = [d for d in domains if d in DOMAIN_LOADERS]
        unknown = [d for d in domains if d not in DOMAIN_LOADERS]
        if unknown:
            logger.warning(f"Unknown domains (not in DOMAIN_LOADERS): {unknown}")
    else:
        to_load = [
            d for d in DOMAIN_LOADERS
            if cfg.get("domains", {}).get(d, {}).get("enabled", False)
        ]

    logger.info(f"Building database. Domains to load: {to_load}")
    started_at = datetime.now(timezone.utc)

    # Build in a temp file — only copy to SharePoint if everything succeeds
    # We use mkstemp and immediately close/unlink it so DuckDB can create the file fresh.
    fd, path_str = tempfile.mkstemp(suffix=".duckdb", prefix="aiccon_")
    os.close(fd)
    tmp_path = Path(path_str)
    tmp_path.unlink() # Delete the 0-byte file so DuckDB starts from scratch

    try:
        con = duckdb.connect(str(tmp_path))

        # ── Schema ────────────────────────────────────────────────────────────
        logger.info("── Schema ───────────────────────────────────────")
        _execute_sql_file(con, DIMENSIONS_SQL)
        _execute_sql_file(con, FACT_TABLES_SQL)

        # ── Dimensions ────────────────────────────────────────────────────────
        logger.info("── Dimensions ───────────────────────────────────")
        populate_dimensions(con, cfg)

        # ── Fact tables ───────────────────────────────────────────────────────
        logger.info("── Fact tables ──────────────────────────────────")
        domain_results = {}
        for domain in to_load:
            logger.info(f"Loading domain: {domain}")
            try:
                n_rows = DOMAIN_LOADERS[domain](con, cfg)
                domain_results[domain] = {"status": "success", "rows": n_rows}
            except Exception as e:
                logger.error(f"{domain}: load failed — {e}", exc_info=True)
                domain_results[domain] = {"status": "error", "rows": 0, "error": str(e)}

        # ── Views ─────────────────────────────────────────────────────────────
        logger.info("── Views ────────────────────────────────────────")
        _execute_sql_file(con, VIEWS_SQL)

        con.close()

        # ── Upload ────────────────────────────────────────────────────────────
        logger.info("── Upload ───────────────────────────────────────")
        dest = upload_database(tmp_path, cfg, logger)
        tmp_path.unlink()

    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    finished_at = datetime.now(timezone.utc)
    elapsed = (finished_at - started_at).total_seconds()

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info(f"\n{'═' * 50}")
    logger.info("Database build complete")
    logger.info(f"{'═' * 50}")
    for domain, result in domain_results.items():
        status = result["status"]
        rows   = f"{result['rows']:,} rows" if result["rows"] else ""
        err    = f" — {result.get('error', '')}" if status == "error" else ""
        logger.info(f"  {domain:<20} {status:<10} {rows}{err}")
    logger.info(f"Output: {dest}")
    logger.info(f"Elapsed: {elapsed:.1f}s")

    write_pipeline_log(cfg, run_summary={
        "stage":          "build_database",
        "started_at":     started_at.isoformat(),
        "finished_at":    finished_at.isoformat(),
        "elapsed_seconds": elapsed,
        "domains_loaded": to_load,
        "results":        domain_results,
        "output_path":    str(dest),
    })

    return dest


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the aiccon DuckDB database.")
    parser.add_argument(
        "--domain", type=str, default=None,
        help="Load a single domain only (e.g. --domain social_economy).",
    )
    parser.add_argument(
        "--config", type=str, default=None,
        help="Path to settings.yaml (default: auto-detected).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    config  = load_config(settings_path=args.config)
    domains = [args.domain] if args.domain else None

    dest = build_database(domains=domains, config=config)
    print(f"\nDatabase written to: {dest}")
    sys.exit(0)