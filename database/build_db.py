"""
database/build_db.py

Loads processed parquet files from GCS into BigQuery.

What this script does, in order:
    1. Checks GCS and BigQuery are reachable
    2. Creates dimension tables if they do not exist (idempotent)
    3. Creates fact tables if they do not exist (idempotent)
    4. Upserts dimension tables from mapping CSVs (safe to re-run)
    5. Replaces each active domain's fact table with its latest processed parquet
    6. Writes pipeline log to GCS

Keys and re-runs:
    Dimension rows keep their surrogate key for ever. Re-running updates their
    attributes (names, labels) from the mapping CSVs and gives genuinely new
    rows keys from MAX(key) + 1, so a new row can never reuse an existing key.
    Fact tables are replaced in full on every run, because each processed
    parquet is a complete snapshot of its domain. Running this script twice
    therefore gives the same result as running it once.

Exit code is 1 if any domain failed to load, so run_pipeline.py and
GitHub Actions can see the failure.

Note: This script will be superseded by dbt once the transformation layer
is introduced. At that point, schema management and mart models will move
to dbt/models/. The domain loader logic (key resolution, geo upserts) will
move to dbt staging models.

Adding a new domain:
    1. Add a loader function following the pattern of _load_social_economy()
    2. Register it with @domain_loader('your_domain_name')
    SQL schema changes go in fact_tables.sql, not here.

Run:
    python -m database.build_db
    python -m database.build_db --domain social_economy
    python -m database.build_db --domain labour
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from google.cloud import bigquery, storage

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.loaders.base_loader import get_logger, load_config, processed_gcs_prefix
from ingestion.loaders.gcs_uploader import write_pipeline_log, check_gcs_available

logger = get_logger("database.build_db")

HERE = Path(__file__).resolve().parent
SCHEMA_DIR = HERE / "schema"

DIMENSIONS_SQL  = SCHEMA_DIR / "dimensions.sql"
FACT_TABLES_SQL = SCHEMA_DIR / "fact_tables.sql"


# ── Domain registry ───────────────────────────────────────────────────────────

DOMAIN_LOADERS: dict[str, callable] = {}


def domain_loader(name: str):
    """Decorator to register a domain loader function."""
    def decorator(fn):
        DOMAIN_LOADERS[name] = fn
        return fn
    return decorator


# ── BigQuery helpers ──────────────────────────────────────────────────────────

def _get_bq_client(config: dict) -> bigquery.Client:
    return bigquery.Client(
        project=config["bigquery"]["project"],
        location=config["bigquery"].get("location", "US"),
    )


def _full_table(config: dict, table: str) -> str:
    """Return a fully-qualified BigQuery table reference: project.dataset.table"""
    return f"{config['bigquery']['project']}.{config['bigquery']['dataset']}.{table}"


def _execute_sql_file(client: bigquery.Client, path: Path, config: dict) -> None:
    """
    Execute a SQL file against BigQuery.

    Splits on semicolons and runs each non-empty statement separately, since
    BigQuery does not support multi-statement queries in the standard client.
    Replaces the placeholder {DATASET} with the real dataset name.
    """
    logger.info(f"Executing {path.name}")
    sql = path.read_text(encoding="utf-8")
    sql = sql.replace("{DATASET}", config["bigquery"]["dataset"])
    sql = sql.replace("{PROJECT}", config["bigquery"]["project"])

    # Strip all comments (full-line and inline) before splitting on semicolons.
    # This prevents semicolons inside comments from being treated as statement
    # separators (e.g. "-- semicolon-separated: 'NUTS0;NUTS2'" or
    # "enforce these; integrity..." on a wrapped comment line).
    clean_lines = []
    for line in sql.splitlines():
        # Remove inline comment: find -- that isn't inside a string literal.
        # Simple approach: strip from first -- occurrence. This is safe for our
        # SQL files which don't use -- inside string values.
        stripped = line.split("--")[0].rstrip()
        if stripped:
            clean_lines.append(stripped)

    sql_clean = "\n".join(clean_lines)
    statements = [s.strip() for s in sql_clean.split(";") if s.strip()]

    for i, stmt in enumerate(statements):
        try:
            client.query(stmt).result()
        except Exception as e:
            logger.error(
                f"  {path.name}: statement {i + 1}/{len(statements)} failed.\n"
                f"  Statement:\n{stmt}\n"
                f"  Error: {e}"
            )
            raise

    logger.info(f"  {path.name} executed successfully ({len(statements)} statements)")


def _run_query(client: bigquery.Client, sql: str) -> list:
    """Run a query and return all rows as a list of tuples."""
    return [tuple(row.values()) for row in client.query(sql).result()]


def _load_df_to_bq(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_id: str,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    schema_table: str | None = None,
) -> int:
    """
    Load a pandas DataFrame into a BigQuery table via a parquet temp file.

    Derives the pyarrow schema from the BigQuery table definition so that
    all columns -- including all-null ones -- are written with the correct
    type. String columns are normalised (float NaN -> None) before serialisation
    so pyarrow does not raise ArrowTypeError on mixed-type object columns.

    For temporary staging tables that do not exist yet in BigQuery, pass
    schema_table (the table the temp data will be merged into) to type the
    columns from that table; otherwise typing falls back to inference.
    """
    import tempfile
    import os
    import pyarrow as pa

    # Map BigQuery field types to pyarrow types
    _BQ_TO_PA = {
        "STRING":    pa.string(),
        "BYTES":     pa.large_binary(),
        "INTEGER":   pa.int64(),
        "INT64":     pa.int64(),
        "FLOAT":     pa.float64(),
        "FLOAT64":   pa.float64(),
        "NUMERIC":   pa.float64(),
        "BOOLEAN":   pa.bool_(),
        "BOOL":      pa.bool_(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "DATE":      pa.date32(),
        "TIME":      pa.time64("us"),
        "DATETIME":  pa.timestamp("us", tz="UTC"),
    }

    # Try to fetch the BQ table schema; fall back for temp/non-existent tables
    bq_schema: dict = {}
    try:
        bq_table = client.get_table(schema_table or table_id)
        for field in bq_table.schema:
            bq_schema[field.name] = _BQ_TO_PA.get(field.field_type, pa.string())
    except Exception:
        pass  # table does not exist yet (e.g. _tmp_ staging tables) -- use fallback

    df = df.copy()

    pa_fields = []
    for col in df.columns:
        if col in bq_schema:
            pa_type = bq_schema[col]
        else:
            # Fallback inference for columns not present in BQ schema
            series = df[col]
            if pd.api.types.is_integer_dtype(series):
                pa_type = pa.int64()
            elif pd.api.types.is_float_dtype(series):
                pa_type = pa.float64()
            elif pd.api.types.is_bool_dtype(series):
                pa_type = pa.bool_()
            elif pd.api.types.is_datetime64_any_dtype(series):
                pa_type = pa.timestamp("us", tz="UTC")
            else:
                pa_type = pa.string()
        pa_fields.append(pa.field(col, pa_type))
    pa_schema = pa.schema(pa_fields)

    # Normalise string/bytes columns: any non-string value (float NaN, numpy
    # NaN) must become None before pyarrow serialises the column, otherwise
    # pyarrow raises ArrowTypeError ("Expected bytes, got a float object").
    string_types = {pa.string(), pa.large_binary()}
    for f in pa_fields:
        if f.type in string_types:
            col = df[f.name]
            # Convert everything that is not already a str (or None) to None
            df[f.name] = col.where(col.apply(lambda x: x is None or isinstance(x, str)), other=None)

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.PARQUET,
    )
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        df.to_parquet(tmp_path, index=False, engine="pyarrow", schema=pa_schema)
        with open(tmp_path, "rb") as f:
            job = client.load_table_from_file(
                f, table_id, job_config=job_config,
                location=client.location,
            )
        job.result()
    finally:
        os.unlink(tmp_path)
    return len(df)

def _find_latest_processed(config: dict, domain: str) -> str | None:
    """
    Find the most recently written processed parquet for a domain in GCS.

    Files are date-stamped (e.g. social_economy_2024-11.parquet) so
    lexicographic sort = chronological sort. Returns a GCS URI or None.
    """
    bucket_name = config["gcs"]["bucket"]
    prefix_uri = processed_gcs_prefix(config, domain)
    # Convert gs://bucket/prefix to just the prefix string for the GCS API
    blob_prefix = prefix_uri[len(f"gs://{bucket_name}/"):]

    client = storage.Client()
    blobs = [
        b for b in client.list_blobs(bucket_name, prefix=blob_prefix)
        if b.name.endswith(".parquet")
    ]

    if not blobs:
        logger.warning(f"No processed parquet found for domain '{domain}' in {prefix_uri}")
        return None

    # Sort by blob name — date suffix makes this chronological
    blobs.sort(key=lambda b: b.name, reverse=True)
    if len(blobs) > 1:
        logger.info(
            f"Multiple processed files for '{domain}' — "
            f"using most recent: {blobs[0].name}"
        )

    uri = f"gs://{bucket_name}/{blobs[0].name}"
    return uri


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_mappings_dir() -> Path:
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        candidate = parent / "processing" / "mappings"
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Could not find processing/mappings/ directory.")


def _next_surrogate_key(client: bigquery.Client, config: dict, table: str, key_col: str) -> int:
    """Return the next available surrogate key for a BigQuery table."""
    full = _full_table(config, table)
    rows = _run_query(client, f"SELECT COALESCE(MAX({key_col}), 0) + 1 FROM `{full}`")
    return rows[0][0]


# ── Dimension population ──────────────────────────────────────────────────────
#
# Every populate_* function goes through _upsert_dimension(), which matches
# rows on the table's natural key (nuts_code, year, source_id, ...):
#   - existing rows keep their surrogate key and have their attributes
#     refreshed from the mapping CSV, so fixing a name in a CSV fixes it here
#   - new rows get keys from MAX(key) + 1
#
# Previously each function numbered rows 1..n by their position in the CSV.
# A new row then took key n+1, which could already belong to a row added
# elsewhere (e.g. a Eurostat country auto-inserted into dim_geography), giving
# duplicate keys: fanned-out joins in SQL and refresh errors in PowerBI.

# Eurostat "Extra-Regio" codes (ITZ, ITZZ, ITZZZ, DEZ, ...): economic activity
# that cannot be attributed to any region. Not real territories, so they are
# excluded from both the geography dimension and the fact tables.
EXTRA_REGIO_PATTERN = r"^[A-Z]{2}Z+$"


def _blank(v) -> bool:
    """True for None, NaN and empty or whitespace-only strings."""
    return v is None or (isinstance(v, float) and pd.isna(v)) or (isinstance(v, str) and not v.strip())


def _upsert_dimension(
    client: bigquery.Client,
    config: dict,
    table: str,
    key_col: str,
    natural_key: list[str],
    df: pd.DataFrame,
) -> tuple[int, int]:
    """
    Upsert rows into a dimension table, matching on its natural key.

    Existing rows keep their surrogate key and have every other column
    refreshed from df. New rows are inserted with keys from MAX(key) + 1.
    Columns of df that the table does not have are ignored; table columns
    missing from df are loaded as NULL. Rows already in the table but not in
    df (e.g. Eurostat countries auto-inserted by _upsert_geo_rows) are left
    untouched.

    Returns (n_inserted, n_updated).
    """
    full = _full_table(config, table)
    schema_cols = [f.name for f in client.get_table(full).schema]

    df = df.copy()
    for c in natural_key:
        df[c] = df[c].where(df[c].notna(), None)
    incomplete = df[natural_key].isna().any(axis=1)
    if incomplete.any():
        logger.warning(
            f"  {table}: {int(incomplete.sum())} rows skipped because "
            f"their natural key {natural_key} is incomplete"
        )
        df = df[~incomplete]
    df = df.drop_duplicates(subset=natural_key, keep="last")

    # Natural key -> existing surrogate key
    nk_sql = ", ".join(f"`{c}`" for c in natural_key)
    existing = {
        tuple(row[:-1]): row[-1]
        for row in _run_query(client, f"SELECT {nk_sql}, `{key_col}` FROM `{full}`")
    }

    next_key = _next_surrogate_key(client, config, table, key_col)
    keys, n_new = [], 0
    for nk in df[natural_key].itertuples(index=False, name=None):
        if nk in existing:
            keys.append(existing[nk])
        else:
            keys.append(next_key)
            next_key += 1
            n_new += 1
    df[key_col] = keys
    df = df.reindex(columns=schema_cols)

    tmp_table = _full_table(config, f"_tmp_{table}_upsert")
    _load_df_to_bq(client, df, tmp_table, write_disposition="WRITE_TRUNCATE", schema_table=full)

    on    = " AND ".join(f"T.`{c}` = S.`{c}`" for c in natural_key)
    attrs = [c for c in schema_cols if c != key_col and c not in natural_key]
    cols  = ", ".join(f"`{c}`" for c in schema_cols)
    vals  = ", ".join(f"S.`{c}`" for c in schema_cols)
    when_matched = (
        "WHEN MATCHED THEN UPDATE SET " + ", ".join(f"`{c}` = S.`{c}`" for c in attrs)
        if attrs else ""
    )
    try:
        client.query(f"""
            MERGE `{full}` T
            USING `{tmp_table}` S
            ON {on}
            {when_matched}
            WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})
        """).result()
    finally:
        client.delete_table(tmp_table, not_found_ok=True)

    return n_new, len(df) - n_new


def _log_dimension(client: bigquery.Client, config: dict, table: str, n_new: int, n_upd: int) -> int:
    n = _run_query(client, f"SELECT COUNT(*) FROM `{_full_table(config, table)}`")[0][0]
    logger.info(f"  {table}: {n:,} rows ({n_new} inserted, {n_upd} refreshed)")
    return n


def populate_dim_geography(client: bigquery.Client, config: dict, mappings_dir: Path) -> int:
    """
    Upsert dim_geography from nuts_istat.csv, then remove Extra-Regio codes.

    Rows whose code combines several NUTS units, such as 'ITC;ITH' (Nord),
    'ITF;ITG' (Mezzogiorno) or 'ITH1;ITH2' (Trentino-Alto Adige), are ISTAT
    aggregates: ISTAT publishes data for them but NUTS does not define them.
    They used to be skipped here and then auto-inserted without names by
    _upsert_geo_rows when they appeared in the facts. They are now loaded
    deliberately, with their names, geo_source = 'istat_aggregate' and
    nuts_level = NULL. The NULL level is intentional: giving Trentino-Alto
    Adige level 2 would double count Bolzano and Trento in any sum over regions.
    """
    path = mappings_dir / "nuts_istat.csv"
    df = pd.read_csv(path, dtype=str)
    df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)

    def _val(r, col):
        v = r.get(col)
        return None if _blank(v) else v

    rows = []
    for _, r in df.iterrows():
        # Some NUTS1 rows have nuts_code empty with the code in istat_code
        # instead. Only use that fallback when it looks like a NUTS code.
        nuts_code = _val(r, "nuts_code")
        if nuts_code is None:
            fallback = _val(r, "istat_code")
            nuts_code = fallback if fallback and fallback.startswith("IT") else None

        is_aggregate = bool(nuts_code) and ";" in nuts_code
        level = _val(r, "nuts_level")
        rows.append({
            "nuts_code":         nuts_code,
            "nuts_level":        None if (is_aggregate or level is None) else int(float(level)),
            "nuts_name_it":      _val(r, "nuts_name_it"),
            "nuts_name_en":      _val(r, "nuts_name_en"),
            "istat_code":        _val(r, "istat_code"),
            "country_code":      "IT",
            "region_name":       _val(r, "region_name"),
            "macro_area":        _val(r, "macro_area"),
            "municipality_type": None,
            "geo_source":        "istat_aggregate" if is_aggregate else "nuts_istat_csv",
            "is_active":         True,
        })

    geo_df = pd.DataFrame(rows)
    geo_df = geo_df[geo_df["nuts_code"].notna()]
    geo_df = geo_df[~geo_df["nuts_code"].str.match(EXTRA_REGIO_PATTERN)]
    geo_df["nuts_level"] = pd.array(geo_df["nuts_level"], dtype="Int64")

    n_new, n_upd = _upsert_dimension(
        client, config, "dim_geography", "geo_key", ["nuts_code"], geo_df
    )

    # Rows loaded from an earlier version of the CSV whose code is no longer
    # in it (e.g. ITC45, ITG25: pre-2021 NUTS codes) are marked inactive
    # rather than deleted, so any fact row still pointing at them stays valid.
    # Filter on is_active in PowerBI to hide them.
    full = _full_table(config, "dim_geography")
    codes_sql = ", ".join(f"'{c}'" for c in geo_df["nuts_code"])
    stale_where = (
        "geo_source IN ('nuts_istat_csv', 'istat_aggregate') "
        f"AND nuts_code NOT IN ({codes_sql}) AND IFNULL(is_active, TRUE)"
    )
    stale = [r[0] for r in _run_query(client, f"SELECT nuts_code FROM `{full}` WHERE {stale_where}")]
    if stale:
        client.query(f"UPDATE `{full}` SET is_active = FALSE WHERE {stale_where}").result()
        logger.info(f"  dim_geography: marked {len(stale)} codes no longer in nuts_istat.csv as inactive: {sorted(stale)}")

    # Extra-Regio rows auto-inserted by earlier runs. The fact loaders no
    # longer produce them, and every run replaces the fact tables, so nothing
    # will reference these keys once the facts are reloaded.
    full = _full_table(config, "dim_geography")
    extra_regio = f"REGEXP_CONTAINS(nuts_code, r'{EXTRA_REGIO_PATTERN}')"
    n_er = _run_query(client, f"SELECT COUNT(*) FROM `{full}` WHERE {extra_regio}")[0][0]
    if n_er:
        client.query(f"DELETE FROM `{full}` WHERE {extra_regio}").result()
        logger.info(f"  dim_geography: removed {n_er} Extra-Regio rows")

    return _log_dimension(client, config, "dim_geography", n_new, n_upd)


def populate_dim_time(
    client: bigquery.Client,
    config: dict,
    year_range: tuple[int, int] = (1990, 2030),
) -> int:
    """Upsert dim_time with annual rows. Safe to re-run."""
    time_df = pd.DataFrame([
        {
            "year":             year,
            "period_type":      "A",
            "period_label":     str(year),
            "reference_period": str(year),
        }
        for year in range(year_range[0], year_range[1] + 1)
    ])
    n_new, n_upd = _upsert_dimension(client, config, "dim_time", "time_key", ["year"], time_df)
    return _log_dimension(client, config, "dim_time", n_new, n_upd)


def populate_dim_source(client: bigquery.Client, config: dict, mappings_dir: Path) -> int:
    """Upsert dim_source from domain_sources.csv. Safe to re-run."""
    df = pd.read_csv(mappings_dir / "domain_sources.csv", dtype=str)
    df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)
    df["priority"] = pd.to_numeric(df["priority"], errors="coerce").astype("Int64")
    if "provider" not in df.columns:
        df["provider"] = df["source_id"].str.split("_").str[0]

    n_new, n_upd = _upsert_dimension(client, config, "dim_source", "source_key", ["source_id"], df)
    return _log_dimension(client, config, "dim_source", n_new, n_upd)


def populate_dim_legal_form(client: bigquery.Client, config: dict, mappings_dir: Path) -> int:
    """
    Upsert dim_legal_form from legal_form_map.csv. Safe to re-run.

    The natural key is (source_system, source_code): one row per code in each
    source system. It used to be (unified_category, source_system), which is
    not unique (three ISTAT codes map to cooperativa_sociale).
    """
    df = pd.read_csv(mappings_dir / "legal_form_map.csv", dtype=str)
    df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)

    n_new, n_upd = _upsert_dimension(
        client, config, "dim_legal_form", "legal_form_key", ["source_system", "source_code"], df
    )
    return _log_dimension(client, config, "dim_legal_form", n_new, n_upd)


def populate_dim_indicator(client: bigquery.Client, config: dict) -> int:
    """
    Upsert dim_indicator with all known indicator rows. Safe to re-run.

    New indicators can be added anywhere in this list: keys are matched on
    indicator_code, never on position.
    """
    rows = [
        # Social economy — volunteering
        ("volunteering_rate",               "Tasso di volontariato",                    "Volunteering rate",                        "mixed",              "social_economy", "Dataset VOLON1_1 contains multiple data_types — check unit column per row"),
        ("volunteering_activity_share",     "Quota per tipo di attività",               "Share by activity type",                   "percentage",         "social_economy", None),
        ("volunteering_years_share",        "Quota per anni di attività",               "Share by years active",                    "percentage",         "social_economy", None),
        ("org_volunteering_sector_share",   "Quota volontariato organizzato per settore","Organised volunteering share by sector",   "percentage",         "social_economy", None),
        ("org_volunteering_orgtype_share",  "Quota per tipo organizzazione",            "Organised volunteering share by org type", "percentage",         "social_economy", None),
        ("org_volunteering_motivation_share","Quota per motivazione",                   "Volunteering share by motivation",         "percentage",         "social_economy", None),
        ("org_volunteering_impact_share",   "Quota per impatto personale",              "Volunteering share by personal impact",    "percentage",         "social_economy", None),
        # Social economy — associationism
        ("association_membership_rate",     "Tasso di associazionismo",                 "Association membership rate",              "percentage",         "social_economy", None),
        # Social economy — Eurostat employment
        ("n_employed",                      "Occupati",                                 "Employed persons",                         "thousands_persons",  "social_economy", "Eurostat: THS or THS_PER. ISTAT: absolute count from LFS."),
        ("n_local_units",                   "Unità locali",                             "Local units",                              "count",              "social_economy", "From sbs_r_nuts2021, indic_sbs=LOC_NR"),
        # Labour
        ("labour_force",                    "Forze di lavoro",                          "Labour force",                             "thousands_persons",  "labour",         "ISTAT FORZLVMENS1_1: employed + unemployed aged 15+, monthly, national only"),
        ("employment_rate",                 "Tasso di occupazione",                     "Employment rate",                          "percentage",         "labour",         "ISTAT TAXOCCU1_5 (NUTS3); Eurostat lfst_r_lfe2emprtn (NUTS2 IT + NUTS0 EU)"),
        ("unemployment_rate",               "Tasso di disoccupazione",                  "Unemployment rate",                        "percentage",         "labour",         "ISTAT TAXDISOCCU1_5/6/8; Eurostat lfst_r_lfu3rt / lfst_r_lfur2gan"),
        ("inactivity_rate",                 "Tasso di inattività",                      "Inactivity rate",                          "percentage",         "labour",         "ISTAT TAXINATT1_5 (NUTS3)"),
        ("neet_rate",                       "Quota NEET",                               "NEET rate",                                "percentage",         "labour",         "ISTAT NEET1_11 (NUTS2); NEET1_9 (macro-areas, citizenship). Ages 15-34."),
        # Placeholder rows for future domains
        ("n_resident_foreign",              "Popolazione straniera residente",          "Resident foreign population",              "count",              "immigration",    None),
        ("at_risk_of_poverty_rate",         "Tasso di rischio povertà",                 "At-risk-of-poverty rate",                  "percentage",         "poverty",        None),
        ("social_expenditure_pct_gdp",      "Spesa sociale % PIL",                      "Social expenditure as % of GDP",           "percentage_gdp",     "welfare",        None),
    ]
    ind_df = pd.DataFrame(rows, columns=[
        "indicator_code", "label_it", "label_en", "unit_default", "domain", "notes"
    ])
    n_new, n_upd = _upsert_dimension(
        client, config, "dim_indicator", "indicator_key", ["indicator_code"], ind_df
    )
    return _log_dimension(client, config, "dim_indicator", n_new, n_upd)


def populate_dimensions(client: bigquery.Client, config: dict) -> None:
    """Populate all shared dimension tables. Called once before any domain loads."""
    mappings_dir = _find_mappings_dir()
    logger.info("Populating dimension tables")
    populate_dim_geography(client, config, mappings_dir)
    populate_dim_time(client, config)
    populate_dim_source(client, config, mappings_dir)
    populate_dim_legal_form(client, config, mappings_dir)
    populate_dim_indicator(client, config)


# ── Dimension key lookups ─────────────────────────────────────────────────────

def _build_geo_lookup(client: bigquery.Client, config: dict) -> dict[str, int]:
    full = _full_table(config, "dim_geography")
    rows = _run_query(client, f"SELECT nuts_code, geo_key FROM `{full}` WHERE nuts_code IS NOT NULL")
    return {r[0]: r[1] for r in rows}


def _build_time_lookup(client: bigquery.Client, config: dict) -> dict[int, int]:
    full = _full_table(config, "dim_time")
    rows = _run_query(client, f"SELECT year, time_key FROM `{full}`")
    return {r[0]: r[1] for r in rows}


def _build_source_lookup(client: bigquery.Client, config: dict) -> dict[str, int]:
    full = _full_table(config, "dim_source")
    rows = _run_query(client, f"SELECT source_id, source_key FROM `{full}`")
    return {r[0]: r[1] for r in rows}


def _build_indicator_lookup(client: bigquery.Client, config: dict) -> dict[str, int]:
    full = _full_table(config, "dim_indicator")
    rows = _run_query(client, f"SELECT indicator_code, indicator_key FROM `{full}`")
    return {r[0]: r[1] for r in rows}


def _build_legal_form_lookup(client: bigquery.Client, config: dict) -> dict[tuple, int]:
    full = _full_table(config, "dim_legal_form")
    rows = _run_query(client, f"SELECT unified_category, source_system, legal_form_key FROM `{full}`")
    return {(r[0], r[1]): r[2] for r in rows}


def _upsert_geo_rows(
    client: bigquery.Client,
    config: dict,
    nuts_codes: list[str],
    geo_lookup: dict[str, int],
) -> dict[str, int]:
    """
    Insert any nuts_codes not yet in dim_geography (e.g. non-Italian EU
    countries from Eurostat) and return the updated lookup dict.
    """
    new_codes = [c for c in nuts_codes if c and c not in geo_lookup]
    if not new_codes:
        return geo_lookup

    new_rows = []
    for code in set(new_codes):
        if not code or len(code) < 2:
            continue
        new_rows.append({
            "nuts_code":         code,
            "nuts_level":        len(code) - 2 if len(code) <= 5 else None,
            "nuts_name_it":      None,
            "nuts_name_en":      None,
            "istat_code":        None,
            "country_code":      code[:2],
            "region_name":       None,
            "macro_area":        None,
            "municipality_type": None,
            "geo_source":        "eurostat_auto",
            "is_active":         True,
        })

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        new_df["nuts_level"] = pd.array(new_df["nuts_level"], dtype="Int64")
        # Assign geo_keys continuing from the current max
        next_key = _next_surrogate_key(client, config, "dim_geography", "geo_key")
        new_df.insert(0, "geo_key", range(next_key, next_key + len(new_df)))
        full = _full_table(config, "dim_geography")
        tmp_table = _full_table(config, "_tmp_geo_upsert")
        _load_df_to_bq(client, new_df, tmp_table, write_disposition="WRITE_TRUNCATE")

        client.query(f"""
            MERGE `{full}` T
            USING `{tmp_table}` S ON T.nuts_code = S.nuts_code
            WHEN NOT MATCHED THEN INSERT (
                geo_key, nuts_code, nuts_level, nuts_name_it, nuts_name_en, istat_code,
                country_code, region_name, macro_area, municipality_type,
                geo_source, is_active
            ) VALUES (
                S.geo_key, S.nuts_code, S.nuts_level, S.nuts_name_it, S.nuts_name_en, S.istat_code,
                S.country_code, S.region_name, S.macro_area, S.municipality_type,
                S.geo_source, S.is_active
            )
        """).result()

        client.delete_table(tmp_table, not_found_ok=True)
        logger.info(f"  Auto-inserted {len(new_rows)} new geo rows from Eurostat data")
        return _build_geo_lookup(client, config)

    return geo_lookup


def _resolve_keys(
    df: pd.DataFrame,
    client: bigquery.Client,
    config: dict,
    domain: str,
) -> pd.DataFrame:
    """
    Replace natural key columns with surrogate integer keys.

    Rows where a required key cannot be resolved (unknown geo, time, source,
    or indicator) are dropped with a warning. legal_form_key is nullable —
    null values are expected and kept.
    """
    if df.empty:
        return df

    # 1. GEOGRAPHY
    geo_map = _build_geo_lookup(client, config)
    df["geo_key"] = df["nuts_code"].map(geo_map)
    missing_geo = df["geo_key"].isna().sum()
    if missing_geo:
        bad = df.loc[df["geo_key"].isna(), "nuts_code"].unique()[:10]
        # Use repr() to expose any hidden whitespace or encoding issues
        bad_repr = [repr(v) for v in bad]
        logger.warning(f"  {missing_geo} rows dropped: nuts_code not in dim_geography: {bad_repr}")
    df = df.dropna(subset=["geo_key"])
    df["geo_key"] = df["geo_key"].astype(int)

    # 2. TIME — resolve any time format (YYYY, YYYY-MM, YYYY-QN) to year integer
    time_map = _build_time_lookup(client, config)
    df["_year_int"] = (
        df["time"].astype(str)
                  .str.extract(r"(\d{4})", expand=False)
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

    # 3. SOURCE — compound key: "{source_id}_{dataset_code}"
    src_map = _build_source_lookup(client, config)
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
    ind_map = _build_indicator_lookup(client, config)
    df["indicator_key"] = df["indicator_code"].map(ind_map)
    missing_ind = df["indicator_key"].isna().sum()
    if missing_ind:
        bad = df.loc[df["indicator_key"].isna(), "indicator_code"].unique()[:10]
        logger.warning(
            f"  {missing_ind} rows dropped: indicator_code not in dim_indicator: {list(bad)}. "
            "Add these to the rows list in populate_dim_indicator()."
        )
    df = df.dropna(subset=["indicator_key"])
    df["indicator_key"] = df["indicator_key"].astype(int)

    # 5. LEGAL FORM (nullable — null is expected for most rows)
    # KNOWN LIMITATION: this looks up (unified_category, None), but every row
    # of legal_form_map.csv has a source_system, so it never matches and
    # legal_form_key is always NULL. No current dataset carries a legal form,
    # so nothing is lost yet. When the first one is loaded (non-profit census,
    # RUNTS, Registro Imprese), resolve on (source_system, source_code), the
    # natural key dim_legal_form now uses.
    if "legal_form_unified" in df.columns:
        lf_map = _build_legal_form_lookup(client, config)
        df["legal_form_key"] = df["legal_form_unified"].map(
            lambda v: lf_map.get((v, None)) if pd.notna(v) else None
        )
    else:
        df["legal_form_key"] = None

    return df


# ── Domain loaders ────────────────────────────────────────────────────────────

def _filter_geography(df: pd.DataFrame, domain: str) -> pd.DataFrame:
    """
    Keep Italian territories at every level plus national (NUTS0) totals for
    other countries. Drop Extra-Regio codes and non-Italian sub-national rows.

    Rows with no nuts_code are geography the merge step could not map. They
    used to disappear inside the 'non-IT' count; they are now reported
    separately by dataset, because they are a mapping problem rather than
    data we chose to exclude. Likewise, ISTAT rows dropped as 'non-Italian'
    are impossible in principle and are reported as a warning.
    """
    if "nuts_code" not in df.columns:
        return df

    code = df["nuts_code"].map(lambda v: v.strip() if isinstance(v, str) else None)
    code = code.where(~code.isin(["", "nan", "None", "<NA>"]), None)

    unmatched   = code.isna()
    extra_regio = code.str.match(EXTRA_REGIO_PATTERN, na=False)
    in_scope    = code.str.startswith("IT", na=False) | code.str.len().eq(2)
    keep        = in_scope & ~extra_regio & ~unmatched
    foreign     = ~keep & ~unmatched & ~extra_regio

    if unmatched.any():
        by_ds = df.loc[unmatched, "dataset_code"].value_counts()
        logger.warning(
            f"  {domain}: {int(unmatched.sum()):,} rows dropped with no nuts_code "
            "(geography not mapped in the merge step), by dataset: "
            + ", ".join(f"{k} ({v:,})" for k, v in by_ds.items())
        )
    if extra_regio.any():
        logger.info(f"  {domain}: dropped {int(extra_regio.sum()):,} Extra-Regio rows")
    if foreign.any():
        logger.info(f"  {domain}: dropped {int(foreign.sum()):,} non-Italian sub-national rows")
        if "source_id" in df.columns:
            suspicious = foreign & df["source_id"].astype(str).str.lower().eq("istat")
            if suspicious.any():
                sample = sorted(code[suspicious].dropna().unique())[:10]
                logger.warning(
                    f"  {domain}: {int(suspicious.sum()):,} of those are ISTAT rows, which "
                    f"should all be Italian. Their codes, e.g. {sample}, are probably "
                    "unmapped: check nuts_istat.csv and nuts_mapper.py"
                )

    out = df[keep].copy()
    out["nuts_code"] = code[keep]
    return out


def _replace_fact_table(client: bigquery.Client, config: dict, table: str, df: pd.DataFrame) -> int:
    """
    Replace the entire contents of a fact table with df.

    Each processed parquet is a complete snapshot of its domain, so the table
    is replaced on every run. (It used to be appended to, which duplicated
    every row each time the database stage ran.)

    WRITE_TRUNCATE is atomic: if the load fails, the previous contents stay.
    It also replaces the table's schema with that of the loaded file, so df is
    first aligned to the existing schema (missing columns added as NULL),
    which keeps the schema exactly as fact_tables.sql defines it.
    """
    full = _full_table(config, table)
    schema_cols = [f.name for f in client.get_table(full).schema]
    unknown = [c for c in df.columns if c not in schema_cols]
    if unknown:
        logger.warning(f"  {table}: columns not in the table schema, not loaded: {unknown}")

    df = df.reindex(columns=schema_cols)
    df["fact_key"] = range(1, len(df) + 1)
    _load_df_to_bq(client, df, full, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    n = _run_query(client, f"SELECT COUNT(*) FROM `{full}`")[0][0]
    logger.info(f"  {table}: replaced, {n:,} rows")
    return n


@domain_loader("social_economy")
def _load_social_economy(client: bigquery.Client, config: dict) -> int:
    """Replace fact_social_economy with the latest social economy processed parquet."""
    uri = _find_latest_processed(config, "social_economy")
    if uri is None:
        logger.warning("social_economy: no processed parquet found — skipping.")
        return 0

    logger.info(f"  Loading {uri}")
    df = pd.read_parquet(uri, engine="pyarrow")
    logger.info(f"  Read {len(df):,} rows from processed parquet")

    qa_cols = ["geo_unmatched", "legal_form_unmatched"]
    df = df.drop(columns=[c for c in qa_cols if c in df.columns])

    df = _filter_geography(df, "social_economy")

    geo_lookup = _build_geo_lookup(client, config)
    if "nuts_code" in df.columns:
        geo_lookup = _upsert_geo_rows(
            client, config, df["nuts_code"].dropna().unique().tolist(), geo_lookup
        )

    df = _resolve_keys(df=df, client=client, config=config, domain="social_economy")
    if df.empty:
        logger.warning("social_economy: all rows dropped during key resolution.")
        return 0

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
    df = df[[c for c in fact_columns if c in df.columns]]

    return _replace_fact_table(client, config, "fact_social_economy", df)


@domain_loader("labour")
def _load_labour(client: bigquery.Client, config: dict) -> int:
    """
    Replace fact_labour with the latest labour processed parquet.

    Geography strategy:
      - All NUTS0 national totals (2-char codes) for every country.
      - Italian sub-national codes (starting 'IT') for NUTS2/NUTS3 detail.
      - All other sub-national non-Italian rows are dropped.
    """
    uri = _find_latest_processed(config, "labour")
    if uri is None:
        logger.warning("labour: no processed parquet found — skipping.")
        return 0

    logger.info(f"  Loading {uri}")
    df = pd.read_parquet(uri, engine="pyarrow")
    logger.info(f"  Read {len(df):,} rows from processed parquet")

    qa_cols = ["geo_unmatched", "legal_form_unmatched"]
    df = df.drop(columns=[c for c in qa_cols if c in df.columns])

    df = _filter_geography(df, "labour")

    geo_lookup = _build_geo_lookup(client, config)
    if "nuts_code" in df.columns:
        geo_lookup = _upsert_geo_rows(
            client, config, df["nuts_code"].dropna().unique().tolist(), geo_lookup
        )

    # Monthly rows store time as 'YYYY-MM' — extract year for dim_time lookup
    if "time" in df.columns:
        df["time"] = df["time"].astype(str).str.extract(r"^(\d{4})", expand=False)

    df = _resolve_keys(df=df, client=client, config=config, domain="labour")
    if df.empty:
        logger.warning("labour: all rows dropped during key resolution.")
        return 0

    fact_columns = [
        "fact_key", "geo_key", "time_key", "source_key", "indicator_key",
        "value", "unit",
        "frequency", "period_label",
        "gender", "age_group",
        "education", "citizenship", "adjustment", "unemployment_duration",
        "nace_code", "nace_label_en",
        "dataset_code", "extracted_at",
    ]
    df = df[[c for c in fact_columns if c in df.columns]]

    return _replace_fact_table(client, config, "fact_labour", df)


# ── When adding a new domain, copy this template: ────────────────────────────
#
# @domain_loader("immigration")
# def _load_immigration(client: bigquery.Client, config: dict) -> int:
#     uri = _find_latest_processed(config, "immigration")
#     if uri is None:
#         logger.warning("immigration: no processed parquet found — skipping.")
#         return 0
#
#     logger.info(f"  Loading {uri}")
#     df = pd.read_parquet(uri, engine="pyarrow")
#
#     qa_cols = ["geo_unmatched", "legal_form_unmatched"]
#     df = df.drop(columns=[c for c in qa_cols if c in df.columns])
#     df = _filter_geography(df, "immigration")
#
#     geo_lookup = _build_geo_lookup(client, config)
#     if "nuts_code" in df.columns:
#         geo_lookup = _upsert_geo_rows(
#             client, config, df["nuts_code"].dropna().unique().tolist(), geo_lookup
#         )
#
#     df = _resolve_keys(df=df, client=client, config=config, domain="immigration")
#     if df.empty:
#         return 0
#
#     fact_columns = [
#         "fact_key", "geo_key", "time_key", "source_key", "indicator_key",
#         "value", "unit",
#         "nationality", "permit_type", "migration_flow",
#         "gender", "age_group",
#         "dataset_code", "extracted_at",
#     ]
#     df = df[[c for c in fact_columns if c in df.columns]]
#     return _replace_fact_table(client, config, "fact_immigration", df)


# ── Main build function ───────────────────────────────────────────────────────

def build_database(
    domains: list[str] | None = None,
    config: dict | None = None,
) -> dict[str, dict]:
    """
    Build the aiccon BigQuery dataset from processed parquet files in GCS.

    Parameters
    ----------
    domains : list[str], optional
        Specific domains to load. If None, loads all domains that are both
        registered in DOMAIN_LOADERS and enabled in settings.yaml.
    config : dict, optional
        Pre-loaded config. Loaded from disk if not provided.

    Returns
    -------
    dict
        Per-domain results: {domain: {"status": "success" | "error", "rows": n, ...}}.
        A failed domain does not stop the others, so callers must check this.
    """
    cfg = config or load_config()

    if not check_gcs_available(cfg, logger):
        raise RuntimeError("GCS bucket is not reachable.")

    client = _get_bq_client(cfg)

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

    # ── Schema ────────────────────────────────────────────────────────────────
    logger.info("── Schema ───────────────────────────────────────")
    _execute_sql_file(client, DIMENSIONS_SQL, cfg)
    _execute_sql_file(client, FACT_TABLES_SQL, cfg)

    # ── Dimensions ────────────────────────────────────────────────────────────
    logger.info("── Dimensions ───────────────────────────────────")
    populate_dimensions(client, cfg)

    # ── Diagnostic: check key Italian geo codes loaded correctly ─────────────
    geo_check = _run_query(client, f"""
        SELECT nuts_code, nuts_level
        FROM `{_full_table(cfg, 'dim_geography')}`
        WHERE nuts_code IN ('IT', 'ITC', 'ITH', 'ITI', 'ITF', 'ITG',
                            'ITC1', 'ITC2', 'ITC3', 'ITC4',
                            'ITH3', 'ITH4', 'ITH5', 'ITI1')
        ORDER BY nuts_level, nuts_code
    """)
    logger.info(f"── Geo diagnostic: {len(geo_check)} key codes found in dim_geography")
    for row in geo_check:
        logger.info(f"    {row[0]:<10} level={row[1]}")

    # ── Fact tables ───────────────────────────────────────────────────────────
    logger.info("── Fact tables ──────────────────────────────────")
    domain_results = {}
    for domain in to_load:
        logger.info(f"Loading domain: {domain}")
        try:
            n_rows = DOMAIN_LOADERS[domain](client, cfg)
            domain_results[domain] = {"status": "success", "rows": n_rows}
        except Exception as e:
            logger.error(f"{domain}: load failed — {e}", exc_info=True)
            domain_results[domain] = {"status": "error", "rows": 0, "error": str(e)}

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
    logger.info(f"BigQuery dataset: {cfg['bigquery']['project']}.{cfg['bigquery']['dataset']}")
    logger.info(f"Elapsed: {elapsed:.1f}s")

    write_pipeline_log(cfg, run_summary={
        "stage":           "build_database",
        "started_at":      started_at.isoformat(),
        "finished_at":     finished_at.isoformat(),
        "elapsed_seconds": elapsed,
        "domains_loaded":  to_load,
        "results":         domain_results,
        "bigquery_dataset": f"{cfg['bigquery']['project']}.{cfg['bigquery']['dataset']}",
    })

    return domain_results


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the aiccon BigQuery dataset.")
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
    results = build_database(domains=domains, config=config)
    sys.exit(1 if any(r["status"] == "error" for r in results.values()) else 0)