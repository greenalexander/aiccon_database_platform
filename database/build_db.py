"""
database/build_db.py

Loads processed parquet files from GCS into BigQuery.

What this script does, in order:
    1. Checks GCS and BigQuery are reachable
    2. Creates dimension tables if they do not exist (idempotent)
    3. Creates fact tables if they do not exist (idempotent)
    4. Populates dimension tables from mapping CSVs (MERGE — safe to re-run)
    5. Loads each active domain's processed parquet into its fact table
    6. Writes pipeline log to GCS

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
) -> int:
    """
    Load a pandas DataFrame into a BigQuery table via a parquet temp file.

    Derives the pyarrow schema from the BigQuery table definition so that
    all columns -- including all-null ones -- are written with the correct
    type. String columns are normalised (float NaN -> None) before serialisation
    so pyarrow does not raise ArrowTypeError on mixed-type object columns.

    For temporary staging tables that do not exist yet in BigQuery, falls back
    to inference-based typing.
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
        bq_table = client.get_table(table_id)
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

def populate_dim_geography(client: bigquery.Client, config: dict, mappings_dir: Path) -> int:
    """
    Populate dim_geography from nuts_istat.csv using MERGE (upsert).
    Safe to re-run — existing rows are not duplicated.
    """
    path = mappings_dir / "nuts_istat.csv"
    df = pd.read_csv(path, dtype=str)
    df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)

    rows = []
    for _, r in df.iterrows():
        # Some NUTS1 rows have nuts_code empty with the code in istat_code instead
        nuts_code = r.get("nuts_code") or r.get("istat_code") or None
        # Skip multi-code rows like 'ITH1;ITH2' — can't map to a single geo_key
        if nuts_code and ";" in str(nuts_code):
            continue
        rows.append({
            "nuts_code":         nuts_code,
            "nuts_level":        int(r["nuts_level"]) if pd.notna(r.get("nuts_level")) else None,
            "nuts_name_it":      r.get("nuts_name_it") or None,
            "nuts_name_en":      r.get("nuts_name_en") or None,
            "istat_code":        r.get("istat_code")   or None,
            "country_code":      "IT",
            "region_name":       r.get("region_name")  or None,
            "macro_area":        r.get("macro_area")   or None,
            "municipality_type": None,  # always null for CSV rows
            "geo_source":        "nuts_istat_csv",
            "is_active":         True,
        })

    geo_df = pd.DataFrame(rows)
    geo_df = geo_df[geo_df["nuts_code"].notna()]
    geo_df = geo_df.drop_duplicates(subset=["nuts_code"], keep="last")
    geo_df["nuts_level"] = pd.array(geo_df["nuts_level"], dtype="Int64")
    # Assign surrogate keys starting from 1
    geo_df.insert(0, "geo_key", range(1, len(geo_df) + 1))
    full = _full_table(config, "dim_geography")

    tmp_table = _full_table(config, "_tmp_geo_load")
    _load_df_to_bq(client, geo_df, tmp_table, write_disposition="WRITE_TRUNCATE")

    client.query(f"""
        MERGE `{full}` T
        USING `{tmp_table}` S ON T.nuts_code = S.nuts_code
        WHEN NOT MATCHED THEN INSERT (
            geo_key, nuts_code, nuts_level, nuts_name_it, nuts_name_en, istat_code,
            country_code, region_name, macro_area, municipality_type, geo_source, is_active
        ) VALUES (
            S.geo_key, S.nuts_code, S.nuts_level, S.nuts_name_it, S.nuts_name_en, S.istat_code,
            S.country_code, S.region_name, S.macro_area, S.municipality_type,
            S.geo_source, S.is_active
        )
    """).result()

    client.delete_table(tmp_table, not_found_ok=True)

    n = _run_query(client, f"SELECT COUNT(*) FROM `{full}`")[0][0]
    logger.info(f"  dim_geography: {n} rows")
    return n


def populate_dim_time(
    client: bigquery.Client,
    config: dict,
    year_range: tuple[int, int] = (1990, 2030),
) -> int:
    """Populate dim_time with annual rows. Safe to re-run."""
    rows = [
        {
            "year":             year,
            "period_type":      "A",
            "period_label":     str(year),
            "reference_period": str(year),
        }
        for year in range(year_range[0], year_range[1] + 1)
    ]
    time_df = pd.DataFrame(rows)
    time_df.insert(0, "time_key", range(1, len(time_df) + 1))
    full = _full_table(config, "dim_time")
    tmp_table = _full_table(config, "_tmp_time_load")
    _load_df_to_bq(client, time_df, tmp_table, write_disposition="WRITE_TRUNCATE")

    client.query(f"""
        MERGE `{full}` T
        USING `{tmp_table}` S ON T.year = S.year
        WHEN NOT MATCHED THEN INSERT (time_key, year, period_type, period_label, reference_period)
        VALUES (S.time_key, S.year, S.period_type, S.period_label, S.reference_period)
    """).result()

    client.delete_table(tmp_table, not_found_ok=True)

    n = _run_query(client, f"SELECT COUNT(*) FROM `{full}`")[0][0]
    logger.info(f"  dim_time: {n} rows ({year_range[0]}–{year_range[1]})")
    return n


def populate_dim_source(client: bigquery.Client, config: dict, mappings_dir: Path) -> int:
    """Populate dim_source from domain_sources.csv. Safe to re-run."""
    path = mappings_dir / "domain_sources.csv"
    df = pd.read_csv(path, dtype=str)
    df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)
    df["priority"] = pd.to_numeric(df["priority"], errors="coerce")
    if "provider" not in df.columns:
        df["provider"] = df["source_id"].str.split("_").str[0]

    full = _full_table(config, "dim_source")
    df.insert(0, "source_key", range(1, len(df) + 1))
    tmp_table = _full_table(config, "_tmp_source_load")
    _load_df_to_bq(client, df, tmp_table, write_disposition="WRITE_TRUNCATE")

    client.query(f"""
        MERGE `{full}` T
        USING `{tmp_table}` S ON T.source_id = S.source_id
        WHEN NOT MATCHED THEN INSERT (
            source_key, source_id, provider, source_name, source_name_it, domain, access_type,
            priority, update_frequency, territorial_levels, temporal_coverage, notes
        ) VALUES (
            S.source_key, S.source_id, S.provider, S.source_name, S.source_name_it, S.domain,
            S.access_type, S.priority, S.update_frequency, S.territorial_levels,
            S.temporal_coverage, S.notes
        )
    """).result()

    client.delete_table(tmp_table, not_found_ok=True)

    n = _run_query(client, f"SELECT COUNT(*) FROM `{full}`")[0][0]
    logger.info(f"  dim_source: {n} rows")
    return n


def populate_dim_legal_form(client: bigquery.Client, config: dict, mappings_dir: Path) -> int:
    """Populate dim_legal_form from legal_form_map.csv. Safe to re-run."""
    path = mappings_dir / "legal_form_map.csv"
    df = pd.read_csv(path, dtype=str)
    df = df.apply(lambda c: c.str.strip() if c.dtype == object else c)

    full = _full_table(config, "dim_legal_form")
    df.insert(0, "legal_form_key", range(1, len(df) + 1))
    tmp_table = _full_table(config, "_tmp_legal_form_load")
    _load_df_to_bq(client, df, tmp_table, write_disposition="WRITE_TRUNCATE")

    client.query(f"""
        MERGE `{full}` T
        USING `{tmp_table}` S
            ON T.unified_category = S.unified_category
            AND T.source_system = S.source_system
        WHEN NOT MATCHED THEN INSERT (
            legal_form_key, unified_category, unified_category_en, nace_primary,
            ets_classification, source_system, source_code, source_label_it
        ) VALUES (
            S.legal_form_key, S.unified_category, S.unified_category_en, S.nace_primary,
            S.ets_classification, S.source_system, S.source_code, S.source_label_it
        )
    """).result()

    client.delete_table(tmp_table, not_found_ok=True)

    n = _run_query(client, f"SELECT COUNT(*) FROM `{full}`")[0][0]
    logger.info(f"  dim_legal_form: {n} rows")
    return n


def populate_dim_indicator(client: bigquery.Client, config: dict) -> int:
    """
    Populate dim_indicator with all known indicator rows. Safe to re-run.

    These rows were previously in dimensions.sql as INSERT OR IGNORE statements,
    which BigQuery does not support. They are defined here instead and loaded
    via MERGE so re-runs do not create duplicates.
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
    ind_df.insert(0, "indicator_key", range(1, len(ind_df) + 1))

    full      = _full_table(config, "dim_indicator")
    tmp_table = _full_table(config, "_tmp_indicator_load")
    _load_df_to_bq(client, ind_df, tmp_table, write_disposition="WRITE_TRUNCATE")

    client.query(f"""
        MERGE `{full}` T
        USING `{tmp_table}` S ON T.indicator_code = S.indicator_code
        WHEN NOT MATCHED THEN INSERT (
            indicator_key, indicator_code, label_it, label_en, unit_default, domain, notes
        ) VALUES (
            S.indicator_key, S.indicator_code, S.label_it, S.label_en,
            S.unit_default, S.domain, S.notes
        )
    """).result()

    client.delete_table(tmp_table, not_found_ok=True)

    n = _run_query(client, f"SELECT COUNT(*) FROM `{full}`")[0][0]
    logger.info(f"  dim_indicator: {n} rows")
    return n


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
            "Add these to the INSERT OR IGNORE block in dimensions.sql."
        )
    df = df.dropna(subset=["indicator_key"])
    df["indicator_key"] = df["indicator_key"].astype(int)

    # 5. LEGAL FORM (nullable — null is expected for most rows)
    if "legal_form_unified" in df.columns:
        lf_map = _build_legal_form_lookup(client, config)
        df["legal_form_key"] = df["legal_form_unified"].map(
            lambda v: lf_map.get((v, None)) if pd.notna(v) else None
        )
    else:
        df["legal_form_key"] = None

    return df


# ── Domain loaders ────────────────────────────────────────────────────────────

@domain_loader("social_economy")
def _load_social_economy(client: bigquery.Client, config: dict) -> int:
    """Load the social economy processed parquet into fact_social_economy."""
    uri = _find_latest_processed(config, "social_economy")
    if uri is None:
        logger.warning("social_economy: no processed parquet found — skipping.")
        return 0

    logger.info(f"  Loading {uri}")
    df = pd.read_parquet(uri, engine="pyarrow")
    logger.info(f"  Read {len(df):,} rows from processed parquet")

    qa_cols = ["geo_unmatched", "legal_form_unmatched"]
    df = df.drop(columns=[c for c in qa_cols if c in df.columns])

    # Geo filter: keep Italian territories and NUTS0 country totals only
    if "nuts_code" in df.columns:
        mask = df["nuts_code"].str.startswith("IT", na=False) | df["nuts_code"].str.len().eq(2)
        before = len(df)
        df = df[mask].copy()
        logger.info(f"  Filtered out {before - len(df)} granular non-IT rows.")

    geo_lookup = _build_geo_lookup(client, config)
    if "nuts_code" in df.columns:
        geo_lookup = _upsert_geo_rows(
            client, config, df["nuts_code"].dropna().unique().tolist(), geo_lookup
        )

    df = _resolve_keys(df=df, client=client, config=config, domain="social_economy")
    if df.empty:
        logger.warning("social_economy: all rows dropped during key resolution.")
        return 0

    start_key = _next_surrogate_key(client, config, "fact_social_economy", "fact_key")
    df["fact_key"] = range(start_key, start_key + len(df))

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

    full = _full_table(config, "fact_social_economy")
    _load_df_to_bq(client, df, full, write_disposition="WRITE_APPEND")

    n = _run_query(client, f"SELECT COUNT(*) FROM `{full}`")[0][0]
    logger.info(f"  fact_social_economy: {n:,} rows total")
    return n


@domain_loader("labour")
def _load_labour(client: bigquery.Client, config: dict) -> int:
    """
    Load the labour processed parquet into fact_labour.

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

    start_key = _next_surrogate_key(client, config, "fact_labour", "fact_key")
    df["fact_key"] = range(start_key, start_key + len(df))

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

    full = _full_table(config, "fact_labour")
    _load_df_to_bq(client, df, full, write_disposition="WRITE_APPEND")

    n = _run_query(client, f"SELECT COUNT(*) FROM `{full}`")[0][0]
    logger.info(f"  fact_labour: {n:,} rows total")
    return n


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
#     start_key = _next_surrogate_key(client, config, "fact_immigration", "fact_key")
#     df["fact_key"] = range(start_key, start_key + len(df))
#
#     fact_columns = [
#         "fact_key", "geo_key", "time_key", "source_key", "indicator_key",
#         "value", "unit",
#         "nationality", "permit_type", "migration_flow",
#         "gender", "age_group",
#         "dataset_code", "extracted_at",
#     ]
#     available = [c for c in fact_columns if c in df.columns]
#     df = df[available]
#
#     full = _full_table(config, "fact_immigration")
#     _load_df_to_bq(client, df, full, write_disposition="WRITE_APPEND")
#     n = _run_query(client, f"SELECT COUNT(*) FROM `{full}`")[0][0]
#     logger.info(f"  fact_immigration: {n:,} rows total")
#     return n


# ── Main build function ───────────────────────────────────────────────────────

def build_database(
    domains: list[str] | None = None,
    config: dict | None = None,
) -> None:
    """
    Build the aiccon BigQuery dataset from processed parquet files in GCS.

    Parameters
    ----------
    domains : list[str], optional
        Specific domains to load. If None, loads all domains that are both
        registered in DOMAIN_LOADERS and enabled in settings.yaml.
    config : dict, optional
        Pre-loaded config. Loaded from disk if not provided.
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
    build_database(domains=domains, config=config)
    sys.exit(0)