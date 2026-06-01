"""
processing/integrate/merge_social_economy.py

Combines harmonised data from ISTAT and Eurostat for the social economy domain
into a single processed parquet file, applying priority rules where sources overlap.

ISTAT datasets (volunteering and associationism surveys):
    Volunteering (Indagine sul Volontariato, feb 2026):
        85_84_DF_DCSA_VOLON1_1   volunteering rate by gender, age, form
        85_84_DF_DCSA_VOLON1_2   activity type by gender and form
        85_84_DF_DCSA_VOLON1_3   years active by gender and form
        85_84_DF_DCSA_VOLON1_4   by educational level and labour status
        85_84_DF_DCSA_VOLON1_5   by household size and economic resources
        85_84_DF_DCSA_VOLON1_6   by region and municipality type

    Organised volunteering (same survey, organisational lens):
        85_171_DF_DCSA_VOLON_ORG1_1   by sector and multi-membership
        85_171_DF_DCSA_VOLON_ORG1_2   by institutional type (ODV, APS, …)
        85_171_DF_DCSA_VOLON_ORG1_3   by motivation
        85_171_DF_DCSA_VOLON_ORG1_4   by personal impact

    Associationism (Aspetti della Vita Quotidiana, set 2025):
        83_63_DF_DCCV_AVQ_PERSONE_129   by age
        83_63_DF_DCCV_AVQ_PERSONE_130   by age and educational level
        83_63_DF_DCCV_AVQ_PERSONE_131   by occupational status
        83_63_DF_DCCV_AVQ_PERSONE_132   by region and municipality type

Eurostat datasets (regional employment context):
    nama_10r_3empers    regional employment by NACE aggregate O-U (NUTS2)
    sbs_r_nuts2021      local units by NACE and NUTS2 region

Output written to:
    GCS: aiccon-data/processed/social_economy/
"""

from __future__ import annotations

import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.loaders.base_loader import get_logger, load_config, raw_gcs_prefix
from processing.harmonise.nuts_mapper import harmonise_geo, load_nuts_mapping
from processing.harmonise.legal_form_normaliser import harmonise_legal_form, load_legal_form_mapping

logger = get_logger("merge.social_economy")

DOMAIN = "social_economy"

SOURCE_PRIORITY = {
    "istat":            1,
    "eurostat":         2,
    "runts":            3,
    "camere_commercio": 3,
    "ag_entrate":       4,
}


# ── Load reference tables ─────────────────────────────────────────────────────

def _find_mappings_dir() -> Path:
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        candidate = parent / "processing" / "mappings"
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Could not find processing/mappings/ directory.")


def load_nace_labels(mappings_dir: Path | None = None) -> pd.DataFrame:
    d = mappings_dir or _find_mappings_dir()
    df = pd.read_csv(d / "nace_labels.csv", dtype=str)
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)
    return df.set_index("nace_code")


# ── Output schema ─────────────────────────────────────────────────────────────

OUTPUT_COLUMNS = [
    "source_id", "dataset_code", "indicator_code", "value", "unit",
    "time",
    "nuts_code", "nuts_level", "country_code", "nuts_name_it", "nuts_name_en", "macro_area",
    "legal_form_unified", "legal_form_unified_en", "ets_classification",
    "nace_code", "nace_label_en",
    "gender", "age_group",
    "volunteering_form", "activity_type", "years_active",
    "education", "labour_status", "household_size", "econ_resources", "municipality_type",
    "org_sector", "org_type", "motivation", "personal_impact", "multi_membership",
    "association_type",
    "priority", "extracted_at",
    "geo_unmatched", "legal_form_unmatched",
]


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[OUTPUT_COLUMNS]


# ── GCS raw file helpers ──────────────────────────────────────────────────────

def _find_latest_raw_gcs(config: dict, dataset_code: str) -> str | None:
    """
    Find the most recently written raw parquet for a dataset code in GCS.
    Raw files are named {dataset_code}_{YYYY-MM}.parquet — lexicographic sort
    is chronological because of the date suffix.
    Returns a GCS URI or None.
    """
    bucket_name = config["gcs"]["bucket"]
    prefix_uri  = raw_gcs_prefix(config, DOMAIN)
    blob_prefix = prefix_uri[len(f"gs://{bucket_name}/"):]

    client = storage.Client()
    blobs  = [
        b for b in client.list_blobs(bucket_name, prefix=blob_prefix)
        if b.name.endswith(".parquet") and
           Path(b.name).stem.startswith(dataset_code)
    ]

    if not blobs:
        logger.warning(f"No raw parquet found for dataset '{dataset_code}' in {prefix_uri}")
        return None

    blobs.sort(key=lambda b: b.name, reverse=True)
    if len(blobs) > 1:
        logger.debug(
            f"Multiple raw files for '{dataset_code}' — "
            f"using most recent: {blobs[0].name}"
        )

    return f"gs://{bucket_name}/{blobs[0].name}"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise_time(df: pd.DataFrame) -> pd.DataFrame:
    if "time" not in df.columns:
        for alt in ["time_period", "anno", "year"]:
            if alt in df.columns:
                df["time"] = df[alt].astype(str)
                return df
        logger.warning("No time column found — 'time' will be null.")
        df["time"] = pd.NA
    return df


# ── ISTAT processor ───────────────────────────────────────────────────────────

ISTAT_DATASET_META: dict[str, tuple[str, str]] = {
    "85_84_DF_DCSA_VOLON1_1":        ("volunteering_rate",                  "mixed"),
    "85_84_DF_DCSA_VOLON1_2":        ("volunteering_activity_share",         "percentage"),
    "85_84_DF_DCSA_VOLON1_3":        ("volunteering_years_share",            "percentage"),
    "85_84_DF_DCSA_VOLON1_4":        ("volunteering_rate",                   "percentage"),
    "85_84_DF_DCSA_VOLON1_5":        ("volunteering_rate",                   "percentage"),
    "85_84_DF_DCSA_VOLON1_6":        ("volunteering_rate",                   "percentage"),
    "85_171_DF_DCSA_VOLON_ORG1_1":   ("org_volunteering_sector_share",       "percentage"),
    "85_171_DF_DCSA_VOLON_ORG1_2":   ("org_volunteering_orgtype_share",      "percentage"),
    "85_171_DF_DCSA_VOLON_ORG1_3":   ("org_volunteering_motivation_share",   "percentage"),
    "85_171_DF_DCSA_VOLON_ORG1_4":   ("org_volunteering_impact_share",       "percentage"),
    "83_63_DF_DCCV_AVQ_PERSONE_129": ("association_membership_rate",         "percentage"),
    "83_63_DF_DCCV_AVQ_PERSONE_130": ("association_membership_rate",         "percentage"),
    "83_63_DF_DCCV_AVQ_PERSONE_131": ("association_membership_rate",         "percentage"),
    "83_63_DF_DCCV_AVQ_PERSONE_132": ("association_membership_rate",         "percentage"),
}


def process_istat_dataset(
    uri: str,
    dataset_code: str,
    nuts_mapping: pd.DataFrame,
    lf_mapping: pd.DataFrame,
    nace_labels: pd.DataFrame,
) -> pd.DataFrame:
    """Load one ISTAT raw parquet from GCS, harmonise geography, reshape to output schema."""
    logger.info(f"  Processing ISTAT {dataset_code}")
    df = pd.read_parquet(uri, engine="pyarrow")
    logger.info(f"    Loaded {len(df):,} rows")

    geo_col = next(
        (c for c in ["geo", "itter107", "territory", "ref_area"] if c in df.columns),
        None,
    )
    if geo_col:
        df[geo_col] = df[geo_col].astype(str).str.strip().str.zfill(2)
        df = harmonise_geo(df, source="istat", mapping=nuts_mapping, geo_col=geo_col)
    else:
        logger.info(f"    {dataset_code}: no geo column — national-level dataset.")
        df["nuts_code"]     = "IT"
        df["nuts_level"]    = 0
        df["country_code"]  = "IT"
        df["nuts_name_it"]  = "Italia"
        df["nuts_name_en"]  = "Italy"
        df["macro_area"]    = pd.NA
        df["geo_unmatched"] = False

    df = harmonise_legal_form(df, source="istat", mapping=lf_mapping)

    df["nace_code"]     = pd.NA
    df["nace_label_en"] = pd.NA

    df["source_id"]    = "istat"
    df["dataset_code"] = dataset_code
    df["priority"]     = SOURCE_PRIORITY["istat"]

    indicator_code, unit = ISTAT_DATASET_META.get(
        dataset_code, ("unknown_indicator", "unknown_unit")
    )
    if indicator_code == "unknown_indicator":
        logger.warning(
            f"    {dataset_code} not in ISTAT_DATASET_META — "
            "add it to get correct indicator_code and unit."
        )
    df["indicator_code"] = indicator_code
    df["unit"]           = unit

    df = _normalise_time(df)

    if "value" not in df.columns:
        logger.warning(f"    {dataset_code}: no 'value' column — skipping.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    result = enforce_schema(df)
    logger.info(f"    → {len(result):,} rows after processing")
    return result


# ── Eurostat processor ────────────────────────────────────────────────────────

EUROSTAT_DATASET_META: dict[str, tuple[str, str]] = {
    "nama_10r_3empers": ("n_employed",    "thousands_persons"),
    "lfsa_egan22d":     ("n_employed",    "thousands_persons"),
    "sbs_r_nuts2021":   ("n_local_units", "count"),
}


def process_eurostat_dataset(
    uri: str,
    dataset_code: str,
    nace_labels: pd.DataFrame,
) -> pd.DataFrame:
    """Load one Eurostat raw parquet from GCS, harmonise geo, reshape to output schema."""
    logger.info(f"  Processing Eurostat {dataset_code}")
    df = pd.read_parquet(uri, engine="pyarrow")
    logger.info(f"    Loaded {len(df):,} rows")

    df = harmonise_geo(df, source="eurostat")

    if "nace_r2" in df.columns:
        df["nace_code"]     = df["nace_r2"].astype(str).str.strip().str.upper()
        df["nace_label_en"] = df["nace_code"].map(nace_labels["label_en"].to_dict())
    else:
        df["nace_code"]     = pd.NA
        df["nace_label_en"] = pd.NA

    df = _normalise_time(df)

    df["source_id"]    = "eurostat"
    df["dataset_code"] = dataset_code
    df["priority"]     = SOURCE_PRIORITY["eurostat"]
    df["gender"]       = pd.NA

    indicator_code, unit = EUROSTAT_DATASET_META.get(
        dataset_code, ("unknown_indicator", "unknown_unit")
    )
    df["indicator_code"] = indicator_code
    df["unit"]           = unit

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    result = enforce_schema(df)
    logger.info(f"    → {len(result):,} rows after processing")
    return result


# ── Priority resolution ───────────────────────────────────────────────────────

def resolve_priority(df: pd.DataFrame) -> pd.DataFrame:
    dedup_keys = [
        "indicator_code", "nuts_code", "time",
        "legal_form_unified", "nace_code", "gender",
        "age_group", "volunteering_form", "org_type",
    ]
    keys = [k for k in dedup_keys if k in df.columns]

    before = len(df)
    df = (
        df.sort_values("priority", ascending=True)
          .drop_duplicates(subset=keys, keep="first")
    )
    after = len(df)
    if before > after:
        logger.info(f"Priority resolution: removed {before - after:,} lower-priority duplicates.")
    return df


# ── Main merge function ───────────────────────────────────────────────────────

def merge_social_economy(config: dict) -> pd.DataFrame:
    """
    Load, harmonise, and merge all social economy raw parquet files from GCS.

    Returns a single long-format DataFrame in OUTPUT_COLUMNS order,
    ready to be written to the processed layer and loaded into BigQuery.
    """
    nuts_mapping = load_nuts_mapping()
    lf_mapping   = load_legal_form_mapping()
    nace_labels  = load_nace_labels()

    frames: list[pd.DataFrame] = []

    # ── ISTAT ─────────────────────────────────────────────────────────────────
    logger.info("── ISTAT datasets ───────────────────────────────")
    for dataset_code in ISTAT_DATASET_META:
        uri = _find_latest_raw_gcs(config, dataset_code)
        if uri is None:
            continue
        df = process_istat_dataset(uri, dataset_code, nuts_mapping, lf_mapping, nace_labels)
        if not df.empty:
            frames.append(df)

    # ── Eurostat ──────────────────────────────────────────────────────────────
    logger.info("── Eurostat datasets ────────────────────────────")
    for dataset_code in EUROSTAT_DATASET_META:
        uri = _find_latest_raw_gcs(config, dataset_code)
        if uri is None:
            continue
        df = process_eurostat_dataset(uri, dataset_code, nace_labels)
        if not df.empty:
            frames.append(df)

    # ── Combine ───────────────────────────────────────────────────────────────
    if not frames:
        logger.error("No data to merge — check that raw parquet files exist in GCS.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"Combined {len(combined):,} rows from {len(frames)} datasets")

    combined = resolve_priority(combined)

    combined["nuts_level"] = pd.to_numeric(combined["nuts_level"], errors="coerce").astype(float)
    combined["time"]       = pd.to_numeric(combined["time"],       errors="coerce").astype(float)

    str_cols = ["nuts_code", "nuts_name_en", "macro_area", "indicator_code"]
    for col in str_cols:
        if col in combined.columns:
            combined[col] = combined[col].fillna("").astype(str)

    logger.info(f"After priority resolution: {len(combined):,} rows")

    combined["nuts_code"] = combined["nuts_code"].str.strip()
    combined = combined[
        ~combined["nuts_code"].isin(["EU27_2020", "EU27", "EA", "EA19", "EA20"])
    ]

    combined["time"]       = pd.to_numeric(combined["time"],       errors="coerce").astype("Int64")
    combined["nuts_level"] = pd.to_numeric(combined["nuts_level"], errors="coerce").astype("Int64")

    logger.info(f"Final dataset: {len(combined):,} rows")
    return combined


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from ingestion.loaders.gcs_uploader import upload_processed

    config = load_config()
    df     = merge_social_economy(config)

    if df.empty:
        print("No data to write.")
        sys.exit(1)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False, prefix=f"{DOMAIN}_") as tmp:
        tmp_path = Path(tmp.name)

    df.to_parquet(tmp_path, index=False, engine="pyarrow")
    dest = upload_processed(tmp_path, DOMAIN, config, logger,
                            dest_filename=f"{DOMAIN}_{datetime.now().strftime('%Y-%m')}.parquet")
    tmp_path.unlink()

    print(f"\nProcessed file → {dest}")
    print(f"Shape: {df.shape}")
    print("\nIndicator breakdown:")
    print(
        df.groupby(["source_id", "indicator_code", "unit"])
          .size()
          .rename("n_rows")
          .to_string()
    )