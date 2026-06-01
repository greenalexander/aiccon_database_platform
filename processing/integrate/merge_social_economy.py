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
    bd_enace2_r3        enterprises by NACE and NUTS2 region

Note on dataset heterogeneity:
    ISTAT survey datasets classify observations by individual characteristics
    (age, gender, education, region) — NOT by legal form or NACE code.
    legal_form_unified and nace_code will be null for most ISTAT rows here.
    Eurostat datasets classify by NACE — legal_form_unified will be null there.
    This is correct and expected: the two sources answer different questions.

Output written to:
    {SHAREPOINT_ROOT}/aiccon-data/processed/social_economy/
"""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path
from datetime import datetime

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ingestion.loaders.base_loader import get_logger, load_config, raw_path
from processing.harmonise.nuts_mapper import harmonise_geo, load_nuts_mapping
from processing.harmonise.legal_form_normaliser import harmonise_legal_form, load_legal_form_mapping

logger = get_logger("merge.social_economy")

DOMAIN = "social_economy"

SOURCE_PRIORITY = {
    "istat":             1,
    "eurostat":          2,
    "runts":             3,
    "camere_commercio":  3,
    "ag_entrate":        4,
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
#
# Columns present in every processed row. Columns not applicable to a given
# dataset are included as pd.NA — this keeps the schema uniform across all
# sources and makes DuckDB loading straightforward.
#
# Survey-specific dimension columns (volunteering_form, activity_type, etc.)
# are preserved as-is from the raw data. They arrive already renamed via
# istat.py's RENAME_MAP, so no further translation is needed here.

OUTPUT_COLUMNS = [
    # Provenance
    "source_id",
    "dataset_code",
    "indicator_code",
    "value",
    "unit",
    # Time
    "time",
    # Geography (unified)
    "nuts_code",
    "nuts_level",
    "country_code",
    "nuts_name_it",
    "nuts_name_en",
    "macro_area",
    # Legal form (null for survey/NACE datasets)
    "legal_form_unified",
    "legal_form_unified_en",
    "ets_classification",
    # NACE (null for survey/legal-form datasets)
    "nace_code",
    "nace_label_en",
    # Demographics (null where not a dimension in the dataset)
    "gender",
    "age_group",
    # Survey dimensions — volunteering
    "volunteering_form",     # ORGVOL / DIRVOL / TOTAL
    "activity_type",
    "years_active",
    "education",
    "labour_status",
    "household_size",
    "econ_resources",
    "municipality_type",
    # Survey dimensions — organised volunteering
    "org_sector",
    "org_type",
    "motivation",
    "personal_impact",
    "multi_membership",
    # Survey dimensions — associationism
    "association_type",
    # Pipeline metadata
    "priority",
    "extracted_at",
    # QA flags (kept in processed layer for inspection, dropped before DB load)
    "geo_unmatched",
    "legal_form_unmatched",
]


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the DataFrame has exactly OUTPUT_COLUMNS in the right order.
    Missing columns are added as pd.NA; extra columns are dropped.
    """
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[OUTPUT_COLUMNS]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_latest_raw(raw_dir: Path, dataset_code: str) -> Path | None:
    """
    Return the most recently written parquet for a dataset code, or None.
    Raw files are named {dataset_code}_{YYYY-MM}.parquet.
    """
    matches = sorted(raw_dir.glob(f"{dataset_code}_*.parquet"), reverse=True)
    if not matches:
        logger.warning(f"No raw parquet found for dataset '{dataset_code}' in {raw_dir}")
        return None
    if len(matches) > 1:
        logger.debug(
            f"Multiple raw files for '{dataset_code}' — using most recent: {matches[0].name}"
        )
    return matches[0]


def _normalise_time(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure a 'time' column exists, trying common alternative names."""
    if "time" not in df.columns:
        for alt in ["time_period", "anno", "year"]:
            if alt in df.columns:
                df["time"] = df[alt].astype(str)
                return df
        logger.warning("No time column found — 'time' will be null.")
        df["time"] = pd.NA
    return df


# ── ISTAT processor ───────────────────────────────────────────────────────────

# Maps each ISTAT dataset code to the indicator_code and unit that describe
# what 'value' means in that dataset. Extend this dict as you add datasets.
ISTAT_DATASET_META: dict[str, tuple[str, str]] = {
    # Volunteering
    "85_84_DF_DCSA_VOLON1_1":   ("volunteering_rate",          "mixed"),
    # ^ this dataset contains multiple data_types (%, thousands, hours, abs count)
    # 'mixed' signals to PowerBI that the unit column must be read per row
    "85_84_DF_DCSA_VOLON1_2":   ("volunteering_activity_share", "percentage"),
    "85_84_DF_DCSA_VOLON1_3":   ("volunteering_years_share",    "percentage"),
    "85_84_DF_DCSA_VOLON1_4":   ("volunteering_rate",           "percentage"),
    "85_84_DF_DCSA_VOLON1_5":   ("volunteering_rate",           "percentage"),
    "85_84_DF_DCSA_VOLON1_6":   ("volunteering_rate",           "percentage"),
    # Organised volunteering
    "85_171_DF_DCSA_VOLON_ORG1_1": ("org_volunteering_sector_share",  "percentage"),
    "85_171_DF_DCSA_VOLON_ORG1_2": ("org_volunteering_orgtype_share", "percentage"),
    "85_171_DF_DCSA_VOLON_ORG1_3": ("org_volunteering_motivation_share", "percentage"),
    "85_171_DF_DCSA_VOLON_ORG1_4": ("org_volunteering_impact_share",  "percentage"),
    # Associationism
    "83_63_DF_DCCV_AVQ_PERSONE_129": ("association_membership_rate", "percentage"),
    "83_63_DF_DCCV_AVQ_PERSONE_130": ("association_membership_rate", "percentage"),
    "83_63_DF_DCCV_AVQ_PERSONE_131": ("association_membership_rate", "percentage"),
    "83_63_DF_DCCV_AVQ_PERSONE_132": ("association_membership_rate", "percentage"),
}


def process_istat_dataset(
    path: Path,
    dataset_code: str,
    nuts_mapping: pd.DataFrame,
    lf_mapping: pd.DataFrame,
    nace_labels: pd.DataFrame,
) -> pd.DataFrame:
    """
    Load one ISTAT raw parquet, harmonise geography, and reshape to output schema.

    Legal form harmonisation is attempted but will produce null values for most
    of these survey datasets — that's expected and correct.
    """
    logger.info(f"  Processing ISTAT {dataset_code}")
    df = pd.read_parquet(path)
    logger.info(f"    Loaded {len(df):,} rows")

    # Geography — try common column names
    geo_col = next(
        (c for c in ["geo", "itter107", "territory", "ref_area"] if c in df.columns),
        None,
    )
    if geo_col:
        df[geo_col] = df[geo_col].astype(str).str.strip().str.zfill(2)
        df = harmonise_geo(df, source="istat", mapping=nuts_mapping, geo_col=geo_col)
    else:
        logger.info(f"    {dataset_code}: no geo column — national-level dataset.")
        df["nuts_code"]    = "IT"
        df["nuts_level"]   = 0
        df["country_code"] = "IT"
        df["nuts_name_it"] = "Italia"
        df["nuts_name_en"] = "Italy"
        df["macro_area"]   = pd.NA
        df["geo_unmatched"] = False

    # Legal form — will be null for survey data, which is fine
    df = harmonise_legal_form(df, source="istat", mapping=lf_mapping)

    # NACE — not a dimension in these survey datasets
    df["nace_code"]    = pd.NA
    df["nace_label_en"] = pd.NA

    # Standard fields
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

    # Value
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
    # Regional employment by NACE aggregate O-U (NUTS0 + NUTS2, all EU countries)
    "nama_10r_3empers": ("n_employed",     "thousands_persons"),
    # National employment by detailed NACE sections (NUTS0 only, all EU countries)
    # Provides Q86/Q87/Q88/P85/S94/S96 breakdown not available at regional level
    "lfsa_egan22d":     ("n_employed",     "thousands_persons"),
    # Local units by NACE and NUTS2 region (replaces bd_enace2_r3)
    # indic_sbs=LOC_NR = number of local units
    "sbs_r_nuts2021":   ("n_local_units",  "count"),
}


def process_eurostat_dataset(
    path: Path,
    dataset_code: str,
    nace_labels: pd.DataFrame,
) -> pd.DataFrame:
    """
    Load one Eurostat raw parquet, harmonise geo, and reshape to output schema.
    No legal form dimension — those columns will be null.
    NACE codes from the nace_r2 column where present.
    """
    logger.info(f"  Processing Eurostat {dataset_code}")
    df = pd.read_parquet(path)
    logger.info(f"    Loaded {len(df):,} rows")

    df = harmonise_geo(df, source="eurostat")

    # NACE
    if "nace_r2" in df.columns:
        df["nace_code"]    = df["nace_r2"].astype(str).str.strip().str.upper()
        df["nace_label_en"] = df["nace_code"].map(nace_labels["label_en"].to_dict())
    else:
        df["nace_code"]    = pd.NA
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
    """
    Where multiple sources provide the same indicator for the same
    geo + time + dimension combination, keep the highest-priority row.

    In practice this mainly affects Italian NUTS0 employment figures
    that appear in both ISTAT and Eurostat.
    """
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
        logger.info(
            f"Priority resolution: removed {before - after:,} lower-priority duplicates."
        )
    return df


# ── Main merge function ───────────────────────────────────────────────────────

def merge_social_economy(config: dict) -> pd.DataFrame:
    """
    Load, harmonise, and merge all social economy raw parquet files.

    Returns a single long-format DataFrame in OUTPUT_COLUMNS order,
    ready to be written to the processed layer and loaded into DuckDB.
    """
    raw_dir      = raw_path(config, DOMAIN)
    nuts_mapping = load_nuts_mapping()
    lf_mapping   = load_legal_form_mapping()
    nace_labels  = load_nace_labels()

    frames: list[pd.DataFrame] = []

    # ── ISTAT ─────────────────────────────────────────────────────────────────
    logger.info("── ISTAT datasets ───────────────────────────────")
    for dataset_code in ISTAT_DATASET_META:
        path = _find_latest_raw(raw_dir, dataset_code)
        if path is None:
            continue
        df = process_istat_dataset(
            path, dataset_code, nuts_mapping, lf_mapping, nace_labels
        )
        if not df.empty:
            frames.append(df)

    # ── Eurostat ──────────────────────────────────────────────────────────────
    logger.info("── Eurostat datasets ────────────────────────────")
    for dataset_code in EUROSTAT_DATASET_META:
        path = _find_latest_raw(raw_dir, dataset_code)
        if path is None:
            continue
        df = process_eurostat_dataset(path, dataset_code, nace_labels)
        if not df.empty:
            frames.append(df)

    # ── Combine ───────────────────────────────────────────────────────────────
    if not frames:
        logger.error(
            "No data to merge — check that raw parquet files exist in "
            f"{raw_dir}"
        )
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"Combined {len(combined):,} rows from {len(frames)} datasets")

    combined = resolve_priority(combined)

# --- Power BI Compatible Type Harmonization ---
    # 1. Force numeric columns to standard float64 (handles NaNs safely for Power BI)
    combined['nuts_level'] = pd.to_numeric(combined['nuts_level'], errors='coerce').astype(float)
    combined['time'] = pd.to_numeric(combined['time'], errors='coerce').astype(float)
    
    # 2. Force text columns to be clean strings and fill NAs with empty strings
    # Power BI sometimes crashes when it finds 'None' objects in string columns
    str_cols = ['nuts_code', 'nuts_name_en', 'macro_area', 'indicator_code']
    for col in str_cols:
        if col in combined.columns:
            combined[col] = combined[col].fillna("").astype(str)

    logger.info(f"After priority resolution: {len(combined):,} rows")

    # ── Final cleanup ────────────────────────────────────────────
    # Remove eurostat aggregate geo codes that aren't meaningful territories
    combined["nuts_code"] = combined["nuts_code"].str.strip()
    combined = combined[~combined["nuts_code"].isin(["EU27_2020", "EU27", "EA", "EA19", "EA20"])]

    # Cast types to be PowerBI-safe
    combined["time"] = pd.to_numeric(combined["time"], errors="coerce").astype("Int64")
    combined["nuts_level"] = pd.to_numeric(combined["nuts_level"], errors="coerce").astype("Int64")

    logger.info(f"Final dataset: {len(combined):,} rows")

    return combined

"""
    # ── Final Geography Enrichment ────────────────────────────────────────────
    try:
        # Load mapping (your CSV uses semicolon and has extra empty columns at the end)
        nuts_ref = pd.read_csv(config['paths']['nuts_istat'], sep=';').iloc[:, :9]
        
        # Create a join key: prioritize nuts_code, fallback to istat_code (for NUTS 1)
        nuts_ref['join_key'] = nuts_ref['nuts_code'].fillna(nuts_ref['istat_code'])
        
        # Keep only what we need for the join
        nuts_ref = nuts_ref[['join_key', 'nuts_name_en', 'nuts_level', 'macro_area']].drop_duplicates()

        # Drop existing sparse columns to avoid duplicates/suffixes
        cols_to_overwrite = ['nuts_name_en', 'nuts_level', 'macro_area']
        combined = combined.drop(columns=[c for c in cols_to_overwrite if c in combined.columns])

        # Perform the merge
        combined = combined.merge(
            nuts_ref, 
            left_on='nuts_code', 
            right_on='join_key', 
            how='left'
        ).drop(columns=['join_key'])

        # Filter out the non-Italian EU aggregate you found
        combined['nuts_code'] = combined['nuts_code'].str.strip()
        combined = combined[combined['nuts_code'] != 'EU27_2020']

        # Now force types back to integer (this will work because NaNs are gone)
        combined['time'] = combined['time'].astype(int)
        combined['nuts_level'] = combined['nuts_level'].astype(int)        
        logger.info(f"Enriched geography and removed EU aggregate. Final rows: {len(combined):,}")
        
    except Exception as e:
        logger.error(f"Failed to enrich geography: {e}")
    
    logger.info(f"Final filtered dataset size: {len(combined):,} rows")

    return combined
"""

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from ingestion.loaders.sharepoint_loader import upload_processed

    config  = load_config()
    df      = merge_social_economy(config)

    if df.empty:
        print("No data to write.")
        sys.exit(1)

    # NEW: Generate a readable, dated filename
    datestamp = datetime.now().strftime("%Y_%m") # Produces "2026_04"
    filename = f"{DOMAIN}_{datestamp}.parquet"
    
    # Create a local path for the upload process
    tmp_path = Path(filename)

    # Save using the new name
    df.to_parquet(tmp_path, index=False, engine="pyarrow")
    
    dest = upload_processed(tmp_path, DOMAIN, config, logger)

    print(f"\nProcessed file → {dest}")
    print(f"Shape: {df.shape}")
    print(f"\nIndicator breakdown:")
    print(df.groupby(["source_id", "indicator_code", "unit"])
            .size()
            .rename("n_rows")
            .to_string())