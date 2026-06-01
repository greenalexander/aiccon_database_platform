"""
processing/integrate/merge_labour.py

Combines harmonised data from ISTAT and Eurostat for the labour domain
into a single processed parquet file, applying priority rules where sources overlap.

ISTAT datasets (Rilevazione sulle Forze di Lavoro, RFL):

    Labour force:
        150_873_DF_DCCV_FORZLVMENS1_1   labour force 15+ (thousands), monthly,
                                         by sex and age — raw data, national only

    NEETs:
        172_931_DF_DCCV_NEET1_11        NEET rate 15–34, by sex, age and region (NUTS2)
        172_931_DF_DCCV_NEET1_9         NEET rate 15–34, by sex, age and citizenship,
                                         macro-areas only

    Inactivity rate:
        152_913_DF_DCCV_TAXINATT1_5     inactivity rate, by sex, age and province (NUTS3)

    Unemployment rate:
        151_914_DF_DCCV_TAXDISOCCU1_8   unemployment rate, by sex, age and province (NUTS3)
        151_914_DF_DCCV_TAXDISOCCU1_6   unemployment rate, by sex, age and education (NUTS2)
        151_914_DF_DCCV_TAXDISOCCU1_5   unemployment rate, by sex and detailed age (NUTS2)

    Employment rate:
        150_915_DF_DCCV_TAXOCCU1_5      employment rate, by sex, age and province (NUTS3)

Eurostat datasets (LFS regional and national):

    National employment by NACE activity:
        lfsa_egan22d        employed persons by NACE two-digit activity, sex and age
                            (national level, all EU countries)

    Regional unemployment rates (NUTS2, Italy + NUTS0 all countries):
        lfst_r_lfu3rt       unemployment rate by sex, age and educational attainment
        lfst_r_lfur2gan     unemployment rate by sex, age and citizenship

    Regional employment rates (NUTS2, Italy + NUTS0 all countries):
        lfst_r_lfe2emprtn   employment rate by sex, education and citizenship
                            (age omitted — returns total working-age aggregate)

Note on source priority:
    For Italy, ISTAT is always preferred over Eurostat for overlapping indicators
    (employment/unemployment/inactivity rates). Eurostat is used for all other
    countries and for the NACE-level employment breakdown, which ISTAT does not
    publish in this form.

Note on dataset heterogeneity:
    ISTAT datasets classify observations by individual characteristics (sex, age,
    region, education). nace_code will be null for all ISTAT rows here.
    Eurostat's lfsa_egan22d classifies by NACE — demographic columns will be
    partial or null there. This is correct: the two sources answer different questions.

Output written to:
    {SHAREPOINT_ROOT}/aiccon-data/processed/labour/
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ingestion.loaders.base_loader import get_logger, load_config, raw_path
from processing.harmonise.nuts_mapper import harmonise_geo, load_nuts_mapping
from processing.harmonise.legal_form_normaliser import harmonise_legal_form, load_legal_form_mapping

logger = get_logger("merge.labour")

DOMAIN = "labour"

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
    d  = mappings_dir or _find_mappings_dir()
    df = pd.read_csv(d / "nace_labels.csv", dtype=str)
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)
    return df.set_index("nace_code")


# ── Output schema ─────────────────────────────────────────────────────────────
#
# All rows from all sources conform to this column list. Columns not applicable
# to a given dataset are included as pd.NA so DuckDB loading stays clean.
#
# Survey/rate dimension columns (education, citizenship, adjustment, etc.)
# arrive already renamed via the RENAME_MAP in each ingestion script, so no
# further translation is needed here.

OUTPUT_COLUMNS = [
    # Provenance
    "source_id",
    "dataset_code",
    "indicator_code",
    "value",
    "unit",
    # Time
    "time",
    "frequency",            # A=annual, M=monthly, Q=quarterly
    "period_label",         # full sub-annual label: 'YYYY-MM' / 'YYYY-QN' / 'YYYY'
    # Geography (unified)
    "nuts_code",
    "nuts_level",
    "country_code",
    "nuts_name_it",
    "nuts_name_en",
    "macro_area",
    # Legal form (null for all labour datasets)
    "legal_form_unified",
    "legal_form_unified_en",
    "ets_classification",
    # NACE (populated for lfsa_egan22d; null for rate/NEET datasets)
    "nace_code",
    "nace_label_en",
    # Demographics
    "gender",
    "age_group",
    # Labour-specific dimensions
    "education",            # ISCED level (Eurostat) or edu_lev_highest (ISTAT)
    "citizenship",          # NAT/FOR/TOTAL (Eurostat) or ITL/FRG/TOTAL (ISTAT NEET)
    "adjustment",           # N=raw, Y=seasonally adjusted (ISTAT monthly only)
    "unemployment_duration", # always TOTAL in current datasets; kept for schema stability
    # Pipeline metadata
    "priority",
    "extracted_at",
    # QA flags
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
        logger.warning(
            f"No raw parquet found for dataset '{dataset_code}' in {raw_dir}"
        )
        return None
    if len(matches) > 1:
        logger.debug(
            f"Multiple raw files for '{dataset_code}' — "
            f"using most recent: {matches[0].name}"
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

# Maps each ISTAT dataset code to (indicator_code, unit).
# 'unit' describes what 'value' means; extend as new datasets are added.
ISTAT_DATASET_META: dict[str, tuple[str, str]] = {
    # Labour force (monthly, thousands)
    "150_873_DF_DCCV_FORZLVMENS1_1": ("labour_force",          "thousands_persons"),
    # NEETs
    "172_931_DF_DCCV_NEET1_11":      ("neet_rate",             "percentage"),
    "172_931_DF_DCCV_NEET1_9":       ("neet_rate",             "percentage"),
    # Inactivity rate
    "152_913_DF_DCCV_TAXINATT1_5":   ("inactivity_rate",       "percentage"),
    # Unemployment rate
    "151_914_DF_DCCV_TAXDISOCCU1_8": ("unemployment_rate",     "percentage"),
    "151_914_DF_DCCV_TAXDISOCCU1_6": ("unemployment_rate",     "percentage"),
    "151_914_DF_DCCV_TAXDISOCCU1_5": ("unemployment_rate",     "percentage"),
    # Employment rate
    "150_915_DF_DCCV_TAXOCCU1_5":    ("employment_rate",       "percentage"),
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

    Geography: ISTAT labour datasets use ITTER107 codes (REF_AREA column),
    which map to NUTS codes via harmonise_geo(). The monthly labour force
    dataset (FORZLVMENS1_1) is national-only (IT), so geo is assigned directly.

    Legal form: not a dimension in any of these datasets — will be null.
    NACE: not a dimension in ISTAT RFL datasets — will be null.
    """
    logger.info(f"  Processing ISTAT {dataset_code}")
    df = pd.read_parquet(path)
    logger.info(f"    Loaded {len(df):,} rows")

    # Geography
    geo_col = next(
        (c for c in ["geo", "itter107", "ref_area", "territory"] if c in df.columns),
        None,
    )
    if geo_col:
        df[geo_col] = df[geo_col].astype(str).str.strip()
        df = harmonise_geo(df, source="istat", mapping=nuts_mapping, geo_col=geo_col)
    else:
        # Monthly labour force dataset is national-only; no geo column
        logger.info(f"    {dataset_code}: no geo column — assigning national (IT).")
        df["nuts_code"]     = "IT"
        df["nuts_level"]    = 0
        df["country_code"]  = "IT"
        df["nuts_name_it"]  = "Italia"
        df["nuts_name_en"]  = "Italy"
        df["macro_area"]    = pd.NA
        df["geo_unmatched"] = False

    # Legal form — null for all RFL datasets
    df = harmonise_legal_form(df, source="istat", mapping=lf_mapping)

    # NACE — not a dimension in RFL datasets
    df["nace_code"]     = pd.NA
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

    # Rename ISTAT dimension columns to schema names where they differ
    rename = {
        "gender":                 "gender",        # already correct from RENAME_MAP
        "age_group":              "age_group",
        "education":              "education",
        "citizenship":            "citizenship",
        "adjustment":             "adjustment",
        "unemployment_duration":  "unemployment_duration",
        "frequency":              "frequency",
    }
    for src, dst in rename.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]

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

# Maps each Eurostat dataset code to (indicator_code, unit).
EUROSTAT_DATASET_META: dict[str, tuple[str, str]] = {
    # National employment by NACE two-digit activity (all EU countries)
    "lfsa_egan22d":      ("n_employed",        "thousands_persons"),
    # Regional unemployment rates (NUTS2 Italy + NUTS0 all countries)
    "lfst_r_lfu3rt":     ("unemployment_rate", "percentage"),
    "lfst_r_lfur2gan":   ("unemployment_rate", "percentage"),
    # Regional employment rates (NUTS2 Italy + NUTS0 all countries)
    "lfst_r_lfe2emprtn": ("employment_rate",   "percentage"),
}


def process_eurostat_dataset(
    path: Path,
    dataset_code: str,
    nace_labels: pd.DataFrame,
) -> pd.DataFrame:
    """
    Load one Eurostat raw parquet, harmonise geo, and reshape to output schema.

    No legal form dimension — those columns will be null.
    NACE codes are populated from the nace_r2 column where present (lfsa_egan22d).
    Education, citizenship and sex dimensions are passed through to schema columns.
    """
    logger.info(f"  Processing Eurostat {dataset_code}")
    df = pd.read_parquet(path)
    logger.info(f"    Loaded {len(df):,} rows")

    df = harmonise_geo(df, source="eurostat")

    # NACE — only populated for lfsa_egan22d
    if "nace_r2" in df.columns:
        df["nace_code"]     = df["nace_r2"].astype(str).str.strip().str.upper()
        df["nace_label_en"] = df["nace_code"].map(nace_labels["label_en"].to_dict())
    else:
        df["nace_code"]     = pd.NA
        df["nace_label_en"] = pd.NA

    # Map Eurostat dimension columns to schema names
    col_map = {
        "sex":      "gender",
        "age":      "age_group",
        "isced11":  "education",
        "citizen":  "citizenship",
        "freq":     "frequency",
    }
    for src, dst in col_map.items():
        if src in df.columns:
            df[dst] = df[src]

    df = _normalise_time(df)

    df["source_id"]    = "eurostat"
    df["dataset_code"] = dataset_code
    df["priority"]     = SOURCE_PRIORITY["eurostat"]

    indicator_code, unit = EUROSTAT_DATASET_META.get(
        dataset_code, ("unknown_indicator", "unknown_unit")
    )
    if indicator_code == "unknown_indicator":
        logger.warning(
            f"    {dataset_code} not in EUROSTAT_DATASET_META — "
            "add it to get correct indicator_code and unit."
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

    In practice this mainly affects Italian national (IT) employment and
    unemployment rates that appear in both ISTAT (province-level rolled up)
    and Eurostat (NUTS0). ISTAT (priority=1) is always preferred for Italy.
    """
    dedup_keys = [
        "indicator_code", "nuts_code", "time",
        "nace_code", "gender", "age_group",
        "education", "citizenship", "frequency",
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

def merge_labour(config: dict) -> pd.DataFrame:
    """
    Load, harmonise, and merge all labour raw parquet files.

    Returns a single long-format DataFrame in OUTPUT_COLUMNS order,
    ready to be written to the processed layer and loaded into DuckDB / Power BI.
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

    # ── Power BI type harmonisation ───────────────────────────────────────────
    # The monthly labour force series (FORZLVMENS1_1) stores time as 'YYYY-MM'.
    # Power BI and dim_time both work on integer years, so we need to:
    #   1. Copy the full value to period_label before destroying it
    #   2. Extract just the 4-digit year for the time column
    # Annual and quarterly rows already have a plain year (or year.0 float),
    # so the extract is safe for all rows.
    if "time" in combined.columns:
        time_str = combined["time"].astype(str)
        # Populate period_label from the full time value where not already set
        if "period_label" not in combined.columns:
            combined["period_label"] = pd.NA
        mask_no_label = combined["period_label"].isna() | (combined["period_label"] == "")
        combined.loc[mask_no_label, "period_label"] = time_str[mask_no_label]
        # Extract year (first 4 digits) for the numeric time column
        combined["time"] = time_str.str.extract(r"^(\d{4})", expand=False)

    # Force numeric columns to float64 first (handles NaN safely)
    combined["nuts_level"] = pd.to_numeric(
        combined["nuts_level"], errors="coerce"
    ).astype(float)
    combined["time"] = pd.to_numeric(
        combined["time"], errors="coerce"
    ).astype(float)

    # Force key text columns to clean strings; Power BI crashes on bare None objects
    str_cols = ["nuts_code", "nuts_name_en", "macro_area", "indicator_code",
                "frequency", "education", "citizenship"]
    for col in str_cols:
        if col in combined.columns:
            combined[col] = combined[col].fillna("").astype(str)

    logger.info(f"After priority resolution: {len(combined):,} rows")

    # ── Final cleanup ──────────────────────────────────────────────────────────
    # Remove Eurostat aggregate geo codes that aren't meaningful territories
    combined["nuts_code"] = combined["nuts_code"].str.strip()
    combined = combined[
        ~combined["nuts_code"].isin(["EU27_2020", "EU27", "EA", "EA19", "EA20"])
    ]

    # Promote floats to nullable integers for cleaner Power BI display
    combined["time"]       = pd.to_numeric(
        combined["time"], errors="coerce"
    ).astype("Int64")
    combined["nuts_level"] = pd.to_numeric(
        combined["nuts_level"], errors="coerce"
    ).astype("Int64")

    logger.info(f"Final dataset: {len(combined):,} rows")
    return combined


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from ingestion.loaders.sharepoint_loader import upload_processed

    config = load_config()
    df     = merge_labour(config)

    if df.empty:
        print("No data to write.")
        sys.exit(1)

    datestamp = datetime.now().strftime("%Y_%m")
    filename  = f"{DOMAIN}_{datestamp}.parquet"
    tmp_path  = Path(filename)

    df.to_parquet(tmp_path, index=False, engine="pyarrow")

    dest = upload_processed(tmp_path, DOMAIN, config, logger)

    print(f"\nProcessed file → {dest}")
    print(f"Shape: {df.shape}")
    print("\nIndicator breakdown:")
    print(
        df.groupby(["source_id", "indicator_code", "unit"])
          .size()
          .rename("n_rows")
          .to_string()
    )