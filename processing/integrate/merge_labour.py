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

Note on source priority:
    For Italy, ISTAT is always preferred over Eurostat for overlapping indicators.
    Eurostat is used for all other countries and for the NACE-level employment
    breakdown, which ISTAT does not publish in this form.

Output written to:
    GCS: aiccon-data/processed/labour/
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.loaders.base_loader import get_logger, load_config, raw_gcs_prefix
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

OUTPUT_COLUMNS = [
    "source_id", "dataset_code", "indicator_code", "value", "unit",
    "time", "frequency", "period_label",
    "nuts_code", "nuts_level", "country_code", "nuts_name_it", "nuts_name_en", "macro_area",
    "legal_form_unified", "legal_form_unified_en", "ets_classification",
    "nace_code", "nace_label_en",
    "gender", "age_group",
    "education", "citizenship", "adjustment", "unemployment_duration",
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

ISTAT_DATASET_META: dict[str, tuple[str, str]] = {
    "150_873_DF_DCCV_FORZLVMENS1_1": ("labour_force",      "thousands_persons"),
    "172_931_DF_DCCV_NEET1_11":      ("neet_rate",         "percentage"),
    "172_931_DF_DCCV_NEET1_9":       ("neet_rate",         "percentage"),
    "152_913_DF_DCCV_TAXINATT1_5":   ("inactivity_rate",   "percentage"),
    "151_914_DF_DCCV_TAXDISOCCU1_8": ("unemployment_rate", "percentage"),
    "151_914_DF_DCCV_TAXDISOCCU1_6": ("unemployment_rate", "percentage"),
    "151_914_DF_DCCV_TAXDISOCCU1_5": ("unemployment_rate", "percentage"),
    "150_915_DF_DCCV_TAXOCCU1_5":    ("employment_rate",   "percentage"),
}


def process_istat_dataset(
    uri: str,
    dataset_code: str,
    nuts_mapping: pd.DataFrame,
    lf_mapping: pd.DataFrame,
    nace_labels: pd.DataFrame,
) -> pd.DataFrame:
    """
    Load one ISTAT raw parquet from GCS, harmonise geography, reshape to output schema.
    """
    logger.info(f"  Processing ISTAT {dataset_code}")
    df = pd.read_parquet(uri, engine="pyarrow")
    logger.info(f"    Loaded {len(df):,} rows")

    geo_col = next(
        (c for c in ["geo", "itter107", "ref_area", "territory"] if c in df.columns),
        None,
    )
    if geo_col:
        df[geo_col] = df[geo_col].astype(str).str.strip()
        df = harmonise_geo(df, source="istat", mapping=nuts_mapping, geo_col=geo_col)
    else:
        logger.info(f"    {dataset_code}: no geo column — assigning national (IT).")
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

    rename = {
        "gender":                "gender",
        "age_group":             "age_group",
        "education":             "education",
        "citizenship":           "citizenship",
        "adjustment":            "adjustment",
        "unemployment_duration": "unemployment_duration",
        "frequency":             "frequency",
    }
    for src, dst in rename.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]

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
    "lfsa_egan22d":      ("n_employed",        "thousands_persons"),
    "lfst_r_lfu3rt":     ("unemployment_rate", "percentage"),
    "lfst_r_lfur2gan":   ("unemployment_rate", "percentage"),
    "lfst_r_lfe2emprtn": ("employment_rate",   "percentage"),
}


def process_eurostat_dataset(
    uri: str,
    dataset_code: str,
    nace_labels: pd.DataFrame,
) -> pd.DataFrame:
    """
    Load one Eurostat raw parquet from GCS, harmonise geo, reshape to output schema.
    """
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

    col_map = {
        "sex":     "gender",
        "age":     "age_group",
        "isced11": "education",
        "citizen": "citizenship",
        "freq":    "frequency",
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
    ISTAT (priority=1) is always preferred for Italy.
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
        logger.info(f"Priority resolution: removed {before - after:,} lower-priority duplicates.")
    return df


# ── Main merge function ───────────────────────────────────────────────────────

def merge_labour(config: dict) -> pd.DataFrame:
    """
    Load, harmonise, and merge all labour raw parquet files from GCS.

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

    # Period label: copy full time value before truncating to year
    if "time" in combined.columns:
        time_str = combined["time"].astype(str)
        if "period_label" not in combined.columns:
            combined["period_label"] = pd.NA
        mask_no_label = combined["period_label"].isna() | (combined["period_label"] == "")
        combined.loc[mask_no_label, "period_label"] = time_str[mask_no_label]
        combined["time"] = time_str.str.extract(r"^(\d{4})", expand=False)

    combined["nuts_level"] = pd.to_numeric(combined["nuts_level"], errors="coerce").astype(float)
    combined["time"]       = pd.to_numeric(combined["time"],       errors="coerce").astype(float)

    str_cols = ["nuts_code", "nuts_name_en", "macro_area", "indicator_code",
                "frequency", "education", "citizenship"]
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
    import tempfile

    config = load_config()
    df     = merge_labour(config)

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