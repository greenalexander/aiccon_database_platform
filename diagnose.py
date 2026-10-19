import pandas as pd
from ingestion.loaders.base_loader import load_config
from processing.integrate.merge_labour import _find_latest_raw_gcs
from database.build_db import _find_latest_processed

cfg  = load_config()
code = "150_915_DF_DCCV_TAXOCCU1_5"   # employment rate by province

raw = pd.read_parquet(_find_latest_raw_gcs(cfg, code))
geo = next(c for c in ["geo", "itter107", "ref_area", "territory"] if c in raw.columns)
print(f"RAW: column '{geo}', {raw[geo].nunique()} distinct codes, first 40:")
print(sorted(raw[geo].astype(str).str.strip().unique())[:40])

proc = pd.read_parquet(_find_latest_processed(cfg, "labour"))
sub  = proc[proc["dataset_code"] == code]
print(f"\nPROCESSED: {len(sub):,} rows")
print(sub.groupby(["nuts_level", "geo_unmatched"], dropna=False).size())
print(sub["nuts_code"].value_counts(dropna=False).head(15))