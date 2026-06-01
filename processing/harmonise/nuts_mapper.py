"""
processing/harmonise/nuts_mapper.py

Harmonises geographic codes from all data sources to a unified NUTS-based key.

Sources use different geographic identifiers:
    - Eurostat : NUTS codes directly (IT, ITC4, ITC41, ...)
    - ISTAT    : own territorial codes (001, 01, ITA, ...) via ITTER107 dimension

This module loads the nuts_istat.csv mapping table and provides functions
to add a unified `nuts_code` and `nuts_level` column to any raw DataFrame.

Unmatched codes are flagged rather than silently dropped, so you can see
what needs to be added to the mapping table.

Eurostat already uses NUTS codes, so there's no lookup needed. The harmonise_eurostat_geo
function still adds the standardised nuts_code, nuts_level, and country_code columns so 
the output schema is identical to the ISTAT path.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


# ── Load mapping table ────────────────────────────────────────────────────────

def load_nuts_mapping(mappings_dir: Path | None = None) -> pd.DataFrame:
    """
    Load the nuts_istat.csv mapping table.

    Parameters
    ----------
    mappings_dir : Path, optional
        Directory containing nuts_istat.csv.
        Defaults to processing/mappings/ relative to the project root.

    Returns
    -------
    pd.DataFrame
        Mapping table with istat_code, nuts_code, nuts_level, nuts_name_it,
        nuts_name_en, region_name, macro_area columns.
    """
    if mappings_dir is None:
        here = Path(__file__).resolve()
        # Walk up to find processing/mappings/
        for parent in [here, *here.parents]:
            candidate = parent / "processing" / "mappings" / "nuts_istat.csv"
            if candidate.exists():
                mappings_dir = candidate.parent
                break
        else:
            raise FileNotFoundError(
                "Could not find processing/mappings/nuts_istat.csv. "
                "Run from the project root."
            )

    path = mappings_dir / "nuts_istat.csv"
    df = pd.read_csv(path, dtype=str)

    # Strip whitespace that sometimes creeps into CSV editors
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)

    logger.info(f"Loaded NUTS mapping: {len(df)} rows from {path}")
    return df


# ── Eurostat geo harmonisation ────────────────────────────────────────────────

def harmonise_eurostat_geo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Harmonise geographic codes in a Eurostat raw DataFrame.

    Eurostat already uses NUTS codes in the `geo` column, so this function
    just validates them, infers the NUTS level from code length, and adds
    standardised column names consistent with the ISTAT pipeline.

    Parameters
    ----------
    df : pd.DataFrame
        Raw Eurostat DataFrame with a `geo` column.

    Returns
    -------
    pd.DataFrame
        DataFrame with added columns:
            nuts_code   — same as geo for Eurostat (already NUTS)
            nuts_level  — 0, 1, 2, or 3 inferred from code length
            country_code — first two characters of geo (ISO 3166-1 alpha-2)
    """
    if "geo" not in df.columns:
        logger.warning("harmonise_eurostat_geo: no 'geo' column found — skipping.")
        return df

    df = df.copy()
    df["nuts_code"] = df["geo"].str.strip().str.upper()

    # NUTS level from code length: IT=2→0, ITC=3→1, ITC4=4→2, ITC41=5→3
    code_len = df["nuts_code"].str.len()
    df["nuts_level"] = code_len.map({2: 0, 3: 1, 4: 2, 5: 3})

    # Flag codes that don't match any known NUTS length (e.g. "EA", "EU27_2020")
    non_nuts = df["nuts_level"].isna()
    if non_nuts.any():
        odd_codes = df.loc[non_nuts, "nuts_code"].unique().tolist()
        logger.info(
            f"harmonise_eurostat_geo: {non_nuts.sum()} rows have non-standard geo codes "
            f"(aggregates like EA, EU27): {odd_codes[:10]}. "
            "These are kept but nuts_level will be null."
        )

    df["country_code"] = df["nuts_code"].str[:2]

    return df


# ── ISTAT geo harmonisation ───────────────────────────────────────────────────

def harmonise_istat_geo(
    df: pd.DataFrame,
    mapping: pd.DataFrame,
    geo_col: str = "geo",
) -> pd.DataFrame:
    """
    Harmonise geographic codes in an ISTAT raw DataFrame.

    ISTAT uses its own territorial codes (ITTER107 dimension), which this
    function maps to NUTS codes using the nuts_istat.csv mapping table.

    Parameters
    ----------
    df : pd.DataFrame
        Raw ISTAT DataFrame. The geographic column may be named 'geo',
        'itter107', or 'territory' depending on the dataset — pass the
        actual name as geo_col.
    mapping : pd.DataFrame
        Loaded nuts_istat.csv mapping table (from load_nuts_mapping()).
    geo_col : str
        Name of the geographic code column in df.

    Returns
    -------
    pd.DataFrame
        DataFrame with added columns:
            nuts_code    — mapped NUTS code (null if unmatched)
            nuts_level   — 0, 1, 2, or 3
            nuts_name_it — Italian place name
            nuts_name_en — English place name
            country_code — always "IT" for ISTAT data
            macro_area   — Nord-Ovest, Nord-Est, Centro, Sud, Isole
            geo_unmatched — True if the ISTAT code had no mapping (for QA)
    """
    if geo_col not in df.columns:
        # Try common alternative column names
        for alt in ["itter107", "territory", "ref_area"]:
            if alt in df.columns:
                geo_col = alt
                logger.info(f"harmonise_istat_geo: using '{geo_col}' as geo column.")
                break
        else:
            logger.warning(
                f"harmonise_istat_geo: geo column '{geo_col}' not found and no "
                "alternative found. Skipping geo harmonisation."
            )
            return df

    df = df.copy()

    # Normalise the ISTAT code — strip whitespace and uppercase
    raw_geo = df[geo_col].astype(str).str.strip().str.upper()

    # Build lookup: istat_code (uppercased) → mapping row
    lookup = mapping.copy()
    lookup["istat_code_upper"] = lookup["istat_code"].str.upper()
    lookup = lookup.set_index("istat_code_upper")

    # Map each code
    cols_to_add = ["nuts_code", "nuts_level", "nuts_name_it", "nuts_name_en",
                   "region_name", "macro_area"]

    mapped = raw_geo.map(lookup["nuts_code"].to_dict())
    df["nuts_code"] = mapped
    df["nuts_level"] = raw_geo.map(lookup["nuts_level"].to_dict())
    df["nuts_name_it"] = raw_geo.map(lookup["nuts_name_it"].to_dict())
    df["nuts_name_en"] = raw_geo.map(lookup["nuts_name_en"].to_dict())
    df["region_name"] = raw_geo.map(lookup["region_name"].to_dict())
    df["macro_area"] = raw_geo.map(lookup["macro_area"].to_dict())
    df["country_code"] = "IT"
    df["geo_unmatched"] = df["nuts_code"].isna()

    # Report unmatched codes so the mapping table can be extended
    unmatched = df.loc[df["geo_unmatched"], geo_col].unique()
    if len(unmatched) > 0:
        logger.warning(
            f"harmonise_istat_geo: {len(unmatched)} ISTAT geo codes not found in "
            f"nuts_istat.csv mapping: {sorted(unmatched)[:20]}. "
            "Add these to processing/mappings/nuts_istat.csv."
        )
    else:
        logger.info("harmonise_istat_geo: all geo codes matched successfully.")

    matched = (~df["geo_unmatched"]).sum()
    logger.info(
        f"harmonise_istat_geo: {matched:,}/{len(df):,} rows matched to NUTS codes."
    )

    return df


# ── Convenience dispatcher ────────────────────────────────────────────────────

def harmonise_geo(
    df: pd.DataFrame,
    source: str,
    mapping: pd.DataFrame | None = None,
    geo_col: str = "geo",
) -> pd.DataFrame:
    """
    Dispatch geo harmonisation based on source name.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame to harmonise.
    source : str
        Source identifier: "eurostat" or "istat".
    mapping : pd.DataFrame, optional
        Pre-loaded NUTS mapping table. If None, loads from disk.
        Pass a pre-loaded table when processing multiple datasets in a loop
        to avoid re-reading the CSV each time.
    geo_col : str
        Name of the geographic column (used for ISTAT only).

    Returns
    -------
    pd.DataFrame
        DataFrame with standardised geo columns added.

    Example
    -------
    >>> mapping = load_nuts_mapping()
    >>> df_eurostat = harmonise_geo(df_raw, source="eurostat")
    >>> df_istat = harmonise_geo(df_raw, source="istat", mapping=mapping)
    """
    if source == "eurostat":
        return harmonise_eurostat_geo(df)
    elif source == "istat":
        if mapping is None:
            mapping = load_nuts_mapping()
        return harmonise_istat_geo(df, mapping, geo_col=geo_col)
    else:
        raise ValueError(
            f"harmonise_geo: unknown source '{source}'. Expected 'eurostat' or 'istat'."
        )