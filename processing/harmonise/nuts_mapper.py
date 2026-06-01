"""
processing/harmonise/nuts_mapper.py

Harmonises geographic codes from all data sources to a unified NUTS-based key.

Sources use different geographic identifiers:
    - Eurostat : NUTS codes directly (IT, ITC4, ITC41, ...)
    - ISTAT    : own territorial codes (001, 01, ITA, ...) via ITTER107 dimension
                 Some ISTAT datasets (notably the RFL labour series) use NUTS-style
                 codes directly (ITC11, ITC12 ...) rather than numeric ISTAT codes.

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
            nuts_code    — same as geo for Eurostat (already NUTS)
            nuts_level   — 0, 1, 2, or 3 inferred from code length
            country_code — first two characters of geo (ISO 3166-1 alpha-2)
    """
    if "geo" not in df.columns:
        logger.warning("harmonise_eurostat_geo: no 'geo' column found — skipping.")
        return df

    df = df.copy()
    df["nuts_code"] = df["geo"].str.strip().str.upper()

    code_len = df["nuts_code"].str.len()
    df["nuts_level"] = code_len.map({2: 0, 3: 1, 4: 2, 5: 3})

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

    Two lookup passes are attempted:
        1. Primary:  match raw geo code against istat_code column in the mapping.
                     This covers numeric codes like '001' (Torino), '01' (Piemonte).
        2. Fallback: match raw geo code against nuts_code column in the mapping.
                     This covers ISTAT labour (RFL) datasets that use NUTS-style
                     codes directly, e.g. 'ITC11', 'ITH55', rather than numeric codes.

    Parameters
    ----------
    df : pd.DataFrame
        Raw ISTAT DataFrame.
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
    raw_geo = df[geo_col].astype(str).str.strip().str.upper()

    # ── Primary lookup: istat_code → nuts_code ────────────────────────────────
    # Covers numeric ISTAT codes like '001', '037', 'ITA' (national)
    lookup_by_istat = mapping.copy()
    lookup_by_istat["_key"] = lookup_by_istat["istat_code"].str.upper()
    lookup_by_istat = lookup_by_istat.dropna(subset=["_key"])
    lookup_by_istat = lookup_by_istat.set_index("_key")

    # ── Fallback lookup: nuts_code → nuts_code (identity mapping) ────────────
    # Covers RFL labour datasets that already use NUTS codes like 'ITC11', 'ITH55'
    # Filter to rows with valid non-empty nuts_code and no semicolons
    lookup_by_nuts = mapping.copy()
    lookup_by_nuts["_key"] = lookup_by_nuts["nuts_code"].str.upper()
    lookup_by_nuts = lookup_by_nuts[
        lookup_by_nuts["_key"].notna() &
        (lookup_by_nuts["_key"] != "") &
        (~lookup_by_nuts["_key"].str.contains(";", na=False))
    ]
    lookup_by_nuts = lookup_by_nuts.drop_duplicates(subset=["_key"]).set_index("_key")

    cols = ["nuts_code", "nuts_level", "nuts_name_it", "nuts_name_en",
            "region_name", "macro_area"]

    # Apply primary lookup
    for col in cols:
        df[col] = raw_geo.map(lookup_by_istat[col].to_dict() if col in lookup_by_istat.columns else {})

    # Apply fallback for rows that didn't match the primary lookup
    unmatched_mask = df["nuts_code"].isna()
    if unmatched_mask.any():
        for col in cols:
            if col not in lookup_by_nuts.columns:
                continue
            fallback = raw_geo[unmatched_mask].map(lookup_by_nuts[col].to_dict())
            df.loc[unmatched_mask, col] = fallback
        n_recovered = (~df["nuts_code"].isna() & unmatched_mask).sum()
        if n_recovered > 0:
            logger.info(
                f"harmonise_istat_geo: {n_recovered:,} rows matched via NUTS code "
                "fallback lookup (RFL-style codes like ITC11, ITH55)."
            )

    df["country_code"] = "IT"
    df["geo_unmatched"] = df["nuts_code"].isna()

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