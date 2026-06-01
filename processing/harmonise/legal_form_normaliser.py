"""
processing/harmonise/legal_form.py

Maps source-specific legal form codes to unified categories defined in
processing/mappings/legal_form_map.csv.

Sources encode legal form differently:
    - ISTAT  : dimension codes like "COOP_SOC_A", "ODV", "APS" (TIPOENTE or
               FORMAGIURIDICA dimension, varies by dataset)
    - RUNTS  : section codes A–G
    - Camere di Commercio: their own classification

The unified_category column in legal_form_map.csv is the common key used
in the database dimension table dim_legal_form.

Datasets that have no legal form dimension (e.g. Eurostat regional employment,
which is by NACE not legal form) are passed through unchanged — this module
adds a null legal_form_unified column so the schema stays consistent.

Unmatched codes are flagged rather than silently dropped, so you can see
what needs to be added to the mapping table.
_unmatched boolean column and log a warning listing the unknown codes.

GENDER_MAP covers every variant I've seen across ISTAT datasets: numeric codes 
("1", "2", "9"), letter codes ("M", "F", "T"), and full strings ("MALES", "TOTAL"). 

"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


# ── Load mapping table ────────────────────────────────────────────────────────

def load_legal_form_mapping(mappings_dir: Path | None = None) -> pd.DataFrame:
    """
    Load the legal_form_map.csv mapping table.

    Returns
    -------
    pd.DataFrame
        Mapping with columns: source_system, source_code, source_label_it,
        unified_category, unified_category_en, nace_primary, ets_classification.
    """
    if mappings_dir is None:
        here = Path(__file__).resolve()
        for parent in [here, *here.parents]:
            candidate = parent / "processing" / "mappings" / "legal_form_map.csv"
            if candidate.exists():
                mappings_dir = candidate.parent
                break
        else:
            raise FileNotFoundError(
                "Could not find processing/mappings/legal_form_map.csv."
            )

    path = mappings_dir / "legal_form_map.csv"
    df = pd.read_csv(path, dtype=str)
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)
    logger.info(f"Loaded legal form mapping: {len(df)} rows from {path}")
    return df


# ── Core mapping function ─────────────────────────────────────────────────────

def map_legal_forms(
    df: pd.DataFrame,
    source: str,
    mapping: pd.DataFrame,
    legal_form_col: str | None = None,
) -> pd.DataFrame:
    """
    Add unified legal form columns to a raw DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame from any source.
    source : str
        Source system identifier matching the source_system column in
        legal_form_map.csv: "istat", "runts", "camere_commercio", "eurostat".
    mapping : pd.DataFrame
        Pre-loaded legal_form_map.csv (from load_legal_form_mapping()).
    legal_form_col : str, optional
        Name of the legal form column in df. If None, the function tries
        common column names: 'legal_form', 'tipoente', 'formagiuridica',
        'tipocoop', 'sezregister'.

    Returns
    -------
    pd.DataFrame
        DataFrame with added columns:
            legal_form_unified     — unified category key (e.g. "cooperativa_sociale")
            legal_form_unified_en  — English label
            nace_primary           — primary NACE mapping for this legal form
            ets_classification     — "ets", "ets_eligible", "non_ets", or null
            legal_form_unmatched   — True if source code had no mapping (for QA)

    Notes
    -----
    Datasets with no legal form dimension (e.g. Eurostat employment by NACE)
    receive null values in all added columns. This is expected and correct —
    those datasets are classified by NACE, not by legal form.
    """
    df = df.copy()

    # Detect the legal form column if not specified
    if legal_form_col is None:
        candidates = ["legal_form", "tipoente", "formagiuridica", "tipocoop",
                      "sezregister", "forma_giuridica"]
        for col in candidates:
            if col in df.columns:
                legal_form_col = col
                logger.info(f"map_legal_forms: using '{col}' as legal form column.")
                break

    # If no legal form column exists, add null columns and return
    if legal_form_col is None or legal_form_col not in df.columns:
        logger.info(
            f"map_legal_forms ({source}): no legal form column found. "
            "Adding null legal form columns — this is expected for NACE-based datasets."
        )
        df["legal_form_unified"] = pd.NA
        df["legal_form_unified_en"] = pd.NA
        df["nace_primary"] = pd.NA
        df["ets_classification"] = pd.NA
        df["legal_form_unmatched"] = False
        return df

    # Filter mapping to this source system
    source_mapping = mapping[mapping["source_system"] == source].copy()

    if source_mapping.empty:
        logger.warning(
            f"map_legal_forms: no mapping rows found for source_system='{source}'. "
            "Check legal_form_map.csv. Adding null legal form columns."
        )
        df["legal_form_unified"] = pd.NA
        df["legal_form_unified_en"] = pd.NA
        df["nace_primary"] = pd.NA
        df["ets_classification"] = pd.NA
        df["legal_form_unmatched"] = True
        return df

    # Build lookup dictionaries keyed on uppercased source_code
    source_mapping = source_mapping.copy()
    source_mapping["source_code_upper"] = source_mapping["source_code"].str.upper()
    lk = source_mapping.set_index("source_code_upper")

    raw_codes = df[legal_form_col].astype(str).str.strip().str.upper()

    df["legal_form_unified"] = raw_codes.map(lk["unified_category"].to_dict())
    df["legal_form_unified_en"] = raw_codes.map(lk["unified_category_en"].to_dict())
    df["nace_primary"] = raw_codes.map(lk["nace_primary"].to_dict())
    df["ets_classification"] = raw_codes.map(lk["ets_classification"].to_dict())
    df["legal_form_unmatched"] = df["legal_form_unified"].isna()

    # Report unmatched codes
    unmatched_codes = df.loc[df["legal_form_unmatched"], legal_form_col].unique()
    if len(unmatched_codes) > 0:
        logger.warning(
            f"map_legal_forms ({source}): {len(unmatched_codes)} codes not in mapping: "
            f"{sorted(unmatched_codes)[:20]}. "
            "Add these to processing/mappings/legal_form_map.csv."
        )
    else:
        logger.info(
            f"map_legal_forms ({source}): all {len(df):,} rows matched successfully."
        )

    return df


# ── Gender normalisation ──────────────────────────────────────────────────────

# ISTAT gender codes vary slightly across datasets
GENDER_MAP = {
    "1": "male",
    "2": "female",
    "9": "total",
    "T": "total",
    "M": "male",
    "F": "female",
    "MF": "total",
    "TOTAL": "total",
    "MALES": "male",
    "FEMALES": "female",
}


def normalise_gender(df: pd.DataFrame, gender_col: str = "gender") -> pd.DataFrame:
    """
    Normalise gender codes to consistent English labels: male / female / total.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with a gender column.
    gender_col : str
        Name of the gender column. Also tries 'sesso' if not found.

    Returns
    -------
    pd.DataFrame
        DataFrame with a standardised `gender` column.
        Original column preserved as `gender_raw` if it had a different name.
    """
    df = df.copy()

    # Find the gender column
    if gender_col not in df.columns:
        for alt in ["sesso", "sex", "sexe"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "gender_raw"})
                gender_col = "gender_raw"
                break
        else:
            logger.info("normalise_gender: no gender column found — skipping.")
            return df

    raw = df[gender_col].astype(str).str.strip().str.upper()
    df["gender"] = raw.map(GENDER_MAP)

    unmatched = df["gender"].isna() & raw.notna() & (raw != "NAN")
    if unmatched.any():
        odd = raw[unmatched].unique().tolist()
        logger.warning(
            f"normalise_gender: {unmatched.sum()} rows have unrecognised gender codes: "
            f"{odd}. These will be null in output."
        )

    return df


# ── Convenience wrapper ───────────────────────────────────────────────────────

def harmonise_legal_form(
    df: pd.DataFrame,
    source: str,
    mapping: pd.DataFrame | None = None,
    legal_form_col: str | None = None,
    normalise_gender_col: bool = True,
) -> pd.DataFrame:
    """
    Full legal form harmonisation: map legal forms + normalise gender.

    This is the function called by the integration pipeline.

    Parameters
    ----------
    df : pd.DataFrame
        Raw or partially processed DataFrame.
    source : str
        Source system: "istat", "eurostat", "runts", "camere_commercio".
    mapping : pd.DataFrame, optional
        Pre-loaded legal_form_map.csv. Loaded from disk if not provided.
    legal_form_col : str, optional
        Override for the legal form column name.
    normalise_gender_col : bool
        Whether to also normalise the gender column. Default True.

    Returns
    -------
    pd.DataFrame
        DataFrame with unified legal form and gender columns.

    Example
    -------
    >>> mapping = load_legal_form_mapping()
    >>> df = harmonise_legal_form(df_raw, source="istat", mapping=mapping)
    """
    if mapping is None:
        mapping = load_legal_form_mapping()

    df = map_legal_forms(df, source=source, mapping=mapping,
                         legal_form_col=legal_form_col)

    if normalise_gender_col:
        df = normalise_gender(df)

    return df