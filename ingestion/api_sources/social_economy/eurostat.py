"""
ingestion/api_sources/social_economy/eurostat.py

Fetches social economy relevant data from the Eurostat SDMX-JSON API.

Datasets fetched (configured in config/settings.yaml):
    - nama_10r_3empers  : Regional employment by NACE activity (NUTS2)
    - bd_enace2_r3      : Enterprises by NACE and region (NUTS2)

Eurostat API reference:
    https://wikis.ec.europa.eu/display/EUROSTATHELP/API+Statistics+-+data+query

Output:
    One parquet file per dataset written to:
    {SHAREPOINT_ROOT}/aiccon-data/raw/social_economy/

Run directly to fetch all configured datasets:
    python -m ingestion.api_sources.social_economy.eurostat
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Allow running as a script from the project root
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from ingestion.loaders.base_loader import BaseLoader, get_logger, load_config

logger = get_logger("eurostat.social_economy")


# ── Constants ─────────────────────────────────────────────────────────────────

# Eurostat SDMX-JSON endpoint
# Full URL pattern:
#   /statistics/1.0/data/{dataset}?{dimension}={codes}&format=JSON&lang=EN
EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# NACE codes relevant to the social economy
SOCIAL_ECONOMY_NACE_AGG = ["O-U"]
SOCIAL_ECONOMY_NACE_NAGG = ["P85", "Q86","Q87","Q88", "S94", "S96"]

# We want data for all EU/EEA countries plus UK (for historical comparison)
# Empty string means "all available" in the Eurostat API
ALL_GEOS = ""


# ── Response parser ───────────────────────────────────────────────────────────

def parse_eurostat_response(data: dict, dataset_code: str) -> pd.DataFrame:
    """
    Convert a raw Eurostat SDMX-JSON response into a tidy long-format DataFrame.

    Eurostat returns data in a compact format where:
    - `dimension` describes every axis (time, geo, nace, unit, etc.)
    - `value` is a flat dict mapping a position index string to the numeric value

    This function reconstructs the full cartesian coordinates for each observation.

    Parameters
    ----------
    data : dict
        Parsed JSON response from the Eurostat API.
    dataset_code : str
        The dataset identifier, used for logging and metadata.

    Returns
    -------
    pd.DataFrame
        Tidy DataFrame with one row per observation and columns for each dimension.
    """
    try:
        dims = data["dimension"]
        values = data["value"]
    except KeyError as e:
        raise ValueError(
            f"Unexpected Eurostat response structure for {dataset_code}: missing key {e}"
        ) from e

    if not values:
        logger.warning(f"{dataset_code}: API returned zero observations.")
        return pd.DataFrame()

    # Build ordered list of dimension names and their code→label maps
    dim_names = list(dims.keys())
    dim_sizes = [len(dims[d]["category"]["index"]) for d in dim_names]

    # category["index"] maps code → position; invert to position → code
    dim_pos_to_code = []
    dim_pos_to_label = []
    for d in dim_names:
        cat = dims[d]["category"]
        pos_to_code = {v: k for k, v in cat["index"].items()}
        pos_to_label = cat.get("label", {})  # not always present
        dim_pos_to_code.append(pos_to_code)
        dim_pos_to_label.append(pos_to_label)

    # Reconstruct rows from the flat value index
    # The flat index encodes position as: i0 * (s1*s2*...) + i1 * (s2*...) + ... + iN
    rows = []
    total = 1
    for s in dim_sizes:
        total *= s

    # Pre-compute strides for each dimension (row-major order)
    strides = []
    stride = 1
    for s in reversed(dim_sizes):
        strides.insert(0, stride)
        stride *= s

    for flat_idx_str, obs_value in values.items():
        flat_idx = int(flat_idx_str)
        row = {}
        remaining = flat_idx
        for i, (name, stride_val) in enumerate(zip(dim_names, strides)):
            pos = remaining // stride_val
            remaining = remaining % stride_val
            code = dim_pos_to_code[i].get(pos, f"unknown_{pos}")
            label = dim_pos_to_label[i].get(code, "")
            row[name] = code
            if label and label != code:
                row[f"{name}_label"] = label
        row["value"] = obs_value
        rows.append(row)

    df = pd.DataFrame(rows)

    # Standardise column names to lowercase
    df.columns = [c.lower() for c in df.columns]

    logger.info(f"{dataset_code}: parsed {len(df):,} observations.")
    return df


# ── API fetch helpers ─────────────────────────────────────────────────────────

def fetch_dataset_single(
    session,
    dataset_code: str,
    filters: dict[str, list[str]],
    base_url: str = EUROSTAT_BASE,
) -> dict:
    """
    Fetch a single Eurostat dataset and return the raw parsed JSON.

    Filters must already be scoped to a size the API will accept. 
    Call fetch_dataset_paginated() to handle large datasets automatically. 
    """

    url = f"{base_url}/{dataset_code}"

    # Build query params — Eurostat accepts repeated params for multi-value filters
    params = [("format", "JSON"), ("lang", "EN")]
    for dim, codes in filters.items():
        if codes:
            for code in codes:
                params.append((dim, code))

    logger.debug(f"GET {url} | params: {params}")
    response = session.get_with_retry(url, params=params)
    return response.json()

def fetch_dataset_paginated(
    session,
    dataset_code: str,
    base_filters: dict[str, list[str]],
    paginate_on: str,
    base_url: str = EUROSTAT_BASE,
) -> pd.DataFrame:
    
    """
    Fetch a large Eurostat dataset by splitting on one dimension to avoid 413 errors. 

    Eurostat enforces a response size limit. Requesting all countries x all years x multiple
    NACE codes in one call exceeds it. This function fetches one value of paginate_on
    at a time and concatenates results.

    Parameters
    ----------
    session:
        Retry-wrapped requests session. 
    dataset_code: str
        Eurostat dataset identifier.
    base_filters: dict
        Filters to apply on every request. The dimension named by "paginate_on" 
        must be a list with more than one value - it will be split across requests.
    paginate_on: str
        The dimension name to paginate over, e.g., "nace_r2" 
    base_url: str
        Eurostat API base URL

    Returns
    -------
    pd.DataFrame
        Concatenated results from all paginated requests.
    """

    paginate_values = base_filters.get(paginate_on, [])
    if not paginate_values:
        raise ValueError(
            f"fetch_dataset_paginated: '{paginate_on}' not found in base_filters "
            f"or has no values. Got: {base_filters}"
        )
    
    frames = []
    for value in paginate_values:
        filters = {**base_filters, paginate_on: [value]}
        logger.info(f"Fetching {dataset_code} [{paginate_on}={value}]")
        try:
            data = fetch_dataset_single(session, dataset_code, filters, base_url)
            df = parse_eurostat_response(data, f"{dataset_code}[{value}]")
            if not df.empty:
                frames.append(df)
        except Exception as e:
            logger.warning(f"{dataset_code} [{paginate_on}={value}]: failed - {e}. Skipping."
            )

    if not frames:
        logger.warning(f"{dataset_code}: all paginated requests failed or returned empty.")
        return pd.DataFrame()
    
    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"{dataset_code}: combined {len(combined):,} rows across {len(frames)} requests")
    return combined

# ── Dataset-specific fetch functions ──────────────────────────────────────────

def fetch_regional_employment(session, config: dict) -> pd.DataFrame:
    """
    Fetch nama_10r_3empers: Regional employment by NACE activity.

    Covers employment (thousands of persons) by NACE Rev.2 aggregated sections
    at NUTS0 and NUTS2 level for all available years and countries.

    Fetched one NACE code at a time to stay under Eurostat's 413 size limit.

    Notes
    -----
    - NUTS2 regions change over time (NUTS 2016, 2021 vintages).
      We fetch all and let the harmonisation step handle vintage mapping.
    - Unit THS = thousands of persons employed.
    """
    dataset_code = "nama_10r_3empers"
    logger.info(f"Fetching {dataset_code} (paginating by NACE code)")

    df = fetch_dataset_paginated(
        session,
        dataset_code,
        base_filters={
            "nace_r2": SOCIAL_ECONOMY_NACE_AGG,  # paginate on this dimension
            "unit": ["THS"],              # thousands of persons
            "time": ["2021", "2022", "2023", "2024"],
            "wstatus": ["EMP"],             # MANDATORY: EMP = Employed persons
        },
        paginate_on="nace_r2",
    )

    if df.empty:
        return df
    
    if "geo" in df.columns:
        df["nuts_level"] = df["geo"].str.len().map({2: 0, 3: 1, 4: 2, 5: 3})

    df.attrs["dataset_code"] = dataset_code
    df.attrs["description"] = "Regional employment by NACE (NUTS2), thousands of persons"
    return df

def fetch_national_employment(session, config: dict) -> pd.DataFrame:
    """
    Fetch lfsa_egan22d: Employed persons by detailed economic activity (NACE Rev. 2 two-digit level).

    Covers employment (thousands of persons) by NACE Rev.2 detailed sections
    at NUTS0 (national) level only.

    Fetched one NACE code at a time to stay under Eurostat's 413 size limit.

    Notes
    -----
    - Unit THS_PER = thousands of persons employed.
    """
    dataset_code = "lfsa_egan22d"
    logger.info(f"Fetching {dataset_code} (paginating by NACE code)")

    df = fetch_dataset_paginated(
        session,
        dataset_code,
        base_filters={
            "nace_r2": SOCIAL_ECONOMY_NACE_NAGG,  # paginate on this dimension
            "unit": ["THS_PER"],              # thousands of persons
            "time": ["2021", "2022", "2023", "2024"],
        },
        paginate_on="nace_r2",
    )

    if df.empty:
        return df
    
    if "geo" in df.columns:
        df["nuts_level"] = df["geo"].str.len().map({2: 0, 3: 1, 4: 2, 5: 3})

    df.attrs["dataset_code"] = dataset_code
    df.attrs["description"] = "Regional employment by NACE (NUTS2), thousands of persons"
    return df


def fetch_enterprises_by_nace(session, config: dict) -> pd.DataFrame:
    """
    Fetch sbs_r_nuts2021: Number of local units by NACE and NUTS2 region.
    
    Fetched one NACE code at a time to stay under Eurostat's 413 size limit.

    Notes
    -----
    - Unit: number of local units
    - S94 ommitted

    """

    dataset_code = "sbs_r_nuts2021"
    logger.info(f"Fetching {dataset_code} (paginating by NACE code)")

    df=fetch_dataset_paginated(
        session,
        dataset_code,
        base_filters = {
                "freq": ["A"],                    # Annual frequency
                "nace_r2": ["P85", "Q86", "Q87", "Q88", "S96"],            # Sections
                "indic_sbs": ["LOC_NR"],           # Number of local units
                "time": ["2021", "2022", "2023"]
            },
        paginate_on="nace_r2",
    )

    if df.empty:
        return df

    if "geo" in df.columns:
        df["nuts_level"] = df["geo"].str.len().map({2: 0, 3: 1, 4: 2, 5: 3})

    df.attrs["dataset_code"] = dataset_code
    df.attrs["description"] = "Number of enterprises by NACE and NUTS2 region"
    return df

# ── Main loader class ─────────────────────────────────────────────────────────

class EurostatSocialEconomyLoader(BaseLoader):
    """
    Fetches all configured Eurostat datasets for the social economy domain
    and saves them as raw parquet files.
    """

    SOURCE_ID = "eurostat"
    DOMAIN = "social_economy"

    def fetch(self) -> list[pd.DataFrame]:
        """
        Fetch Eurostat social economy datasets, paginated to avoid 413 error.

        """
        results = []

        # 1. Regional employment by NACE (primary comparative dataset)
        df_emp = fetch_regional_employment(self.session, self.config)
        results.append(df_emp)

        # 1.5. National employment by NACE (for comparison and to fill gaps in regional data)
        df_emp_nat = fetch_national_employment(self.session, self.config)
        results.append(df_emp_nat)

        # 2. Enterprises by NACE and region
        df_ent = fetch_enterprises_by_nace(self.session, self.config)
        results.append(df_ent)

        non_empty = [df for df in results if not df.empty]
        self.logger.info(
            f"Fetched {len(non_empty)}/{len(results)} datasets successfully."
        )
        return results


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    config = load_config()
    loader = EurostatSocialEconomyLoader(config=config)
    written = loader.run()
    print(f"\nDone. Files written:")
    for p in written:
        print(f"  {p}")