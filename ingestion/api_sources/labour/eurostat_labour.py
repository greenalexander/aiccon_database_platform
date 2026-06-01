"""
ingestion/api_sources/labour/eurostat.py

Fetches labour market data from the Eurostat SDMX-JSON API.

Datasets fetched:

  National employment by detailed NACE activity
    - lfsa_egan22d       : Employed persons by NACE Rev.2 two-digit activity,
                           sex and age. National level (NUTS0). All macro-
                           sections of the economy (sections A–U plus TOTAL).

  Regional unemployment rates (NUTS2)
    - lfst_r_lfu3rt      : Unemployment rate by sex, age and educational
                           attainment level (NUTS2)
    - lfst_r_lfur2gan    : Unemployment rate by sex, age and citizenship
                           (national / foreign) (NUTS2)

  Regional employment rates (NUTS2)
    - lfst_r_lfe2emprtn  : Employment rate by sex, educational attainment
                           and citizenship (NUTS2). Age dimension omitted to
                           keep response size manageable.

Geography strategy
    All four datasets cover NUTS0 (national totals) for all available
    countries. Sub-national breakdown (NUTS2) is fetched only for Italy
    (geo codes starting with "IT"), since the dashboard's territorial
    analysis is Italy-focused. For all other countries, only national
    totals are retained.

Eurostat API reference:
    https://wikis.ec.europa.eu/display/EUROSTATHELP/API+Statistics+-+data+query

Output:
    One parquet file per dataset written to:
    {SHAREPOINT_ROOT}/aiccon-data/raw/labour/

Run directly to fetch all configured datasets:
    python -m ingestion.api_sources.labour.eurostat
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from ingestion.loaders.base_loader import BaseLoader, get_logger, load_config

logger = get_logger("eurostat.labour")


# ── Constants ─────────────────────────────────────────────────────────────────

EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# ── NACE codes for lfsa_egan22d ───────────────────────────────────────────────
# All two-digit codes available in the dataset, grouped by NACE Rev.2 section.
# lfsa_egan22d does not publish section-letter aggregates (A, B, C…), so we
# request every two-digit code plus the overall TOTAL and paginate by section
# group to stay under Eurostat's response-size limit.
#
# Pagination groups — each group becomes one API request:
NACE_BY_SECTION: dict[str, list[str]] = {
    "TOTAL":  ["TOTAL"],                                # All activities
    "A":      ["A01", "A02", "A03"],                    # Agriculture, forestry, fishing
    "B":      ["B05", "B06", "B07", "B08", "B09"],      # Mining and quarrying
    "C":      ["C10", "C11", "C12", "C13", "C14",       # Manufacturing
               "C15", "C16", "C17", "C18", "C19",
               "C20", "C21", "C22", "C23", "C24",
               "C25", "C26", "C27", "C28", "C29",
               "C30", "C31", "C32", "C33"],
    "D_E":    ["D35", "E36", "E37", "E38", "E39"],      # Utilities + water/waste
    "F":      ["F41", "F42", "F43"],                    # Construction
    "G":      ["G45", "G46", "G47"],                    # Wholesale and retail trade
    "H":      ["H49", "H50", "H51", "H52", "H53"],      # Transport and storage
    "I":      ["I55", "I56"],                           # Accommodation and food service
    "J":      ["J58", "J59", "J60", "J61", "J62",       # Information and communication
               "J63"],
    "K":      ["K64", "K65", "K66"],                    # Financial and insurance
    "L_M_N":  ["L68",                                   # Real estate
               "M69", "M70", "M71", "M72", "M73",       # Professional/scientific/technical
               "M74", "M75",
               "N77", "N78", "N79", "N80", "N81",       # Administrative and support
               "N82"],
    "O_P_Q":  ["O84",                                   # Public admin and defence
               "P85",                                   # Education
               "Q86", "Q87", "Q88"],                    # Human health and social work
    "R_S_T_U": ["R90", "R91", "R92", "R93",             # Arts, entertainment, recreation
                "S94", "S95", "S96",                    # Other service activities
                "T97", "T98",                           # Household activities
                "U99"],                                 # Extraterritorial organisations
}

# Flat list of all NACE codes, used only for documentation
ALL_NACE_CODES: list[str] = [
    code for codes in NACE_BY_SECTION.values() for code in codes
]

# ── Shared dimension constants ────────────────────────────────────────────────

# ISCED 2011 education levels (lfst_r_lfu3rt, lfst_r_lfe2emprtn)
ISCED11_LEVELS = [
    "ED0-2",   # Less than primary, primary and lower secondary
    "ED3_4",   # Upper secondary and post-secondary non-tertiary
    "ED5-8",   # Tertiary
    "TOTAL",   # All levels combined
]

# Citizenship (lfst_r_lfur2gan, lfst_r_lfe2emprtn)
CITIZEN_CODES = ["NAT", "FOR", "TOTAL"]

# Sex
SEX_CODES = ["T", "M", "F"]

# Age bands for datasets that include age
AGE_CODES_LFS = ["Y15-24", "Y15-64", "Y15-74", "Y20-64", "Y25-74", "TOTAL"]

# Time range
TIME_START = 2021
TIME_END   = 2026   # update when new releases land
TIME_RANGE = [str(y) for y in range(TIME_START, TIME_END + 1)]

# ── Geography filter ──────────────────────────────────────────────────────────
# Italy NUTS2 region codes (21 regions + autonomous provinces).
# Sub-national data is only fetched for Italy; all other countries are
# represented by their NUTS0 national total only.
# These are the NUTS 2021 codes; kept here for use in post-fetch filtering.
ITALY_NUTS2 = [
    "ITC1", "ITC2", "ITC3", "ITC4",          # Nord-ovest
    "ITD1", "ITD2", "ITD3", "ITD4", "ITD5",  # Nord-est
    "ITE1", "ITE2", "ITE3", "ITE4",          # Centro
    "ITF1", "ITF2", "ITF3", "ITF4",
    "ITF5", "ITF6",                           # Sud
    "ITG1", "ITG2",                           # Isole
]


def filter_geo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retain only the geographies relevant for the dashboard:
      - All NUTS0 national totals (2-character geo codes)
      - Italian NUTS2 regions (4-character codes starting with "IT")

    Drops all other sub-national observations (non-Italian NUTS1/2/3),
    which are not needed for comparative context.
    """
    if "geo" not in df.columns:
        return df
    mask = (df["geo"].str.len() == 2) | (df["geo"].str.startswith("IT"))
    dropped = (~mask).sum()
    if dropped:
        logger.debug(f"filter_geo: dropped {dropped:,} non-Italian sub-national rows.")
    return df[mask].copy()


# ── Response parser ───────────────────────────────────────────────────────────

def parse_eurostat_response(data: dict, dataset_code: str) -> pd.DataFrame:
    """
    Convert a raw Eurostat SDMX-JSON response into a tidy long-format DataFrame.

    Eurostat returns data in a compact format where:
    - ``dimension`` describes every axis (time, geo, nace, unit, etc.)
    - ``value`` is a flat dict mapping a position index string to the numeric value

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
        dims   = data["dimension"]
        values = data["value"]
    except KeyError as e:
        raise ValueError(
            f"Unexpected Eurostat response structure for {dataset_code}: missing key {e}"
        ) from e

    if not values:
        logger.warning(f"{dataset_code}: API returned zero observations.")
        return pd.DataFrame()

    dim_names = list(dims.keys())
    dim_sizes = [len(dims[d]["category"]["index"]) for d in dim_names]

    dim_pos_to_code  = []
    dim_pos_to_label = []
    for d in dim_names:
        cat = dims[d]["category"]
        pos_to_code  = {v: k for k, v in cat["index"].items()}
        pos_to_label = cat.get("label", {})
        dim_pos_to_code.append(pos_to_code)
        dim_pos_to_label.append(pos_to_label)

    strides = []
    stride  = 1
    for s in reversed(dim_sizes):
        strides.insert(0, stride)
        stride *= s

    rows = []
    for flat_idx_str, obs_value in values.items():
        flat_idx  = int(flat_idx_str)
        row       = {}
        remaining = flat_idx
        for i, (name, stride_val) in enumerate(zip(dim_names, strides)):
            pos       = remaining // stride_val
            remaining = remaining % stride_val
            code      = dim_pos_to_code[i].get(pos, f"unknown_{pos}")
            label     = dim_pos_to_label[i].get(code, "")
            row[name] = code
            if label and label != code:
                row[f"{name}_label"] = label
        row["value"] = obs_value
        rows.append(row)

    df = pd.DataFrame(rows)
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
    Fetch a single Eurostat dataset slice and return the raw parsed JSON.

    Filters must already be scoped to a size the API will accept.
    Call ``fetch_dataset_paginated()`` to handle large datasets automatically.
    """
    url    = f"{base_url}/{dataset_code}"
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

    Eurostat enforces a response-size limit. This function fetches one value of
    ``paginate_on`` at a time and concatenates the results.

    Parameters
    ----------
    session :
        Retry-wrapped requests session.
    dataset_code : str
        Eurostat dataset identifier.
    base_filters : dict
        Filters applied to every request. The dimension named by ``paginate_on``
        must be present and will be split one value per request.
    paginate_on : str
        Dimension name to paginate over, e.g. ``"nace_r2"`` or ``"isced11"``.
    base_url : str
        Eurostat API base URL.

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
            df   = parse_eurostat_response(data, f"{dataset_code}[{value}]")
            if not df.empty:
                frames.append(df)
        except Exception as e:
            logger.warning(
                f"{dataset_code} [{paginate_on}={value}]: failed — {e}. Skipping."
            )

    if not frames:
        logger.warning(f"{dataset_code}: all paginated requests failed or returned empty.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info(
        f"{dataset_code}: combined {len(combined):,} rows across {len(frames)} requests."
    )
    return combined


def fetch_dataset_paginated_by_groups(
    session,
    dataset_code: str,
    base_filters: dict[str, list[str]],
    paginate_dim: str,
    groups: dict[str, list[str]],
    base_url: str = EUROSTAT_BASE,
) -> pd.DataFrame:
    """
    Fetch a dataset by making one request per group of dimension values.

    Used for ``lfsa_egan22d`` where NACE codes are grouped by section to
    balance request granularity against API response-size limits. Each group
    key is a human-readable label (e.g. ``"C"`` for Manufacturing); the
    corresponding list contains the codes sent in that request.

    Parameters
    ----------
    session :
        Retry-wrapped requests session.
    dataset_code : str
        Eurostat dataset identifier.
    base_filters : dict
        Filters applied on every request. ``paginate_dim`` must NOT be in
        ``base_filters``; it is injected per-group.
    paginate_dim : str
        Dimension name to supply per-group, e.g. ``"nace_r2"``.
    groups : dict[str, list[str]]
        Mapping of group label → list of dimension codes for that group.
    base_url : str
        Eurostat API base URL.

    Returns
    -------
    pd.DataFrame
        Concatenated results from all group requests.
    """
    frames = []
    for group_label, codes in groups.items():
        filters = {**base_filters, paginate_dim: codes}
        logger.info(
            f"Fetching {dataset_code} [section={group_label}, "
            f"{len(codes)} NACE code(s)]"
        )
        try:
            data = fetch_dataset_single(session, dataset_code, filters, base_url)
            df   = parse_eurostat_response(
                data, f"{dataset_code}[{group_label}]"
            )
            if not df.empty:
                frames.append(df)
        except Exception as e:
            logger.warning(
                f"{dataset_code} [section={group_label}]: failed — {e}. Skipping."
            )

    if not frames:
        logger.warning(
            f"{dataset_code}: all section requests failed or returned empty."
        )
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info(
        f"{dataset_code}: combined {len(combined):,} rows across "
        f"{len(frames)} section requests."
    )
    return combined


# ── Dataset-specific fetch functions ─────────────────────────────────────────

def fetch_national_employment_by_nace(session, config: dict) -> pd.DataFrame:
    """
    lfsa_egan22d — Employed persons by NACE Rev.2 two-digit activity.

    Annual LFS-based estimate of employment (thousands of persons) broken down
    by all NACE Rev.2 two-digit economic activities (sections A–U) plus the
    overall TOTAL, sex and age group. National level (NUTS0) only — no
    regional breakdown is available at this level of NACE detail.

    Fetched in section groups (13 requests) to stay under Eurostat's
    response-size limit while covering the complete economy.

    Dimensions : freq(A), unit(THS_PER), nace_r2, sex, age, geo, time.
    Unit       : THS_PER — thousands of persons employed.
    Geography  : NUTS0 national totals (no sub-national breakdown available).
    Coverage   : 2021 onwards.

    NACE section groups fetched
    ---------------------------
    TOTAL    : All activities aggregate
    A        : Agriculture, forestry and fishing (A01–A03)
    B        : Mining and quarrying (B05–B09)
    C        : Manufacturing (C10–C33, 24 two-digit codes)
    D_E      : Utilities, water supply, waste management (D35, E36–E39)
    F        : Construction (F41–F43)
    G        : Wholesale and retail trade (G45–G47)
    H        : Transport and storage (H49–H53)
    I        : Accommodation and food service (I55–I56)
    J        : Information and communication (J58–J63)
    K        : Financial and insurance activities (K64–K66)
    L_M_N    : Real estate, professional, scientific, administrative (L68–N82)
    O_P_Q    : Public admin, education, health and social work (O84–Q88)
    R_S_T_U  : Arts, recreation, other services, households (R90–U99)
    """
    dataset_code = "lfsa_egan22d"
    logger.info(f"Fetching {dataset_code} (paginating by NACE section group)")

    df = fetch_dataset_paginated_by_groups(
        session,
        dataset_code,
        base_filters={
            "unit": ["THS_PER"],
            "sex":  SEX_CODES,
            "age":  ["Y_GE15", "Y15-24", "Y15-64", "Y20-64", "Y25-64", "TOTAL"],
            "time": TIME_RANGE,
        },
        paginate_dim="nace_r2",
        groups=NACE_BY_SECTION,
    )

    if df.empty:
        return df

    # lfsa_egan22d is national-only; nuts_level is always 0 but mark it
    # explicitly for consistency with the regional datasets.
    if "geo" in df.columns:
        df["nuts_level"] = 0

    df.attrs["dataset_code"] = dataset_code
    df.attrs["description"]  = (
        "Employed persons by all NACE two-digit activities, sex and age "
        "(national totals, thousands)"
    )
    df.attrs["geo_note"] = "National level (NUTS0) only — no sub-national breakdown available"
    df.attrs["unit"]     = "thousands of persons"
    return df


def fetch_unemployment_rate_by_education(session, config: dict) -> pd.DataFrame:
    """
    lfst_r_lfu3rt — Unemployment rate by sex, age and educational attainment (NUTS2).

    Annual unemployment rate (%) for the population aged 15–74, broken down
    by sex, broad age groups and ISCED 2011 educational attainment level.
    Covers all EU/EEA member states at NUTS2 regional granularity plus
    national totals. Non-Italian sub-national rows are dropped post-fetch
    via ``filter_geo()``.

    Pagination is on ``isced11`` (4 values × all geographies per request).

    Dimensions : unit(PC), sex, age, isced11, geo, time.
    Unit       : PC — percentage of labour force.
    Geography  : NUTS0 all countries + NUTS2 Italy only.
    Coverage   : 2021 onwards.

    ISCED 2011 codes
    ----------------
    ED0-2  : Less than primary, primary and lower secondary
    ED3_4  : Upper secondary and post-secondary non-tertiary
    ED5-8  : Tertiary education
    TOTAL  : All levels combined
    """
    dataset_code = "lfst_r_lfu3rt"
    logger.info(f"Fetching {dataset_code} (paginating by ISCED level)")

    df = fetch_dataset_paginated(
        session,
        dataset_code,
        base_filters={
            "isced11": ISCED11_LEVELS,
            "unit":    ["PC"],
            "sex":     SEX_CODES,
            "age":     AGE_CODES_LFS,
            "time":    TIME_RANGE,
        },
        paginate_on="isced11",
    )

    if df.empty:
        return df

    df = filter_geo(df)

    if "geo" in df.columns:
        df["nuts_level"] = df["geo"].str.len().map({2: 0, 3: 1, 4: 2, 5: 3})

    df.attrs["dataset_code"]    = dataset_code
    df.attrs["description"]     = (
        "Unemployment rate (%) by sex, age and educational attainment — "
        "NUTS0 all countries, NUTS2 Italy only"
    )
    df.attrs["geo_note"]        = "NUTS0 (all) + NUTS2 Italy only"
    df.attrs["unit"]            = "% of labour force"
    df.attrs["isced11_labels"]  = {
        "ED0-2": "Less than primary / primary / lower secondary",
        "ED3_4": "Upper secondary and post-secondary non-tertiary",
        "ED5-8": "Tertiary education",
        "TOTAL": "All levels",
    }
    return df


def fetch_unemployment_rate_by_citizenship(session, config: dict) -> pd.DataFrame:
    """
    lfst_r_lfur2gan — Unemployment rate by sex, age and citizenship (NUTS2).

    Annual unemployment rate (%) broken down by sex, broad age groups and
    citizenship status (nationals / foreigners / total). Covers all EU/EEA
    member states at NUTS2 regional granularity plus national totals.
    Non-Italian sub-national rows are dropped post-fetch via ``filter_geo()``.

    Pagination is on ``citizen`` (3 values × all geographies per request).

    Dimensions : unit(PC), sex, age, citizen, geo, time.
    Unit       : PC — percentage of labour force.
    Geography  : NUTS0 all countries + NUTS2 Italy only.
    Coverage   : 2021 onwards.

    Citizenship codes
    -----------------
    NAT   : Nationals (citizens of the reporting country)
    FOR   : Foreigners (citizens of another country)
    TOTAL : Total (nationals + foreigners)
    """
    dataset_code = "lfst_r_lfur2gan"
    logger.info(f"Fetching {dataset_code} (paginating by citizenship)")

    df = fetch_dataset_paginated(
        session,
        dataset_code,
        base_filters={
            "citizen": CITIZEN_CODES,
            "unit":    ["PC"],
            "sex":     SEX_CODES,
            "age":     AGE_CODES_LFS,
            "time":    TIME_RANGE,
        },
        paginate_on="citizen",
    )

    if df.empty:
        return df

    df = filter_geo(df)

    if "geo" in df.columns:
        df["nuts_level"] = df["geo"].str.len().map({2: 0, 3: 1, 4: 2, 5: 3})

    df.attrs["dataset_code"]       = dataset_code
    df.attrs["description"]        = (
        "Unemployment rate (%) by sex, age and citizenship — "
        "NUTS0 all countries, NUTS2 Italy only"
    )
    df.attrs["geo_note"]           = "NUTS0 (all) + NUTS2 Italy only"
    df.attrs["unit"]               = "% of labour force"
    df.attrs["citizenship_labels"] = {
        "NAT":   "Nationals",
        "FOR":   "Foreigners",
        "TOTAL": "Total",
    }
    return df


def fetch_employment_rate_by_education_citizenship(session, config: dict) -> pd.DataFrame:
    """
    lfst_r_lfe2emprtn — Employment rate by sex, education and citizenship (NUTS2).

    Annual employment rate (%) broken down by sex, ISCED 2011 educational
    attainment level and citizenship status. The ``age`` dimension is
    intentionally omitted: including it would multiply the row count ~6×
    without adding meaningful value for the dashboard's comparative use case.
    Non-Italian sub-national rows are dropped post-fetch via ``filter_geo()``.

    Pagination is on ``isced11`` (4 values). Each request covers all
    geographies × all citizenship × all sex values for one education level.

    Dimensions : unit(PC), sex, isced11, citizen, geo, time.
                 (age is not filtered — omitting it from the request means
                 the API returns the total/default age aggregate.)
    Unit       : PC — percentage of population in working-age group.
    Geography  : NUTS0 all countries + NUTS2 Italy only.
    Coverage   : 2021 onwards.

    ISCED 2011 codes
    ----------------
    ED0-2  : Less than primary, primary and lower secondary
    ED3_4  : Upper secondary and post-secondary non-tertiary
    ED5-8  : Tertiary education
    TOTAL  : All levels combined

    Citizenship codes
    -----------------
    NAT   : Nationals
    FOR   : Foreigners
    TOTAL : All citizenships combined
    """
    dataset_code = "lfst_r_lfe2emprtn"
    logger.info(f"Fetching {dataset_code} (paginating by ISCED level, age omitted)")

    df = fetch_dataset_paginated(
        session,
        dataset_code,
        base_filters={
            "isced11": ISCED11_LEVELS,
            "citizen": CITIZEN_CODES,
            "unit":    ["PC"],
            "sex":     SEX_CODES,
            # age intentionally omitted — API returns total age aggregate
            "time":    TIME_RANGE,
        },
        paginate_on="isced11",
    )

    if df.empty:
        return df

    df = filter_geo(df)

    if "geo" in df.columns:
        df["nuts_level"] = df["geo"].str.len().map({2: 0, 3: 1, 4: 2, 5: 3})

    df.attrs["dataset_code"]       = dataset_code
    df.attrs["description"]        = (
        "Employment rate (%) by sex, educational attainment and citizenship — "
        "NUTS0 all countries, NUTS2 Italy only (age: total)"
    )
    df.attrs["geo_note"]           = "NUTS0 (all) + NUTS2 Italy only"
    df.attrs["unit"]               = "% of population in working-age group"
    df.attrs["age_note"]           = (
        "Age dimension not filtered; API returns the default aggregate "
        "(typically Y20-64 or total working-age population depending on geo)."
    )
    df.attrs["isced11_labels"]     = {
        "ED0-2": "Less than primary / primary / lower secondary",
        "ED3_4": "Upper secondary and post-secondary non-tertiary",
        "ED5-8": "Tertiary education",
        "TOTAL": "All levels",
    }
    df.attrs["citizenship_labels"] = {
        "NAT":   "Nationals",
        "FOR":   "Foreigners",
        "TOTAL": "Total",
    }
    return df


# ── Main loader class ─────────────────────────────────────────────────────────

class EurostatLabourLoader(BaseLoader):
    """
    Fetches all Eurostat datasets for the labour market domain and saves
    them as raw parquet files.

    Covers two thematic modules:
      1. National employment  — employed persons by all NACE activities,
                                sex and age (national level only) (1 dataset)
      2. Regional rates       — unemployment and employment rates at NUTS2
                                with education and citizenship breakdowns,
                                Italy sub-national + all-country nationals
                                (3 datasets)

    Each dataset is fetched independently via pagination to stay under
    Eurostat's response-size limit. A failure in one does not block the
    others; failed datasets are logged as warnings.
    """

    SOURCE_ID = "eurostat"
    DOMAIN    = "labour"

    DATASETS: list[tuple] = [

        # Module 1 — National employment by NACE (all sections)
        (fetch_national_employment_by_nace,
         "National employment by NACE activity — all sections (lfsa_egan22d)"),

        # Module 2 — Regional rates (NUTS2, Italy + all NUTS0)
        (fetch_unemployment_rate_by_education,
         "Unemployment rate by education — NUTS2 IT + NUTS0 (lfst_r_lfu3rt)"),

        (fetch_unemployment_rate_by_citizenship,
         "Unemployment rate by citizenship — NUTS2 IT + NUTS0 (lfst_r_lfur2gan)"),

        (fetch_employment_rate_by_education_citizenship,
         "Employment rate by education & citizenship — NUTS2 IT + NUTS0 (lfst_r_lfe2emprtn)"),
    ]

    def fetch(self) -> list[pd.DataFrame]:
        """
        Fetch all configured Eurostat labour market datasets.

        Returns
        -------
        list[pd.DataFrame]
            One DataFrame per entry in DATASETS, in order.
            Empty DataFrames indicate fetch/parse failures.
        """
        results: list[pd.DataFrame] = []
        modules = {
            0: "── Module 1: National employment by NACE ────────",
            1: "── Module 2: Regional rates (NUTS2) ──────────────",
        }

        for idx, (fetch_fn, label) in enumerate(self.DATASETS):
            if idx in modules:
                self.logger.info(modules[idx])

            try:
                df = fetch_fn(self.session, self.config)
                if not df.empty:
                    df.attrs["filename_suffix"] = df.attrs.get(
                        "dataset_code", f"dataset_{idx + 1}"
                    )
                    status = f"{len(df):,} rows"
                else:
                    status = "⚠ empty"

                self.logger.info(
                    f"  [{idx + 1:02d}/{len(self.DATASETS)}] {label}: {status}"
                )
                results.append(df)

            except Exception as exc:
                self.logger.warning(
                    f"  [{idx + 1:02d}/{len(self.DATASETS)}] {label}: FAILED — {exc}. "
                    "Skipping; pipeline continues."
                )
                results.append(pd.DataFrame())

        non_empty  = sum(1 for df in results if not df.empty)
        total_rows = sum(len(df) for df in results)
        self.logger.info(
            f"\nEurostat fetch complete: {non_empty}/{len(self.DATASETS)} datasets "
            f"retrieved, {total_rows:,} total observations."
        )
        return results


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    config = load_config()
    loader = EurostatLabourLoader(config=config)
    written = loader.run()
    print("\nDone. Files written:")
    for p in written:
        print(f"  {p}")