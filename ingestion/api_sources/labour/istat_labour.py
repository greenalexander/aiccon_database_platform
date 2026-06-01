"""
ingestion/api_sources/labour/istat.py

Fetches labour market data from the ISTAT SDMX REST API.

Datasets fetched:

  Labour force (source: Rilevazione sulle Forze di Lavoro, RFL)
    - 150_873_DF_DCCV_FORZLVMENS1_1  : Labour force 15+ (thousands), monthly,
                                        by sex and age; raw and seasonally adjusted.
                                        National total only. Series from 2004.

  NEETs — young people not in employment, education or training
    - 172_931_DF_DCCV_NEET1_11       : NEET rate 15–34, by sex, age and region (NUTS2).
                                        Annual and quarterly. Series from 2018.
    - 172_931_DF_DCCV_NEET1_9        : NEET rate 15–34, by sex, age and citizenship
                                        (Italian / foreign / total). Macro-areas only.
                                        Annual and quarterly. Series from 2018.

  Inactivity rate
    - 152_913_DF_DCCV_TAXINATT1_5    : Inactivity rate, by sex, age and province (NUTS3).
                                        Annual and quarterly. Series from 2004.

  Unemployment rate
    - 151_914_DF_DCCV_TAXDISOCCU1_8  : Unemployment rate, by sex, age and province (NUTS3).
                                        Annual and quarterly. Series from 2004.
    - 151_914_DF_DCCV_TAXDISOCCU1_6  : Unemployment rate, by sex, age and educational
                                        level (3 levels). Regional (NUTS2).
                                        Annual and quarterly. Series from 2004.
    - 151_914_DF_DCCV_TAXDISOCCU1_5  : Unemployment rate, by sex and broad age bands,
                                        by region (NUTS2).
                                        Annual and quarterly. Series from 2004.

  Employment rate
    - 150_915_DF_DCCV_TAXOCCU1_5     : Employment rate, by sex, age and province (NUTS3).
                                        Annual and quarterly. Series from 2004.

ISTAT SDMX API reference:
    https://esploradati.istat.it/SDMXWS/rest

API URL pattern:
    GET /data/{dataflow_id}/{dimension_key}/ALL
        ?startPeriod=YYYY&endPeriod=YYYY&format=csv

Notes on the endpoint:
    The production endpoint for this loader is esploradati.istat.it (not the
    legacy sdmx.istat.it). Dataflow IDs are numeric strings (e.g. "85_84_DF_…")
    rather than mnemonic codes. The agency prefix in the URL path is not used —
    the dataflow ID alone identifies the series. CSV format is preferred over
    compact XML for this endpoint as it is substantially simpler to parse and
    the response size is comparable for these datasets.

Output:
    One parquet file per dataset written to:
    {SHAREPOINT_ROOT}/aiccon-data/raw/labour/

Run directly:
    python -m ingestion.api_sources.labour.istat_labour
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from ingestion.loaders.base_loader import BaseLoader, get_logger, load_config

logger = get_logger("istat.labour")


# ── Constants ─────────────────────────────────────────────────────────────────

ISTAT_BASE = "https://esploradati.istat.it/SDMXWS/rest"

# Column rename map applied uniformly to all datasets after CSV parsing.
# Keys are lowercase SDMX dimension names as returned by the API.
RENAME_MAP = {
    # ── Universal SDMX bookkeeping ─────────────────────────────────────────
    "obs_value":              "value",
    "time_period":            "time",

    # ── Common LFS dimensions ──────────────────────────────────────────────
    "ref_area":               "geo",
    "freq":                   "frequency",       # A=annual, M=monthly, Q=quarterly
    "sex":                    "gender",
    "age":                    "age_group",
    "data_type":              "data_type",        # EMP_R / UNEM_R / INAC_R / FOR / NEET_I

    # ── Seasonal adjustment (FORZLVMENS1_1) ────────────────────────────────
    "adjustment":             "adjustment",       # N=raw, Y=seasonally adjusted

    # ── Education breakdown (TAXDISOCCU1_6) ────────────────────────────────
    "edu_lev_highest":        "education",

    # ── Citizenship breakdown (NEET1_9) ────────────────────────────────────
    "citizenship":            "citizenship",      # ITL=Italian, FRG=foreign, TOTAL

    # ── Unemployment duration (TAXDISOCCU flows — always TOTAL here) ───────
    "duration_unemployment":  "unemployment_duration",
}


# ── CSV fetch & parse ─────────────────────────────────────────────────────────

def fetch_istat_csv(
    session,
    dataflow_id: str,
    dimension_key: str = "all",
    start_period: str | None = None,
    end_period: str | None = None,
) -> pd.DataFrame:
    """
    Fetch a single ISTAT dataflow as CSV and return a tidy DataFrame.

    The esploradati.istat.it endpoint accepts ``format=csv`` and returns a
    standard comma-separated file with a header row, which is far easier to
    parse than compact SDMX-XML. One row per observation.

    Parameters
    ----------
    session :
        Retry-wrapped requests session from BaseLoader.
    dataflow_id : str
        Full numeric dataflow ID, e.g. ``"85_84_DF_DCSA_VOLON1_1"``.
    dimension_key : str
        SDMX key string. Use ``"all"`` (case-insensitive) to fetch every
        combination. For targeted slices supply a dot-separated key matching
        the dataflow's DSD dimension order, e.g. ``"A.IT...."``.
        Wildcard positions should be left empty (consecutive dots).
    start_period : str, optional
        ISO year string, e.g. ``"2015"``. Omit to retrieve the full series.
    end_period : str, optional
        ISO year string, e.g. ``"2023"``. Omit for the latest available year.

    Returns
    -------
    pd.DataFrame
        Tidy long-format DataFrame with standardised column names.
        Returns an empty DataFrame on HTTP or parse errors (logged as warnings).
    """
    # Normalise key: ISTAT uses uppercase "ALL" in the path
    path_key = "ALL" if dimension_key.lower() == "all" else dimension_key

    url = f"{ISTAT_BASE}/data/{dataflow_id}/{path_key}/ALL"

    params: dict[str, str] = {"format": "csv"}
    if start_period:
        params["startPeriod"] = start_period
    if end_period:
        params["endPeriod"] = end_period

    logger.info(f"Fetching {dataflow_id} (key={path_key}, {start_period or '*'}→{end_period or '*'})")

    try:
        response = session.get_with_retry(url, params=params)
        response.raise_for_status()
    except Exception as e:
        logger.warning(f"{dataflow_id}: HTTP error — {e}")
        return pd.DataFrame()

    if not response.text.strip():
        logger.warning(f"{dataflow_id}: empty response body.")
        return pd.DataFrame()

    try:
        df = pd.read_csv(io.StringIO(response.text), low_memory=False)
    except Exception as e:
        logger.warning(f"{dataflow_id}: CSV parse error — {e}")
        return pd.DataFrame()

    # Standardise column names: lowercase + apply rename map
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={k: v for k, v in RENAME_MAP.items() if k in df.columns})

    # Coerce value column to numeric; drop unflagged missing observations
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])

    # Drop SDMX bookkeeping columns that carry no analytical content
    drop_cols = [c for c in df.columns if c in {"obs_status", "obs_conf", "obs_pre_break", "dataflow"}]
    df = df.drop(columns=drop_cols, errors="ignore")

    logger.info(f"{dataflow_id}: {len(df):,} observations loaded.")
    return df


# ── Dataset-specific fetch functions ─────────────────────────────────────────
#
# Each function is a thin wrapper around fetch_istat_csv that supplies the
# correct dataflow ID, any dimension filters, and start_period.
# Metadata is attached to df.attrs for downstream processing.
#
# Geo granularity used in each dataset (deepest available per the API):
#   FORZLVMENS1_1              — national total only (no regional breakdown)
#   NEET1_11                   — regions (NUTS2)
#   NEET1_9                    — macro-areas only (but adds citizenship split)
#   TAXINATT1_5                — provinces (NUTS3)
#   TAXDISOCCU1_8              — provinces (NUTS3)
#   TAXDISOCCU1_6              — regions (NUTS2)  [education breakdown]
#   TAXDISOCCU1_5              — regions (NUTS2)  [broad age bands]
#   TAXOCCU1_5                 — provinces (NUTS3)
#
# Time coverage: start_period="2015" for all series except the two NEET flows,
# which only begin in 2018.

# ── Labour force ──────────────────────────────────────────────────────────────


def fetch_labour_force_monthly(session) -> pd.DataFrame:
    """
    150_873_DF_DCCV_FORZLVMENS1_1 — Labour force 15+, monthly.
 
    Total labour force (employed + unemployed, aged 15–89) in thousands,
    broken down by sex and age group. Monthly frequency; national total only
    (no regional breakdown available for this flow).
 
    Raw (non-seasonally-adjusted) series requested via ADJUSTMENT=N.
    Both the raw and the seasonally-adjusted editions share the same
    dataflow; filtering to N keeps the series comparable with the annual
    and quarterly rate flows and avoids doubling the row count.
 
    Dimensions: FREQ(M), REF_AREA(IT), DATA_TYPE(FOR), ADJUSTMENT(N/Y),
                SEX, AGE.
    Series from: 2004 (fetched from 2015).
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="150_873_DF_DCCV_FORZLVMENS1_1",
        start_period="2015",
    )
    if not df.empty:
        # Drop seasonally-adjusted observations; keep raw data only.
        if "adjustment" in df.columns:
            df = df[df["adjustment"] == "N"]
        df.attrs["dataset_code"] = "150_873_DF_DCCV_FORZLVMENS1_1"
        df.attrs["description"]  = (
            "Labour force 15+ (thousands), monthly, by sex and age — raw data"
        )
        df.attrs["frequency"]    = "monthly"
        df.attrs["geo_note"]     = "National total only"
        df.attrs["unit"]         = "thousands of persons"
    return df
 
 
# ── NEETs ─────────────────────────────────────────────────────────────────────
 
def fetch_neet_by_sex_age_region(session) -> pd.DataFrame:
    """
    172_931_DF_DCCV_NEET1_11 — NEET rate 15–34, by sex, age and region.
 
    Incidence of young people aged 15–34 not in employment, education or
    training (NEET), broken down by sex, age band (15–24, 15–29, 15–34,
    18–29) and region (NUTS2). Annual and quarterly frequency.
 
    This is the most geographically granular NEET series available: it
    reaches individual regions (21 NUTS2 units) plus macro-area aggregates.
    Citizenship and education are fixed at total in this flow; use
    NEET1_9 for the citizenship split.
 
    Dimensions: FREQ, REF_AREA (NUTS2), DATA_TYPE(NEET_I), SEX, AGE.
    Series from: 2018 (earliest available; start_period capped accordingly).
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="172_931_DF_DCCV_NEET1_11",
        start_period="2018",
    )
    if not df.empty:
        # Drop single-value bookkeeping dimensions that add no information.
        drop_cols = [c for c in df.columns if c in {
            "labprof_status_a", "euro_labour_status",
            "edu_lev_highest", "role_in_household",
        }]
        df = df.drop(columns=drop_cols, errors="ignore")
        df.attrs["dataset_code"] = "172_931_DF_DCCV_NEET1_11"
        df.attrs["description"]  = (
            "NEET rate (% aged 15–34), by sex, age band and region (NUTS2)"
        )
        df.attrs["geo_note"]     = "Regional level (NUTS2) + macro-areas"
        df.attrs["unit"]         = "% of age group"
    return df
 
 
def fetch_neet_by_sex_age_citizenship(session) -> pd.DataFrame:
    """
    172_931_DF_DCCV_NEET1_9 — NEET rate 15–34, by sex, age and citizenship.
 
    Incidence of NEETs broken down by sex, age band and citizenship
    (Italian / foreign / total). Geography is limited to macro-areas
    (Nord, Centro, Mezzogiorno, IT), making this flow complementary to
    NEET1_11: use NEET1_11 for the regional picture, NEET1_9 for the
    Italian-vs-foreign split at macro level.
 
    Dimensions: FREQ, REF_AREA (macro-areas), DATA_TYPE(NEET_I),
                SEX, AGE, CITIZENSHIP (ITL/FRG/TOTAL).
    Series from: 2018 (earliest available).
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="172_931_DF_DCCV_NEET1_9",
        start_period="2018",
    )
    if not df.empty:
        drop_cols = [c for c in df.columns if c in {
            "labprof_status_a", "euro_labour_status",
            "edu_lev_highest", "role_in_household",
        }]
        df = df.drop(columns=drop_cols, errors="ignore")
        df.attrs["dataset_code"]  = "172_931_DF_DCCV_NEET1_9"
        df.attrs["description"]   = (
            "NEET rate (% aged 15–34), by sex, age band and citizenship "
            "(Italian / foreign / total) — macro-areas"
        )
        df.attrs["geo_note"]      = "Macro-areas only (Nord, Centro, Mezzogiorno, IT)"
        df.attrs["unit"]          = "% of age group"
        df.attrs["citizenship_codes"] = {
            "ITL":   "Italian citizens",
            "FRG":   "Foreign citizens",
            "TOTAL": "Total",
        }
    return df
 
 
# ── Inactivity rate ───────────────────────────────────────────────────────────
 
def fetch_inactivity_rate_by_province(session) -> pd.DataFrame:
    """
    152_913_DF_DCCV_TAXINATT1_5 — Inactivity rate, by sex, age and province.
 
    Share of the working-age population that is neither employed nor
    actively seeking work. Broken down by sex and age group (15–24 through
    55–74, plus broader aggregates). The most geographically granular
    inactivity series available: province level (NUTS3, ~110 units) plus
    all higher aggregates (regions, macro-areas, national).
 
    Dimensions: FREQ, REF_AREA (NUTS3), DATA_TYPE(INAC_R), SEX, AGE.
    Series from: 2004 (fetched from 2015).
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="152_913_DF_DCCV_TAXINATT1_5",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "152_913_DF_DCCV_TAXINATT1_5"
        df.attrs["description"]  = (
            "Inactivity rate (%), by sex, age group and province (NUTS3)"
        )
        df.attrs["geo_note"]     = "Province level (NUTS3) + all higher aggregates"
        df.attrs["unit"]         = "% of population in age group"
    return df
 
 
# ── Unemployment rate ─────────────────────────────────────────────────────────
 
def fetch_unemployment_rate_by_province(session) -> pd.DataFrame:
    """
    151_914_DF_DCCV_TAXDISOCCU1_8 — Unemployment rate, by sex, age and province.
 
    Share of the labour force that is unemployed, broken down by sex and
    age group (15–24 through 50–74). The most geographically granular
    unemployment series available: province level (NUTS3, ~110 units).
    Use this as the primary unemployment dataset for territorial analysis.
 
    Dimensions: FREQ, REF_AREA (NUTS3), DATA_TYPE(UNEM_R), SEX, AGE.
    Series from: 2004 (fetched from 2015).
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="151_914_DF_DCCV_TAXDISOCCU1_8",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "151_914_DF_DCCV_TAXDISOCCU1_8"
        df.attrs["description"]  = (
            "Unemployment rate (%), by sex, age group and province (NUTS3)"
        )
        df.attrs["geo_note"]     = "Province level (NUTS3) + all higher aggregates"
        df.attrs["unit"]         = "% of labour force"
    return df
 
 
def fetch_unemployment_rate_by_education(session) -> pd.DataFrame:
    """
    151_914_DF_DCCV_TAXDISOCCU1_6 — Unemployment rate, by sex, age and education.
 
    Unemployment rate cross-tabulated by sex, broad age group (15–64,
    15–74, 20–64) and highest educational level attained:
      - 13 : no qualification, primary or lower secondary
      -  7 : upper / post-secondary (diploma)
      - 11 : tertiary (degree, doctoral, specialisation)
      - 99 : total
 
    Geography reaches region level (NUTS2); province breakdown is not
    available for this education-disaggregated flow.
 
    Dimensions: FREQ, REF_AREA (NUTS2), DATA_TYPE(UNEM_R),
                SEX, AGE, EDU_LEV_HIGHEST.
    Series from: 2004 (fetched from 2015).
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="151_914_DF_DCCV_TAXDISOCCU1_6",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"]   = "151_914_DF_DCCV_TAXDISOCCU1_6"
        df.attrs["description"]    = (
            "Unemployment rate (%), by sex, age group, educational level "
            "and region (NUTS2)"
        )
        df.attrs["geo_note"]       = "Regional level (NUTS2) + macro-areas + national"
        df.attrs["unit"]           = "% of labour force"
        df.attrs["education_codes"] = {
            "13": "No qualification / primary / lower secondary",
            "7":  "Upper and post-secondary (diploma)",
            "11": "Tertiary (degree, doctoral, specialisation)",
            "99": "Total",
        }
    return df
 
 
def fetch_unemployment_rate_by_age_region(session) -> pd.DataFrame:
    """
    151_914_DF_DCCV_TAXDISOCCU1_5 — Unemployment rate, by sex, detailed age and region.
 
    Unemployment rate with the broadest age disaggregation available at
    regional level: 13 distinct age bands from 15–24 through 55–64, plus
    standard aggregates (15–64, 15–74, 20–64). Broken down by sex and
    region (NUTS2).
 
    Complements TAXDISOCCU1_8 (which goes to province but has fewer age
    bands) and TAXDISOCCU1_6 (which adds education but has only three
    broad age groups).
 
    Dimensions: FREQ, REF_AREA (NUTS2), DATA_TYPE(UNEM_R), SEX, AGE.
    Series from: 2004 (fetched from 2015).
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="151_914_DF_DCCV_TAXDISOCCU1_5",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "151_914_DF_DCCV_TAXDISOCCU1_5"
        df.attrs["description"]  = (
            "Unemployment rate (%), by sex, detailed age band and region (NUTS2)"
        )
        df.attrs["geo_note"]     = "Regional level (NUTS2) + macro-areas + national"
        df.attrs["unit"]         = "% of labour force"
    return df
 
 
# ── Employment rate ───────────────────────────────────────────────────────────
 
def fetch_employment_rate_by_province(session) -> pd.DataFrame:
    """
    150_915_DF_DCCV_TAXOCCU1_5 — Employment rate, by sex, age and province.
 
    Share of the working-age population that is employed, broken down by
    sex and age group (15–24 through 55–64, plus standard aggregates
    including the EU-standard 20–64 band). The most geographically
    granular employment rate series available: province level (NUTS3,
    ~110 units) plus all higher aggregates.
 
    Dimensions: FREQ, REF_AREA (NUTS3), DATA_TYPE(EMP_R), SEX, AGE.
    Series from: 2004 (fetched from 2015).
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="150_915_DF_DCCV_TAXOCCU1_5",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "150_915_DF_DCCV_TAXOCCU1_5"
        df.attrs["description"]  = (
            "Employment rate (%), by sex, age group and province (NUTS3)"
        )
        df.attrs["geo_note"]     = "Province level (NUTS3) + all higher aggregates"
        df.attrs["unit"]         = "% of population in age group"
    return df
 
 


# ── Main loader class ─────────────────────────────────────────────────────────

class IstatLabourLoader(BaseLoader):
    """
    Fetches all ISTAT datasets for the labour market domain and saves
    them as raw parquet files.
 
    Covers four thematic modules:
      1. Labour force      — monthly headcount (1 dataflow)
      2. NEETs             — by region and by citizenship (2 dataflows)
      3. Inactivity rate   — province-level breakdown (1 dataflow)
      4. Unemployment rate — province, education, and detailed age cuts
                             (3 dataflows)
      5. Employment rate   — province-level breakdown (1 dataflow)
 
    All series fetched from 2015 onwards, except the NEET flows which
    begin in 2018. The monthly labour force series is filtered to raw
    (non-seasonally-adjusted) data only.
 
    Each dataset is fetched independently. A failure in one does not
    block the others; failed datasets are logged as warnings and an
    empty DataFrame is returned in their place so downstream counts
    remain consistent.
    """

    SOURCE_ID = "istat"
    DOMAIN    = "labour"


    # ── Dataset registry ──────────────────────────────────────────────────────
    # Ordered by module and priority. Each tuple: (fetch_fn, log_label).
    DATASETS: list[tuple] = [
 
        # Module 1 — Labour force
        (fetch_labour_force_monthly,             "Labour force: monthly headcount"),
 
        # Module 2 — NEETs
        (fetch_neet_by_sex_age_region,           "NEETs: by sex, age and region"),
        (fetch_neet_by_sex_age_citizenship,      "NEETs: by sex, age and citizenship"),
 
        # Module 3 — Inactivity rate
        (fetch_inactivity_rate_by_province,      "Inactivity rate: by province"),
 
        # Module 4 — Unemployment rate
        (fetch_unemployment_rate_by_province,    "Unemployment rate: by province"),
        (fetch_unemployment_rate_by_education,   "Unemployment rate: by education level"),
        (fetch_unemployment_rate_by_age_region,  "Unemployment rate: detailed age by region"),
 
        # Module 5 — Employment rate
        (fetch_employment_rate_by_province,      "Employment rate: by province"),
    ]
 
    def fetch(self) -> list[pd.DataFrame]:
        """
        Fetch all configured ISTAT labour market datasets.
 
        Returns
        -------
        list[pd.DataFrame]
            One DataFrame per entry in DATASETS, in order.
            Empty DataFrames indicate fetch/parse failures.
        """
        results: list[pd.DataFrame] = []
        modules = {
            0: "── Module 1: Labour force ────────────────────",
            1: "── Module 2: NEETs ───────────────────────────",
            3: "── Module 3: Inactivity rate ─────────────────",
            4: "── Module 4: Unemployment rate ───────────────",
            7: "── Module 5: Employment rate ─────────────────",
        }
 
        for idx, (fetch_fn, label) in enumerate(self.DATASETS):
            if idx in modules:
                self.logger.info(modules[idx])
 
            try:
                df = fetch_fn(self.session)
                if not df.empty:
                    # CRITICAL: Attach the unique dataflow_id as a suffix.
                    # This prevents the BaseLoader from overwriting the same
                    # file 8 times.
                    df.attrs["filename_suffix"] = df.attrs.get(
                        "dataset_code", f"dataset_{idx + 1}"
                    )
                    status = f"{len(df):,} rows"
                else:
                    status = "⚠ empty"
 
                self.logger.info(f"  [{idx + 1:02d}/{len(self.DATASETS)}] {label}: {status}")
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
            f"\nISTAT fetch complete: {non_empty}/{len(self.DATASETS)} datasets retrieved, "
            f"{total_rows:,} total observations."
        )
        return results
 

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    config = load_config()
    loader = IstatLabourLoader(config=config)
    written = loader.run()
    print("\nDone. Files written:")
    for p in written:
        print(f"  {p}")