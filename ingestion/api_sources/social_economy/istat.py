"""
ingestion/api_sources/social_economy/istat.py

Fetches social economy data from the ISTAT SDMX REST API.

Datasets fetched:

  Volunteering (source: Indagine sul Volontariato, aggiornamento feb 2026)
    - 85_84_DF_DCSA_VOLON1_1   : Volunteering by gender, age, form (org. vs direct)
    - 85_84_DF_DCSA_VOLON1_2   : Type of activity performed by gender and form
    - 85_84_DF_DCSA_VOLON1_3   : Years of volunteering activity by gender and form
    - 85_84_DF_DCSA_VOLON1_4   : Volunteering by educational level and labour status
    - 85_84_DF_DCSA_VOLON1_5   : Volunteering by household size and economic resources
    - 85_84_DF_DCSA_VOLON1_6   : Volunteering by region and type of municipality

  Organised volunteering (same survey, organisational breakdown)
    - 85_171_DF_DCSA_VOLON_ORG1_1 : By sector of organisation and multi-membership
    - 85_171_DF_DCSA_VOLON_ORG1_2 : By institutional type (ODV, APS, etc.)
    - 85_171_DF_DCSA_VOLON_ORG1_3 : By motivation for volunteering
    - 85_171_DF_DCSA_VOLON_ORG1_4 : By personal impact reported

  Associationism (source: Aspetti della Vita Quotidiana, aggiornamento set 2025)
    - 83_63_DF_DCCV_AVQ_PERSONE_129 : Membership in associations by age
    - 83_63_DF_DCCV_AVQ_PERSONE_130 : Membership by age and educational level
    - 83_63_DF_DCCV_AVQ_PERSONE_131 : Membership by occupational status
    - 83_63_DF_DCCV_AVQ_PERSONE_132 : Membership by region and municipality type

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
    {SHAREPOINT_ROOT}/aiccon-data/raw/social_economy/

Run directly:
    python -m ingestion.api_sources.social_economy.istat
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from ingestion.loaders.base_loader import BaseLoader, get_logger, load_config

logger = get_logger("istat.social_economy")


# ── Constants ─────────────────────────────────────────────────────────────────

ISTAT_BASE = "https://esploradati.istat.it/SDMXWS/rest"

# Column rename map applied uniformly to all datasets after CSV parsing.
# Keys are lowercase SDMX dimension names as returned by the API.
RENAME_MAP = {
    "obs_value":          "value",
    "time_period":        "time",
    "ref_area":           "geo",
    "sex":                "gender",
    "age":                "age_group",
    "data_type":          "data_type",
    "form_volunt":        "volunteering_form",
    "type_activity_performed": "activity_type",
    "years_of_activities":     "years_active",
    "edu_lev_highest":    "education",
    "labour_profess_status_c": "labour_status",
    "number_household_comp":   "household_size",
    "fam_econ_resources": "econ_resources",
    "main_act_sector":    "org_sector",
    "institut_setting":   "org_type",
    "reason":             "motivation",
    "personal_impact":    "personal_impact",
    "y_multi_membership": "multi_membership",
    "tipo_attivita":      "activity_type",
    "tipo_associazione":  "association_type",
    "tipo_comune":        "municipality_type",
}

# Volunteering form codes for human-readable notes
FORM_VOLUNT_LABELS = {
    "ORGVOL":  "organised (via associations/organisations)",
    "DIRVOL":  "direct (informal, outside organisations)",
    "TOTAL":   "total (organised + direct)",
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
# correct dataflow ID, any dimension key pre-filter, and start_period where
# relevant. Metadata is attached to df.attrs for downstream processing.
#
# Dimension key pre-filters use the pattern documented in get_constraints:
#   FREQ.REF_AREA.DATA_TYPE.{domain-dims...}
# Leaving a position empty (consecutive dots) means "all values".
# We restrict REF_AREA to IT (national total) by default to keep response
# sizes manageable; regional breakdowns are fetched in dedicated functions.

# ── Volunteering: form, age, gender ──────────────────────────────────────────

def fetch_volunteering_by_age_gender(session) -> pd.DataFrame:
    """
    85_84_DF_DCSA_VOLON1_1 — Volunteering: gender, age and form.

    Headline dataset for the volunteering module. Contains the share of the
    population aged 15+ engaged in volunteering, broken down by:
      - sex (1=male, 2=female, 9=total)
      - age group (Y15-24, Y25-34, Y35-44, Y45-54, Y55-64, Y65-74, Y_GE75, Y_GE15)
      - form of volunteering (ORGVOL, DIRVOL, TOTAL)
      - data type: percentage (HAUA_15), estimated thousands (HUA_15),
                   mean weekly hours (PGE15_UAPV), total volunteers abs (PGE15_UAAV)

    This is the primary KPI dataset for the dashboard and research output.
    Annual. Reference year: 2023.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="85_84_DF_DCSA_VOLON1_1",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "85_84_DF_DCSA_VOLON1_1"
        df.attrs["description"]  = "Volunteering rate by gender, age group, and form (organised / direct)"
        df.attrs["key_metric"]   = "HAUA_15 = % of population ≥15 who volunteered"
        df.attrs["form_labels"]  = FORM_VOLUNT_LABELS
        df.attrs["start_period"] = "2015"
    return df


def fetch_volunteering_activity_type(session) -> pd.DataFrame:
    """
    85_84_DF_DCSA_VOLON1_2 — Type of activity performed.

    What volunteers actually do: emergency/rescue, social assistance,
    cultural activities, sport support, tutoring, etc.
    Cross-tabulated by gender and volunteering form (organised vs direct).
    Useful for characterising the nature of voluntary effort.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="85_84_DF_DCSA_VOLON1_2",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "85_84_DF_DCSA_VOLON1_2"
        df.attrs["description"] = "Type of activity performed by volunteers, by gender and form"
    return df


def fetch_volunteering_years_active(session) -> pd.DataFrame:
    """
    85_84_DF_DCSA_VOLON1_3 — Years of volunteering activity.

    How long volunteers have been active: <1 year, 1–2, 3–5, 6–10, >10.
    Useful for measuring loyalty/retention in the voluntary sector.
    Cross-tabulated by gender and form.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="85_84_DF_DCSA_VOLON1_3",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "85_84_DF_DCSA_VOLON1_3"
        df.attrs["description"] = "Years of volunteering activity by gender and form"
    return df


def fetch_volunteering_by_education_labour(session) -> pd.DataFrame:
    """
    85_84_DF_DCSA_VOLON1_4 — Volunteering by educational level and labour status.

    Rates and absolute estimates by highest education level attained
    and main labour market status (employed, unemployed, retired, student…).
    Key input for socioeconomic profiling of volunteers.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="85_84_DF_DCSA_VOLON1_4",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "85_84_DF_DCSA_VOLON1_4"
        df.attrs["description"] = "Volunteering by educational level and labour status"
    return df


def fetch_volunteering_by_household(session) -> pd.DataFrame:
    """
    85_84_DF_DCSA_VOLON1_5 — Volunteering by household size and economic resources.

    Volunteering rate by number of household components and self-assessed
    household economic resources (adequate / inadequate). Useful for
    understanding the relationship between social capital and economic conditions.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="85_84_DF_DCSA_VOLON1_5",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "85_84_DF_DCSA_VOLON1_5"
        df.attrs["description"] = "Volunteering by household size and economic resources"
    return df


def fetch_volunteering_by_region(session) -> pd.DataFrame:
    """
    85_84_DF_DCSA_VOLON1_6 — Volunteering by region and municipality type.

    Territorial breakdown of volunteering rates: all 21 regions plus
    municipality type (metropolitan, mid-size, small). Essential for
    North/Centre/South comparisons and urban/rural analysis.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="85_84_DF_DCSA_VOLON1_6",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"]       = "85_84_DF_DCSA_VOLON1_6"
        df.attrs["description"]       = "Volunteering rate by region and municipality type"
        df.attrs["territorial_note"]  = "Regional level (NUTS2) + municipality type"
    return df


# ── Organised volunteering: organisational breakdown ─────────────────────────

def fetch_org_volunteering_by_sector(session) -> pd.DataFrame:
    """
    85_171_DF_DCSA_VOLON_ORG1_1 — Organised volunteering by sector and multi-membership.

    Share of organised volunteers active in each sector (sport, culture,
    social assistance, civil protection, health, environment, religion,
    education, trade unions, other) and whether they belong to multiple
    organisations simultaneously. Cross-tabulated by gender.

    Key dataset for understanding where voluntary effort is concentrated
    within the formal third sector.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="85_171_DF_DCSA_VOLON_ORG1_1",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "85_171_DF_DCSA_VOLON_ORG1_1"
        df.attrs["description"] = (
            "Organised volunteering by main activity sector and multi-membership, by gender"
        )
        df.attrs["sector_note"] = (
            "Sector codes: 1=Sport, 1_CULREC=Culture/Recreation, 2=Trade unions, "
            "3=Health, 4=Social assistance, 5=Religion, 6=Environment, "
            "7=Education, 8=Civil protection, 9=Other, 10=Welfare/civil, 11=Politics, 12=Other"
        )
    return df


def fetch_org_volunteering_by_org_type(session) -> pd.DataFrame:
    """
    85_171_DF_DCSA_VOLON_ORG1_2 — Organised volunteering by institutional type.

    Which type of organisation volunteers work within: voluntary organisations
    (ODV), associations of social promotion (APS), social cooperatives,
    religious bodies, sports clubs, foundations, informal groups, etc.
    Cross-tabulated by gender. Directly maps to the legal categories defined
    by the Italian Third Sector Code (D.Lgs. 117/2017).
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="85_171_DF_DCSA_VOLON_ORG1_2",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "85_171_DF_DCSA_VOLON_ORG1_2"
        df.attrs["description"] = "Organised volunteering by institutional type (ODV, APS, cooperatives…)"
        df.attrs["legal_framework"] = "D.Lgs. 117/2017 — Codice del Terzo Settore"
    return df


def fetch_org_volunteering_motivations(session) -> pd.DataFrame:
    """
    85_171_DF_DCSA_VOLON_ORG1_3 — Organised volunteering by motivation.

    Why people volunteer: altruistic values, religious faith, social
    belonging, personal enrichment, professional skills development,
    reciprocity (helping those who helped them), etc. By gender.
    Important for donor/volunteer recruitment strategy research.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="85_171_DF_DCSA_VOLON_ORG1_3",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "85_171_DF_DCSA_VOLON_ORG1_3"
        df.attrs["description"] = "Motivations for organised volunteering, by gender"
    return df


def fetch_org_volunteering_personal_impact(session) -> pd.DataFrame:
    """
    85_171_DF_DCSA_VOLON_ORG1_4 — Organised volunteering: personal impact.

    Self-reported personal benefits of volunteering: new friendships,
    sense of usefulness, professional skills, improved wellbeing,
    civic engagement, etc. By gender. Relevant for social return on
    investment (SROI) frameworks and third-sector advocacy.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="85_171_DF_DCSA_VOLON_ORG1_4",
        start_period="2015",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "85_171_DF_DCSA_VOLON_ORG1_4"
        df.attrs["description"] = "Personal impact reported by organised volunteers, by gender"
    return df


# ── Associationism ────────────────────────────────────────────────────────────

def fetch_associationism_by_age(session) -> pd.DataFrame:
    """
    83_63_DF_DCCV_AVQ_PERSONE_129 — Membership in associations by age.

    Share of people belonging to at least one association, by detailed
    age group. Covers cultural, political, religious, sports, consumers'
    and other associations. From the annual Aspects of Daily Life survey.
    Annual series; last update September 2025.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="83_63_DF_DCCV_AVQ_PERSONE_129",
        start_period="2010",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "83_63_DF_DCCV_AVQ_PERSONE_129"
        df.attrs["description"] = "Association membership rate by age group"
        df.attrs["survey"]      = "Aspetti della Vita Quotidiana (AVQ)"
    return df


def fetch_associationism_by_age_education(session) -> pd.DataFrame:
    """
    83_63_DF_DCCV_AVQ_PERSONE_130 — Membership by age and educational level.

    Cross-tabulation of association membership by age group and highest
    educational qualification attained. Useful for identifying which
    segments of the population are more or less civically engaged.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="83_63_DF_DCCV_AVQ_PERSONE_130",
        start_period="2010",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "83_63_DF_DCCV_AVQ_PERSONE_130"
        df.attrs["description"] = "Association membership by age and educational level"
        df.attrs["survey"]      = "Aspetti della Vita Quotidiana (AVQ)"
    return df


def fetch_associationism_by_labour_status(session) -> pd.DataFrame:
    """
    83_63_DF_DCCV_AVQ_PERSONE_131 — Membership by occupational status.

    Association membership rates by condition and position in the labour
    market (employee, self-employed, unemployed, retired, student,
    homemaker, etc.). Relevant for understanding the relationship between
    work, time availability, and civil participation.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="83_63_DF_DCCV_AVQ_PERSONE_131",
        start_period="2010",
    )
    if not df.empty:
        df.attrs["dataset_code"] = "83_63_DF_DCCV_AVQ_PERSONE_131"
        df.attrs["description"] = "Association membership by occupational status"
        df.attrs["survey"]      = "Aspetti della Vita Quotidiana (AVQ)"
    return df


def fetch_associationism_by_region(session) -> pd.DataFrame:
    """
    83_63_DF_DCCV_AVQ_PERSONE_132 — Membership by region and municipality type.

    Territorial breakdown of association membership: all Italian regions
    and municipality size class. Complements the volunteering regional
    dataset (85_84_DF_DCSA_VOLON1_6) for a full territorial picture
    of civil society participation.
    """
    df = fetch_istat_csv(
        session,
        dataflow_id="83_63_DF_DCCV_AVQ_PERSONE_132",
        start_period="2010",
    )
    if not df.empty:
        df.attrs["dataset_code"]      = "83_63_DF_DCCV_AVQ_PERSONE_132"
        df.attrs["description"]      = "Association membership by region and municipality type"
        df.attrs["survey"]           = "Aspetti della Vita Quotidiana (AVQ)"
        df.attrs["territorial_note"] = "Regional level (NUTS2) + municipality type"
    return df


# ── Main loader class ─────────────────────────────────────────────────────────

class IstatSocialEconomyLoader(BaseLoader):
    """
    Fetches all ISTAT datasets for the social economy domain and saves
    them as raw parquet files.

    Covers three thematic modules:
      1. Volunteering — form, demographics, territory (6 dataflows)
      2. Organised volunteering — organisational breakdown (4 dataflows)
      3. Associationism — demographics and territory (4 dataflows)

    Each dataset is fetched independently. A failure in one does not
    block the others; failed datasets are logged as warnings and an
    empty DataFrame is returned in their place so downstream counts
    remain consistent.
    """

    SOURCE_ID = "istat"
    DOMAIN    = "social_economy"

    # ── Dataset registry ──────────────────────────────────────────────────────
    # Ordered by module and priority. Each tuple: (fetch_fn, log_label).
    DATASETS: list[tuple] = [

        # Module 1 — Volunteering
        (fetch_volunteering_by_age_gender,        "Volunteering: age, gender, form"),
        (fetch_volunteering_activity_type,         "Volunteering: activity types"),
        (fetch_volunteering_years_active,          "Volunteering: years active"),
        (fetch_volunteering_by_education_labour,   "Volunteering: education & labour status"),
        (fetch_volunteering_by_household,          "Volunteering: household & economic resources"),
        (fetch_volunteering_by_region,             "Volunteering: regional breakdown"),

        # Module 2 — Organised volunteering (organisational lens)
        (fetch_org_volunteering_by_sector,         "Org. volunteering: sector"),
        (fetch_org_volunteering_by_org_type,       "Org. volunteering: org. type (ODV/APS/…)"),
        (fetch_org_volunteering_motivations,       "Org. volunteering: motivations"),
        (fetch_org_volunteering_personal_impact,   "Org. volunteering: personal impact"),

        # Module 3 — Associationism
        (fetch_associationism_by_age,              "Associationism: by age"),
        (fetch_associationism_by_age_education,    "Associationism: by age & education"),
        (fetch_associationism_by_labour_status,    "Associationism: by labour status"),
        (fetch_associationism_by_region,           "Associationism: regional breakdown"),
    ]

    def fetch(self) -> list[pd.DataFrame]:
        """
        Fetch all configured ISTAT social economy datasets.

        Returns
        -------
        list[pd.DataFrame]
            One DataFrame per entry in DATASETS, in order.
            Empty DataFrames indicate fetch/parse failures.
        """
        results: list[pd.DataFrame] = []
        modules = {
            0: "── Module 1: Volunteering ────────────────────",
            6: "── Module 2: Organised volunteering ──────────",
            10: "── Module 3: Associationism ──────────────────",
        }

        for idx, (fetch_fn, label) in enumerate(self.DATASETS):
            if idx in modules:
                self.logger.info(modules[idx])

            try:
                df = fetch_fn(self.session)
                if not df.empty:
                # CRITICAL: Attach the unique dataflow_id as a suffix
                    # This prevents the BaseLoader from overwriting the same file 14 times.
                    df.attrs["filename_suffix"] = df.attrs.get("dataflow_id", f"dataset_{idx+1}")
                    status = f"{len(df):,} rows"
                else:
                    status = "⚠ empty"

                self.logger.info(f"  [{idx+1:02d}/{len(self.DATASETS)}] {label}: {status}")
                results.append(df)
            except Exception as exc:
                self.logger.warning(
                    f"  [{idx+1:02d}/{len(self.DATASETS)}] {label}: FAILED — {exc}. "
                    "Skipping; pipeline continues."
                )
                results.append(pd.DataFrame())

        non_empty = sum(1 for df in results if not df.empty)
        total_rows = sum(len(df) for df in results)
        self.logger.info(
            f"\nISTAT fetch complete: {non_empty}/{len(self.DATASETS)} datasets retrieved, "
            f"{total_rows:,} total observations."
        )
        return results


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    config = load_config()
    loader = IstatSocialEconomyLoader(config=config)
    written = loader.run()
    print("\nDone. Files written:")
    for p in written:
        print(f"  {p}")