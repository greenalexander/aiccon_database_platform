This project is under active development. The architecture diagram and stack table reflect the target state.

# aiccon-data

A contextual statistics platform for social economy research. Collects data from European and Italian public APIs, harmonises it into a unified star-schema database organised by thematic domain — social economy, labour market, welfare, immigration, poverty, SDGs, and housing — and serves it through PowerBI dashboards and a natural language query interface.

The pipeline runs monthly on a schedule via GitHub Actions, writes intermediate artefacts to Google Cloud Storage, and loads the final dataset into BigQuery. An LLM-powered summary is generated automatically after each refresh. A Streamlit app allows natural language querying of the database via Gemini.

---

## Stack

| Layer | Technology | Why |
|---|---|---|
| Ingestion & processing | Python 3.11 | Mature SDMX/API client ecosystem; `pandas` and `pyarrow` for parquet |
| Intermediate storage | Google Cloud Storage | Durable, versioned parquet staging between pipeline stages; free tier sufficient for this data volume |
| Analytical database | BigQuery | Serverless, scalable, native PowerBI connector; SQL-compatible with DuckDB used in local dev |
| Transformation layer | dbt (BigQuery adapter) | Declarative SQL models, built-in testing, auto-generated documentation; clean separation of raw → staging → mart |
| Orchestration / CI/CD | GitHub Actions | Scheduled monthly runs, per-stage manual triggers, no infrastructure to manage |
| GenAI summaries | Gemini API (gemini-1.5-flash) | Monthly plain-language digest generated automatically post-refresh; cost-free on free tier at this call volume |
| NL query interface | Streamlit + Gemini | Text-to-SQL over the BigQuery schema; deployed on Streamlit Community Cloud |
| Local development | DuckDB | Fast local iteration against parquet files without a cloud connection |

---

## Architecture

```
APIs (Eurostat, ISTAT)
        ↓  ingest
raw parquet files (GCS: aiccon-data/raw/)
        ↓  process
processed parquet files (GCS: aiccon-data/processed/)
        ↓  dbt (staging → marts)
BigQuery dataset (aiccon_data)
        ↓                    ↓
PowerBI dashboards     Streamlit NL query app
                             ↓
                    Gemini monthly summary
```

GitHub Actions triggers the full pipeline on the first of each month, then calls the Gemini summary generation step. Individual stages can also be triggered manually via `workflow_dispatch`.

---

## Thematic domains

The database is organised into thematic domains. Each domain is a self-contained set of datasets sharing the same geographic and time dimensions. All domains share the same dimension tables — adding a new domain never requires changing existing tables.

| Domain | Status | Description |
|---|---|---|
| **Social economy** | ✅ Active | Volunteering rates, associationism, employment in social sectors, local units by NACE |
| **Labour** | ✅ Active | Employment, unemployment and inactivity rates, NEETs, labour force by NACE — Italy (NUTS3) and EU comparison |
| **Immigration** | 🔲 Stub | Resident foreign population, permits, asylum applications |
| **Welfare** | 🔲 Stub | Social spending, services, long-term care |
| **Poverty** | 🔲 Stub | At-risk-of-poverty, material deprivation, inequality |
| **SDGs** | 🔲 Stub | Italy and EU progress on Agenda 2030 indicators |
| **Housing** | 🔲 Stub | Affordability, social housing, homelessness |

New domains can be added without modifying the existing database structure. See [Adding a new domain](#adding-a-new-domain) below.

---

## Data sources

### APIs (automated)

| Source | Access | Coverage |
|---|---|---|
| Eurostat | SDMX / JSON API | EU27, NUTS0–2, all available years |
| ISTAT | SDMX API (esploradati.istat.it) | Italy, NUTS0–3, all available years |

### Manual downloads (not automated)

| Source | Coverage | Notes |
|---|---|---|
| RUNTS | Italy | Registry of third sector entities (post-2022); download from MLPS portal |
| Registro Imprese / Camere di Commercio | Italy, provincial | Cooperative register; export from InfoCamere |
| Agenzia delle Entrate | Italy | Fiscal data on nonprofits; annual publication |
| Ministero dell'Interno | Italy | Asylum and permit data not in ISTAT API |

Manual files are stored in `ingestion/manual_sources/` and are **not committed to Git**. Record the download date, source URL, and file description in `docs/source_log.md` each time they are refreshed.

---

## Repository structure

```
aiccon-data-platform/
├── ingestion/
│   ├── api_sources/
│   │   ├── social_economy/
│   │   │   ├── eurostat.py        fetches Eurostat social economy datasets
│   │   │   └── istat.py           fetches ISTAT social economy datasets
│   │   └── labour/
│   │       ├── eurostat.py        fetches Eurostat labour market datasets
│   │       └── istat.py           fetches ISTAT labour market datasets (RFL)
│   ├── loaders/
│   │   ├── base_loader.py         shared base class, retry logic, parquet I/O
│   │   └── gcs_uploader.py        writes raw and processed parquet to GCS
│   └── manual_sources/            raw downloaded files — gitignored
│
├── processing/
│   ├── harmonise/
│   │   ├── nuts_mapper.py         ISTAT codes → NUTS codes
│   │   └── legal_form.py          source legal forms → unified categories
│   ├── integrate/
│   │   ├── merge_social_economy.py harmonise + merge all social economy sources
│   │   └── merge_labour.py         harmonise + merge all labour sources
│   ├── mappings/
│   │   ├── nuts_istat.csv         NUTS ↔ ISTAT territorial codes (all 107 provinces)
│   │   ├── legal_form_map.csv     Italian legal forms ↔ unified categories ↔ NACE
│   │   ├── nace_labels.csv        NACE Rev.2 codes with Italian/English labels
│   │   ├── sdg_indicators.csv     SDG goal+target ↔ ISTAT/Eurostat indicator codes
│   │   └── domain_sources.csv     source registry with priority and coverage
│   └── pipeline.py                orchestrates ingestion and processing for all active domains
│
├── dbt/
│   ├── models/
│   │   ├── staging/               one model per raw source, light cleaning only
│   │   └── marts/                 joined, business-logic models consumed by PowerBI and the query app
│   ├── tests/                     dbt schema tests (not_null, unique, referential integrity)
│   ├── dbt_project.yml
│   └── profiles.yml.example       BigQuery connection template
│
├── database/
│   ├── schema/
│   │   ├── dimensions.sql         shared dimension tables (geo, time, source, indicator)
│   │   ├── fact_tables.sql        one fact table per domain, stubs for future domains
│   │   └── views.sql              analytical views (legacy; superseded by dbt marts)
│   └── tests/
│       └── test_integrity.py      row counts, null checks, orphan key checks
│
├── genai/
│   ├── summarise.py               generates monthly plain-language digest via Gemini
│   └── prompts/
│       └── monthly_summary.txt    prompt template for the summary generation step
│
├── query_app/
│   ├── app.py                     Streamlit NL query interface
│   ├── sql_generator.py           text-to-SQL via Gemini with schema injection
│   └── query_handler.py           BigQuery execution, error handling, result formatting
│
├── .github/
│   └── workflows/
│       ├── monthly_pipeline.yml   scheduled full run (1st of month) + summary generation
│       └── manual_pipeline.yml    workflow_dispatch with stage and domain inputs
│
├── config/
│   ├── settings.yaml              GCS paths, BigQuery dataset, active domains, API scope
│   └── .env.example               credential template (copy to .env)
│
├── docs/
│   ├── data_dictionary.md         field definitions and units for every table
│   ├── source_log.md              download dates, dataset codes, known issues
│   ├── decisions.md               architectural and data decisions with rationale
│   └── maintenance.md             how to add new domains
│
├── run_pipeline.py                entry point: ingest → process → dbt run → integrity checks
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Setup

**Requirements**: Python 3.11+, Google Cloud SDK, dbt-bigquery

```bash
git clone <repo-url>
cd aiccon-data
pip install -r requirements.txt
cp config/.env.example config/.env
```

**GCP credentials**: The pipeline authenticates via a service account. For local development, download the service account key JSON and set:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

In GitHub Actions, credentials are stored as a repository secret (`GCP_SA_KEY`) and injected at runtime — no key files are committed.

Edit `config/.env` and set `GCS_BUCKET`, `BQ_PROJECT`, and `BQ_DATASET` to your GCP resource names.

**dbt**: Copy `dbt/profiles.yml.example` to `~/.dbt/profiles.yml` and fill in your BigQuery project details.

---

## Running the pipeline

**Full monthly update** (all stages, all active domains):
```bash
python run_pipeline.py
```

**Single stage**:
```bash
python run_pipeline.py --stage ingest     # fetch from APIs, write raw parquet to GCS
python run_pipeline.py --stage process    # re-process existing raw files
python run_pipeline.py --stage database   # run dbt models only (fastest)
```

**Single domain** (useful when building or debugging a new domain):
```bash
python run_pipeline.py --domain social_economy
```

**Generate monthly summary only**:
```bash
python genai/summarise.py
```

**Integrity checks** (run after every pipeline execution):
```bash
python -m database.tests.test_integrity
```

### When to run which stage

| Situation | Command |
|---|---|
| Monthly update | `python run_pipeline.py` |
| Fixed a mapping CSV, no new data | `python run_pipeline.py --stage process` then `--stage database` |
| Fixed a bug in a dbt model | `python run_pipeline.py --stage database` |
| Building a new domain | `python run_pipeline.py --domain {new_domain}` |

### Debugging failures

1. Read terminal output — `ERROR` lines include the full Python traceback
2. Check `pipeline_log.json` in GCS `aiccon-data/database/` for a structured JSON summary
3. Run the failing stage in isolation: `python run_pipeline.py --stage process --domain social_economy`
4. Run individual scripts directly for maximum detail: `python -m ingestion.api_sources.social_economy.istat`
5. For dbt failures: `dbt run --select <model_name>` and check `dbt/logs/dbt.log`

---

## Database schema

### Shared dimension tables (never change when adding a domain)

| Table | Description |
|---|---|
| `dim_geography` | All territories from EU countries to Italian provinces (NUTS0–3) |
| `dim_time` | Annual periods 1990–2030 |
| `dim_source` | Source registry with priority ranking |
| `dim_legal_form` | Italian legal forms mapped to unified categories and NACE |
| `dim_indicator` | Indicator catalogue with Italian and English labels |

### Fact tables (one per domain)

| Table | Status | Key dimensions |
|---|---|---|
| `fact_social_economy` | ✅ Active | `legal_form_key`, `nace_code`, `gender`, `age_group`, `volunteering_form`, `org_type`, `association_type` |
| `fact_labour` | ✅ Active | `nace_code`, `gender`, `age_group`, `education`, `citizenship`, `frequency`, `period_label`, `adjustment` |
| `fact_immigration` | 🔲 Stub | `nationality`, `permit_type`, `migration_flow` |
| `fact_welfare` | 🔲 Stub | `service_type`, `beneficiary_group` |
| `fact_poverty` | 🔲 Stub | `population_group`, `deprivation_type` |
| `fact_sdg` | 🔲 Stub | `sdg_goal`, `sdg_target` |
| `fact_housing` | 🔲 Stub | `tenure_type` |

### dbt marts (PowerBI and query app connect to these)

dbt staging models clean and type-cast raw BigQuery tables. Mart models join dimensions to fact tables and apply business logic. Run `dbt docs generate && dbt docs serve` for full interactive documentation.

**Social economy marts:**

| Model | Description |
|---|---|
| `mart_social_economy` | Full flat model — primary PowerBI source for social economy |
| `mart_se_volunteering_national` | National volunteering rates by year |
| `mart_se_volunteering_regional` | Regional volunteering rates |
| `mart_se_associationism_national` | Association membership rates by demographic |
| `mart_se_employment_eu` | EU employment comparison by NACE |
| `mart_se_local_units_regional` | Local units by NACE and region |

**Labour marts:**

| Model | Description |
|---|---|
| `mart_labour` | Full flat model — primary PowerBI source for labour |
| `mart_labour_rates_italy` | Employment, unemployment and inactivity rates for Italy (national, annual) |
| `mart_labour_rates_regional` | Employment and unemployment rates at NUTS2/NUTS3 for Italy |
| `mart_labour_neet` | NEET rate by region, age and citizenship |
| `mart_labour_employment_by_nace` | Employed persons by economic activity across EU |
| `mart_labour_unemployment_by_education` | Unemployment rate by education level — Italy and EU |
| `mart_labour_employment_edu_citizenship` | Employment rate by education × citizenship |
| `mart_labour_force_monthly` | Monthly labour force headcount for Italy |

---

## GenAI monthly summary

After each pipeline run, `genai/summarise.py` queries the BigQuery mart tables for the most recently updated indicators and sends them to Gemini with a structured prompt. The output is a plain-language summary of what changed in the latest data refresh — new data points, notable trends, any source issues flagged during ingestion.

The summary is written to GCS (`aiccon-data/summaries/YYYY-MM.md`) and printed to the GitHub Actions run log.

---

## Natural language query interface

The Streamlit app at `query_app/app.py` allows plain-language querying of the BigQuery database. The flow is:

1. User enters a question in natural language
2. The BigQuery schema (table names, column names, descriptions from `docs/data_dictionary.md`) is injected into a Gemini prompt
3. Gemini returns a SQL query
4. The query is validated (table/column names checked against the schema before execution) and run against BigQuery
5. Results are displayed as a table, with the generated SQL shown beneath for transparency

Ambiguous queries (references to undefined geographies, unmapped indicator names) are caught at the validation step and returned to the user with a clarifying prompt rather than executed.

Deploy locally:
```bash
streamlit run query_app/app.py
```

Or via Streamlit Community Cloud (free tier) — connect the GitHub repo and set GCP credentials as Streamlit secrets.

---

## Adding a new domain

The social economy and labour domains are the templates. For each new domain:

1. **Create fetcher scripts** in `ingestion/api_sources/{domain}/`
   following `labour/eurostat.py` and `labour/istat.py`

2. **Create a merge script** at `processing/integrate/merge_{domain}.py`
   following `merge_labour.py`

3. **Register in four places**:
   - `run_pipeline.py` → add loader classes to `DOMAIN_INGESTION_CLASSES`
   - `processing/pipeline.py` → add to `DOMAIN_PROCESSORS`
   - `dbt/models/` → add staging and mart models
   - `config/settings.yaml` → set `enabled: true`

4. **Fill in the stubs**:
   - `database/schema/fact_tables.sql` → replace `-- TODO` with actual columns
   - `database/tests/test_integrity.py` → fill in the stub check function

5. **Update the mappings**:
   - `processing/mappings/domain_sources.csv` → add source rows
   - `processing/mappings/nace_labels.csv` → add any new NACE codes if needed

6. **Document**:
   - `docs/data_dictionary.md` → add fact table field definitions (also used by the NL query app for schema injection)
   - `docs/source_log.md` → add dataset codes, fetch dates, and known issues
   - `docs/decisions.md` → log any non-obvious choices

---

## Known limitations

- **NACE O-U at NUTS2**: Eurostat regional employment (`nama_10r_3empers`) only provides the O-U aggregate at NUTS2. Q, P, and S94 are not separable at regional level. Use `lfsa_egan22d` for sector detail (national level only).

- **Labour sub-national scope**: Eurostat LFS regional datasets (`lfst_r_lfu3rt`, `lfst_r_lfur2gan`, `lfst_r_lfe2emprtn`) are fetched for all countries but non-Italian sub-national rows are dropped in the merge step. All non-Italian countries are represented at NUTS0 (national total) only.

- **NEET series starts 2018**: Both ISTAT NEET datasets begin in 2018. There is no comparable regional NEET series from 2015 like the other RFL datasets.

- **RUNTS, Registro Imprese, Agenzia delle Entrate** are not API-integratable. They require manual downloading and integration.

- **BigQuery free tier**: The project is designed to stay within BigQuery's free tier (10 GB storage, 1 TB queries/month). The NL query interface validates and limits queries before execution to avoid unexpectedly large scans.





Run a single domain in isolation: python run_pipeline.py --stage ingest --domain social_economy
Check GCS to confirm parquet files landed in the right folders
Check the filenames - see if they include datetime stamps. 
Run --stage database and check BigQuery tables exist and have rows

Only then run the full pipeline

1. Delete the BigQuery current facts tables for social economy and labour. (It is appending to past versions)
2. Rerun the --stage process --domain labour. (check for provincial data warnings)
3. Rerun the --stage database

Identify how to connect PowerBI to BigQuery database.
Identify how to update integrity_tests.py (or maybe the step is to set up tests within GC, Bigquery or storage? I don't know).
Then after these things we move towards the CICD with GitActions.