This project is under active development. The architecture diagram and stack table describe the target state; components marked *planned* are not built yet.

# aiccon-data

A contextual statistics platform for social economy research. Collects data from European and Italian public APIs, harmonises it into a unified star-schema database organised by thematic domain (social economy, labour market, welfare, immigration, poverty, SDGs and housing) and serves it through PowerBI dashboards and, later, a natural language query interface.

The pipeline runs automatically on the 1st of each month via GitHub Actions. It writes intermediate artefacts to Google Cloud Storage, loads the final dataset into BigQuery and then runs integrity checks against it. The same workflow can be started by hand from GitHub, for any stage and domain.

---

## Stack

| Layer | Technology | Why |
|---|---|---|
| Ingestion & processing | Python 3.12 | Mature SDMX/API client ecosystem; `pandas` and `pyarrow` for parquet |
| Intermediate storage | Google Cloud Storage | Durable parquet staging between pipeline stages; free tier sufficient for this data volume |
| Analytical database | BigQuery | Serverless, native PowerBI connector |
| Integrity checks | Python (`database/tests/test_integrity.py`) | Row counts, nulls, key uniqueness, orphan keys, coverage and freshness checks after every load |
| Orchestration / CI/CD | GitHub Actions | Scheduled monthly run plus manual runs by stage and domain; no infrastructure to manage; free for public repositories |
| Cloud authentication from CI | Workload Identity Federation | No long-lived service account key stored anywhere (see `docs/decisions.md`) |
| Transformation layer (*planned*) | dbt (BigQuery adapter) | Declarative SQL models, built-in testing, generated documentation; raw → staging → mart |
| GenAI summaries (*planned*) | Gemini API | Monthly plain-language digest generated after each refresh |
| NL query interface (*planned*) | Streamlit + Gemini | Text-to-SQL over the BigQuery schema |
| Local development | uv, DuckDB | uv manages the local Python environment; DuckDB for quick local exploration of parquet files |

---

## Architecture

```
APIs (Eurostat, ISTAT)
        ↓  ingest
raw parquet files (GCS: aiccon-data/raw/{domain}/)
        ↓  process
processed parquet files (GCS: aiccon-data/processed/{domain}/)
        ↓  database (build_db.py; dbt planned)
BigQuery dataset (aiccon_data)
        ↓  integrity checks (test_integrity.py)
        ↓                    ↓
PowerBI dashboards     Streamlit NL query app (planned)
                             ↓
                    Gemini monthly summary (planned)
```

GitHub Actions runs ingest → process → database → integrity checks on the 1st of each month. Any stage and domain can also be run by hand from the Actions tab (see [Automated runs (GitHub Actions)](#automated-runs-github-actions)).

---

## Thematic domains

The database is organised into thematic domains. Each domain is a self-contained set of datasets sharing the same geographic and time dimensions. All domains share the same dimension tables, so adding a new domain never requires changing existing tables.

| Domain | Status | Description |
|---|---|---|
| **Social economy** | ✅ Active | Volunteering rates, associationism, employment in social sectors, local units by NACE |
| **Labour** | ✅ Active | Employment, unemployment and inactivity rates, NEETs, labour force by NACE: Italy (NUTS3) and EU comparison |
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
| ISTAT | SDMX REST API (`esploradati.istat.it/SDMXWS/rest`) | Italy, NUTS0–3, all available years |

ISTAT limits its SDMX service to 5 queries per minute per IP address and blocks addresses that exceed it for one to two days. `base_loader.py` therefore spaces requests to ISTAT at least 13 seconds apart, retries included. This adds up to about five minutes to a full run.

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
aiccon_database_platform/
├── ingestion/
│   ├── api_sources/
│   │   ├── social_economy/
│   │   │   ├── eurostat.py          fetches Eurostat social economy datasets
│   │   │   └── istat.py             fetches ISTAT social economy datasets
│   │   └── labour/
│   │       ├── eurostat_labour.py   fetches Eurostat labour market datasets
│   │       └── istat_labour.py      fetches ISTAT labour market datasets (RFL)
│   ├── loaders/
│   │   ├── base_loader.py           shared base class, retry logic, ISTAT pacing, parquet I/O
│   │   └── gcs_uploader.py          writes raw and processed parquet to GCS
│   └── manual_sources/              raw downloaded files (gitignored)
│
├── processing/
│   ├── harmonise/
│   │   ├── nuts_mapper.py           ISTAT codes → NUTS codes
│   │   └── legal_form_normaliser.py source legal forms → unified categories
│   ├── integrate/
│   │   ├── merge_social_economy.py  harmonise + merge all social economy sources
│   │   └── merge_labour.py          harmonise + merge all labour sources
│   ├── mappings/
│   │   ├── nuts_istat.csv           NUTS 2024 ↔ ISTAT territorial codes (all 107 provinces)
│   │   ├── legal_form_map.csv       Italian legal forms ↔ unified categories ↔ NACE
│   │   ├── nace_labels.csv          NACE Rev.2 codes with Italian/English labels
│   │   ├── sdg_indicators.csv       SDG goal+target ↔ ISTAT/Eurostat indicator codes
│   │   └── domain_sources.csv       source registry with priority and coverage
│   └── pipeline.py                  orchestrates processing for all active domains
│
├── database/
│   ├── build_db.py                  loads processed parquet from GCS into BigQuery
│   ├── schema/
│   │   ├── dimensions.sql           shared dimension tables (geo, time, source, legal form, indicator)
│   │   ├── fact_tables.sql          one fact table per domain, stubs for future domains
│   │   └── views.sql                analytical views (legacy; to be superseded by dbt marts)
│   └── tests/
│       └── test_integrity.py        post-load integrity checks (see "Integrity checks")
│
├── dbt/                             (planned) staging and mart models
├── genai/                           (planned) Gemini monthly summary
├── query_app/                       (planned) Streamlit NL query interface
│
├── .github/
│   └── workflows/
│       └── pipeline.yml             monthly schedule + manual runs, then integrity checks
│
├── config/
│   ├── settings.yaml                GCS layout, BigQuery location, active domains, API scope
│   └── .env.example                 template for local environment variables (copy to .env)
│
├── docs/
│   ├── data_dictionary.md           field definitions and units for every table
│   ├── source_log.md                download dates, dataset codes, known issues
│   ├── decisions.md                 architectural and data decisions with rationale
│   └── maintenance.md               how to add new domains
│
├── run_pipeline.py                  entry point: ingest → process → database
├── requirements.txt                 runtime dependencies, pinned (used by GitHub Actions)
├── pyproject.toml, uv.lock          local environment managed by uv
├── .gitignore
└── README.md
```

---

## Setup (local)

**Requirements**: Python 3.12, [uv](https://docs.astral.sh/uv/), access to the GCP project `aiccon-database-platform`.

```powershell
git clone https://github.com/greenalexander/aiccon_database_platform.git
cd aiccon_database_platform
uv sync
copy config\.env.example config\.env
```

`uv sync` creates `.venv` from `uv.lock`. Alternatively, `pip install -r requirements.txt` in any Python 3.12 environment installs exactly what the pipeline needs (this is what GitHub Actions does).

**Dependencies.** `requirements.txt` lists only the packages the code imports at runtime, pinned to tested versions. When you add a package the pipeline imports (`uv add ...`), add it to `requirements.txt` as well, or the GitHub run will fail with `ModuleNotFoundError`. Tidying `pyproject.toml` so that GitHub Actions can install from `uv.lock` instead is an open item.

**Environment variables.** Edit `config/.env` and set:

| Variable | Value |
|---|---|
| `GCS_BUCKET` | `data-aiccon-database-platform` |
| `BQ_PROJECT` | `aiccon-database-platform` |
| `BQ_DATASET` | `aiccon_data` |
| `GOOGLE_APPLICATION_CREDENTIALS` | path to the local service account key, e.g. `config/service-account-key.json` |

The code loads `config/.env` if it exists but never overrides variables that are already set, so the same code runs unchanged in GitHub Actions, where no `.env` exists.

**Local GCP credentials.** Local runs authenticate as the service account `aiccon-pipeline@aiccon-database-platform.iam.gserviceaccount.com` using a key file. The key must never be committed: `.gitignore` excludes `.env` and every `*.json` file. GitHub Actions does **not** use this key (see below).

---

## Running the pipeline

**Full update** (all stages, all active domains):
```bash
python run_pipeline.py
```

**Single stage**:
```bash
python run_pipeline.py --stage ingest     # fetch from APIs, write raw parquet to GCS
python run_pipeline.py --stage process    # re-process existing raw files
python run_pipeline.py --stage database   # reload BigQuery from processed files (fastest)
```

**Single domain** (useful when building or debugging a domain):
```bash
python run_pipeline.py --domain social_economy
```

**Integrity checks** (run automatically after every GitHub run):
```bash
python -m database.tests.test_integrity
python -m database.tests.test_integrity --domain labour
```

The pipeline exits with code 1 if any stage fails, including a dataset that an API failed to return. Later stages still run: if a download fails, processing falls back to the previous raw file for that dataset.

### When to run which stage

| Situation | Command |
|---|---|
| Monthly update | Automatic (GitHub Actions), or `python run_pipeline.py` |
| Fixed a mapping CSV, no new data needed | `--stage process`, then `--stage database` |
| Fixed a bug in the database loader | `--stage database` |
| Building a new domain | `--domain {new_domain}` |

---

## Automated runs (GitHub Actions)

The workflow is defined in `.github/workflows/pipeline.yml`.

### When it runs

- **Scheduled:** at 06:17 UTC on the 1st of every month (07:17 Italian time in winter, 08:17 in summer). Cron schedules in GitHub Actions are in UTC. GitHub may start scheduled runs late at busy times, which is why the minute is not :00.
- **Manually:** on GitHub, go to **Actions → Monthly pipeline → Run workflow**, choose a stage (`all`, `ingest`, `process`, `database`) and a domain (`all`, `social_economy`, `labour`), and tick or untick the integrity checks. `--stage database` is a quick test: it takes about two minutes and makes no API calls.

Only one run can be active at a time; a run started while another is in progress waits for it.

### What a run does

1. Checks out the repository on a fresh Ubuntu 24.04 machine (a GitHub-hosted *runner*).
2. Authenticates to Google Cloud with Workload Identity Federation.
3. Installs Python 3.12 and `requirements.txt`.
4. Runs `python run_pipeline.py` with the chosen stage and domain.
5. Runs the integrity checks. They run **even if the pipeline failed**, but not if the run was cancelled or never reached the pipeline step.

The run is marked as failed if either the pipeline or the integrity checks fail. The integrity report appears on the run's **Summary** page.

### Authentication: Workload Identity Federation

No service account key is stored in GitHub. On each run, GitHub issues a short-lived signed token identifying the repository; Google exchanges it for a short-lived token for a dedicated service account. The rationale is in `docs/decisions.md`.

| Component | Value |
|---|---|
| Workload identity pool | `github` (global) |
| Provider | `aiccon-data-repo`, issuer `https://token.actions.githubusercontent.com` |
| Attribute condition | `assertion.repository == 'greenalexander/aiccon_database_platform'` |
| Service account | `github-actions-pipeline@aiccon-database-platform.iam.gserviceaccount.com` |
| Its roles | `roles/storage.objectAdmin` on the bucket; `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the project |
| Who may impersonate it | `roles/iam.workloadIdentityUser` granted to `principalSet://…/workloadIdentityPools/github/attribute.repository/greenalexander/aiccon_database_platform` |

**If the repository is renamed or moved** (for example to an AICCON organisation account), authentication will stop working until both the attribute condition and the `workloadIdentityUser` binding are updated to the new `owner/repo` name.

### Configuration: repository variables

The workflow reads five **repository variables** (Settings → Secrets and variables → Actions → Variables). None of them is a credential, so no **secrets** are used.

| Variable | Value |
|---|---|
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | `projects/1080974298195/locations/global/workloadIdentityPools/github/providers/aiccon-data-repo` |
| `GCP_SERVICE_ACCOUNT` | `github-actions-pipeline@aiccon-database-platform.iam.gserviceaccount.com` |
| `GCS_BUCKET` | `data-aiccon-database-platform` |
| `BQ_PROJECT` | `aiccon-database-platform` |
| `BQ_DATASET` | `aiccon_data` |

### Notifications

GitHub emails the outcome of a run according to your personal notification settings (the Actions section of your GitHub notification settings, where you can choose to be notified of failed runs only). Notifications for **manual** runs go to whoever started the run. Notifications for **scheduled** runs go to whoever created the workflow or last edited its `cron` line, or to whoever last re-enabled the workflow after it was disabled.

### When a run fails

1. Open the run in the Actions tab, click the job **pipeline**, and expand the step with the red cross. The cog icon offers the raw log.
2. Check the run's **Summary** page for the integrity report.
3. If ISTAT is the cause (HTTP 500 errors from `esploradati.istat.it`), check `https://esploradati.istat.it/SDMXWS/rest/dataflow/all/all/latest` in a browser. When it returns XML again, re-run the workflow by hand. ISTAT's SDMX service has outages lasting hours to a day; the data browser can work while the SDMX service does not. There is deliberately no automatic retry.
4. Otherwise, follow [Debugging failures](#debugging-failures).

### Keeping the schedule alive

In public repositories, GitHub disables scheduled workflows after 60 days without repository activity; according to user reports, it emails a warning a few days beforehand. During active development this does not arise. If it happens, open **Actions → Monthly pipeline** and click **Enable workflow**. To prevent it during a quiet period, push any commit. An empty commit should be enough, though GitHub does not document exactly what counts as activity: `git commit --allow-empty -m "Keep scheduled workflow active"` then `git push`.

### Cost

GitHub Actions is free for public repositories on standard runners. A full run takes roughly 15–25 minutes, depending on ISTAT. BigQuery and GCS usage stays within the free tier; integrity-check queries are capped at 1 GB billed each.

---

## Integrity checks

`database/tests/test_integrity.py` checks the BigQuery dataset after each load. It exits with code 1 if any check **FAILs** or **could not run**; **WARN** results are reported but never fail a run. In GitHub Actions it also writes a markdown report to the run's Summary page.

**Dimension tables** (always checked): row counts, required columns not null, surrogate and natural key uniqueness, Italian NUTS0/2/3 coverage in `dim_geography`, and Italian codes that did not come from `nuts_istat.csv` (FAIL: they mean the mapping is incomplete).

**Fact tables** (one block per domain, configured in `FACT_CHECKS`): row count, nulls and `fact_key` uniqueness; grain duplicates (the signature of a double load); orphan foreign keys; plausible value bounds (counts ≥ 0, rates 0–100); rows from each expected provider; Italian NUTS coverage; number of other countries; expected indicators and frequencies; optional columns not entirely empty; and (WARN) whether each expected dataset is present and was extracted within 45 days (`--max-age-days` to change).

**Known accepted gap:** the labour check expects data for at least **105 of 107** provinces, not 107, because two provinces are currently missing. This is deliberate, so that a failed run always signals something new. Restore 107 in `FACT_CHECKS` once the gap is fixed.

**Adding a domain:** add an entry to `FACT_CHECKS`. A domain enabled in `settings.yaml` without an entry produces a WARN.

---

## Debugging failures

1. Read the log: locally the terminal, on GitHub the failed step's log. `ERROR` lines include the full Python traceback.
2. Check `pipeline_log.json` in GCS `aiccon-data/database/` for a structured JSON summary of the most recent run.
3. Run the failing stage in isolation: `python run_pipeline.py --stage process --domain social_economy`
4. Run individual scripts directly for maximum detail, e.g. `python -m ingestion.api_sources.social_economy.istat` or `python -m database.build_db --domain labour`.
5. Re-run the integrity checks: `python -m database.tests.test_integrity`

---

## Database schema

### Shared dimension tables (never change when adding a domain)

| Table | Description |
|---|---|
| `dim_geography` | All territories from EU countries to Italian provinces (NUTS0–3, NUTS 2024), plus ISTAT aggregates such as Nord and Mezzogiorno (`nuts_level` NULL). Retired codes are kept with `is_active = FALSE` |
| `dim_time` | Annual periods 1990–2030 |
| `dim_source` | Source registry with priority ranking |
| `dim_legal_form` | Italian legal forms mapped to unified categories and NACE |
| `dim_indicator` | Indicator catalogue with Italian and English labels |

Dimension rows keep their surrogate keys across runs; fact tables are replaced in full on every load.

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

`fact_labour` mixes annual, quarterly and monthly rows, all attached to the annual `time_key`, and stores totals next to their components. Any aggregation must filter on `frequency` and on the relevant totals; the planned dbt marts will encode this once.

### dbt marts (*planned*)

PowerBI and the query app will connect to mart models rather than the raw fact tables.

**Social economy marts:** `mart_social_economy` (full flat model), `mart_se_volunteering_national`, `mart_se_volunteering_regional`, `mart_se_associationism_national`, `mart_se_employment_eu`, `mart_se_local_units_regional`.

**Labour marts:** `mart_labour` (full flat model), `mart_labour_rates_italy`, `mart_labour_rates_regional`, `mart_labour_neet`, `mart_labour_employment_by_nace`, `mart_labour_unemployment_by_education`, `mart_labour_employment_edu_citizenship`, `mart_labour_force_monthly`.

When the marts exist, list them under `"views"` in `FACT_CHECKS` so the integrity checks confirm they exist and return rows.

---

## GenAI monthly summary (*planned*)

After each pipeline run, `genai/summarise.py` will query the mart tables for the most recently updated indicators and ask Gemini for a plain-language summary of what changed. The output will be written to GCS (`aiccon-data/summaries/YYYY-MM.md`) and added as a step to the GitHub workflow.

## Natural language query interface (*planned*)

A Streamlit app (`query_app/app.py`) will accept plain-language questions, generate SQL with Gemini using the schema and `docs/data_dictionary.md`, validate table and column names before execution, and show the results with the generated SQL. Ambiguous questions will be returned with a clarifying prompt rather than executed.

---

## Adding a new domain

The social economy and labour domains are the templates. For each new domain:

1. **Create fetcher scripts** in `ingestion/api_sources/{domain}/`, following `labour/eurostat_labour.py` and `labour/istat_labour.py`.
2. **Create a merge script** at `processing/integrate/merge_{domain}.py`, following `merge_labour.py`.
3. **Register it**:
   - `run_pipeline.py` → add loader classes to `DOMAIN_INGESTION_CLASSES`
   - `processing/pipeline.py` → add to `DOMAIN_PROCESSORS`
   - `database/build_db.py` → add a loader with `@domain_loader("{domain}")`
   - `config/settings.yaml` → set `enabled: true`
4. **Fill in the stubs**: replace the `-- TODO` columns in `database/schema/fact_tables.sql`.
5. **Add integrity checks**: add an entry to `FACT_CHECKS` in `database/tests/test_integrity.py`.
6. **Update the mappings**: `processing/mappings/domain_sources.csv` (source rows) and `nace_labels.csv` if needed.
7. **Update the GitHub workflow**: add the domain to the `domain` options in `.github/workflows/pipeline.yml` so it can be run on its own.
8. **Add any new packages** to `requirements.txt`.
9. **Document**: `docs/data_dictionary.md`, `docs/source_log.md`, and `docs/decisions.md` for any non-obvious choice.

---

## Known limitations

- **NACE O-U at NUTS2**: Eurostat regional employment (`nama_10r_3empers`) only provides the O-U aggregate at NUTS2. Q, P and S94 are not separable at regional level. Use `lfsa_egan22d` for sector detail (national level only).
- **Labour sub-national scope**: Eurostat LFS regional datasets (`lfst_r_lfu3rt`, `lfst_r_lfur2gan`, `lfst_r_lfe2emprtn`) are fetched for all countries, but non-Italian sub-national rows are dropped when loading. Other countries are represented at NUTS0 only.
- **NEET series starts 2018**: both ISTAT NEET datasets begin in 2018.
- **Provincial labour coverage**: 105 of 107 provinces currently have data (see Integrity checks).
- **ISTAT SDMX availability**: the SDMX service has outages, and requests are paced to respect its 5-per-minute limit. A failed monthly run caused by ISTAT is re-run by hand.
- **RUNTS, Registro Imprese, Agenzia delle Entrate** are not available through APIs and require manual download and integration.
- **BigQuery free tier**: the project is designed to stay within BigQuery's free tier (10 GB storage, 1 TB queries per month).

---

## Open items

- Fix the two missing provinces in the labour data, then restore 107 in the integrity check.
- Investigate `150_873_DF_DCCV_FORZLVMENS1_1` (monthly labour force): its latest successful extraction is months old.
- Build the dbt staging and mart models, then connect PowerBI to the marts rather than the raw fact tables.
- Tidy `pyproject.toml` (move Jupyter and other tools to the `dev` group, declare `google-cloud-bigquery`, `google-cloud-storage` and `PyYAML`, drop unused `pandas-gbq`), then install from `uv.lock` in GitHub Actions and retire `requirements.txt`.
- Gemini monthly summary and Streamlit query app.