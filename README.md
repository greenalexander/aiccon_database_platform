# aiccon-data

Contextual statistics platform for AICCON research and projects. Integrates European, national (Italy), and subnational data across the thematic areas of AICCON's work — social economy, welfare, immigration, social impact, and sustainable development — and makes them available through PowerBI dashboards.

Statistical context platform for AICCON research and projects. Collects data from European and Italian public APIs, harmonises it into a unified database divided into thematic (social economy, welfare, immigration, poverty, labour market, SDGs, and housing), and serves it to colleagues through PowerBI dashboards.

Maintained by one person. Updated monthly via a local Python pipeline. Colleagues access the data through PowerBI only — no coding required on their end.

---

## Purpose

Researchers and project staff at AICCON regularly need statistical context for their work: size of the third sector in a given region, poverty rates, immigration flows, SDG indicator progress, social spending trends. This project collects that data from authoritative public sources, integrates it into a unified database keyed by geography and time, and surfaces it in PowerBI for filtering and exploration.

A Python pipeline fetches data from Eurostat and ISTAT APIs, harmonises geographic codes (NUTS/ISTAT), legal form classifications, and indicator units, then loads everything into a DuckDB star schema. PowerBI connects to the resulting `.duckdb` file on SharePoint.

```
APIs (Eurostat, ISTAT)
        ↓  ingest
raw parquet files (SharePoint/aiccon-data/raw/)
        ↓  process
processed parquet files (SharePoint/aiccon-data/processed/)
        ↓  database
aiccon.duckdb (SharePoint/aiccon-data/database/)
        ↓
PowerBI dashboards
```

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
aiccon-data/
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
│   │   └── sharepoint_uploader.py writes files to SharePoint synced folder
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
│   │   ├── sdg_indicators.csv#   SDG goal+target <-> ISTAT/Eurostat indicator codes
│   │   └── domain_sources.csv     source registry with priority and coverage
│   └── pipeline.py                orchestrates processing for all active domains
│
├── database/
│   ├── schema/
│   │   ├── dimensions.sql         shared dimension tables (geo, time, source, indicator)
│   │   ├── fact_tables.sql        one fact table per domain, stubs for future domains
│   │   └── views.sql              analytical views consumed by PowerBI
│   ├── build_db.py                builds aiccon.duckdb from processed parquet
│   └── tests/
│       └── test_integrity.py      row counts, null checks, orphan key checks
│
├── config/
│   ├── settings.yaml              SharePoint paths, active domains, API scope
│   └── .env.example               credential template (copy to .env)
│
├── docs/
│   ├── data_dictionary.md         field definitions and units for every table
│   ├── source_log.md              download dates, dataset codes, known issues
│   ├── decisions.md               architectural and data decisions with rationale
│   └── maintenance.md             how to add new domains
│
├── run_pipeline.py           # Entry point: ingest -> process -> build database
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Setup

**Requirements**: Python 3.11+, OneDrive sync client running

```bash
git clone 
cd aiccon-data
pip install -r requirements.txt
cp config/.env.example config/.env
```

Edit `config/.env` and set `SHAREPOINT_ROOT` to the full path of your SharePoint synced folder.

---

## Running the pipeline

**Full monthly update** (all stages, all active domains):
```bash
python run_pipeline.py
```
**Single stage**:
```bash
python run_pipeline.py --stage ingest     # fetch from APIs only
python run_pipeline.py --stage process    # re-process existing raw files
python run_pipeline.py --stage database   # rebuild DuckDB only (fastest)
```

**Single domain** (useful when building or debugging a new domain):
```bash
python run_pipeline.py --domain social_economy
```

**Integrity checks** (run after every build):
```bash
python -m database.tests.test_integrity
```

### When to run which stage

| Situation | Command |
|---|---|
| Monthly update | `python run_pipeline.py` |
| Fixed a mapping CSV, no new data | `python run_pipeline.py --stage process` then `--stage database` |
| Fixed a bug in a merge script | `python run_pipeline.py --stage database` |
| Building a new domain | `python run_pipeline.py --domain {new_domain}` |

### Debugging failures

1. Read terminal output — `ERROR` lines include the full Python traceback
2. Check `pipeline_log.json` in SharePoint `database/` folder for a structured JSON summary
3. Run the failing stage in isolation: `python run_pipeline.py --stage process --domain social_economy`
4. Run individual scripts directly for maximum detail:
   `python -m ingestion.api_sources.social_economy.istat`

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

### PowerBI views

Connect PowerBI to these views rather than the raw fact tables.

**Social economy:**

| View | Description |
|---|---|
| `vw_social_economy` | Full flat view — primary PowerBI source for social economy |
| `vw_se_volunteering_national` | National volunteering rates by year — line charts |
| `vw_se_volunteering_regional` | Regional volunteering rates — map visuals |
| `vw_se_associationism_national` | Association membership rates by demographic |
| `vw_se_employment_eu` | EU employment comparison by NACE |
| `vw_se_local_units_regional` | Local units by NACE and region — density maps |

**Labour:**

| View | Description |
|---|---|
| `vw_labour` | Full flat view — primary PowerBI source for labour |
| `vw_labour_rates_italy` | Employment, unemployment and inactivity rates for Italy (national, annual) — line charts |
| `vw_labour_rates_regional` | Employment and unemployment rates at NUTS2/NUTS3 for Italy — map visuals |
| `vw_labour_neet` | NEET rate by region, age and citizenship |
| `vw_labour_employment_by_nace` | Employed persons by economic activity across EU — sector comparison |
| `vw_labour_unemployment_by_education` | Unemployment rate by education level — Italy and EU |
| `vw_labour_employment_edu_citizenship` | Employment rate by education × citizenship — Italy and EU |
| `vw_labour_force_monthly` | Monthly labour force headcount for Italy — use `period_label` as x-axis |

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
   - `database/build_db.py` → add to `DOMAIN_LOADERS`
   - `config/settings.yaml` → set `enabled: true`

4. **Fill in the stubs**:
   - `database/schema/fact_tables.sql` → replace `-- TODO` with actual columns
   - `database/schema/views.sql` → add base view and summary views
   - `database/tests/test_integrity.py` → fill in the stub check function

5. **Update the mappings**:
   - `processing/mappings/domain_sources.csv` → add source rows
   - `processing/mappings/nace_labels.csv` → add any new NACE codes if needed

6. **Document**:
   - `docs/data_dictionary.md` → add fact table field definitions and caveats
   - `docs/source_log.md` → add dataset codes, fetch dates, and known issues
   - `docs/decisions.md` → log any non-obvious choices

---

## Known limitations

- **NACE O-U at NUTS2**: Eurostat regional employment (`nama_10r_3empers`) only provides the O-U aggregate at NUTS2. Q, P, and S94 are not separable at regional level. Use `lfsa_egan22d` for sector detail (national level only).

- **Labour sub-national scope**: Eurostat LFS regional datasets (`lfst_r_lfu3rt`, `lfst_r_lfur2gan`, `lfst_r_lfe2emprtn`) are fetched for all countries but non-Italian sub-national rows are dropped in the merge step. All non-Italian countries are represented at NUTS0 (national total) only.

- **NEET series starts 2018**: Both ISTAT NEET datasets begin in 2018. There is no comparable regional NEET series from 2015 like the other RFL datasets.

- **RUNTS, Registro Imprese, Agenzia delle Entrate** are not API-integratable. They require manual downloading of data and integration.

- **Single-writer database**: DuckDB does not support concurrent writes. This is not a current issue (one maintainer, monthly batch updates) but would need to change if the project moves to multi-user cloud execution.

---

## Maintainer

**[Alexander Patrick Green]** — AICCON
Last pipeline run: see SharePoint `aiccon-data/database/pipeline_log.json`
Contact: [alexander.green@aiccon.it]