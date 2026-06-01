# Source Log

Record of every data source: when it was last refreshed, where it came from, and any issues encountered. Updated every time the pipeline is run or download new manual data.

---

## API sources (automated)

These are fetched automatically by the pipeline. Log any changes to dataset
codes, endpoint URLs, or data availability here so future-you has context.

### Eurostat

| Dataset | Code | Last fetched | Notes |
|---|---|---|---|
| Regional employment by NACE aggregate | `nama_10r_3empers` | 2026-05 | NACE O-U only at NUTS2 — Q/P/S94 not separable at regional level. Fetched with `wstatus=EMP`, `unit=THS` |
| National employment by detailed NACE | `lfsa_egan22d` | 2026-05 | NUTS0 only. Provides Q86/Q87/Q88/P85/S94/S96 breakdown. Unit THS_PER |
| Local units by NACE and NUTS2 | `sbs_r_nuts2021` | 2026-05 | Replaced `bd_enace2_r3` which returned 413 errors. S94 not available in this dataset |

**Endpoint**: `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data`

**Known issues**:
- `soc_econ_emp` (pilot social economy satellite account) returns 404 — removed permanently
- 413 errors occur when requesting multiple NACE codes + all geographies + all years in one call — solved by paginating on NACE code
- `bd_enace2_r3` replaced by `sbs_r_nuts2021` (May 2026) — same organisational data but updated dataset

#### Labour market

| Dataset | Code | Last fetched | Notes |
|---|---|---|---|
| National employment by all NACE activities | `lfsa_egan22d` | 2026-05 | NUTS0 only. All NACE two-digit codes (sections A–U plus TOTAL), fetched in 13 section groups. Unit THS_PER. Sex and broad age bands |
| Unemployment rate by education level | `lfst_r_lfu3rt` | 2026-05 | NUTS2 (Italy) + NUTS0 (all EU). ISCED 2011 levels ED0-2 / ED3_4 / ED5-8 / TOTAL. Paginated by education level |
| Unemployment rate by citizenship | `lfst_r_lfur2gan` | 2026-05 | NUTS2 (Italy) + NUTS0 (all EU). Citizenship: NAT / FOR / TOTAL. Paginated by citizenship |
| Employment rate by education and citizenship | `lfst_r_lfe2emprtn` | 2026-05 | NUTS2 (Italy) + NUTS0 (all EU). Age dimension omitted — returns total working-age aggregate. Paginated by education level |

---

### ISTAT

**Endpoint**: `https://esploradati.istat.it/SDMXWS/rest`

All active datasets are now served from `esploradati.istat.it` with numeric dataflow IDs.

#### Volunteering survey (Indagine sul Volontariato, February 2026)

| Dataset | Code | Last fetched | Notes |
|---|---|---|---|
| Volunteering by gender, age, form | `85_84_DF_DCSA_VOLON1_1` | 2026-05 | Multiple data_types in one dataset: percentage rate, thousands, mean hours, absolute count. Check `unit` column per row |
| Volunteering by activity type | `85_84_DF_DCSA_VOLON1_2` | 2026-05 | |
| Volunteering by years active | `85_84_DF_DCSA_VOLON1_3` | 2026-05 | |
| Volunteering by education and labour status | `85_84_DF_DCSA_VOLON1_4` | 2026-05 | |
| Volunteering by household and economic resources | `85_84_DF_DCSA_VOLON1_5` | 2026-05 | |
| Volunteering by region and municipality type | `85_84_DF_DCSA_VOLON1_6` | 2026-05 | Regional breakdown — only ISTAT social economy dataset with NUTS2 granularity |

#### Organised volunteering (same survey, organisational lens)

| Dataset | Code | Last fetched | Notes |
|---|---|---|---|
| By sector and multi-membership | `85_171_DF_DCSA_VOLON_ORG1_1` | 2026-05 | |
| By institutional type (ODV, APS, …) | `85_171_DF_DCSA_VOLON_ORG1_2` | 2026-05 | Maps to legal forms in D.Lgs. 117/2017 |
| By motivation | `85_171_DF_DCSA_VOLON_ORG1_3` | 2026-05 | |
| By personal impact | `85_171_DF_DCSA_VOLON_ORG1_4` | 2026-05 | Relevant for SROI frameworks |

#### Associationism (Aspetti della Vita Quotidiana, September 2025)

| Dataset | Code | Last fetched | Notes |
|---|---|---|---|
| By age | `83_63_DF_DCCV_AVQ_PERSONE_129` | 2026-05 | |
| By age and educational level | `83_63_DF_DCCV_AVQ_PERSONE_130` | 2026-05 | |
| By occupational status | `83_63_DF_DCCV_AVQ_PERSONE_131` | 2026-05 | |
| By region and municipality type | `83_63_DF_DCCV_AVQ_PERSONE_132` | 2026-05 | |

**To find new ISTAT dataset codes**: browse `https://esploradati.istat.it` and search by theme. The numeric prefix in the code (e.g. `85_`, `83_`) corresponds to the survey or data collection series. Codes are stable within a release but may change when ISTAT publishes a new edition of a survey.

#### Labour market (Rilevazione sulle Forze di Lavoro, RFL)

Data from 2015 onwards for all datasets except the two NEET flows which start in 2018.

| Dataset | Code | Last fetched | Notes |
|---|---|---|---|
| Labour force monthly (raw) | `150_873_DF_DCCV_FORZLVMENS1_1` | 2026-05 | National only. Monthly frequency. Raw data only (ADJUSTMENT=N filtered post-fetch). Sex × age groups. Series from 2004, fetched from 2015 |
| NEET rate by sex, age and region | `172_931_DF_DCCV_NEET1_11` | 2026-05 | NUTS2 regions + macro-areas. Annual and quarterly. Start 2018 (earliest available). Citizenship always TOTAL |
| NEET rate by sex, age and citizenship | `172_931_DF_DCCV_NEET1_9` | 2026-05 | Macro-areas only. Citizenship split: ITL / FRG / TOTAL. Annual and quarterly. Start 2018 |
| Inactivity rate by province | `152_913_DF_DCCV_TAXINATT1_5` | 2026-05 | NUTS3 (provinces) + all higher aggregates. Annual and quarterly |
| Unemployment rate by province | `151_914_DF_DCCV_TAXDISOCCU1_8` | 2026-05 | NUTS3 (provinces) + all higher aggregates. Annual and quarterly |
| Unemployment rate by education | `151_914_DF_DCCV_TAXDISOCCU1_6` | 2026-05 | NUTS2 (regions). Education breakdown (3 levels). Annual and quarterly |
| Unemployment rate by detailed age | `151_914_DF_DCCV_TAXDISOCCU1_5` | 2026-05 | NUTS2 (regions). Broadest age disaggregation (13 bands). Annual and quarterly |
| Employment rate by province | `150_915_DF_DCCV_TAXOCCU1_5` | 2026-05 | NUTS3 (provinces) + all higher aggregates. Annual and quarterly |

---

## Manual sources (not automated)

Download these periodically and place files in `ingestion/manual_sources/{source}/`. Record every download here. Not to be committed to Git.

### RUNTS — Registro Unico Terzo Settore

| Downloaded | File | Source URL | Notes |
|---|---|---|---|
| — | — | https://www.lavoro.gov.it/temi-e-priorita/Terzo-settore-e-responsabilita-sociale-delle-imprese/focus-on/Registro-unico-nazionale-del-Terzo-settore | Not yet downloaded |

Download format: CSV export by legal form and region.
Update frequency: annual (snapshot usually published Q1).

### Registro Imprese — Camere di Commercio

| Downloaded | File | Source URL | Notes |
|---|---|---|---|
| — | — | https://www.infocamere.it | Not yet downloaded |

Download format: Excel export from InfoCamere movimprese or stock data.
Update frequency: quarterly (use annual snapshot for consistency).

### Agenzia delle Entrate — ETS fiscali

| Downloaded | File | Source URL | Notes |
|---|---|---|---|
| — | — | https://www.agenziaentrate.gov.it/portale/web/guest/schede/agevolazioni/enti-del-terzo-settore | Not yet downloaded |

Download format: PDF tables — will need manual extraction or copy-paste to CSV.
Update frequency: annual publication, usually Q2 of the following year.

---

## Update checklist (run monthly)

```
[ ] Run python run_pipeline.py
[ ] Check terminal output for ERROR lines
[ ] Open pipeline_log.json and verify all stages show "complete"
[ ] Run python -m database.tests.test_integrity
[ ] Update "Last fetched" dates in this file for all datasets that ran
[ ] Note any new warnings, changed dataset codes, or API errors
[ ] Check ISTAT esploradati.istat.it for new RFL release dates (usually mid-month)
[ ] Refresh PowerBI dataset
```