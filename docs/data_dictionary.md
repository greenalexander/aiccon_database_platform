# Data Dictionary

Definitions, units, and notes for every field in the aiccon-data database.
Updated whenever a new domain or dataset is added.

Last updated: May 2026

---

## Shared dimension tables

These tables are used by every domain. They aren't directly queried in PowerBI — they are joined automatically through the analytical views (`vw_*`).

### `dim_geography`

| Column | Type | Description |
|---|---|---|
| `geo_key` | integer | Surrogate primary key |
| `nuts_code` | text | NUTS code (e.g. `IT`, `ITH5`, `ITH55`). For non-EU countries of origin (future immigration domain), ISO 3166-1 alpha-3 is used instead |
| `nuts_level` | integer | 0 = country, 1 = macro-region, 2 = region (NUTS2), 3 = province (NUTS3) |
| `nuts_name_it` | text | Italian place name |
| `nuts_name_en` | text | English place name |
| `istat_code` | text | ISTAT territorial code (e.g. `037` for Bologna province) |
| `country_code` | text | ISO 3166-1 alpha-2 (e.g. `IT`, `FR`, `DE`) |
| `region_name` | text | Italian region name (null for non-Italian rows) |
| `macro_area` | text | Italian macro-area: `Nord-Ovest`, `Nord-Est`, `Centro`, `Sud`, `Isole` (null for non-Italian rows) |
| `municipality_type` | text | Municipality size class from ISTAT surveys (e.g. `>250k`, `50-250k`, `<10k`). Populated only for rows representing a size class rather than a specific territory |
| `geo_source` | text | `nuts_istat_csv` (from mapping table), `eurostat_auto` (inserted automatically when Eurostat data contained an unknown code), `manual` |
| `is_active` | boolean | `false` for discontinued NUTS codes from previous vintages |

### `dim_time`

| Column | Type | Description |
|---|---|---|
| `time_key` | integer | Surrogate primary key |
| `year` | integer | Reference year (e.g. `2021`) |
| `period_type` | text | `A` = annual (all current data). `Q` and `M` reserved for future sub-annual data |
| `period_label` | text | Human-readable label (e.g. `2021`) |
| `reference_period` | text | Extended label where useful (e.g. `2021 (Census)`) |

### `dim_source`

| Column | Type | Description |
|---|---|---|
| `source_key` | integer | Surrogate primary key |
| `source_id` | text | Short identifier matching `domain_sources.csv` (e.g. `istat_85_84_DF_DCSA_VOLON1_1`) |
| `source_name` | text | English source name |
| `source_name_it` | text | Italian source name |
| `domain` | text | Primary thematic domain this source belongs to |
| `access_type` | text | `api` or `manual` |
| `priority` | integer | Source priority for deduplication: 1 = highest (ISTAT), 2 = Eurostat, 3 = RUNTS/Camere di Commercio, 4 = Agenzia delle Entrate |
| `update_frequency` | text | How often the source publishes new data |
| `territorial_levels` | text | Semicolon-separated list of available territorial levels (e.g. `NUTS0;NUTS2`) |
| `temporal_coverage` | text | Years covered (e.g. `2015-present`) |
| `notes` | text | Important caveats about coverage, methodology, or known issues |

### `dim_legal_form`

| Column | Type | Description |
|---|---|---|
| `legal_form_key` | integer | Surrogate primary key |
| `unified_category` | text | Unified category used as the join key across sources (e.g. `cooperativa_sociale`, `odv`, `aps`, `fondazione`) |
| `unified_category_en` | text | English label |
| `nace_primary` | text | Primary NACE Rev. 2 mapping (e.g. `Q87;Q88` for social cooperatives) |
| `ets_classification` | text | `ets` = registered in RUNTS under D.Lgs. 117/2017; `ets_eligible` = can register but may not be; `non_ets` = excluded from ETS scope |
| `source_system` | text | Source this row came from: `istat`, `runts`, `camere_commercio`, `eurostat` |
| `source_code` | text | Original code in the source system (e.g. `COOP_SOC_A`) |
| `source_label_it` | text | Original Italian label in the source system |

### `dim_indicator`

| Column | Type | Description |
|---|---|---|
| `indicator_key` | integer | Surrogate primary key |
| `indicator_code` | text | Short code used in fact tables (e.g. `volunteering_rate`, `employment_rate`) |
| `label_it` | text | Italian label for PowerBI display |
| `label_en` | text | English label |
| `unit_default` | text | Most common unit for this indicator (see unit reference below) |
| `domain` | text | Primary domain this indicator belongs to |
| `notes` | text | Caveats, methodology notes, or cross-references |

---

## Social economy fact table — `fact_social_economy` / `vw_social_economy`

### Core fields (present in every row)

| Column | Type | Description |
|---|---|---|
| `fact_key` | integer | Surrogate primary key |
| `geo_key` | integer | Foreign key → `dim_geography` |
| `time_key` | integer | Foreign key → `dim_time` |
| `source_key` | integer | Foreign key → `dim_source` |
| `indicator_key` | integer | Foreign key → `dim_indicator` |
| `value` | float | The numeric observation |
| `unit` | text | Unit of measurement for this specific row (see unit reference below) |
| `dataset_code` | text | Original dataset identifier for traceability (e.g. `85_84_DF_DCSA_VOLON1_1`) |
| `extracted_at` | timestamp | UTC timestamp when the raw data was fetched from the API |

### Legal form fields (populated for organisational datasets; null for survey data)

| Column | Type | Description |
|---|---|---|
| `legal_form_key` | integer | Foreign key → `dim_legal_form`. Null for NACE-based and survey datasets |
| `ets_classification` | text | `ets` / `ets_eligible` / `non_ets`. Null where legal form is not a dimension |

### NACE fields (populated for Eurostat datasets; null for survey data)

| Column | Type | Description |
|---|---|---|
| `nace_code` | text | NACE Rev. 2 / ATECO 2007 code (e.g. `Q87`, `P85`, `O-U`). Note: `O-U` is an aggregate used in `nama_10r_3empers` at NUTS2 level — Q, P, and S94 are not separable at this territorial level |
| `nace_label_en` | text | English label from `nace_labels.csv` |

### Demographic fields (populated where available; null otherwise)

| Column | Type | Description |
|---|---|---|
| `gender` | text | `male`, `female`, or `total`. Null for Eurostat regional employment datasets which do not provide a gender breakdown |
| `age_group` | text | Age group label as provided by ISTAT (e.g. `15-34`, `35-64`, `65+`). Null where age is not a dimension |

### Volunteering survey dimensions
*(from ISTAT Indagine sul Volontariato, datasets `85_84_DF_DCSA_VOLON1_*`)*

| Column | Type | Description |
|---|---|---|
| `volunteering_form` | text | `ORGVOL` = organised volunteering (through an organisation); `DIRVOL` = direct/informal volunteering; `total` = both combined |
| `activity_type` | text | Type of activity: emergency rescue, social assistance, culture, sport, tutoring, environmental protection, etc. |
| `years_active` | text | Duration of volunteering: `<1yr`, `1-2`, `3-5`, `6-10`, `>10` years |
| `education` | text | Highest educational qualification of the volunteer |
| `labour_status` | text | Labour market status: employed, unemployed, retired, student, homemaker, etc. |
| `household_size` | text | Number of people in the volunteer's household |
| `econ_resources` | text | Self-assessed household economic resources: adequate, scarce, insufficient |
| `municipality_type` | text | Size class of municipality of residence |

### Organised volunteering dimensions
*(from ISTAT Indagine sul Volontariato, datasets `85_171_DF_DCSA_VOLON_ORG1_*`)*

| Column | Type | Description |
|---|---|---|
| `org_sector` | text | Sector of the host organisation: sport, culture, social assistance, civil protection, health, environment, education, etc. |
| `org_type` | text | Type of host organisation: ODV, APS, cooperative sociale, religious body, sports club, foundation, other. Maps to legal forms in D.Lgs. 117/2017 |
| `motivation` | text | Primary motivation for volunteering: altruism, faith, social belonging, personal enrichment, skills development, reciprocity, etc. |
| `personal_impact` | text | Self-reported personal benefit: new friendships, sense of usefulness, new skills, improved wellbeing, civic engagement, etc. Relevant for SROI-framework analyses |
| `multi_membership` | text | Whether the volunteer is active in one organisation or multiple organisations simultaneously |

### Associationism dimensions
*(from ISTAT Aspetti della Vita Quotidiana, datasets `83_63_DF_DCCV_AVQ_PERSONE_*`)*

| Column | Type | Description |
|---|---|---|
| `association_type` | text | Type of association the respondent belongs to: cultural, sports, religious, environmental, political, professional, charity, etc. |

---

## Labour fact table — `fact_labour` / `vw_labour`

### Core fields (present in every row)

| Column | Type | Description |
|---|---|---|
| `fact_key` | integer | Surrogate primary key |
| `geo_key` | integer | Foreign key → `dim_geography` |
| `time_key` | integer | Foreign key → `dim_time`. Always resolves to the annual row (by year) even for monthly observations — use `period_label` for sub-annual granularity |
| `source_key` | integer | Foreign key → `dim_source` |
| `indicator_key` | integer | Foreign key → `dim_indicator` |
| `value` | float | The numeric observation |
| `unit` | text | Unit of measurement for this row (see unit reference below) |
| `dataset_code` | text | Original dataset identifier (e.g. `150_915_DF_DCCV_TAXOCCU1_5`, `lfst_r_lfu3rt`) |
| `extracted_at` | timestamp | UTC timestamp when the raw data was fetched |

### Sub-annual time fields

| Column | Type | Description |
|---|---|---|
| `frequency` | text | `A` = annual, `M` = monthly, `Q` = quarterly. All ISTAT rate datasets and all Eurostat datasets are annual. Only `150_873_DF_DCCV_FORZLVMENS1_1` (labour force headcount) is monthly |
| `period_label` | text | For annual rows: the year as a string (e.g. `2023`). For monthly rows: `YYYY-MM` (e.g. `2023-04`). For quarterly rows: `YYYY-QN` (e.g. `2023-Q2`). Use this column as the x-axis for time series charts involving the monthly series |

### Demographic fields

| Column | Type | Description |
|---|---|---|
| `gender` | text | `T` = total, `M` = male, `F` = female. Consistent across ISTAT and Eurostat sources in this domain |
| `age_group` | text | Age band in NUTS/LFS notation (e.g. `Y15-74`, `Y15-24`, `Y20-64`, `TOTAL`). Null for `lfst_r_lfe2emprtn` where age was omitted at fetch time (returns total working-age aggregate) |

### Labour-specific dimension fields

| Column | Type | Description |
|---|---|---|
| `education` | text | Educational attainment level. For Eurostat sources (`lfst_r_lfu3rt`, `lfst_r_lfe2emprtn`): ISCED 2011 codes — `ED0-2` (primary/lower secondary), `ED3_4` (upper secondary/post-secondary), `ED5-8` (tertiary), `TOTAL`. For ISTAT `TAXDISOCCU1_6`: ISTAT `edu_lev_highest` codes. Null for datasets where education is not a dimension |
| `citizenship` | text | Citizenship status. For Eurostat sources (`lfst_r_lfur2gan`, `lfst_r_lfe2emprtn`): `NAT` = nationals, `FOR` = foreigners, `TOTAL`. For ISTAT NEET datasets: `ITL` = Italian citizens, `FRG` = foreign citizens, `TOTAL`. Null for datasets where citizenship is not a dimension |
| `adjustment` | text | Seasonal adjustment flag. `N` = raw (not seasonally adjusted), `Y` = seasonally adjusted. Populated only for `150_873_DF_DCCV_FORZLVMENS1_1` (monthly labour force); null for all other datasets. The pipeline fetches raw data only (`N`) |
| `unemployment_duration` | text | Duration of unemployment spell. Always `TOTAL` in current datasets (no duration breakdown fetched). Retained for schema stability |

### NACE fields (populated for `lfsa_egan22d` only; null for all rate and NEET datasets)

| Column | Type | Description |
|---|---|---|
| `nace_code` | text | NACE Rev. 2 two-digit code (e.g. `P85`, `Q86`, `O84`, `TOTAL`). Populated only for `lfsa_egan22d` (employed persons by economic activity). All major sections A–U are fetched |
| `nace_label_en` | text | English label from `nace_labels.csv` |

---

## Indicators reference

### Social economy indicators

| `indicator_code` | Label (EN) | Unit | Source(s) |
|---|---|---|---|
| `volunteering_rate` | Volunteering rate | `mixed` | ISTAT VOLON1_1 |
| `volunteering_activity_share` | Share by activity type | `percentage` | ISTAT VOLON1_2 |
| `volunteering_years_share` | Share by years active | `percentage` | ISTAT VOLON1_3 |
| `org_volunteering_sector_share` | Organised volunteering share by sector | `percentage` | ISTAT VOLON_ORG1_1 |
| `org_volunteering_orgtype_share` | Organised volunteering share by org type | `percentage` | ISTAT VOLON_ORG1_2 |
| `org_volunteering_motivation_share` | Volunteering share by motivation | `percentage` | ISTAT VOLON_ORG1_3 |
| `org_volunteering_impact_share` | Volunteering share by personal impact | `percentage` | ISTAT VOLON_ORG1_4 |
| `association_membership_rate` | Association membership rate | `percentage` | ISTAT AVQ |
| `n_employed` | Employed persons | `thousands_persons` | Eurostat nama_10r_3empers, lfsa_egan22d |
| `n_local_units` | Local units | `count` | Eurostat sbs_r_nuts2021 |

### Labour indicators

| `indicator_code` | Label (EN) | Unit | Source(s) |
|---|---|---|---|
| `labour_force` | Labour force | `thousands_persons` | ISTAT FORZLVMENS1_1 (monthly) |
| `employment_rate` | Employment rate | `percentage` | ISTAT TAXOCCU1_5; Eurostat lfst_r_lfe2emprtn |
| `unemployment_rate` | Unemployment rate | `percentage` | ISTAT TAXDISOCCU1_5/6/8; Eurostat lfst_r_lfu3rt, lfst_r_lfur2gan |
| `inactivity_rate` | Inactivity rate | `percentage` | ISTAT TAXINATT1_5 |
| `neet_rate` | NEET rate | `percentage` | ISTAT NEET1_11 (regions), NEET1_9 (macro-areas + citizenship) |

---

## Unit reference

| Unit value | Meaning | Source |
|---|---|---|
| `percentage` | Share of population or labour force (0–100 scale) | ISTAT rate datasets; Eurostat LFS rate datasets |
| `mixed` | Multiple units in the same dataset — read the `unit` column per row | ISTAT `85_84_DF_DCSA_VOLON1_1` only |
| `thousands_persons` | Thousands of employed persons | Eurostat `nama_10r_3empers`, `lfsa_egan22d`; ISTAT `FORZLVMENS1_1` |
| `count` | Absolute count of units/organisations | Eurostat `sbs_r_nuts2021` |

---

## Analytical views

Connect PowerBI to these views — not to the raw fact tables. Views join all dimension tables and expose human-readable labels.

### Social economy views

| View | Description |
|---|---|
| `vw_social_economy` | Full flat view of all social economy data — use as the primary PowerBI source for this domain |
| `vw_se_volunteering_national` | National volunteering rates by year and form — ready for line charts |
| `vw_se_volunteering_regional` | Regional volunteering rates — ready for map visualisations |
| `vw_se_associationism_national` | National association membership rates by year and demographic |
| `vw_se_employment_eu` | Eurostat employment data for EU comparisons |
| `vw_se_local_units_regional` | Local units by NACE and region — organisational density maps |

### Labour views

| View | Description |
|---|---|
| `vw_labour` | Full flat view of all labour data — primary PowerBI source for this domain |
| `vw_labour_rates_italy` | Employment, unemployment and inactivity rates for Italy (national, annual) — line charts |
| `vw_labour_rates_regional` | Employment and unemployment rates at NUTS2/NUTS3 for Italy — map visualisations |
| `vw_labour_neet` | NEET rate by region, age and (optionally) citizenship — filter `citizenship IS NOT NULL` for the citizenship split |
| `vw_labour_employment_by_nace` | Employed persons by economic activity across EU countries (national level) — sector comparisons |
| `vw_labour_unemployment_by_education` | Unemployment rate by education level — Italy NUTS2 and EU national comparison |
| `vw_labour_employment_edu_citizenship` | Employment rate by education × citizenship — Italy NUTS2 and EU national |
| `vw_labour_force_monthly` | Monthly labour force headcount for Italy (raw series) — use `period_label` as x-axis |

---

## Known limitations and caveats

### Social economy

**NACE O-U aggregate at NUTS2**: The Eurostat dataset `nama_10r_3empers` provides regional employment only at the O-U aggregate level (public administration, education, health, and social services combined). Sections Q, P, and S94 are not separable at NUTS2 level from this source. For sector-specific employment figures, use `lfsa_egan22d` (national level only) or the ISTAT volunteering survey data.

**Census vs. survey data**: ISTAT volunteering data comes from sample surveys, not a census. Figures are estimates with sampling uncertainty. The associationism data (AVQ) has a larger sample and is more stable year-to-year than the volunteering survey.

**RUNTS coverage**: RUNTS only covers entities registered under D.Lgs. 117/2017 (Codice del Terzo Settore) from 2022 onwards. Pre-2022 entity counts from ISTAT census data are not directly comparable to post-2022 RUNTS counts because the legal categories changed with the Codice.

**Temporal comparability**: The ISTAT nonprofit census runs approximately every 5 years (2001, 2011, 2016, 2021). Employment and organisational count figures from census years should not be interpolated between census rounds.

### Labour

**NEET series starts in 2018**: Both NEET datasets (`NEET1_11` and `NEET1_9`) are only available from 2018 onwards. The `start_period="2015"` used for all other ISTAT datasets is overridden to `2018` for these two flows.

**Monthly labour force is national only**: `FORZLVMENS1_1` provides no regional breakdown. All rows in `vw_labour_force_monthly` have `nuts_code = 'IT'`. For regional labour force context, use the annual rate datasets.

**`lfst_r_lfe2emprtn` age dimension**: Age was intentionally omitted from the `lfst_r_lfe2emprtn` fetch to keep response sizes manageable. The API returns the total working-age aggregate. The `age_group` column is null for all rows from this dataset. This is documented in `df.attrs["age_note"]` in the processed parquet.

**Education codes are not harmonised across sources**: Eurostat uses ISCED 2011 codes (`ED0-2`, `ED3_4`, `ED5-8`) while ISTAT `TAXDISOCCU1_6` uses its own `edu_lev_highest` codes. Both are stored as-is in the `education` column. Filter by `source_id` when comparing education breakdowns, or use the Eurostat-only view `vw_labour_unemployment_by_education` for cross-country comparisons.

**Sub-national data for non-Italian countries**: The Eurostat labour datasets fetch sub-national (NUTS2) data only for Italy. All other EU countries are represented by national totals (NUTS0) only. This is by design — AICCON's territorial analysis focus is Italy. The `filter_geo()` function in the merge script enforces this.

**Unemployment duration not broken down**: The `unemployment_duration` column is always `TOTAL` in current datasets. The underlying Eurostat datasets do carry duration codes, but no duration-specific fetch was implemented. The column is retained in the schema for future extension.