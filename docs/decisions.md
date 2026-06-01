# Decisions Log

Record of significant data and architectural decisions made during the build of aiccon-data, with rationale. The goal is that others can understand why things are the way they are without having to reverse-engineer the code.

Entry added whenever a non-obvious choice is made about data scope, source priority, mapping logic, or architecture. 

---

## Architecture

### Decision: synced SharePoint drive rather than SharePoint API
**Date**: 2026-05
**Decision**: Use the OneDrive/SharePoint sync client (local mapped drive) rather than the Office 365 REST API or Microsoft Graph API.
**Rationale**: Simpler implementation — no OAuth flow, no library dependencies beyond the standard library. Since the pipeline runs locally on one machine, the sync client is always available. If the project moves to cloud execution (e.g. GitHub Actions with Azure) this would need to be revisited.
**Revisit when**: automation moves off a local machine.

### Decision: DuckDB as the integration layer
**Date**: 2026-05
**Decision**: Use DuckDB to build the star schema from parquet files, rather than loading data directly into PowerBI or using a cloud database.
**Rationale**: DuckDB can query parquet files directly, is embedded (no server to maintain), and produces a single `.duckdb` file that PowerBI can connect to via ODBC. Colleagues need no database knowledge. The file lives on SharePoint and refreshes monthly.
**Revisit when**: data volume grows beyond ~10GB or concurrent access becomes a requirement (DuckDB does not support multiple simultaneous writers).

### Decision: long-format (tidy) fact tables
**Date**: 2026-05
**Decision**: All fact tables store one observation per row (indicator_code, value, unit) rather than wide-format tables with one column per indicator.
**Rationale**: Long format is source-agnostic — every dataset regardless of structure maps to the same schema. PowerBI handles long-format data well via slicers and filters. Wide format would require schema changes every time a new indicator is added.

### Decision: split schema into three SQL files
**Date**: 2026-05
**Decision**: `dimensions.sql`, `fact_tables.sql`, `views.sql` rather than one monolithic SQL file.
**Rationale**: Separation of concerns — dimensions are stable and shared, fact tables grow one domain at a time, views change frequently as colleagues request new analytical cuts. Keeping them separate makes each file easier to read and reduces the risk of accidentally breaking a dimension table when editing a view.

### Decision: one processed parquet per domain per month
**Date**: 2026-05
**Decision**: The processed layer stores one file per domain named `{domain}_{YYYY_MM}.parquet`. The raw layer accumulates monthly snapshots; the processed layer keeps only the current file (overwritten each run).
**Rationale**: Raw snapshots are valuable for auditing source data changes over time. Processed files are deterministic outputs — they can always be regenerated from the raw files, so accumulating them wastes space.

---

## Data scope

### Decision: Eurostat two-dataset approach for employment
**Date**: 2026-05
**Decision**: Use `nama_10r_3empers` for regional (NUTS2) employment with the O-U aggregate, and `lfsa_egan22d` for national (NUTS0) employment with detailed NACE sections.
**Rationale**: No single Eurostat dataset provides both regional granularity and detailed NACE breakdown simultaneously. `nama_10r_3empers` gives the regional picture (all EU NUTS2 regions) but only at the O-U aggregate level. `lfsa_egan22d` gives Q86/Q87/Q88/P85/S94/S96 separately but only at national level. The two datasets are complementary — regional context and sector detail.

### Decision: replace `bd_enace2_r3` with `sbs_r_nuts2021`
**Date**: 2026-05
**Decision**: `bd_enace2_r3` removed and replaced with `sbs_r_nuts2021` for organisational counts (local units) by NACE and region.
**Rationale**: `bd_enace2_r3` consistently returned 413 errors even after paginating by NACE code. `sbs_r_nuts2021` covers the same analytical need (number of local productive units by NACE and NUTS2) using the updated 2021 NUTS vintage. Note: S94 (membership organisations) is not available in `sbs_r_nuts2021` — this is a coverage gap to document for colleagues.

### Decision: labour domain covers all NACE sections in `lfsa_egan22d`
**Date**: 2026-05
**Decision**: The labour domain fetches all NACE two-digit codes (sections A–U plus TOTAL) from `lfsa_egan22d`, grouped into 13 section bundles per API request. The social economy domain fetches only the social-economy-relevant subset.
**Rationale**: The labour domain serves as contextual background for all AICCON projects, not just social economy ones. Colleagues need to compare the social economy sectors against the broader economy. Fetching all sections adds 13 API requests but avoids needing a separate fetch for context. The `NACE_BY_SECTION` dict in `eurostat_labour_market.py` groups codes into section letters (A, B, C, …) to keep request counts manageable and mirror the logical NACE structure.

### Decision: sub-national labour data only for Italy
**Date**: 2026-05
**Decision**: The Eurostat labour datasets (`lfst_r_lfu3rt`, `lfst_r_lfur2gan`, `lfst_r_lfe2emprtn`) fetch NUTS2 regional data for all available countries, but the `filter_geo()` function in `merge_labour.py` drops all non-Italian sub-national rows before loading into the database. All non-Italian countries are kept at NUTS0 (national total) only.
**Rationale**: AICCON's territorial analysis work is focused on Italy. Keeping non-Italian NUTS2 data would roughly multiply the row count by 15–20× for negligible analytical benefit. National totals are sufficient for EU comparison; Italian regional granularity is where the depth is needed. The filter is applied consistently in both the merge script and the database loader.

### Decision: monthly labour force time key resolves to annual row
**Date**: 2026-05
**Decision**: The monthly `FORZLVMENS1_1` dataset stores `time_key` pointing to the annual `dim_time` row for the corresponding year, not a monthly row. The full `YYYY-MM` label is stored in the `period_label` column directly in `fact_labour`.
**Rationale**: Creating monthly rows in `dim_time` (12 per year × ~20 years = ~240 rows) just to support one dataset would bloat the time dimension for no benefit to any other domain. The `period_label` column in the fact table is a clean solution that keeps `dim_time` purely annual while still allowing PowerBI to sort and filter monthly observations correctly. If a second monthly dataset is added later, the same pattern applies.

### Decision: age dimension omitted from `lfst_r_lfe2emprtn`
**Date**: 2026-05
**Decision**: The employment rate by education and citizenship dataset (`lfst_r_lfe2emprtn`) is fetched without an age filter. This causes the API to return the total working-age aggregate rather than age-specific breakdowns.
**Rationale**: Including age would multiply the row count by approximately 6× (six broad age bands). The analytical value of crossing education × citizenship × age simultaneously is limited for the dashboard's use case — PowerBI users primarily want the education and citizenship cuts, not all three simultaneously. The age note is documented in `df.attrs["age_note"]` and in the data dictionary. If age breakdown for this dataset is needed later, add `AGE_CODES_LFS` back to the `base_filters` in `fetch_employment_rate_by_education_citizenship()`.

---

## Mappings

### Decision: unified_category as the legal form join key
**Date**: 2026-05
**Decision**: Italian legal forms from different sources (ISTAT, RUNTS, Camere di Commercio) are mapped to a `unified_category` (e.g. `cooperativa_sociale`, `odv`, `aps`) rather than to a NACE code directly.
**Rationale**: Italian third-sector legal forms have no direct equivalent in the Eurostat/NACE taxonomy. Mapping to NACE would lose the Italian legal distinction (tipo A vs tipo B cooperative, ODV vs APS) which is analytically important for AICCON research. The unified category preserves the Italian classification while also carrying a `nace_primary` field for cases where an EU comparison is needed.

### Decision: NUTS codes as the universal geography key
**Date**: 2026-05
**Decision**: All territorial data is joined using NUTS codes as the primary geographic key, with ISTAT codes mapped to NUTS via `nuts_istat.csv`.
**Rationale**: NUTS is the common standard across Eurostat and ISTAT. ISTAT uses its own provincial and municipal codes internally but publishes NUTS mappings. Using NUTS as the join key allows Italian subnational data to be placed in the same geographic hierarchy as EU-level data without ambiguity.
**Known issue**: NUTS vintages change (2016 → 2021). Some NUTS2 codes differ between vintages. The current mapping table uses 2021 NUTS. Historic data using 2016 codes may have some mismatches at province level — to be addressed when working with long time series.

### Decision: source priority order
**Date**: 2026-05
**Decision**: When the same indicator appears in multiple sources, priority is:
1. ISTAT (most granular for Italy, official national statistics)
2. Eurostat (EU comparisons, used where ISTAT has gaps)
3. RUNTS / Registro Imprese (entity counts, legal form breakdowns)
4. Agenzia delle Entrate (fiscal data, supplementary only)
**Rationale**: ISTAT is the primary national statistical authority for Italy and provides the most granular territorial breakdowns. Eurostat harmonises across EU countries but aggregates more. Manual sources (RUNTS, Camere di Commercio) are authoritative for entity counts but update less frequently and have less consistent historical depth.

---

## Rejected approaches

### Rejected: wide-format fact tables
Wide-format tables (one column per indicator) were considered and rejected. They require schema changes for every new indicator and make cross-domain queries awkward. Long format is more flexible and works better with PowerBI slicers.

### Rejected: loading data directly into PowerBI without DuckDB
Loading parquet files directly into PowerBI via the Parquet connector was considered. Rejected because: (a) it requires colleagues to know which file to point at; (b) multiple parquet files per domain would need manual union steps in Power Query; (c) there is no star schema optimisation without a proper database layer. DuckDB adds one step but makes PowerBI integration much cleaner.

### Rejected: separate settings files per domain from the start
Splitting `settings.yaml` into per-domain config files was considered early in the project. Deferred rather than rejected — it becomes worthwhile when there are 4+ active domains. The current single-file approach is simpler for one active domain. See the note in `settings.yaml` about when to refactor.