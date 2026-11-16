# Decisions Log

Record of significant data and architectural decisions made during the build of aiccon-data, with rationale. The goal is that others can understand why things are the way they are without having to reverse-engineer the code.

Entry added whenever a non-obvious choice is made about data scope, source priority, mapping logic, or architecture. 

## 2026-11-16: dbt refactor (roadmap step 4)

The following decisions were taken together before building the dbt project that replaces the transformation logic in `database/build_db.py`. The findings that shaped them come first, because several of them changed the plan.

### What we found before deciding

**`views.sql` was never run.** `build_db.py` executes only `dimensions.sql` and `fact_tables.sql`, and `views.sql` uses unqualified table names that BigQuery rejects without a default dataset. `aiccon_data` contains no views: PowerBI reads the tables. The file also contains a bug (`gender IN ('total', NULL)` never matches a NULL). The six stub fact tables (`fact_civil_society`, `fact_housing`, `fact_immigration`, `fact_poverty`, `fact_sdg`, `fact_welfare`) have been empty since they were created on 1 June.

**Geography.** `nuts_istat.csv` is a crosswalk from ISTAT codes to NUTS codes (167 rows, 137 distinct NUTS codes), not a list of territories. ITC2 had two Italian names; "Valle d'Aosta/Vallée d'Aoste" is kept, with straight apostrophes. 108 `istat_code` values start with a zero. The 33 `eurostat_auto` rows in `dim_geography` are all non-Italian country codes, and the 8 inactive codes are pre-revision codes that no fact row references.

**NUTS vintage.** The Italian codes serve as join keys for both NUTS 2021 and NUTS 2024. Statistics Poland's summary of the 2024 revision reports, for Italy, boundary changes to two NUTS2 and two NUTS3 units and one renamed NUTS3 unit. Which units these are should be checked against Eurostat's list of changes and recorded in the seed's description, since boundary changes break series without changing codes. NUTS 2027 (Regulation 2026/195) applies from 2027.

**Codes in `fact_labour`.** ISTAT and Eurostat code the same categories differently (gender `total` and `T`, education `99` and `TOTAL`, citizenship `ITL` and `NAT`). ISTAT's codelists define education `13`, `7`, `11` and `99` as no qualification to lower secondary, upper and post-secondary, tertiary, and total; and citizenship `ITL`, `FRG` and `TOTAL` as Italian, foreign and total. These correspond to Eurostat's ISCED groups and citizenship codes by description, not through an official concordance. A missing breakdown is stored as `''` for education and citizenship and as NULL for the others. `TAXDISOCCU1_6` (education) and `NEET1_9` (citizenship) have components without totals. No dataset has an age total. `lfsa_egan22d` carries the 88 NACE divisions plus `TOTAL`. 42% of `fact_labour` rows are national figures for other countries.

**Reference files.** `domain_sources.csv` registers `lfsa_egan22d` twice, under social_economy and labour; the labour row never matches, because the lookup key is the provider plus the dataset code. `nace_labels.csv` labels five NACE classes (`S94_11`, `S94_12`, `S94_91`, `S94_92`, `S94_99`) as groups, so a sum over groups would count S94 twice, and its `social_economy_relevant` column mixes `TRUE`, `FALSE` and `partial`. `dim_indicator` gives `n_employed` a single domain although both domains use it.

**The social-economy copy of `lfsa_egan22d`.** This confirms and measures a case already listed under the merge issue (2026-11-02). The Eurostat branch of `merge_social_economy.py` drops sex and age, so the 66 combinations (3 sexes, 22 age bands) for each country, year and division become identical, and `resolve_priority()` keeps one at random: 813 rows, none of them identifiable. `nama_10r_3empers` and `sbs_r_nuts2021` are not affected, because every dimension other than territory, time and NACE has a single value. `resolve_priority()` also leaves `dataset_code` out of its keys, so two datasets with the same indicator and priority can silently replace each other. That does not happen with the current extractions.

**Permissions.** `roles/storage.objectAdmin` does not include `storage.buckets.get`, which BigQuery needs to load files from Cloud Storage. `roles/storage.bucketViewer` was granted to `github-actions-pipeline@` on the bucket only.

### Scope: dbt transforms, Python loads

**Decision.** dbt owns everything from a raw BigQuery table onwards: the geography filter and Extra-Regio exclusion, period parsing, code harmonisation, dimensions, facts and marts. Python loads the latest processed parquet of each domain into a raw table, unchanged, and nothing else. `build_db.py` shrinks to that loader. `dimensions.sql`, `fact_tables.sql` (with its empty stubs) and `views.sql` are deleted at switch-over; a future domain gets dbt models when it is built.

**Why.** dbt does not load files, so a small Python step remains in any design. Everything after loading is SQL over tables, which is what dbt is for.

**Consequences.** Rows with an unknown indicator, source or code are no longer dropped with a log warning: they stay in staging and fail a test.

### dbt runs as its own workflow step

**Decision.** A separate step, "Build dbt models", after "Run pipeline". It runs when the chosen stage includes the database (`all` or `database`), and also after a failed pipeline step unless the run was cancelled, as the integrity checks do. It always builds every model, whatever the domain input. Locally the database stage becomes two commands: `python run_pipeline.py --stage database`, then `dbt build`.

**Why.** A step gets its own log section, status and `if:` condition, so a dbt failure is visible as such. Each raw load is all or nothing per domain, so dbt after a partial failure at worst rebuilds from last month's raw table. Dimensions are shared across domains, and a full build takes seconds.

### Raw data: load jobs from Cloud Storage, full refresh, no snapshots

**Decision.** The loader calls `load_table_from_uri` on the latest processed file with `WRITE_TRUNCATE` into `aiccon_raw`, taking the schema from the parquet file, and appends a row per load to `aiccon_raw.load_log` (domain, file, rows, time). No external tables, no incremental loads and no dbt snapshots.

**Why.** Processed files accumulate in GCS, so an external table over a domain's folder would union every monthly snapshot. External tables re-read Cloud Storage on every query and require every reader to have access to the bucket. Batch loads are free and all or nothing. Each processed file is a complete snapshot, so incremental loading would add state and failure modes for no saving. History already exists in the dated processed files and can be loaded later in one go; snapshots now would record merge artefacts as revisions, and a lost snapshot table cannot be rebuilt.

**Consequences.** Loading from Cloud Storage requires the bucket and the dataset to share a location (both us-central1). The rule of 2026-11-02 stands: never delete old processed files. Staging casts every column explicitly, so a type change upstream fails loudly.

### Three datasets, four layers, views inside and tables outside

**Decision.** `aiccon_raw` (written by the loader), `aiccon_staging` (staging, intermediate and seeds) and `aiccon_marts` (dimensions, facts and flat marts). The production target's schema is `aiccon` and the development target's is `dev`, so dbt's default naming (target schema, underscore, custom schema) gives `aiccon_staging` and `dev_staging`, and local work never touches production. Layers: sources; staging (`stg_`, one model per raw table: rename, cast, blanks to NULL, out-of-scope rows flagged with a reason); intermediate (`int_`, business rules, code harmonisation, filtering); marts (`dim_`, `fact_`, `mart_`). Staging and intermediate are views; everything in `aiccon_marts` is a table. No partitioning or clustering.

**Why.** Separate datasets give an access boundary: the Streamlit app and Gemini will see only the marts. `dbt build` skips models downstream of a failed test, so a mart built as a table keeps last month's contents when a test fails, whereas a view would expose the new data at once. The existing names `dim_*` and `fact_*` are kept so that `test_integrity.py` and PowerBI carry over. Storage stays far below the free tier.

### Natural keys, and reference data as seeds

**Decision.** No integer surrogate keys: facts and dimensions join on codes (`nuts_code`, `period_code`, `indicator_code`, the dataset code). `fact_key` and `dim_geography.is_active` are dropped. `dim_geography` is the distinct NUTS codes of the crosswalk, the ISTAT aggregates, and any code found in the facts but not in the crosswalk; a derived Italian code fails the build. Codes containing `;` keep a NULL level. Seeds: `nuts_istat.csv`, `domain_sources.csv`, `legal_form_map.csv`, `nace_labels.csv`, and new `indicators.csv` (from `populate_dim_indicator`, without a domain column, with `headline_age_group`), `breakdown_codes.csv` and a country-names seed for the 33 non-Italian countries, using Eurostat's codes (EL for Greece). Code columns are typed as strings. Each CSV exists once: dbt's `seed-paths` points at `processing/mappings/` if the installed version accepts a path outside the project, otherwise the files move into `dbt/seeds/` and the Python lookups follow. `dim_source` is keyed on the dataset code, without a domain. `dim_legal_form` and the always-NULL `legal_form_key` are deferred until a dataset carries a legal form.

**Why.** Keys must be deterministic, not persistent: dimensions and facts are rebuilt together on every run, and nothing outside the build stores key values. Codes are defined by Eurostat, ISTAT or us, and no slowly changing dimensions are planned. Readable codes also matter for the planned text-to-SQL feature. A NUTS vintage change is a mapping problem that a surrogate key would hide, not solve.

### `fact_labour`: periods, harmonised codes and declared headlines

**Decision.** `dim_time` has one row per period, keyed on `period_code` (`2023`, `2023-Q2`, `2023-04`), with frequency, year, quarter, month and start and end dates. `breakdown_codes.csv` lists every code of every breakdown by provider, with the harmonised code (Eurostat's: `T/M/F`, `NAT/FOR/TOTAL`, `ED0-2/ED3_4/ED5-8/TOTAL`), labels, `is_total`, and numeric bounds for age bands. A NULL breakdown means the dataset has no such breakdown and counts as a total. The facts carry harmonised codes, one `is_total` flag per breakdown and `is_headline` (every breakdown at its total, and the indicator's headline age band). Headline bands: employment rate Y15-64, unemployment rate Y15-74, inactivity rate Y15-64, NEET rate Y15-29. The fact table keeps every source and frequency; marts choose. Marts about Italy use ISTAT; marts comparing countries use Eurostat for every country, Italy included; `dim_source.priority` stays as metadata. Rates are never summed or averaged across breakdowns; NACE counts are summed only within one level, excluding `TOTAL`. Published annual figures are used rather than averages of monthly ones. `lfsa_egan22d` is taken only from the labour data.

**Order of marts.** First `mart_labour` (flat, with labels and flags) and `mart_labour_rates_regional` (ISTAT, Italy at every territorial level, annual, headline rows, employment, unemployment, inactivity and NEET). Then `mart_se_employment_eu` (`nama_10r_3empers`) and `mart_se_local_units_regional` (`sbs_r_nuts2021`). Held until the merge is fixed: `mart_labour_force_monthly` (several editions per observation in `FORZLVMENS1_1`) and the volunteering and association marts.

**Why.** Putting the period in the key removes the annual time key shared by annual, quarterly and monthly rows. A seed makes the meaning of each code explicit and testable instead of inferred from its spelling, and a new code from a source fails the build until someone classifies it. Age bands nest, so a headline must be declared per indicator. A comparison needs every country from the same source, release and revision state.

### Tests in dbt; `test_integrity.py` retired at switch-over

**Decision.** dbt's built-in tests (`unique`, `not_null`, `accepted_values`, `relationships`) plus a few generic tests written in the project (minimum row count, uniqueness of a column combination for the grain, values within bounds by unit, and "contains all of these values"), and singular tests for Italian NUTS coverage and Italian codes missing from the crosswalk. No test packages. Code coverage is tested as `not_null` on the harmonised code after the join to the seed in the intermediate model. Dataset recency is a warning. `store_failures` writes failing rows to the audit dataset (`<target_schema>_dbt_test__audit`). A short script turns `target/run_results.json` into the run's Summary table.

**Why.** Third-party packages may not work under dbt v2, and writing the generic tests is a good way to learn them. A test with one parent reliably stops the models downstream of it; a user report suggests a failing `relationships` test may not. Failing rows are easier to examine with a query than in a public log.

**Consequences.** An error-severity failure fails the step and the run, and the marts keep last month's contents. Warnings do not fail the run. The digest still runs, because it reads GCS. `test_integrity.py` keeps checking `aiccon_data` until switch-over, as an independent check, and is then deleted.

### Running dbt in GitHub Actions, and authentication

**Decision.** dbt-bigquery's `oauth` method, which uses Application Default Credentials: in CI the credentials file written by `google-github-actions/auth`, locally the `aiccon-pipeline` key through `GOOGLE_APPLICATION_CREDENTIALS`. `dbt/profiles.yml` is committed and holds no secrets: two targets, `dev` (the default) and `prod`, the project from `BQ_PROJECT` through `env_var()`, location us-central1, `maximum_bytes_billed` and a job timeout. `DBT_PROFILES_DIR` points at it, and CI passes `--target prod`. No new roles are needed: `roles/bigquery.dataEditor` at project level includes creating datasets, and `roles/bigquery.jobUser` covers queries and the load jobs used for seeds.

**Consequences.** Locally, `GOOGLE_APPLICATION_CREDENTIALS` and `BQ_PROJECT` must be set in the PowerShell session, because dbt does not read `config/.env`.

### dbt Core v1.12, installed through uv

**Decision.** `dbt-core` and `dbt-bigquery` 1.12, declared as `>=1.12,<1.13` in a `dbt` dependency group in `pyproject.toml`, with exact versions in `uv.lock`. Never the PyPI package named `dbt`, which now installs v2. Upgrades only through `uv lock --upgrade-package`. If dbt's dependencies conflict with the pipeline's, dbt gets its own project and lock file in `dbt/`. GitHub Actions installs with `uv sync --locked`, and `requirements.txt` is deleted. `pyproject.toml` is tidied first: `office365-rest-python-client` and `pandas-gbq` removed, `duckdb` and `jupyter` moved to the `dev` group, `google-cloud-storage`, `google-cloud-bigquery` and `PyYAML` declared, `genai` added to the packages built.

**Why.** dbt v2's BigQuery adapter documents no way to use GitHub's federated credentials, and its local adapter is in preview. v1 is Apache 2.0; the full v2 distribution is proprietary. v1.12 supports Python 3.10 to 3.14, and, according to a summary of dbt's version page, is in active support until July 2027; dbt Labs states that v1.13 will receive critical support beyond the usual year. Check both before relying on them. `--locked` makes a run fail if `uv.lock` is out of date with `pyproject.toml`.

**Revisit when** dbt v2's BigQuery adapter is generally available for local use and supports GitHub's federated credentials. Recheck the licence then too: it has changed twice since May 2025.

### Switch-over: a parallel run with a parity check

**Decision.** dbt builds the new datasets while `build_db.py` keeps maintaining `aiccon_data`. A warning-severity singular test compares old and new facts by dataset, indicator, territory and period, on row count and sum of values, with the 813 social-economy `lfsa_egan22d` rows excluded. The switch happens when a full `dbt build --target prod` has no error-severity failures, the parity test returns no rows or every row is explained, and the PowerBI report has been checked against `aiccon_marts`. Retirement then happens in separate commits, each confirmed by a manual run: the Python and SQL of the old database build; the integrity checks, the parity test and the legacy source; `aiccon_data` itself after one more monthly cycle; and the documentation (README, `data_dictionary.md`, whose column descriptions move into the dbt YAML files).

**Why.** Comparing groups rather than rows is independent of the code harmonisation, but still catches a lost, duplicated or altered observation. Manual runs rebuild everything from the same raw files, so the switch does not have to wait for several monthly runs.

**Consequences.** Listing the marts under "views" in `FACT_CHECKS`, planned earlier, is no longer needed: dbt's row-count tests on the marts do that job.

### Open items outside the dbt project

- `merge_social_economy.py`: remove `lfsa_egan22d` from `EUROSTAT_DATASET_META` (this also removes it from the digest's expected datasets), and add `dataset_code` to the keys in `resolve_priority()`. The rest of the merge issue is unchanged.
- `domain_sources.csv`: remove the `eurostat_lfsa_egan22d_labour` row once `dim_source` is keyed on the dataset code.
- `nace_labels.csv`: set the level of the five S94 classes to `class`.
- `pipeline.yml`: `retention-days: 7;` has a stray semicolon (fixed with the dependency tidy-up).
- README: the views, Python 3.11, and the bucket roles are out of date.



## 2026-11-02: Monthly digest with Gemini

The following decisions were taken together when the monthly digest (`genai/`) was built.

### A release digest from metadata, not a summary of values

**Context.** The README planned a summary of changes in the indicators, read from the dbt marts. The marts do not exist yet; fact tables are replaced in full on every load, so BigQuery holds no history; and `fact_labour` mixes frequencies and totals on one time key.

**Decision.** The digest reports which datasets were refreshed, which gained new periods, which were not updated and which are missing. It reports no statistical values and no revisions of values.

**Why.** These facts can be computed reliably from metadata now, without the marts. Comparing values would be unreliable for some datasets until the merge issue below is fixed, because a "revision" could simply be a different arbitrary row surviving de-duplication. A summary of values can follow once the marts and the merge fix exist.

### Previous state from the dated processed files in GCS

**Decision.** For each domain, the digest compares this month's processed parquet with the most recent earlier one, reading only metadata columns. Expected datasets come from the `*_DATASET_META` dicts in the merge modules, and frequency is inferred from the format of the period label.

**Why.** Processed files are dated and accumulate, so the previous state already exists, at no BigQuery cost. Taking expected datasets from configuration means a dataset missing every month is still reported. A dataset whose extraction time did not advance was served from an older raw file, which is the signature of an ISTAT outage.

**Consequences.** A diff of processed files cannot separate a source revision from a pipeline change. Row-count changes are reported with that caveat, and the commit SHA is recorded in the audit file.

### Gemini Developer API free tier, with a key in a billing-free project

**Context.** There is no budget. The options considered were the Gemini Developer API free tier with an API key; Gemini on Agent Platform (formerly Vertex AI) through the existing Workload Identity Federation service account; and the Claude API.

**Decision.** The Gemini Developer API free tier, with an AI Studio key from a separate project, `aiccon-digest`, that has no billing account. The key is stored as the GitHub secret `GEMINI_API_KEY`.

**Why.** The free tier applies only to projects without an active billing account, and billing is enabled on `aiccon-database-platform`, so a key there would be billed. Agent Platform has no standing free tier (its express mode is only for new Google Cloud users) and would bill the main project, however little. The Claude API was not used because the monthly API credits included with a Team plan must be claimed by a Team Owner.

**Consequences.** This is a deliberate exception to the 2026-10-26 decision to store no long-lived credentials: the key can only consume free quota in an empty project, and AI Studio authorisation keys are restricted to the Gemini API by default. Under the Gemini API terms, users in the EEA receive the paid-tier data terms on unpaid quota too, so prompts are not used to improve Google's products. The same terms allow only paid services when "making API Clients available to users" in the EEA. A scheduled job whose output colleagues read was judged not to be such a client. The planned Streamlit app, which sends colleagues' questions to Gemini, probably is, so check this before building it. These are readings of the terms, not legal advice.

### Gemini writes one paragraph, through placeholders, and is checked

**Decision.** Python computes every fact and renders all lists and tables. Gemini writes only the opening paragraph, refers to facts through placeholders such as `{n_aggiornati}`, and sees values only to choose grammatical agreement. A checker rejects the paragraph if it contains digits, Italian number words, month names, quantifiers, trend words, formatting, or unknown or missing placeholders. Python then substitutes the values. If no model produces an acceptable paragraph, a deterministic template paragraph is used.

**Why.** A check that every number in the output appears in the input would miss a correct number attached to the wrong fact, numbers written as words, and invented qualitative claims. With placeholders, correct numbers are guaranteed by construction, and requiring every placeholder stops Gemini expressing a fact in words instead. The residual risk is wording that drifts in meaning without breaking a rule: an early version described missing datasets as "non pervenuti", which implies a cause. The prompt is therefore versioned, and every attempt is kept in the audit file.

### Models, thinking and retries in configuration

**Decision.** Models are tried in the order listed under `genai.models` in `config/settings.yaml` (currently `gemini-3.8-flash`, `gemini-3.6-flash`, `gemini-3.5-flash-lite`), with thinking level `low`, `max_output_tokens` 8192, and at most three tries per model, retrying only HTTP 408, 429 and 5xx.

**Why.** In testing the models often returned HTTP 503 ("high demand"), so a chain of models improves the chance of a Gemini paragraph. Thinking tokens count against `max_output_tokens`: `gemini-3.6-flash` used up to 2,164 thinking tokens for a paragraph of about 100 tokens, even at `low`, so the earlier limit of 2,048 would have truncated it. The SDK retries by default; setting the policy explicitly bounds a run to nine requests, within the free daily limits (20 requests per day for `gemini-3.8-flash` when this was set up).

### The digest step runs even when the pipeline fails, and keeps prose out of the log

**Decision.** The digest step uses the same condition as the integrity checks. Scheduled runs always write the digest; manual runs only when `write_digest` is ticked. The key is passed in the step's `env`, not the job's. The log carries statuses, model names, HTTP status codes and token counts, never prompts or Gemini output. The audit JSON is written before the markdown, and the digest is also appended to the run's Summary page.

**Why.** Stale and missing datasets are the most useful content when a run partly fails. Manual test runs should not overwrite the month's digest by accident. The repository is public, so its logs and Summary pages are public: a step-level secret limits exposure, and the digest contains only dataset names, periods and counts.

**Consequences.** Delivery: the digest is published as a GitHub release, and colleagues are given the link to the Releases page. Email or Teams delivery could later be added with a Power Automate flow watching the releases feed, without sharing credentials

### Merge issue found and deferred

**Context.** Diagnosing the observation key for the digest showed that several ISTAT social-economy dataflows contain more than one indicator, distinguished by a `data_type` (and sometimes `measure`) dimension that processing drops; that `FORZLVMENS1_1` contains several published editions of each observation; and that the social-economy copy of `lfsa_egan22d` drops sex and age. Priority resolution then keeps one arbitrary row per observation, and `VOLON1_4` and `VOLON1_5` disappear entirely.

**Decision.** Deferred, so that this iteration stays focused on the digest. The issue is documented under Known limitations in the README.

**Consequences.** The affected series should not be used for analysis. The fix would map each ISTAT `data_type` (and `measure`) to its own indicator, keep only the latest edition, map sex and age in the social-economy Eurostat processing, and de-duplicate on every dimension, with a check that fails the processing stage if two rows share every dimension.

---

## 2026-10-26: Automation with GitHub Actions

The following decisions were taken together when the pipeline was moved from manual runs on a laptop to GitHub Actions.

### One workflow file for scheduled and manual runs

**Decision.** A single workflow, `.github/workflows/pipeline.yml`, with two triggers: a monthly `schedule` and `workflow_dispatch` (manual runs with a choice of stage, domain and whether to run the integrity checks). This replaces the two files (`monthly_pipeline.yml`, `manual_pipeline.yml`) the README originally planned.

**Why.** Both would run identical steps. Two copies of the same job drift apart over time.

### Workload Identity Federation instead of a service account key

**Context.** The runner needs Google Cloud credentials. The simple option is a service account key stored as a GitHub secret.

**Decision.** Workload Identity Federation with service account impersonation. Pool `github`, provider `aiccon-data-repo`, attribute condition `assertion.repository == 'greenalexander/aiccon_database_platform'`, impersonating `github-actions-pipeline@aiccon-database-platform.iam.gserviceaccount.com`.

**Why.** A key is a long-lived password: valid until deleted and usable from any machine if it leaks. With federation, nothing long-lived is stored; each run receives a short-lived token, and only workflows from this repository are accepted. Impersonating a service account (rather than granting roles directly to the GitHub identity) was chosen because it is the most widely used pattern and works with every client library the pipeline uses, including `gcsfs`.

**Consequences.** Renaming or moving the repository breaks authentication until the attribute condition and the `workloadIdentityUser` binding are updated. Federation errors are terse; the auth action's troubleshooting guide recommends waiting at least five minutes after any change to the pool or provider before testing.

### A dedicated, least-privilege service account for CI

**Decision.** GitHub Actions uses its own service account with `roles/storage.objectAdmin` on the bucket only, and `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the project. The local account `aiccon-pipeline` is unchanged (it keeps `roles/storage.admin`, by choice).

**Why.** Separate accounts make automated activity identifiable in the audit logs, allow either to be revoked without breaking the other, and limit what the CI identity can do to what the pipeline needs. BigQuery roles are granted at project level because the project holds nothing else, so restricting them to the dataset would add complexity for little gain.

### Configuration as repository variables, not secrets

**Decision.** The workflow reads `GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_SERVICE_ACCOUNT`, `GCS_BUCKET`, `BQ_PROJECT` and `BQ_DATASET` from GitHub repository variables. No secrets are used.

**Why.** None of these values is a credential; with federation, proof of identity is the per-run token. Secrets are masked in logs, which only makes typos harder to diagnose. Variables are kept in GitHub rather than in the workflow file so that future workflows (dbt, Gemini) can share them.

**Update (2026-11-02).** The first secret, `GEMINI_API_KEY`, was added for the monthly digest (see that entry).

### `requirements.txt` for CI, `uv.lock` later

**Superseded (2026-11-16).** GitHub Actions installs from `uv.lock` with `uv sync --locked`; `requirements.txt` was deleted.

**Context.** Locally the environment is managed by uv. The old `requirements.txt` was a Windows `pip freeze` that would not install on Linux (an editable install from a local path, and the Windows-only `pywinpty`). `pyproject.toml` lists development tools (Jupyter, DuckDB, office365 client) as runtime dependencies, does not declare three packages the code imports directly (`google-cloud-bigquery`, `google-cloud-storage`, `PyYAML`), and includes `pandas-gbq`, which the code does not appear to use.

**Decision.** `requirements.txt` now lists only the runtime imports, pinned to the tested local versions, and GitHub Actions installs from it with pip. Verified by installing it into a clean virtual environment and running the database stage and the integrity checks.

**Why.** Installing from `pyproject.toml` as it stands would install Jupyter on every run and rely on transitive dependencies for core libraries.

**Consequences.** Until `pyproject.toml` is tidied, any new runtime dependency must be added to both files. A forgotten one fails the GitHub run with `ModuleNotFoundError`. Once tidied (best done when dbt is added), GitHub Actions should install from `uv.lock` and `requirements.txt` can be retired.

### Python 3.12 on the runner

**Decision.** The runner uses Python 3.12, matching the local uv interpreter (3.12.13). The README previously said 3.11.

### Pace ISTAT requests 13 seconds apart

**Context.** ISTAT's SDMX service allows 5 queries per minute per IP address and blocks addresses that exceed it for one to two days. The two ISTAT fetchers send 22 requests in sequence, and failed requests are retried up to five times.

**Decision.** `base_loader.py` enforces at least 13 seconds between the starts of any two requests to `esploradati.istat.it`, retries included, with one clock shared by all loaders in the process.

**Why.** Pacing in the shared session covers retries and the boundary between the social economy and labour loaders, which per-fetcher pacing would not. It adds up to about five minutes per full run. Eurostat is not paced: its limits are unknown and it has never caused problems.

### Integrity checks run even when the pipeline fails

**Decision.** The integrity-check step runs unless the run was cancelled or never reached the pipeline step. The run is marked as failed if either the pipeline or the checks fail. WARN results never fail a run.

**Why.** After a partial failure (for example one ISTAT dataset falling back to last month's file), the checks show what state the database was actually left in.

### Accept the known NUTS3 gap (105 of 107 provinces) in the labour check

**Decision.** The labour integrity check expects at least 105 provinces with data rather than 107.

**Why.** Two provinces are missing, a known data issue deferred for now. Leaving the check at 107 would make every scheduled run fail, and failure emails would stop carrying information. At 105 a red run means something new, and any further loss of coverage still fails.

**Consequences.** Restore 107 once the two provinces are mapped.

### No automatic retry of failed runs

**Context.** ISTAT's SDMX service has frequent outages, from a few hours to about a day (several in October 2026, including one on 26 October during testing, while the ISTAT data browser kept working).

**Decision.** No second scheduled attempt and no early stop when ISTAT is down. A run that fails because of ISTAT is re-run by hand once the service is back.

**Why.** For a monthly pipeline a manual re-run is cheap, and the failure email is the prompt.

### Schedule time

**Decision.** `17 6 1 * *`: 06:17 UTC on the 1st of each month. Cron schedules in GitHub Actions run in UTC (an optional time-zone field exists but is unnecessary here).

**Why.** GitHub delays scheduled runs at busy times, especially at the top of the hour, so the minute is deliberately not :00.

### Keeping the schedule active

**Context.** In public repositories GitHub disables scheduled workflows after 60 days without repository activity.

**Decision.** No keep-alive automation. During active development regular commits prevent it; otherwise, respond to GitHub's warning email with a commit (an empty commit should do) or re-enable the workflow from the Actions tab afterwards.

## Architecture

### Decision: synced SharePoint drive rather than SharePoint API
**Date**: 2026-05
**Decision**: Use the OneDrive/SharePoint sync client (local mapped drive) rather than the Office 365 REST API or Microsoft Graph API.
**Rationale**: Simpler implementation — no OAuth flow, no library dependencies beyond the standard library. Since the pipeline runs locally on one machine, the sync client is always available. If the project moves to cloud execution (e.g. GitHub Actions with Azure) this would need to be revisited.
**Revisit when**: automation moves off a local machine.
**Superseded.** The pipeline now runs on GitHub Actions and keeps its data in GCS and BigQuery (see README). Kept for history.

### Decision: DuckDB as the integration layer
**Date**: 2026-05
**Decision**: Use DuckDB to build the star schema from parquet files, rather than loading data directly into PowerBI or using a cloud database.
**Rationale**: DuckDB can query parquet files directly, is embedded (no server to maintain), and produces a single `.duckdb` file that PowerBI can connect to via ODBC. Colleagues need no database knowledge. The file lives on SharePoint and refreshes monthly.
**Revisit when**: data volume grows beyond ~10GB or concurrent access becomes a requirement (DuckDB does not support multiple simultaneous writers).
**Superseded.** BigQuery is now the analytical database; DuckDB remains only for local exploration of parquet files (see README). Kept for history.

### Decision: long-format (tidy) fact tables
**Date**: 2026-05
**Decision**: All fact tables store one observation per row (indicator_code, value, unit) rather than wide-format tables with one column per indicator.
**Rationale**: Long format is source-agnostic — every dataset regardless of structure maps to the same schema. PowerBI handles long-format data well via slicers and filters. Wide format would require schema changes every time a new indicator is added.

### Decision: split schema into three SQL files
**Superseded (2026-11-16).** The schema is defined by dbt models (see the dbt refactor entry). Kept for history.

**Date**: 2026-05
**Decision**: `dimensions.sql`, `fact_tables.sql`, `views.sql` rather than one monolithic SQL file.
**Rationale**: Separation of concerns — dimensions are stable and shared, fact tables grow one domain at a time, views change frequently as colleagues request new analytical cuts. Keeping them separate makes each file easier to read and reduces the risk of accidentally breaking a dimension table when editing a view.

### Decision: one processed parquet per domain per month
**Date**: 2026-05
**Decision**: The processed layer stores one file per domain named `{domain}_{YYYY_MM}.parquet`. The raw layer accumulates monthly snapshots; the processed layer keeps only the current file (overwritten each run).
**Rationale**: Raw snapshots are valuable for auditing source data changes over time. Processed files are deterministic outputs — they can always be regenerated from the raw files, so accumulating them wastes space.
**Correction (2026-11-02).** The code names processed files by run month (`{domain}_YYYY-MM.parquet`) and never deletes them, so they accumulate: one file per domain for each month in which processing ran. The monthly digest relies on this to compare each month with the previous one, so do not add a clean-up of old processed files without changing the digest.

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
**Superseded (2026-11-16).** In the dbt project, `dim_time` has one row per period, keyed on `period_code`. Kept for history.

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