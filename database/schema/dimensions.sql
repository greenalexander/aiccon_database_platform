-- database/schema/dimensions.sql
--
-- Shared dimension tables for the aiccon-data star schema.
-- BigQuery dialect — executed via build_db.py one statement at a time.
--
-- Key differences from the DuckDB version:
--   • No sequences or nextval() — surrogate keys are managed in Python (build_db.py)
--   • No PRIMARY KEY, UNIQUE, REFERENCES, or INDEX constraints — BigQuery does not
--     enforce these; integrity is guaranteed by the Python loading logic instead
--   • INTEGER → INT64, VARCHAR → STRING, BOOLEAN → BOOL, DOUBLE → FLOAT64
--   • TINYINT / SMALLINT → INT64
--   • All tables use CREATE TABLE IF NOT EXISTS for idempotent runs
--   • INSERT OR IGNORE → handled by MERGE in build_db.py, not in this file
--
-- Load order: this file must be executed BEFORE fact_tables.sql.


-- ── dim_geography ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `{DATASET}`.dim_geography (
    geo_key             INT64,

    nuts_code           STRING,
    nuts_level          INT64,
    nuts_name_it        STRING,
    nuts_name_en        STRING,

    istat_code          STRING,

    country_code        STRING,

    region_name         STRING,
    macro_area          STRING,

    -- Municipality type (from ISTAT survey dimension)
    -- Populated for rows that represent a municipality-size class rather than
    -- a specific territory. Null for all standard NUTS rows.
    municipality_type   STRING,

    geo_source          STRING,
    is_active           BOOL
);


-- ── dim_time ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `{DATASET}`.dim_time (
    time_key        INT64,
    year            INT64,
    period_type     STRING,     -- A=Annual Q=Quarterly M=Monthly
    period_label    STRING,     -- e.g. '2021', '2021-Q3', '2021-11'
    reference_period STRING     -- human-readable, e.g. '2021 (Census)'
);


-- ── dim_source ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `{DATASET}`.dim_source (
    source_key          INT64,
    source_id           STRING,
    provider            STRING,
    source_name         STRING,
    source_name_it      STRING,
    domain              STRING,
    access_type         STRING,     -- 'api' or 'manual'
    priority            INT64,      -- 1=highest (ISTAT) ... 4=lowest
    update_frequency    STRING,
    territorial_levels  STRING,     -- semicolon-separated: 'NUTS0;NUTS2'
    temporal_coverage   STRING,
    notes               STRING
);


-- ── dim_legal_form ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `{DATASET}`.dim_legal_form (
    legal_form_key      INT64,
    unified_category    STRING,
    unified_category_en STRING,
    nace_primary        STRING,
    ets_classification  STRING,
    source_system       STRING,
    source_code         STRING,
    source_label_it     STRING
);


-- ── dim_indicator ─────────────────────────────────────────────────────────────
--
-- Populated by build_db.py via MERGE after this file is executed.
-- The indicator rows are defined in build_db.py rather than here to avoid
-- the INSERT OR IGNORE syntax that DuckDB supports but BigQuery does not.

CREATE TABLE IF NOT EXISTS `{DATASET}`.dim_indicator (
    indicator_key   INT64,
    indicator_code  STRING,
    label_it        STRING,
    label_en        STRING,
    unit_default    STRING,
    domain          STRING,
    notes           STRING
);