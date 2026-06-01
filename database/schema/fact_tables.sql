-- database/schema/fact_tables.sql
--
-- Fact tables for the aiccon-data star schema.
-- BigQuery dialect — executed via build_db.py one statement at a time.
--
-- Key differences from the DuckDB version:
--   • No PRIMARY KEY, REFERENCES, or INDEX constraints
--   • BIGINT → INT64, INTEGER → INT64, VARCHAR → STRING
--   • DOUBLE → FLOAT64, TIMESTAMP → TIMESTAMP
--   • All tables use CREATE TABLE IF NOT EXISTS for idempotent runs
--
-- Load order: dimensions.sql must be executed first.
--
-- ── Adding a new domain ───────────────────────────────────────────────────────
-- 1. Copy a stub table below for your domain
-- 2. Replace placeholder columns with actual dimension columns
-- 3. Add a loader function to database/build_db.py
-- 4. Run build_db.py — this file is executed in full on every run


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 1: SOCIAL ECONOMY
-- Sources: ISTAT volunteering surveys, ISTAT associationism survey, Eurostat
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `{DATASET}`.fact_social_economy (

    fact_key            INT64,

    geo_key             INT64,
    time_key            INT64,
    source_key          INT64,
    indicator_key       INT64,

    value               FLOAT64,
    unit                STRING,

    -- Legal form (social economy specific)
    -- NULL for survey-based datasets and Eurostat rows
    legal_form_key      INT64,
    ets_classification  STRING,

    -- NACE (Eurostat datasets and ISTAT LFS)
    -- NULL for survey datasets
    nace_code           STRING,
    nace_label_en       STRING,

    -- Demographics (ISTAT survey datasets)
    gender              STRING,
    age_group           STRING,

    -- Volunteering dimensions (ISTAT VOLON1_*)
    volunteering_form   STRING,     -- ORGVOL / DIRVOL / total
    activity_type       STRING,
    years_active        STRING,
    education           STRING,
    labour_status       STRING,
    household_size      STRING,
    econ_resources      STRING,
    municipality_type   STRING,

    -- Organised volunteering dimensions (ISTAT VOLON_ORG1_*)
    org_sector          STRING,
    org_type            STRING,
    motivation          STRING,
    personal_impact     STRING,
    multi_membership    STRING,

    -- Associationism dimensions (ISTAT AVQ_PERSONE_*)
    association_type    STRING,

    -- Provenance
    dataset_code        STRING,
    extracted_at        TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 2: IMMIGRATION
-- Status: STUB — fill in when building this domain
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `{DATASET}`.fact_immigration (
    fact_key        INT64,
    geo_key         INT64,
    time_key        INT64,
    source_key      INT64,
    indicator_key   INT64,
    value           FLOAT64,
    unit            STRING,
    -- TODO: nationality STRING, permit_type STRING, migration_flow STRING
    gender          STRING,
    age_group       STRING,
    dataset_code    STRING,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 3: WELFARE AND SOCIAL POLICIES
-- Status: STUB — fill in when building this domain
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `{DATASET}`.fact_welfare (
    fact_key        INT64,
    geo_key         INT64,
    time_key        INT64,
    source_key      INT64,
    indicator_key   INT64,
    value           FLOAT64,
    unit            STRING,
    -- TODO: service_type STRING, beneficiary_group STRING
    gender          STRING,
    age_group       STRING,
    dataset_code    STRING,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 4: POVERTY AND INEQUALITY
-- Status: STUB — fill in when building this domain
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `{DATASET}`.fact_poverty (
    fact_key        INT64,
    geo_key         INT64,
    time_key        INT64,
    source_key      INT64,
    indicator_key   INT64,
    value           FLOAT64,
    unit            STRING,
    -- TODO: population_group STRING, deprivation_type STRING
    gender          STRING,
    age_group       STRING,
    dataset_code    STRING,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 5: LABOUR
-- Sources: ISTAT RFL, Eurostat EU-LFS
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `{DATASET}`.fact_labour (

    fact_key        INT64,

    geo_key         INT64,
    time_key        INT64,
    source_key      INT64,
    indicator_key   INT64,

    value           FLOAT64,
    unit            STRING,

    -- Sub-annual time detail
    -- time_key always resolves to the annual row; sub-annual detail lives here
    frequency       STRING,     -- A=annual / M=monthly / Q=quarterly
    period_label    STRING,     -- '2023' / '2023-04' / '2023-Q2'

    -- Demographics
    gender          STRING,     -- T=total / M=male / F=female
    age_group       STRING,

    -- Labour-specific dimensions
    education               STRING,     -- ISCED 2011 (Eurostat) or ISTAT edu codes
    citizenship             STRING,     -- NAT/FOR/TOTAL (Eurostat); ITL/FRG/TOTAL (ISTAT)
    adjustment              STRING,     -- N=raw / Y=seasonally adjusted (FORZLVMENS1_1 only)
    unemployment_duration   STRING,

    -- NACE (lfsa_egan22d only; NULL for rate/NEET datasets)
    nace_code       STRING,
    nace_label_en   STRING,

    -- Provenance
    dataset_code    STRING,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 6: SUSTAINABLE DEVELOPMENT (SDGs)
-- Status: STUB — fill in when building this domain
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `{DATASET}`.fact_sdg (
    fact_key        INT64,
    geo_key         INT64,
    time_key        INT64,
    source_key      INT64,
    indicator_key   INT64,
    value           FLOAT64,
    unit            STRING,
    sdg_goal        STRING,
    sdg_target      STRING,
    dataset_code    STRING,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 7: CIVIL SOCIETY AND SOCIAL IMPACT
-- Status: STUB — fill in when building this domain
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `{DATASET}`.fact_civil_society (
    fact_key        INT64,
    geo_key         INT64,
    time_key        INT64,
    source_key      INT64,
    indicator_key   INT64,
    value           FLOAT64,
    unit            STRING,
    gender          STRING,
    dataset_code    STRING,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 8: HOUSING AND URBAN WELFARE
-- Status: STUB — fill in when building this domain
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS `{DATASET}`.fact_housing (
    fact_key        INT64,
    geo_key         INT64,
    time_key        INT64,
    source_key      INT64,
    indicator_key   INT64,
    value           FLOAT64,
    unit            STRING,
    tenure_type     STRING,
    gender          STRING,
    age_group       STRING,
    dataset_code    STRING,
    extracted_at    TIMESTAMP
);