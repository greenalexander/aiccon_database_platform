-- database/schema/fact_tables.sql
--
-- Fact tables for the aiccon-data star schema.
-- One table per thematic domain, all referencing the shared dimension tables
-- defined in dimensions.sql (which must be loaded first).
--
-- ── Adding a new domain ───────────────────────────────────────────────────────
-- 1. Copy the stub at the bottom of this file for your domain
-- 2. Replace the placeholder column comments with actual dimension columns
-- 3. Run build_db.py — it executes this file in full on every run
-- 4. No changes needed to dimensions.sql or build_db.py
--
-- ── Design principles ─────────────────────────────────────────────────────────
-- • Every fact table shares the same four core columns:
--     geo_key, time_key, source_key, indicator_key
--   These are foreign keys to the shared dimension tables.
--
-- • Domain-specific dimension columns (gender, age_group, legal_form_key, etc.)
--   are added directly to the fact table rather than as separate dimension tables.
--   This keeps queries simple for PowerBI — one join per dimension is enough
--   for the analytical patterns that will be used.
--
-- • All non-core columns are NULLABLE. A row from a national-level dataset will
--   have nuts_level=0 and no legal_form_key. A row from an organisational dataset
--   will have no gender. Nulls are expected and meaningful, not data quality issues.
--
-- • QA flag columns (geo_unmatched, legal_form_unmatched) from the processed
--   parquet are NOT loaded into the fact tables — they belong in the processed
--   layer for inspection, not in the analytical layer.


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 1: SOCIAL ECONOMY
-- Sources: ISTAT volunteering surveys, ISTAT associationism survey, Eurostat
-- Processed parquet: SharePoint/processed/social_economy/
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_social_economy (

    -- ── Surrogate key ─────────────────────────────────────────────────────
    fact_key        BIGINT PRIMARY KEY,

    -- ── Core dimension foreign keys (shared across all domains) ───────────
    geo_key         INTEGER REFERENCES dim_geography(geo_key),
    time_key        INTEGER REFERENCES dim_time(time_key),
    source_key      INTEGER REFERENCES dim_source(source_key),
    indicator_key   INTEGER REFERENCES dim_indicator(indicator_key),

    -- ── Measure ───────────────────────────────────────────────────────────
    value           DOUBLE,
    unit            VARCHAR,        -- 'percentage' / 'thousands_persons' /
                                    -- 'count' / 'mixed' (see dim_indicator.notes)

    -- ── Legal form dimension (social economy specific) ────────────────────
    -- Populated for ISTAT organisational datasets (RUNTS, Camere di Commercio).
    -- NULL for survey-based datasets (volunteering, associationism) and for
    -- all Eurostat rows, which classify by NACE not legal form.
    legal_form_key      INTEGER REFERENCES dim_legal_form(legal_form_key),
    ets_classification  VARCHAR,    -- 'ets' / 'ets_eligible' / 'non_ets' / NULL

    -- ── NACE dimension ────────────────────────────────────────────────────
    -- Populated for Eurostat datasets and ISTAT LFS data.
    -- NULL for survey datasets that classify by volunteer/member characteristics.
    nace_code       VARCHAR,        -- e.g. 'Q87', 'P85', 'O-U' (aggregate)
    nace_label_en   VARCHAR,

    -- ── Demographic dimensions ────────────────────────────────────────────
    -- Available in ISTAT survey datasets. NULL for Eurostat rows.
    gender          VARCHAR,        -- 'male' / 'female' / 'total'
    age_group       VARCHAR,        -- e.g. '15-34' / '35-64' / '65+'

    -- ── Volunteering-specific dimensions ──────────────────────────────────
    -- From ISTAT Indagine sul Volontariato (datasets VOLON1_*)
    volunteering_form   VARCHAR,    -- 'ORGVOL' (organised) / 'DIRVOL' (direct) / 'total'
    activity_type       VARCHAR,    -- emergency rescue / social assistance / culture / etc.
    years_active        VARCHAR,    -- '<1yr' / '1-2' / '3-5' / '6-10' / '>10'
    education           VARCHAR,    -- highest educational qualification
    labour_status       VARCHAR,    -- employed / unemployed / retired / student / etc.
    household_size      VARCHAR,
    econ_resources      VARCHAR,    -- self-assessed economic resources
    municipality_type   VARCHAR,    -- comune > 250k / 50-250k / 10-50k / <10k / rural

    -- ── Organised volunteering dimensions ─────────────────────────────────
    -- From ISTAT datasets VOLON_ORG1_*
    org_sector          VARCHAR,    -- sport / culture / social assistance / health / etc.
    org_type            VARCHAR,    -- ODV / APS / cooperative sociale / etc.
    motivation          VARCHAR,    -- altruism / faith / social belonging / etc.
    personal_impact     VARCHAR,    -- new friendships / sense of usefulness / etc.
    multi_membership    VARCHAR,    -- single org / multiple orgs

    -- ── Associationism dimensions ─────────────────────────────────────────
    -- From ISTAT Aspetti della Vita Quotidiana (AVQ, datasets AVQ_PERSONE_*)
    association_type    VARCHAR,    -- cultural / sports / religious / political / etc.

    -- ── Provenance ────────────────────────────────────────────────────────
    dataset_code    VARCHAR,        -- original dataset identifier for traceability
    extracted_at    TIMESTAMP       -- UTC timestamp from raw layer

);

CREATE INDEX IF NOT EXISTS idx_se_geo       ON fact_social_economy (geo_key);
CREATE INDEX IF NOT EXISTS idx_se_time      ON fact_social_economy (time_key);
CREATE INDEX IF NOT EXISTS idx_se_indicator ON fact_social_economy (indicator_key);
CREATE INDEX IF NOT EXISTS idx_se_source    ON fact_social_economy (source_key);
CREATE INDEX IF NOT EXISTS idx_se_nace      ON fact_social_economy (nace_code);
CREATE INDEX IF NOT EXISTS idx_se_gender    ON fact_social_economy (gender);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 2: IMMIGRATION
-- Status: STUB — fill in when building this domain
--
-- Likely sources:
--   ISTAT:               resident foreign population by nationality/region
--   Ministero Interno:   asylum applications and permits (manual download)
--   Eurostat migr_*:     EU-level migration flows and stocks
--
-- Key dimensions to add (replace the placeholder columns below):
--   nationality         — country of origin (ISO alpha-3)
--   permit_type         — work / family / asylum / study / humanitarian
--   migration_flow      — 'inflow' / 'outflow' / 'stock'
--   legal_status        — regularised / undocumented / asylum seeker
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_immigration (
    fact_key        BIGINT PRIMARY KEY,
    geo_key         INTEGER REFERENCES dim_geography(geo_key),
    time_key        INTEGER REFERENCES dim_time(time_key),
    source_key      INTEGER REFERENCES dim_source(source_key),
    indicator_key   INTEGER REFERENCES dim_indicator(indicator_key),
    value           DOUBLE,
    unit            VARCHAR,
    -- TODO: add domain-specific dimension columns here
    -- e.g. nationality VARCHAR, permit_type VARCHAR, ...
    gender          VARCHAR,
    age_group       VARCHAR,
    dataset_code    VARCHAR,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 3: WELFARE AND SOCIAL POLICIES
-- Status: STUB — fill in when building this domain
--
-- Likely sources:
--   ISTAT BES:           well-being indicators
--   Eurostat soc_*:      social protection expenditure
--   OECD Social Exp:     SOCX database (manual or API)
--
-- Key dimensions to add:
--   service_type        — healthcare / long-term care / childcare / disability
--   beneficiary_group   — elderly / disabled / children / unemployed
--   expenditure_type    — public / private / total
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_welfare (
    fact_key        BIGINT PRIMARY KEY,
    geo_key         INTEGER REFERENCES dim_geography(geo_key),
    time_key        INTEGER REFERENCES dim_time(time_key),
    source_key      INTEGER REFERENCES dim_source(source_key),
    indicator_key   INTEGER REFERENCES dim_indicator(indicator_key),
    value           DOUBLE,
    unit            VARCHAR,
    -- TODO: add domain-specific dimension columns here
    gender          VARCHAR,
    age_group       VARCHAR,
    dataset_code    VARCHAR,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 4: POVERTY AND INEQUALITY
-- Status: STUB — fill in when building this domain
--
-- Likely sources:
--   ISTAT BES / EU-SILC: at-risk-of-poverty, material deprivation
--   Eurostat ilc_*:      income and living conditions
--   OECD:                Gini coefficient, income shares
--
-- Key dimensions to add:
--   population_group    — children / elderly / single parents / migrants
--   deprivation_type    — income / material / severe material
--   income_quintile     — Q1 / Q2 / Q3 / Q4 / Q5
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_poverty (
    fact_key        BIGINT PRIMARY KEY,
    geo_key         INTEGER REFERENCES dim_geography(geo_key),
    time_key        INTEGER REFERENCES dim_time(time_key),
    source_key      INTEGER REFERENCES dim_source(source_key),
    indicator_key   INTEGER REFERENCES dim_indicator(indicator_key),
    value           DOUBLE,
    unit            VARCHAR,
    -- TODO: add domain-specific dimension columns here
    gender          VARCHAR,
    age_group       VARCHAR,
    dataset_code    VARCHAR,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 5: LABOUR
-- Sources: ISTAT RFL (Rilevazione sulle Forze di Lavoro),
--          Eurostat EU-LFS (lfsa_egan22d, lfst_r_lfu3rt,
--                           lfst_r_lfur2gan, lfst_r_lfe2emprtn)
-- Processed parquet: SharePoint/processed/labour/
--
-- Indicators stored:
--   labour_force        — absolute labour force headcount (ISTAT monthly, thousands)
--   employment_rate     — % of working-age population employed
--   unemployment_rate   — % of labour force unemployed
--   inactivity_rate     — % of working-age population inactive
--   neet_rate           — % of 15-34 year-olds not in employment/education/training
--   n_employed          — employed persons by NACE activity (Eurostat, thousands)
--
-- Design notes:
--   • frequency is stored in the fact table rather than dim_time because the monthly
--     labour force series (FORZLVMENS1_1) would otherwise require 12× dim_time rows
--     per year. The time_key here always resolves to the annual row (year only);
--     period_label stores the full sub-annual label ('2023-04', '2023-Q2') where
--     needed, and frequency ('A'/'M'/'Q') distinguishes the cadence.
--   • education maps to ISCED 2011 codes for Eurostat (ED0-2, ED3_4, ED5-8, TOTAL)
--     and to ISTAT edu_lev_highest codes where ISTAT is the source. The view layer
--     (vw_labour) surfaces both as-is — harmonisation is left to the analyst.
--   • citizenship is NAT/FOR/TOTAL for Eurostat and ITL/FRG/TOTAL for ISTAT NEETs.
--   • nace_code is populated only for lfsa_egan22d; NULL for all rate/NEET rows.
--   • adjustment ('N'=raw, 'Y'=seasonally adjusted) is populated only for
--     FORZLVMENS1_1; NULL elsewhere.
--   • legal_form is not a dimension in any labour dataset — no legal_form_key column.
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_labour (

    -- ── Surrogate key ─────────────────────────────────────────────────────
    fact_key        BIGINT PRIMARY KEY,

    -- ── Core dimension foreign keys ───────────────────────────────────────
    geo_key         INTEGER REFERENCES dim_geography(geo_key),
    time_key        INTEGER REFERENCES dim_time(time_key),
    source_key      INTEGER REFERENCES dim_source(source_key),
    indicator_key   INTEGER REFERENCES dim_indicator(indicator_key),

    -- ── Measure ───────────────────────────────────────────────────────────
    value           DOUBLE,
    unit            VARCHAR,        -- 'percentage' / 'thousands_persons'

    -- ── Sub-annual time detail ────────────────────────────────────────────
    -- time_key always resolves to the annual row (year). Sub-annual detail
    -- is captured here to avoid bloating dim_time.
    frequency       VARCHAR(1),     -- 'A'=annual / 'M'=monthly / 'Q'=quarterly
    period_label    VARCHAR,        -- '2023' / '2023-04' / '2023-Q2'

    -- ── Demographic dimensions ────────────────────────────────────────────
    gender          VARCHAR,        -- 'T'=total / 'M'=male / 'F'=female
                                    -- ISTAT uses '9'/'1'/'2' — harmonised in merge
    age_group       VARCHAR,        -- e.g. 'Y15-74', 'Y15-24', 'TOTAL'

    -- ── Labour-specific dimensions ────────────────────────────────────────
    -- education: ISCED 2011 for Eurostat (ED0-2 / ED3_4 / ED5-8 / TOTAL);
    --            edu_lev_highest codes for ISTAT TAXDISOCCU1_6.
    --            NULL for datasets where education is not a dimension.
    education               VARCHAR,

    -- citizenship: NAT/FOR/TOTAL (Eurostat); ITL/FRG/TOTAL (ISTAT NEETs).
    --              NULL for datasets where citizenship is not a dimension.
    citizenship             VARCHAR,

    -- adjustment: 'N'=raw data, 'Y'=seasonally adjusted.
    --             Populated for FORZLVMENS1_1 only; NULL elsewhere.
    adjustment              VARCHAR(1),

    -- unemployment_duration: always 'TOTAL' in current datasets.
    --                        Retained for schema stability if breakdowns are added later.
    unemployment_duration   VARCHAR,

    -- ── NACE (lfsa_egan22d only) ──────────────────────────────────────────
    -- NULL for all rate / NEET datasets.
    nace_code       VARCHAR,        -- e.g. 'P85', 'Q86', 'TOTAL'
    nace_label_en   VARCHAR,

    -- ── Provenance ────────────────────────────────────────────────────────
    dataset_code    VARCHAR,        -- original ISTAT/Eurostat dataset identifier
    extracted_at    TIMESTAMP       -- UTC timestamp from raw layer

);

CREATE INDEX IF NOT EXISTS idx_lab_geo       ON fact_labour (geo_key);
CREATE INDEX IF NOT EXISTS idx_lab_time      ON fact_labour (time_key);
CREATE INDEX IF NOT EXISTS idx_lab_indicator ON fact_labour (indicator_key);
CREATE INDEX IF NOT EXISTS idx_lab_source    ON fact_labour (source_key);
CREATE INDEX IF NOT EXISTS idx_lab_gender    ON fact_labour (gender);
CREATE INDEX IF NOT EXISTS idx_lab_nace      ON fact_labour (nace_code);
CREATE INDEX IF NOT EXISTS idx_lab_edu       ON fact_labour (education);
CREATE INDEX IF NOT EXISTS idx_lab_citizen   ON fact_labour (citizenship);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 6: SUSTAINABLE DEVELOPMENT (SDGs)
-- Status: STUB — fill in when building this domain
--
-- Likely sources:
--   ISTAT SDGs:          Italy's SDG monitoring indicators
--   Eurostat SDG:        EU SDG indicator set (sdg_* datasets)
--
-- Key dimensions to add:
--   sdg_goal            — '1' through '17'
--   sdg_target          — e.g. '1.1', '3.4'
--   sdg_indicator_code  — official UN/Eurostat indicator code
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_sdg (
    fact_key        BIGINT PRIMARY KEY,
    geo_key         INTEGER REFERENCES dim_geography(geo_key),
    time_key        INTEGER REFERENCES dim_time(time_key),
    source_key      INTEGER REFERENCES dim_source(source_key),
    indicator_key   INTEGER REFERENCES dim_indicator(indicator_key),
    value           DOUBLE,
    unit            VARCHAR,
    -- TODO: add domain-specific dimension columns here
    sdg_goal        VARCHAR,        -- '1' … '17'
    sdg_target      VARCHAR,        -- e.g. '3.4'
    dataset_code    VARCHAR,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 7: CIVIL SOCIETY AND SOCIAL IMPACT
-- Status: STUB — fill in when building this domain
--
-- Likely sources:
--   ISTAT:               social capital indicators, trust, civic engagement
--   CSVnet / Fondazioni: voluntary sector statistics (manual)
--
-- Key dimensions to add:
--   org_type            — foundation / association / hybrid
--   impact_area         — education / health / culture / environment
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_civil_society (
    fact_key        BIGINT PRIMARY KEY,
    geo_key         INTEGER REFERENCES dim_geography(geo_key),
    time_key        INTEGER REFERENCES dim_time(time_key),
    source_key      INTEGER REFERENCES dim_source(source_key),
    indicator_key   INTEGER REFERENCES dim_indicator(indicator_key),
    value           DOUBLE,
    unit            VARCHAR,
    -- TODO: add domain-specific dimension columns here
    gender          VARCHAR,
    dataset_code    VARCHAR,
    extracted_at    TIMESTAMP
);


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 8: HOUSING AND URBAN WELFARE
-- Status: STUB — fill in when building this domain
--
-- Likely sources:
--   ISTAT:               housing conditions, overcrowding, social housing
--   Eurostat ilc_h*:     housing cost overburden, deprivation
--   Federcasa:           social housing stock (manual)
--
-- Key dimensions to add:
--   tenure_type         — owner / private rental / social rental / other
--   housing_condition   — overcrowded / inadequate / unaffordable
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_housing (
    fact_key        BIGINT PRIMARY KEY,
    geo_key         INTEGER REFERENCES dim_geography(geo_key),
    time_key        INTEGER REFERENCES dim_time(time_key),
    source_key      INTEGER REFERENCES dim_source(source_key),
    indicator_key   INTEGER REFERENCES dim_indicator(indicator_key),
    value           DOUBLE,
    unit            VARCHAR,
    -- TODO: add domain-specific dimension columns here
    tenure_type     VARCHAR,
    gender          VARCHAR,
    age_group       VARCHAR,
    dataset_code    VARCHAR,
    extracted_at    TIMESTAMP
);