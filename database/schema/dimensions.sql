-- database/schema/dimensions.sql
--
-- Shared dimension tables for the aiccon-data star schema.
--
-- These tables are domain-agnostic and never need to change when a new
-- thematic domain (immigration, welfare, SDGs, etc.) is added. Every
-- new domain's fact table simply references these same dimensions via
-- foreign keys (geo_key, time_key, source_key).
--
-- Load order: this file must be executed BEFORE fact_tables.sql.
--
-- Naming conventions:
--   dim_*       dimension tables
--   fact_*      fact tables (in fact_tables.sql)
--   All keys are INTEGER surrogate keys, not natural keys, so that
--   upstream code changes (e.g. new NUTS vintages) don't cascade into
--   fact table rows.


-- ── dim_geography ─────────────────────────────────────────────────────────────
--
-- Every territory that appears in any dataset, from EU countries (NUTS0)
-- down to Italian provinces (NUTS3).
--
-- How to extend for new domains:
--   Immigration data may introduce non-EU countries (e.g. countries of origin).
--   Add those as rows with nuts_level = 0, country_code = ISO code, and
--   nuts_code = ISO 3166-1 alpha-3 (e.g. 'MAR' for Morocco) — clearly
--   distinguished from NUTS codes by length and naming convention.
--   No schema change needed; just new rows.

CREATE SEQUENCE IF NOT EXISTS seq_geo_key START 1;

CREATE TABLE IF NOT EXISTS dim_geography (
    geo_key         INTEGER PRIMARY KEY DEFAULT nextval('seq_geo_key'),

    -- NUTS geography (primary key for EU + Italian data)
    nuts_code       VARCHAR,            -- e.g. 'IT', 'ITH5', 'ITH55'
    nuts_level      TINYINT,            -- 0=country 1=macro 2=region 3=province
    nuts_name_it    VARCHAR,            -- Italian name
    nuts_name_en    VARCHAR,            -- English name

    -- ISTAT codes (for Italian subnational data)
    istat_code      VARCHAR,            -- e.g. '037' for Bologna province

    -- Country
    country_code    VARCHAR(3),         -- ISO 3166-1 alpha-2 (e.g. 'IT', 'FR')
                                        -- alpha-3 for non-EU origin countries

    -- Italian administrative hierarchy (null for non-Italian rows)
    region_name     VARCHAR,            -- e.g. 'Emilia-Romagna'
    macro_area      VARCHAR,            -- Nord-Ovest / Nord-Est / Centro / Sud / Isole

    -- Municipality type (from ISTAT survey dimension — e.g. 'comune > 250k')
    -- Populated for rows that represent a municipality-size class rather than
    -- a specific territory. Null for all standard NUTS rows.
    municipality_type VARCHAR,

    -- Data quality
    geo_source      VARCHAR,            -- 'nuts_istat_csv' / 'manual' / 'iso'
    is_active       BOOLEAN DEFAULT TRUE, -- FALSE for discontinued NUTS codes

    UNIQUE (nuts_code)
);

CREATE INDEX IF NOT EXISTS idx_geo_nuts_code  ON dim_geography (nuts_code);
CREATE INDEX IF NOT EXISTS idx_geo_nuts_level ON dim_geography (nuts_level);
CREATE INDEX IF NOT EXISTS idx_geo_country    ON dim_geography (country_code);
CREATE INDEX IF NOT EXISTS idx_geo_istat_code ON dim_geography (istat_code);


-- ── dim_time ──────────────────────────────────────────────────────────────────
--
-- Reference period for every observation.
-- Currently annual only (all sources provide yearly data).
-- Sub-annual periods can be added later without changing fact tables —
-- just add new rows with period_type = 'Q' or 'M'.
--
-- How to extend for new domains:
--   No changes needed. SDG data, immigration data, welfare data all use
--   annual or multi-year periods — just insert new rows.

CREATE SEQUENCE IF NOT EXISTS seq_time_key START 1;

CREATE TABLE IF NOT EXISTS dim_time (
    time_key        INTEGER PRIMARY KEY DEFAULT nextval('seq_time_key'),
    year            SMALLINT NOT NULL,          -- e.g. 2021
    period_type     VARCHAR(1) DEFAULT 'A',     -- A=Annual Q=Quarterly M=Monthly
    period_label    VARCHAR,                    -- e.g. '2021', '2021-Q3', '2021-11'
    reference_period VARCHAR,                   -- human-readable, e.g. '2021 (Census)'

    UNIQUE (year, period_type, period_label)
);

CREATE INDEX IF NOT EXISTS idx_time_year ON dim_time (year);


-- ── dim_source ────────────────────────────────────────────────────────────────
--
-- Registry of every data source used in the platform, across all domains.
-- Mirrors domain_sources.csv but lives in the database for easy joining.
--
-- How to extend for new domains:
--   When adding immigration data (Ministero dell'Interno, Eurostat migr_*),
--   insert new rows here. No schema change needed.
--   The domain column links a source to its primary thematic area, but a
--   source can appear in multiple domains — just insert one row per source
--   (sources are not domain-exclusive).

CREATE SEQUENCE IF NOT EXISTS seq_source_key START 1;

CREATE TABLE IF NOT EXISTS dim_source (
    source_key      INTEGER PRIMARY KEY DEFAULT nextval('seq_source_key'),
    source_id       VARCHAR NOT NULL,       -- matches source_id in domain_sources.csv
    provider        VARCHAR,                -- e.g. 'ISTAT', 'Eurostat', 'Ministero dell'Interno'
    source_name     VARCHAR,
    source_name_it  VARCHAR,
    domain          VARCHAR,                -- primary domain: social_economy / immigration / etc.
    access_type     VARCHAR,                -- 'api' or 'manual'
    priority        TINYINT,                -- 1=highest (ISTAT) … 4=lowest
    update_frequency VARCHAR,               -- 'annual' / 'census' / 'monthly' etc.
    territorial_levels VARCHAR,             -- semi-colon separated: 'NUTS0;NUTS2'
    temporal_coverage VARCHAR,              -- e.g. '2010-present'
    notes           VARCHAR,

    UNIQUE (source_id)
);

CREATE INDEX IF NOT EXISTS idx_source_domain ON dim_source (domain);


-- ── dim_legal_form ────────────────────────────────────────────────────────────
--
-- Unified legal form categories, populated from legal_form_map.csv.
-- Used by social_economy fact tables; null foreign key for domains where
-- legal form is not a relevant dimension (immigration, SDGs, etc.).
--
-- How to extend for new domains:
--   Welfare data may introduce new organisational categories (ASL, IPAB, etc.).
--   Add rows to legal_form_map.csv and re-run build_db.py — the table rebuilds
--   from the CSV on every pipeline run.

CREATE SEQUENCE IF NOT EXISTS seq_legal_form_key START 1;

CREATE TABLE IF NOT EXISTS dim_legal_form (
    legal_form_key      INTEGER PRIMARY KEY DEFAULT nextval('seq_legal_form_key'),
    unified_category    VARCHAR NOT NULL,       -- e.g. 'cooperativa_sociale'
    unified_category_en VARCHAR,
    nace_primary        VARCHAR,                -- primary NACE mapping
    ets_classification  VARCHAR,                -- 'ets' / 'ets_eligible' / 'non_ets'
    source_system       VARCHAR,                -- originating source system
    source_code         VARCHAR,                -- original code in that system
    source_label_it     VARCHAR,

    UNIQUE (unified_category, source_system, source_code)
);


-- ── dim_indicator ─────────────────────────────────────────────────────────────
--
-- Catalogue of every indicator_code used across all domains.
-- Provides human-readable labels and units for PowerBI display.
--
-- This is the most important dimension for cross-domain usability:
-- colleagues filter by indicator in PowerBI, so clear labels matter.
--
-- How to extend for new domains:
--   When adding immigration data, insert rows like:
--     ('n_resident_foreign', 'Resident foreign population', 'count', 'immigration')
--     ('asylum_applications', 'Asylum applications', 'count', 'immigration')
--   No schema change needed.

CREATE SEQUENCE IF NOT EXISTS seq_indicator_key START 1;

CREATE TABLE IF NOT EXISTS dim_indicator (
    indicator_key   INTEGER PRIMARY KEY DEFAULT nextval('seq_indicator_key'),
    indicator_code  VARCHAR NOT NULL,       -- matches indicator_code in fact tables
    label_it        VARCHAR,                -- Italian label for PowerBI
    label_en        VARCHAR,                -- English label
    unit_default    VARCHAR,                -- most common unit for this indicator
    domain          VARCHAR,                -- primary domain
    notes           VARCHAR,

    UNIQUE (indicator_code)
);

-- Pre-populate with all known social economy indicators.
-- Add rows for each new domain as they are built.
INSERT OR IGNORE INTO dim_indicator
    (indicator_code, label_it, label_en, unit_default, domain, notes)
VALUES
    -- Social economy — volunteering
    ('volunteering_rate',
     'Tasso di volontariato',
     'Volunteering rate',
     'mixed',
     'social_economy',
     'Dataset VOLON1_1 contains multiple data_types — check unit column per row'),
    ('volunteering_activity_share',
     'Quota per tipo di attività',
     'Share by activity type',
     'percentage',
     'social_economy', NULL),
    ('volunteering_years_share',
     'Quota per anni di attività',
     'Share by years active',
     'percentage',
     'social_economy', NULL),
    ('org_volunteering_sector_share',
     'Quota volontariato organizzato per settore',
     'Organised volunteering share by sector',
     'percentage',
     'social_economy', NULL),
    ('org_volunteering_orgtype_share',
     'Quota per tipo organizzazione',
     'Organised volunteering share by org type',
     'percentage',
     'social_economy', NULL),
    ('org_volunteering_motivation_share',
     'Quota per motivazione',
     'Volunteering share by motivation',
     'percentage',
     'social_economy', NULL),
    ('org_volunteering_impact_share',
     'Quota per impatto personale',
     'Volunteering share by personal impact',
     'percentage',
     'social_economy', NULL),
    -- Social economy — associationism
    ('association_membership_rate',
     'Tasso di associazionismo',
     'Association membership rate',
     'percentage',
     'social_economy', NULL),
    -- Social economy — Eurostat employment
    ('n_employed',
     'Occupati',
     'Employed persons',
     'thousands_persons',
     'social_economy',
     'Eurostat: THS or THS_PER. ISTAT: absolute count from LFS.'),
    -- Social economy — Eurostat organisational
    ('n_local_units',
     'Unità locali',
     'Local units',
     'count',
     'social_economy',
     'From sbs_r_nuts2021, indic_sbs=LOC_NR'),
    -- Labour
    -- n_employed is shared with social_economy (lfsa_egan22d appears in both domains).
    -- The domain column marks 'labour' as the primary home; the social_economy loader
    -- also references this indicator_code. INSERT OR IGNORE ensures only one row.
    ('labour_force',
     'Forze di lavoro',
     'Labour force',
     'thousands_persons',
     'labour',
     'ISTAT FORZLVMENS1_1: employed + unemployed aged 15+, monthly raw data, national only'),
    ('employment_rate',
     'Tasso di occupazione',
     'Employment rate',
     'percentage',
     'labour',
     'ISTAT TAXOCCU1_5 (NUTS3); Eurostat lfst_r_lfe2emprtn (NUTS2 IT + NUTS0 EU)'),
    ('unemployment_rate',
     'Tasso di disoccupazione',
     'Unemployment rate',
     'percentage',
     'labour',
     'ISTAT TAXDISOCCU1_5/6/8; Eurostat lfst_r_lfu3rt / lfst_r_lfur2gan'),
    ('inactivity_rate',
     'Tasso di inattività',
     'Inactivity rate',
     'percentage',
     'labour',
     'ISTAT TAXINATT1_5 (NUTS3)'),
    ('neet_rate',
     'Quota NEET',
     'NEET rate',
     'percentage',
     'labour',
     'ISTAT NEET1_11 (NUTS2 regions); NEET1_9 (macro-areas, citizenship split). Ages 15-34.'),
    -- Placeholder rows for future domains — add detail when building each domain
    ('n_resident_foreign',
     'Popolazione straniera residente',
     'Resident foreign population',
     'count',
     'immigration', NULL),
    ('at_risk_of_poverty_rate',
     'Tasso di rischio povertà',
     'At-risk-of-poverty rate',
     'percentage',
     'poverty', NULL),
    ('social_expenditure_pct_gdp',
     'Spesa sociale % PIL',
     'Social expenditure as % of GDP',
     'percentage_gdp',
     'welfare', NULL)
;