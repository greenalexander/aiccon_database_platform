-- database/schema/views.sql
--
-- Pre-built analytical views for PowerBI consumption.
-- One section per domain. All domains share the same file.
--
-- ── What views are for ────────────────────────────────────────────────────────
-- Raw fact tables are efficient for storage but awkward for PowerBI users
-- because they require joining multiple dimension tables and understanding
-- codes like 'ITH5' or 'ORGVOL'. Views do that work once, here in SQL,
-- so PowerBI connects to a clean flat table with human-readable columns.
--
-- ── Adding a new domain ───────────────────────────────────────────────────────
-- 1. Append a new section at the bottom following the pattern below
-- 2. Create at minimum:
--    a. One "base" view that joins the fact table to all dimension tables
--       and exposes human-readable labels (the pattern used in every domain)
--    b. One or two "summary" views for the most common query patterns
--       (e.g. national totals by year, regional map)
-- 3. Run build_db.py — it executes this file in full on every run
-- 4. In PowerBI: refresh the data source — new views appear automatically
--
-- ── Load order ────────────────────────────────────────────────────────────────
-- This file must be executed AFTER dimensions.sql and fact_tables.sql.
-- build_db.py handles the order.


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 1: SOCIAL ECONOMY
-- ════════════════════════════════════════════════════════════════════════════

-- ── 1a. Base view ─────────────────────────────────────────────────────────────
-- Full flat view of fact_social_economy joined to all dimension tables.
-- This is the primary table colleagues connect to in PowerBI.
-- All dimension codes are replaced with human-readable labels.
-- Column names are in English to work cleanly in PowerBI.

CREATE OR REPLACE VIEW vw_social_economy AS
SELECT
    -- Indicator
    di.indicator_code,
    di.label_it             AS indicator_label_it,
    di.label_en             AS indicator_label_en,

    -- Measure
    f.value,
    f.unit,

    -- Time
    dt.year,
    dt.reference_period,

    -- Geography
    dg.nuts_code,
    dg.nuts_level,
    dg.nuts_name_it,
    dg.nuts_name_en,
    dg.country_code,
    dg.region_name,
    dg.macro_area,
    dg.municipality_type,

    -- Legal form (null for survey/Eurostat rows — this is expected)
    dlf.unified_category        AS legal_form,
    dlf.unified_category_en     AS legal_form_en,
    dlf.ets_classification,

    -- NACE
    f.nace_code,
    f.nace_label_en,

    -- Demographics
    f.gender,
    f.age_group,

    -- Volunteering dimensions
    f.volunteering_form,
    f.activity_type,
    f.years_active,
    f.education,
    f.labour_status,
    f.household_size,
    f.econ_resources,

    -- Organised volunteering dimensions
    f.org_sector,
    f.org_type,
    f.motivation,
    f.personal_impact,
    f.multi_membership,

    -- Associationism dimensions
    f.association_type,

    -- Source
    ds.source_id,
    ds.provider,
    ds.source_name,
    ds.priority             AS source_priority,
    ds.access_type,

    -- Provenance
    f.dataset_code,
    f.extracted_at

FROM fact_social_economy f
LEFT JOIN dim_geography  dg  ON f.geo_key       = dg.geo_key
LEFT JOIN dim_time       dt  ON f.time_key       = dt.time_key
LEFT JOIN dim_source     ds  ON f.source_key     = ds.source_key
LEFT JOIN dim_indicator  di  ON f.indicator_key  = di.indicator_key
LEFT JOIN dim_legal_form dlf ON f.legal_form_key = dlf.legal_form_key
;


-- ── 1b. Volunteering rates — national trend ───────────────────────────────────
-- Simple time series of national volunteering rates for headline charts.
-- Filters to: Italy national level, total gender, organised + direct combined.
-- PowerBI can use this directly for a line chart without any further filtering.

CREATE OR REPLACE VIEW vw_se_volunteering_national AS
SELECT
    year,
    indicator_code,
    indicator_label_it,
    volunteering_form,
    gender,
    value,
    unit,
    source_id,
    dataset_code
FROM vw_social_economy
WHERE
    nuts_code    = 'IT'
    AND nuts_level = 0
    AND indicator_code IN ('volunteering_rate')
ORDER BY year, volunteering_form, gender
;


-- ── 1c. Volunteering rates — regional breakdown ───────────────────────────────
-- Regional volunteering rates for map visualisation in PowerBI.
-- One row per region per year — totals only (gender = 'total').
-- PowerBI map visuals connect to nuts_code or nuts_name_it.

CREATE OR REPLACE VIEW vw_se_volunteering_regional AS
SELECT
    year,
    nuts_code,
    nuts_name_it,
    nuts_name_en,
    macro_area,
    indicator_code,
    volunteering_form,
    value,
    unit,
    source_id,
    dataset_code
FROM vw_social_economy
WHERE
    nuts_level   = 2                        -- NUTS2 = regions
    AND country_code = 'IT'
    AND indicator_code = 'volunteering_rate'
    AND gender IN ('total', NULL)
ORDER BY year, nuts_code
;


-- ── 1d. Associationism — national trend ───────────────────────────────────────

CREATE OR REPLACE VIEW vw_se_associationism_national AS
SELECT
    year,
    indicator_code,
    indicator_label_it,
    association_type,
    age_group,
    gender,
    education,
    labour_status,
    value,
    unit,
    dataset_code
FROM vw_social_economy
WHERE
    nuts_level   = 0
    AND country_code = 'IT'
    AND indicator_code = 'association_membership_rate'
ORDER BY year, association_type, age_group
;


-- ── 1e. Eurostat employment — EU comparison ───────────────────────────────────
-- Employment in NACE O-U (regional) and Q/P/S94/S96 (national) across EU.
-- Useful for positioning Italy relative to other EU countries.

CREATE OR REPLACE VIEW vw_se_employment_eu AS
SELECT
    year,
    nuts_code,
    nuts_name_en,
    country_code,
    nuts_level,
    nace_code,
    nace_label_en,
    indicator_code,
    value,
    unit,
    dataset_code,
    source_id
FROM vw_social_economy
WHERE
    indicator_code = 'n_employed'
    AND provider  = 'eurostat'
ORDER BY year, country_code, nace_code
;


-- ── 1f. Local units — regional breakdown ──────────────────────────────────────
-- Number of local units by NACE and NUTS2 region (from sbs_r_nuts2021).
-- Used for organisational density maps.

CREATE OR REPLACE VIEW vw_se_local_units_regional AS
SELECT
    year,
    nuts_code,
    nuts_name_it,
    nuts_name_en,
    country_code,
    macro_area,
    nace_code,
    nace_label_en,
    value           AS n_local_units,
    dataset_code
FROM vw_social_economy
WHERE
    indicator_code = 'n_local_units'
    AND nuts_level = 2
ORDER BY year, nuts_code, nace_code
;


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 2: IMMIGRATION
-- Status: STUB
--
-- When building this domain, add at minimum:
--
-- a. Base view (vw_immigration) — same pattern as vw_social_economy above,
--    joining fact_immigration to all dimension tables. Add domain-specific
--    columns: nationality, permit_type, migration_flow, legal_status.
--
-- b. vw_immigration_by_region — resident foreign population by Italian region,
--    for map visualisation (nuts_level = 2, country_code = 'IT').
--
-- c. vw_immigration_eu_comparison — Eurostat migr_* data at NUTS0 level,
--    for positioning Italy within EU asylum/migration trends.
--
-- Example skeleton (uncomment and complete when ready):
--
-- CREATE OR REPLACE VIEW vw_immigration AS
-- SELECT
--     di.indicator_label_it, di.indicator_label_en,
--     f.value, f.unit,
--     dt.year,
--     dg.nuts_code, dg.nuts_name_it, dg.country_code, dg.macro_area,
--     f.nationality, f.permit_type, f.migration_flow,
--     f.gender, f.age_group,
--     ds.source_id, f.dataset_code, f.extracted_at
-- FROM fact_immigration f
-- LEFT JOIN dim_geography dg ON f.geo_key      = dg.geo_key
-- LEFT JOIN dim_time      dt ON f.time_key      = dt.time_key
-- LEFT JOIN dim_source    ds ON f.source_key    = ds.source_key
-- LEFT JOIN dim_indicator di ON f.indicator_key = di.indicator_key
-- ;
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 3: WELFARE AND SOCIAL POLICIES
-- Status: STUB
--
-- When building this domain, add at minimum:
--
-- a. vw_welfare — base view joining fact_welfare to all dimensions.
--    Add: service_type, beneficiary_group, expenditure_type.
--
-- b. vw_welfare_spending_eu — social expenditure as % GDP across EU countries
--    for international comparison (nuts_level = 0).
--
-- c. vw_welfare_services_regional — regional breakdown of social service
--    indicators for Italian NUTS2 map visualisation.
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 4: POVERTY AND INEQUALITY
-- Status: STUB
--
-- When building this domain, add at minimum:
--
-- a. vw_poverty — base view joining fact_poverty to all dimensions.
--    Add: population_group, deprivation_type, income_quintile.
--
-- b. vw_poverty_atrisk_regional — at-risk-of-poverty rate by Italian region
--    (nuts_level = 2) for map visualisation. Key indicator for AICCON projects.
--
-- c. vw_poverty_eu_comparison — EU-SILC indicators at NUTS0 for EU comparison.
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 5: LABOUR
-- ════════════════════════════════════════════════════════════════════════════

-- ── 5a. Base view ─────────────────────────────────────────────────────────────
-- Full flat view of fact_labour joined to all dimension tables.
-- Primary connection point for PowerBI. All surrogate keys replaced with
-- human-readable labels. Domain-specific columns passed through as-is.

CREATE OR REPLACE VIEW vw_labour AS
SELECT
    -- Indicator
    di.indicator_code,
    di.label_it             AS indicator_label_it,
    di.label_en             AS indicator_label_en,

    -- Measure
    f.value,
    f.unit,

    -- Time
    dt.year,
    f.frequency,
    f.period_label,

    -- Geography
    dg.nuts_code,
    dg.nuts_level,
    dg.nuts_name_it,
    dg.nuts_name_en,
    dg.country_code,
    dg.region_name,
    dg.macro_area,

    -- Demographics
    f.gender,
    f.age_group,

    -- Labour dimensions
    f.education,
    f.citizenship,
    f.adjustment,
    f.unemployment_duration,

    -- NACE (lfsa_egan22d only; NULL for rate/NEET rows)
    f.nace_code,
    f.nace_label_en,

    -- Source
    ds.source_id,
    ds.provider,
    ds.source_name,
    ds.priority             AS source_priority,

    -- Provenance
    f.dataset_code,
    f.extracted_at

FROM fact_labour f
LEFT JOIN dim_geography  dg  ON f.geo_key      = dg.geo_key
LEFT JOIN dim_time       dt  ON f.time_key      = dt.time_key
LEFT JOIN dim_source     ds  ON f.source_key    = ds.source_key
LEFT JOIN dim_indicator  di  ON f.indicator_key = di.indicator_key
;


-- ── 5b. Key rates — Italy national trend ─────────────────────────────────────
-- Employment, unemployment and inactivity rates for Italy at national level.
-- Annual only. All gender and age combinations preserved so PowerBI slicers
-- can filter interactively. Simple line-chart input for dashboard tiles.

CREATE OR REPLACE VIEW vw_labour_rates_italy AS
SELECT
    year,
    indicator_code,
    indicator_label_it,
    indicator_label_en,
    age_group,
    gender,
    value,
    unit,
    source_id,
    dataset_code
FROM vw_labour
WHERE
    nuts_code      = 'IT'
    AND frequency  = 'A'
    AND indicator_code IN ('employment_rate', 'unemployment_rate', 'inactivity_rate')
ORDER BY indicator_code, age_group, gender, year
;


-- ── 5c. Key rates — Italian territories map ───────────────────────────────────
-- Employment and unemployment rates at NUTS2 (region) and NUTS3 (province)
-- for map visualisation. Covers both ISTAT province-level data and Eurostat
-- NUTS2 data for Italy. All gender and age combinations kept for slicer use.

CREATE OR REPLACE VIEW vw_labour_rates_regional AS
SELECT
    year,
    nuts_code,
    nuts_name_it,
    nuts_name_en,
    macro_area,
    nuts_level,
    indicator_code,
    indicator_label_it,
    age_group,
    gender,
    value,
    unit,
    source_id,
    dataset_code
FROM vw_labour
WHERE
    country_code   = 'IT'
    AND nuts_level IN (2, 3)
    AND frequency  = 'A'
    AND indicator_code IN ('employment_rate', 'unemployment_rate', 'inactivity_rate')
ORDER BY indicator_code, year, nuts_code
;


-- ── 5d. NEET rate ─────────────────────────────────────────────────────────────
-- NEET rate for 15-34 year-olds. Combines:
--   - Italian NUTS2 regional breakdown (ISTAT NEET1_11)
--   - Macro-area breakdown by citizenship (ISTAT NEET1_9: ITL/FRG/TOTAL)
--   - EU country comparison is not separately sourced here; add Eurostat
--     lfst_r_neet if needed.
-- Citizenship is NULL for the regional series and populated for the
-- citizenship-split series — PowerBI can filter on IS NOT NULL to separate them.

CREATE OR REPLACE VIEW vw_labour_neet AS
SELECT
    year,
    nuts_code,
    nuts_name_it,
    nuts_name_en,
    country_code,
    nuts_level,
    macro_area,
    gender,
    age_group,
    citizenship,
    value,
    unit,
    source_id,
    dataset_code
FROM vw_labour
WHERE
    indicator_code = 'neet_rate'
    AND frequency  = 'A'
ORDER BY year, nuts_code, gender, age_group, citizenship
;


-- ── 5e. Employment by NACE — EU comparison ───────────────────────────────────
-- Employed persons by economic activity (lfsa_egan22d) at national level
-- across all EU countries. Useful for positioning Italy's sector employment
-- structure relative to EU peers and for social economy sector analysis
-- (P85 Education, Q86-88 Health/Social work, S94 Membership organisations).

CREATE OR REPLACE VIEW vw_labour_employment_by_nace AS
SELECT
    year,
    nuts_code,
    nuts_name_en,
    country_code,
    nace_code,
    nace_label_en,
    gender,
    age_group,
    value           AS employed_thousands,
    unit,
    dataset_code
FROM vw_labour
WHERE
    indicator_code = 'n_employed'
    AND nuts_level = 0
ORDER BY year, country_code, nace_code
;


-- ── 5f. Unemployment by education — Italy and EU ─────────────────────────────
-- Unemployment rate by ISCED education level (Eurostat lfst_r_lfu3rt and
-- ISTAT TAXDISOCCU1_6). Covers Italian NUTS2 regions and all EU national totals.
-- Key view for AICCON research on skills mismatch and labour market access gaps.

CREATE OR REPLACE VIEW vw_labour_unemployment_by_education AS
SELECT
    year,
    nuts_code,
    nuts_name_en,
    country_code,
    nuts_level,
    macro_area,
    gender,
    age_group,
    education,
    value           AS unemployment_rate,
    unit,
    source_id,
    dataset_code
FROM vw_labour
WHERE
    indicator_code = 'unemployment_rate'
    AND education  IS NOT NULL
    AND frequency  = 'A'
ORDER BY year, nuts_code, education, gender
;


-- ── 5g. Employment rate by education and citizenship — Italy and EU ───────────
-- Employment rate from Eurostat lfst_r_lfe2emprtn: education × citizenship cross.
-- Covers Italian NUTS2 regions and all EU national totals. Age not included
-- (omitted at fetch time; returns total working-age aggregate).
-- Key view for analysing labour market access gaps by qualification and origin.

CREATE OR REPLACE VIEW vw_labour_employment_edu_citizenship AS
SELECT
    year,
    nuts_code,
    nuts_name_en,
    country_code,
    nuts_level,
    macro_area,
    gender,
    education,
    citizenship,
    value           AS employment_rate,
    unit,
    dataset_code
FROM vw_labour
WHERE
    indicator_code  = 'employment_rate'
    AND education   IS NOT NULL
    AND citizenship IS NOT NULL
    AND frequency   = 'A'
ORDER BY year, nuts_code, education, citizenship
;


-- ── 5h. Labour force monthly — Italy ─────────────────────────────────────────
-- Monthly labour force headcount (thousands) from ISTAT FORZLVMENS1_1.
-- Raw (non-seasonally-adjusted) series, national level only.
-- period_label is 'YYYY-MM' — use as the x-axis for time-series charts.

CREATE OR REPLACE VIEW vw_labour_force_monthly AS
SELECT
    year,
    period_label,
    gender,
    age_group,
    value           AS labour_force_thousands,
    dataset_code
FROM vw_labour
WHERE
    indicator_code = 'labour_force'
    AND frequency  = 'M'
    AND adjustment = 'N'
    AND nuts_code  = 'IT'
ORDER BY period_label, gender, age_group
;


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 6: SUSTAINABLE DEVELOPMENT (SDGs)
-- Status: STUB
--
-- When building this domain, add at minimum:
--
-- a. vw_sdg — base view. Add: sdg_goal, sdg_target, sdg_indicator_code.
--
-- b. vw_sdg_italy_progress — Italy's progress on all 17 goals over time,
--    for a dashboard overview. Filter to nuts_level = 0, country_code = 'IT'.
--
-- c. vw_sdg_eu_comparison — Italy vs EU average for selected SDG indicators.
--
-- Tip: SDG indicators map to many different source datasets. The sdg_target
-- column is the most useful filter dimension — colleagues will want to see
-- "all indicators for SDG 3 (Good Health)" in one view.
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 7: CIVIL SOCIETY AND SOCIAL IMPACT
-- Status: STUB
--
-- When building this domain, add at minimum:
--
-- a. vw_civil_society — base view. Add: org_type, impact_area.
--
-- b. vw_social_capital_regional — trust and civic engagement indicators
--    by Italian region. Often combined with volunteering data from
--    social_economy — consider a cross-domain view (see below).
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- DOMAIN 8: HOUSING AND URBAN WELFARE
-- Status: STUB
--
-- When building this domain, add at minimum:
--
-- a. vw_housing — base view. Add: tenure_type, housing_condition.
--
-- b. vw_housing_affordability_regional — housing cost overburden rate
--    by Italian region for map visualisation.
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- CROSS-DOMAIN VIEWS
-- These join data from multiple fact tables and are only possible once
-- both relevant domains have been built and loaded.
-- Add them here as the platform is built out.
-- ════════════════════════════════════════════════════════════════════════════

-- Example: volunteering rate alongside poverty rate by Italian region.
-- Useful for AICCON projects exploring the relationship between civic
-- engagement and social vulnerability.
-- Uncomment when both social_economy and poverty domains are active.
--
-- CREATE OR REPLACE VIEW vw_social_economy_x_poverty AS
-- SELECT
--     se.year,
--     se.nuts_code,
--     se.nuts_name_it,
--     se.macro_area,
--     se.value        AS volunteering_rate,
--     po.value        AS poverty_rate
-- FROM vw_se_volunteering_regional se
-- JOIN (
--     SELECT year, nuts_code, value
--     FROM vw_poverty_atrisk_regional
--     WHERE indicator_code = 'at_risk_of_poverty_rate'
--       AND gender = 'total'
-- ) po ON se.nuts_code = po.nuts_code AND se.year = po.year
-- WHERE se.volunteering_form = 'total'
-- ;