# Maintenance & Expansion Guide

## How to add a new Domain (e.g., Welfare)

To add a new thematic area to the platform, follow these 4 steps:

### 1. Update `settings.yaml`
Add the new domain under the `domains:` key and define its API datasets:
```yaml
domains:
  welfare:
    enabled: true

eurostat:
  welfare:
    - dataset: "spr_exp_sum"
      label: "Social Protection Expenditure"
```

### 2. Create the Loaders 
Create new scripts in `ingestion/api_sources/welfare/`.
Inherit from `BaseLoader(domain="welfare")`. This ensures the raw files go into `raw/welfare/`.

### 3. Create new domain_merge file


### 4. Update the Pipeline 
In `processing/pipeline.py`, add a `process_welfare()` method.
- Use the `NutsMapper` to clean geography.
- Save the result as `processed/fact_welfare.parquet`.

### 5. Update the Database Schema
- Fact Table: Add `database/schema/fact_welfare.sql`.
- View: Add a join in `database/schema/views.sql` to create `v_welfare_comprehensive`.
- Build Script: Add the new SQL files to `database/build_db.py`.

### 6. Update pipeline scripts
- domain_sources.csv, nace_labels.csv
- processing/pipeline.py
- run_pipeline.py

### 7. Update integrity test for new domain

### 8. Update docs
- Data_dictionary, decisions, source_log
- ReadMe


## Troubleshooting 
- API Errors: Check `pipeline_log.json`. If ISTAT is down, wait 1 hour and run `update_database.bat` again.
- Mapping Errors: If a NUTS code shows as "Unknown" in PowerBI, add the missing row to `processing/mappings/nuts_istat.csv`.
