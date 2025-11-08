# CV19global
CV19global on OSF
Data available on the SARS2 project at: https://osf.io/kgxuf/; use cases: (A) https://osf.io/zvpqy, (B) https://osf.io/dcyn2, (C) https://osf.io/za6tc; **CV19global** at: https://osf.io/wxy4r. Tar-gzip archive: https://osf.io/7mcr3.

# COVID-19 Airflow Pipelines

This repository contains three **Apache Airflow DAGs** for building a reproducible research pipeline with COVID-19 and related datasets.  
The DAGs automate **downloading**, **processing**, and **loading** of large datasets into a Postgres database, making them queryable and ready for analysis.

## Overview of DAGs

### 1. `download.py`
- **Purpose**: Downloads raw datasets from public sources.
- **Datasets included**:
  - COVID-19 epidemiology (`owid-covid-data.csv`)
  - Vaccinations (`vaccinations.csv`)
  - Government response indices
  - Weather data
  - Mobility data
  - UV radiation data (converted from `.dat` to `.csv`)
- **Output**: Raw CSV files stored in `/home/smoutsis/data-V/`.

### 2. `upload.py`
- **Purpose**: Uploads processed CSVs into Postgres.
- **Behavior**:
  - Reads line by line to handle large files.
  - Creates or replaces tables in Postgres.
  - Uses an index column (`my_index`) for consistency.
- **Output**: Tables in Postgres (one per CSV).

### 3. `data.py`
- **Purpose**: Full end-to-end pipeline.
  - **Download** datasets (as in `download.py`).
  - **Join & process**: adds country codes, country names, harmonizes dates.
  - **Upload** processed results to Postgres.
- **Output**: Cleaned and harmonized datasets, both as CSVs (`/home/smoutsis/data-V2/`) and in Postgres.

## Requirements

- Python 3.7+
- Apache Airflow
- Python libraries:


- A running **Postgres database** (connection string configured in the scripts).

## Usage

1. Place the DAG files (`download.py`, `upload.py`, `data.py`) into your Airflow DAGs folder.
2. Update paths and Postgres connection details as needed:
 - Data directories (`/home/smoutsis/...`)
 - Postgres connection string in `upload.py` and `data.py`
3. Enable the DAGs in the Airflow web UI.
4. Trigger manually or let them run on the monthly schedule (`@monthly`).

## Notes for Researchers

- These DAGs are designed for **reproducibility** and **transparency**.
- Data sources are public and regularly updated.
- The pipeline overwrites old files with the latest versions.
- Chunked uploads allow very large CSVs to be ingested without memory errors.
- Use the DAGs separately if you only need one stage (e.g., just download or just upload), or use `data.py` for the **full workflow**.

## Author

**Stavros Moutsis**  
smoutsis@pme.duth.gr

**Christos Ouzounis**

cao@csd.auth.gr
