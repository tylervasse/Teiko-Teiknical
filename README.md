# Teiko-Technical
Interactive Streamlit dashboard for analyzing immune cell population dynamics in mock clinical trial data, integrating SQLite backed queries, statistical testing, and visualization.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Repository Contents](#repository-contents)
- [How to Run the Project (GitHub Codespaces)](#how-to-run-the-project-github-codespaces)
  - [Install Python Dependencies](#install-python-dependencies)
  - [Build the SQLite Database (Part 1)](#build-the-sqlite-database-part-1)
  - [Launch the Dashboard (Parts 2–4)](#launch-the-dashboard-parts-2-4)
- [Database Schema Design and Scalability](#database-schema-design-and-scalability)
- [Code Structure Overview](#code-structure-overview)
- [Dashboard Features](#dashboard-features)
- [Link to the Dashboard](#link-to-the-dashboard)
- [Requirements](#requirements)

---

## Project Overview

The goal of this project is to analyze immune cell population data from clinical samples and present the results through an interactive dashboard. The workflow mirrors a real-world analytical pipeline:

1. Load and normalize raw CSV data into a relational SQLite database  
2. Query and aggregate data for analysis  
3. Visualize results through a client-facing dashboard  

---

## Repository Contents

The repository contains the following files:

## Repository Contents

The repository contains the following files:

- `cell-count.csv`  
  Original CSV file provided for the assignment

- `cell_counts.db`  
  Pre-built SQLite database generated from `cell-count.csv`  

- `db_creation.py`  
  Script for database schema creation and data loading  
  (optional: the database is already provided, but can be rebuilt if desired)

- `streamlit_dashboard.py`  
  Streamlit dashboard for Parts 2–4

- `requirements.txt`  
  Python dependencies required to run the project

---

## How to Run the Project (GitHub Codespaces)

These steps assume you are running the project in GitHub Codespaces.

### Install Python Dependencies

From the Codespaces terminal, run:

```bash
pip install -r requirements.txt
```

If you prefer to install dependencies manually:

```bash
pip install streamlit pandas numpy plotly scipy
```

---

### SQLite Database (Part 1)

A pre-built SQLite database (`cell_counts.db`) is included in the repository to ensure:

- Immediate execution in GitHub Codespaces
- Reproducible results
- Consistent SQL-based analytics

**You do not need to rebuild the database to run the dashboard.**

If you wish to regenerate the database from the original CSV for verification purposes, you may optionally run:

```bash
python db_creation.py
```

You should see console output reporting the number of projects, subjects, samples, and cell count records inserted. This serves as a basic sanity check that the database was built successfully.

---

### Launch the Dashboard (Parts 2–4)

Start the Streamlit application:

```bash
streamlit run streamlit_dashboard.py
```

GitHub Codespaces will detect the running server and prompt you to open the forwarded port. Open it in your browser to view the dashboard.

---

## Database Schema Design and Scalability

I designed the database using a normalized relational schema that separates core entities from sample specific details.

### Schema Overview

**projects**
Stores one row per clinical project.

* `project_id` (primary key)

**subjects**
Stores one row per subject or patient.

* `subject_id` (primary key)
* `project_id` (foreign key to `projects`)
* `condition`, `age`, `sex`, `response`

Subject-level metadata is stored once rather than repeated for every sample.

**treatments**
Lookup table for treatment names.

* `treatment_id` (primary key)
* `name` (unique)

**samples**
Stores one row per biological sample.

* `sample_id` (primary key)
* `subject_id` (foreign key to `subjects`)
* `treatment_id` (foreign key to `treatments`)
* `sample_type`, `time_from_treatment_start`

**cell_populations**
Lookup table for immune cell populations.

* `population_id` (primary key)
* `name` (unique)

**cell_counts**
Fact table storing observed counts.

* `sample_id` (foreign key to `samples`)
* `population_id` (foreign key to `cell_populations`)
* `count` (non-negative)

<img width="2117" height="1223" alt="image" src="https://github.com/user-attachments/assets/ce1d82c2-a1aa-4fdb-bee1-e3a4b687c954" />


### Rationale and Scalability

* Normalization prevents duplicated metadata and inconsistent values
* Foreign keys enforce referential integrity
* Long-format measurement storage supports aggregation and statistical analysis

This design scales well to hundreds of projects and thousands of samples. New immune populations or additional studies can be added without schema changes, and indexing supports efficient filtering by project, condition, response, treatment, timepoint, and population.

---

## Code Structure Overview

The project is split into two main components.

### `db_creation.py`

This script handles database creation and data loading. It defines the SQLite schema, validates the input CSV, inserts normalized data into relational tables, and performs basic sanity checks.

### `streamlit_dashboard.py`

This file contains the Streamlit dashboard. The dashboard is organized into three pages corresponding to Parts 2, 3, and 4 of the assignment. SQL is used for joins and aggregation, while Python handles presentation, statistics, and visualization.

Streamlit caching is used to avoid unnecessary recomputation, and custom HTML is used where precise table layout control is required.

---

## Dashboard Features

The dashboard contains three pages, navigable using the arrows in the top-left corner.

### Part 2: Data Overview

* Sidebar filters for subsetting data
* Paginated tables
* CSV export buttons

### Part 3: Statistical Analysis

* Defaults match assignment requirements (melanoma, PBMC, miraclib)
* Boxplots comparing responders and non-responders by immune population
* Statistical significance updates based on selected test and alpha level

### Part 4: Subset Analysis

* Defaults match assignment requirements (melanoma, PBMC, miraclib, time = 0)
* Summary tables showing counts by project, response, and sex
* Required question reports the average number of B cells for melanoma male responders at time 0, formatted to two decimals

---

## Link to the Dashboard

The dashboard is an internal Streamlit application intended to run in a controlled environment such as GitHub Codespaces.

After running:

```bash
streamlit run streamlit_dashboard.py
```

GitHub Codespaces will expose the application on a forwarded port. Opening that port in the browser provides access to the dashboard.

The URL will look similar to:

```text
https://<codespace-name>-8501.app.github.dev
```

This URL is generated dynamically by GitHub Codespaces and will change between sessions. 


