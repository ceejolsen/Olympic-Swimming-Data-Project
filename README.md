# Olympic-Swimming-Data-Project
# 🏊 Olympic Swimming Data Project

Predicting final times in elite 400m freestyle races from early split data.

An end-to-end data pipeline that scrapes official OmegaTiming result PDFs, parses raw timing data, stores it in SQLite, and trains regression models to predict race outcomes from the first 250m of splits.

> **Final Report:** See `Project_Final_Report.pdf`  
> **Notebook:** See `Project.ipynb`

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [How to Run](#how-to-run)
- [Module Reference](#module-reference)
- [Dataset](#dataset)
- [Results](#results)

---

## Overview

This project investigates how early in a 400m freestyle race the final outcome becomes statistically predictable. Using official OmegaTiming PDFs from 150+ elite competitions (2010–2026), we:

1. **Scrape** PDF links from OmegaTiming via Selenium
2. **Parse** split times and swimmer data from PDFs using pdfplumber + regex
3. **Store** cleaned data in SQLite
4. **Train** regression models (Linear, Polynomial, Random Forest, Gradient Boosting)
5. **Analyze** pacing dynamics and feature importance

**Key result:** Polynomial Regression (Degree 2) predicts final times with a cross-validated MAE of **~1.01 seconds** using only the first 250m of splits.

---

## Project Structure

```
.
├── web_scrape.py        # Selenium scraper — collects PDF links from OmegaTiming
├── pdf.py               # Pipeline coordinator — parallel PDF scraping & parsing
├── single_pdf.py        # Core PDF parser — regex-based swimmer/split extraction
├── pdf_debug.py         # Debugging utilities — inspect raw PDF text, test parser
├── ML.py                # Machine learning — model training, evaluation, experiments
├── analysis.py          # Visualization — plots for results, features, residuals
├── sql_export.py        # SQLite export — stores data and runs analytical queries
├── Project.ipynb        # Main notebook — runs the full pipeline end-to-end
├── data/
│   ├── omega_pdfs.csv       # Scraped PDF links (output of web_scrape.py)
│   ├── scraped_results.csv  # Parsed race data (output of pdf.py)
│   └── swim_results.db      # SQLite database (output of sql_export.py)
└── Project_Final_Report.pdf
```

---

## Installation

### Requirements

- Python 3.10+
- Google Chrome (for Selenium scraping)
- ChromeDriver matching your Chrome version

### Install Python dependencies

```bash
pip install -r requirements.txt
```

If you don't have a `requirements.txt`, install manually:

```bash
pip install pandas numpy scikit-learn matplotlib pdfplumber requests \
            selenium tqdm curl_cffi
```

### ChromeDriver setup

The web scraper uses a headless Chrome browser. Make sure ChromeDriver is installed and on your PATH:

```bash
# macOS (Homebrew)
brew install chromedriver

# Or download from https://chromedriver.chromium.org/downloads
# Match the version to your installed Chrome. DOUBLE CHECK THIS
```

---

## How to Run

### Option 1 — Run everything via the notebook (recommended)

Open `Project.ipynb` and run cells top to bottom. The notebook calls functions from the `.py` modules and walks through the full pipeline with explanations.

```bash
jupyter notebook Project.ipynb
```

### Option 2 — Run pipeline steps individually

#### Step 1: Scrape PDF links from OmegaTiming

```python
from web_scrape import get_csv

# Scrape competitions from 2010 through 2024
get_csv(start=2010, end=2024)
# Output: data/omega_pdfs.csv
```

#### Step 2: Parse PDFs into race data

```python
import pandas as pd
from pdf import get_links_df, scrape_omega

links_df = pd.read_csv("data/omega_pdfs.csv")
mens_links, womens_links = get_links_df(links_df)

all_links = mens_links + womens_links
df = scrape_omega(all_links, output_file="data/scraped_results.csv", max_workers=4)
# Output: data/scraped_results.csv
# Supports resume — already-processed links are skipped automatically
```

#### Step 3: Export to SQLite

```python
from sql_export import prepare_for_sql, export_to_sqlite, create_indexes

men_df = pd.read_csv("data/scraped_results.csv")   # filter by your logic
women_df = ...

combined = prepare_for_sql(men_df, women_df)
export_to_sqlite(combined)
create_indexes()
# Output: data/swim_results.db
```

#### Step 4: Train the model

```python
from ML import train_model

model, mae, importances = train_model("data/men.csv", "data/women.csv")
print(f"MAE: {mae:.2f}s")
```

#### Step 5: Predict a single swimmer

```python
from ML import predict

predicted = predict(model, {
    "split_50m": 26.69,
    "split_100m": 55.38,
    "split_150m": 83.74,
    "split_200m": 112.53,
    "split_250m": 140.97,
    "reaction_time": 0.73,
})
print(f"Predicted final time: {predicted}s")
```

#### Step 6: Run experiments and visualizations

```python
from ML import compare_models_cv, progressive_accuracy_experiment
from analysis import plot_progressive_accuracy, plot_model_comparison, plot_feature_importance

# Compare all models with cross-validation
results_df = compare_models_cv("data/men.csv", "data/women.csv")
plot_model_comparison(results_df)

# Progressive accuracy experiment
prog_df = progressive_accuracy_experiment("data/men.csv", "data/women.csv")
plot_progressive_accuracy(prog_df)

# Feature importance
plot_feature_importance(importances)
```

---

## Module Reference

### `web_scrape.py`
Selenium-based scraper for the OmegaTiming portal.

| Function | Description |
|---|---|
| `get_csv(start, end)` | Scrape PDF links for a range of years and save to `data/omega_pdfs.csv` |

### `pdf.py`
Coordinates parallel PDF downloading and parsing.

| Function | Description |
|---|---|
| `scrape_omega(links, output_file, max_workers)` | Parse a list of PDF URLs into a DataFrame with resume support |
| `get_links_df(df)` | Extract men's and women's PDF link lists from the links CSV |
| `time_to_seconds(val)` | Convert `"M:SS.ss"` string to float seconds |

### `single_pdf.py`
Core PDF parsing logic.

| Function | Description |
|---|---|
| `parse_pdf(source)` | Parse one PDF URL or file path into a DataFrame |
| `process_single_link(link)` | Wrapper for `parse_pdf` — returns list of dicts for use with multiprocessing |
| `parse_splits(line)` | Extract cumulative split times from a splits line |
| `split_name(raw)` | Split `"LAST Firstname"` into `(last_name, first_name)` |

### `ML.py`
All machine learning functionality.

| Function | Description |
|---|---|
| `prepare_data(men_input, women_input)` | Load, clean, and feature-engineer the combined dataset |
| `train_model(men_input, women_input)` | Train a Random Forest and return `(model, mae, importances)` |
| `predict(model, splits)` | Predict final time for one swimmer from a dict of split times |
| `predict_batch(model, df)` | Run predictions across a full DataFrame; adds `predicted_time` and `residual` columns |
| `compare_models(men, women)` | Train/test split comparison of Linear, Polynomial, and Random Forest |
| `compare_models_cv(men, women)` | 5-fold cross-validated comparison of all four models |
| `progressive_accuracy_experiment(men, women)` | Train 5 models with increasingly more splits; returns MAE at each stage |

### `analysis.py`
Visualization functions (all produce matplotlib plots).

| Function | Description |
|---|---|
| `plot_progressive_accuracy(prog_df)` | MAE vs. split distance used |
| `plot_feature_importance(importances)` | Horizontal bar chart of feature importances |
| `plot_residual_distribution(df_results)` | Histogram of predicted − actual residuals |
| `plot_split_correlations(df_prepared)` | Correlation matrix heatmap |
| `plot_model_comparison(results_df)` | Bar chart comparing model MAEs |
| `print_biggest_surprises(df_results)` | Print top N swimmers who outperformed predictions |

### `sql_export.py`
SQLite storage and querying.

| Function | Description |
|---|---|
| `prepare_for_sql(men_df, women_df)` | Combine and convert data; adds `sex` column |
| `export_to_sqlite(df)` | Write DataFrame to `swim_results.db` |
| `create_indexes()` | Add indexes on `sex`, `final_time`, `last_name`, `Link` |
| `run_sample_queries()` | Print results of 7 analytical SQL queries |
| `query_to_df(query)` | Run any SQL query and return a DataFrame |

### `pdf_debug.py`
Developer utilities for inspecting and debugging the parser.

| Function | Description |
|---|---|
| `peek(link, n)` | Print first `n` characters of extracted PDF text |
| `inspect_lines(link, n)` | Print numbered lines for regex debugging |
| `test_parser(link)` | Run the full parser on a single PDF and print results |
| `test_swimmer_regex(link, pattern)` | Test a regex pattern against extracted lines |

---

## Dataset

The final cleaned dataset contains **3,074 swimmer entries** from **150+ elite competitions** (2010–2026), with the following columns:

| Column | Type | Description |
|---|---|---|
| `heat` | str | `"Final"`, `"Final A"`, `"Final B"`, `"Final C"` |
| `rank` | int | Finishing rank within heat |
| `lane` | int | Lane number |
| `last_name` | str | Athlete last name |
| `first_name` | str | Athlete first name |
| `reaction_time` | float | Reaction time in seconds (NaN if not recorded) |
| `split_50m` … `split_250m` | float | Cumulative split times in seconds |
| `split_300m`, `split_350m` | float | Late splits (often missing) |
| `final_time` | float | Final race time in seconds |
| `sex` | str | `"M"` or `"F"` (added by `sql_export.py`) |
| `Link` | str | Source PDF URL |

---

## Results

| Model | CV MAE (seconds) |
|---|---|
| **Polynomial Regression (Deg. 2)** | **1.010** |
| Linear Regression | 1.032 |
| Gradient Boosting | 1.068 |
| Random Forest | 1.089 |

**Progressive accuracy** (Random Forest, MAE by furthest split used):

| Furthest Split | MAE (s) |
|---|---|
| 50m | 3.95 |
| 100m | 2.41 |
| 150m | 1.62 |
| 200m | 1.18 |
| 250m | 0.97 |

The race is effectively decided by the **200m–250m mark**. The `split_250m` feature alone carries an importance score of **0.931** in the Random Forest model.

---

## Authors

**Mekael Yesfa & Christopher Olsen** — UCLA PIC 16B, Winter 2025

GitHub: [github.com/ceejolsen/Olympic-Swimming-Data-Project](https://github.com/ceejolsen/Olympic-Swimming-Data-Project)