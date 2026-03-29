# ETL Pipeline with Visualization

A **Streamlit** web app for tabular data: upload CSV, Parquet, JSON, or Excel files, run a configurable **ETL** pipeline, then explore the data with **Seaborn/Matplotlib** charts on a separate page.

## Features

- **ETL Pipeline** (`ETLPipeline.py`): Upload data, tune imputation (numeric/categorical), null-column dropping, duplicate removal, outlier handling (remove or clip via IQR / sigma / percentiles), scaling (Standard or MinMax), encoding (label or one-hot), skew correction, and optional column exclusions. Download the processed table as CSV, Parquet, or JSON.
- **Scheduler** (`ETLPipeline.py`): Automatically re-run the pipeline on a configurable interval (minutes / hours / days) using APScheduler.
- **Visualization** (`pages/Visualise.py`): Build histograms, KDE, box/violin plots, scatter/line/regression plots, bar/count plots, heatmaps, pair plots, and facet grids from uploaded files. Export any chart as a multi-page PDF report.
- **Notebook** (`etl.ipynb`): Jupyter workflow around the same ETL logic in `etl.py`.

## Requirements

- Python 3.10+ recommended  
- Core libraries: `streamlit`, `pandas`, `numpy`, `scikit-learn`, `openpyxl` (for `.xlsx`), `matplotlib`, `seaborn`, `apscheduler`, `pyarrow`

Install all dependencies at once:

```bash
pip install -r requirements.txt
```

Or manually install the essentials:

```bash
pip install streamlit pandas numpy scikit-learn openpyxl matplotlib seaborn pyarrow apscheduler
```

> **Note:** `apscheduler` is required for the scheduler feature. If it is missing, the scheduler section will show a warning and be disabled — the rest of the pipeline will still work.

Add `jupyter` / `ipykernel` if you use `etl.ipynb`.

## Setup

1. Clone or copy this folder and open a terminal inside it.

2. Create and activate a virtual environment (optional but recommended):

   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Run the app

From this directory (`ETL-Pipeline-with-visualization`):

```bash
streamlit run ETLPipeline.py
```

The browser opens the **ETL Pipeline** home page. Use the sidebar to open **Visualise** (Streamlit multipage: `pages/Visualise.py`).

Running the pipeline writes `etl.csv` (or `etl.parquet` / `etl.json` depending on the load format) and `pipeline.db` (SQLite) in the working directory.

## Transform step order

Inside `etl.py` the transform stage runs in this order:

1. Drop high-null columns
2. Fill numeric missing values
3. Fill categorical missing values
4. Remove duplicates
5. Remove or clip outliers
6. Fix skew (`log1p` on positively skewed numeric columns)
7. Scale features (Standard or MinMax)
8. Encode categorical columns
9. Extract datetime features

Skew correction runs **before** scaling so that the scaler operates on already-normalised distributions.

## Project layout

| Path | Role |
|------|------|
| `ETLPipeline.py` | Main Streamlit app — upload, ETL controls, scheduler, download |
| `etl.py` | `ETL_pipeline` class — extract, transform, load |
| `pages/Visualise.py` | Visualization page (must live in `pages/` for Streamlit multipage routing) |
| `etl.ipynb` | Notebook exploration |
| `requirements.txt` | Python dependencies |
| `pipeline.log` | Auto-generated log file (created on first pipeline run) |
| `pipeline.db` | Auto-generated SQLite database (created on first pipeline run) |

## License

No license file is included; add one if you distribute the project.
