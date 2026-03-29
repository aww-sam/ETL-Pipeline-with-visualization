# ETL Pipeline with Visualization

A **Streamlit** web app for tabular data: upload CSV, Parquet, JSON, or Excel files, run a configurable **ETL** pipeline, then explore the data with **Seaborn/Matplotlib** charts on a separate page.

## Features

- **ETL Pipeline** (`ETLPipeline.py`): Upload data, tune imputation (numeric/categorical), null-column dropping, duplicate removal, outlier handling (remove or clip via IQR / sigma / percentiles), scaling (Standard or MinMax), encoding (label or one-hot), skew correction, and optional column exclusions. Download the processed table as CSV, Parquet, or JSON.
- **Visualization** (`pages/Visualise.py`): Build histograms, KDE, box/violin plots, scatter/line/regression plots, bar/count plots, heatmaps, pair plots, and facet grids from uploaded files.
- **Notebook** (`etl.ipynb`): Jupyter workflow around the same ETL logic in `etl.py`.

## Requirements

- Python 3.10+ recommended  
- Core libraries: `streamlit`, `pandas`, `numpy`, `scikit-learn`, `openpyxl` (for `.xlsx`), `matplotlib`, `seaborn`

The bundled `requirements.txt` is a full environment export (conda-style paths). For a fresh virtual environment you can install the essentials with:

```bash
pip install streamlit pandas numpy scikit-learn openpyxl matplotlib seaborn pyarrow
```

Add `jupyter` / `ipykernel` if you use `etl.ipynb`.

## Setup

1. Clone or copy this folder and open a terminal inside it.

2. Create and activate a virtual environment (optional but recommended):

   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. Install dependencies (see above).

## Run the app

From this directory (`ETL-Pipeline-with-visualization`):

```bash
streamlit run ETLPipeline.py
```

The browser opens the **ETL Pipeline** home page. Use the sidebar to open **Visualise** (Streamlit multipage: `pages/Visualise.py`).

Running the pipeline writes `etl.csv` in the working directory when the default load format is CSV (see `etl.py`).

## Project layout

| Path | Role |
|------|------|
| `ETLPipeline.py` | Main Streamlit app — upload, ETL controls, download |
| `etl.py` | `ETL_pipeline` class — extract, transform, load |
| `pages/Visualise.py` | Visualization page |
| `etl.ipynb` | Notebook exploration |

## License

No license file is included; add one if you distribute the project.
