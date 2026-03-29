import streamlit as st
import pandas as pd
import os
import sys
import io
import sqlite3
import logging
from datetime import datetime

sys.path.append(os.path.abspath(".."))
from etl import ETL_pipeline

# ── APScheduler ────────────────────────────────────────────────────────────────
APSCHEDULER_AVAILABLE = True
try:
    from apscheduler.schedulers.background import BackgroundScheduler
except Exception:
    APSCHEDULER_AVAILABLE = False
    BackgroundScheduler = None

if APSCHEDULER_AVAILABLE and 'scheduler' not in st.session_state:
    st.session_state['scheduler'] = BackgroundScheduler()
    st.session_state['scheduler'].start()

if 'schedule_active' not in st.session_state:
    st.session_state['schedule_active'] = False

if 'last_scheduled_run' not in st.session_state:
    st.session_state['last_scheduled_run'] = None


def run_scheduled_pipeline():
    """Called by APScheduler in the background."""
    if 'pipeline_config' not in st.session_state:
        return
    cfg = st.session_state['pipeline_config']
    try:
        e = ETL_pipeline(**cfg['params'])
        st.session_state['df'] = e.pipeline(cfg['file_path'])
        st.session_state['last_scheduled_run'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception as ex:
        logging.getLogger(__name__).error(f"Scheduled run failed: {ex}")


# ── UI ─────────────────────────────────────────────────────────────────────────
st.title("ETL Pipeline")
file = st.file_uploader("Upload file", type=['csv', 'parquet', 'json', 'xlsx'])

if file:
    file_extension = file.name.split('.')[-1]

    if file_extension == 'csv':
        df_raw = pd.read_csv(file)
    elif file_extension == 'parquet':
        df_raw = pd.read_parquet(file)
    elif file_extension == 'json':
        df_raw = pd.read_json(file)
    elif file_extension == 'xlsx':
        df_raw = pd.read_excel(file)

    st.session_state['df_raw'] = df_raw

    with st.expander("PEEK RAW DATASET"):
        st.dataframe(df_raw)

    clip         = 'iqr'
    sclip_thresh = 3

    numeric_fill = st.sidebar.selectbox("Numerical Fill", ['mean', 'median', 'custom'])
    if numeric_fill == 'custom':
        numeric_fill = st.sidebar.number_input("Enter a number", value=0.0)

    categorical_fill = st.sidebar.selectbox("Categorical Fill", ['mode', 'custom'])
    if categorical_fill == 'custom':
        categorical_fill = st.sidebar.text_input("Enter a value", placeholder='"Text or Number"')

    null_thresh     = st.sidebar.number_input("Null Threshold (default=0.5)", value=0.5)
    z_thresh        = st.sidebar.number_input("Z Threshold (default=3.29)",   value=3.29)
    skew_thresh     = st.sidebar.number_input("Skew Threshold (default=1)",   value=1)
    remove_outliers = st.sidebar.checkbox("Remove Outliers", value=True)

    if not remove_outliers:
        clip = st.sidebar.selectbox("Outlier Clipper", ['iqr', 'sigma', 'percentile'])
        if clip == 'sigma':
            sclip_thresh = st.sidebar.number_input("Sigma Value (default=3)", value=3)

    scaler         = st.sidebar.selectbox("Scaler",  ['standard', 'minmax'])
    encoder        = st.sidebar.selectbox("Encoder", ['label', 'onehot'])
    scale_exclude  = st.sidebar.multiselect("Exclude from Scaling",  options=df_raw.columns.tolist())
    encode_exclude = st.sidebar.multiselect("Exclude from Encoding", options=df_raw.columns.tolist())

    st.sidebar.markdown("---")
    st.sidebar.markdown("**SQLite Storage**")
    db_table = st.sidebar.text_input("Table name", value="processed_data")

    # ── Manual run ─────────────────────────────────────────────────────────────
    if st.button("Execute Pipeline"):
        with open(file.name, 'wb') as f:
            f.write(file.getbuffer())

        params = dict(
            numeric_fill=numeric_fill,
            categorical_fill=categorical_fill,
            z_thresh=z_thresh,
            null_thresh=null_thresh,
            skew_thresh=skew_thresh,
            remove_outliers=remove_outliers,
            clip=clip,
            sclip_thresh=sclip_thresh,
            scaler=scaler,
            encoder=encoder,
            scale_exclude=scale_exclude,
            encode_exclude=encode_exclude,
            db_table=db_table,
        )
        st.session_state['pipeline_config'] = {'params': params, 'file_path': file.name}

        e = ETL_pipeline(**params)
        st.session_state['df'] = e.pipeline(file.name)
        st.success('Pipeline executed')

    # ── Scheduler ──────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Scheduler")

    col1, col2 = st.columns(2)
    with col1:
        interval_unit    = st.selectbox("Run every", ['minutes', 'hours', 'days'])
    with col2:
        interval_value   = st.number_input("Interval", min_value=1, value=1)

    if not APSCHEDULER_AVAILABLE:
        st.warning(
            "Scheduler unavailable: APScheduler could not be imported. "
            "Install it with `pip install apscheduler` and restart Streamlit."
        )
    else:
        sched = st.session_state['scheduler']

        if st.button("Start Scheduler"):
            if not st.session_state['schedule_active']:
                sched.add_job(
                    run_scheduled_pipeline,
                    trigger='interval',
                    **{interval_unit: interval_value},
                    id='etl_job',
                    replace_existing=True
                )
                st.session_state['schedule_active'] = True
                st.success(f"Scheduler started — runs every {interval_value} {interval_unit}")
            else:
                st.info("Scheduler is already running")

        if st.button("Stop Scheduler"):
            if st.session_state['schedule_active']:
                sched.remove_job('etl_job')
                st.session_state['schedule_active'] = False
                st.success("Scheduler stopped")

    status = "Running" if st.session_state['schedule_active'] else "Stopped"
    st.caption(f"Status: {status}")
    if st.session_state['last_scheduled_run']:
        st.caption(f"Last scheduled run: {st.session_state['last_scheduled_run']}")

    # ── Results ────────────────────────────────────────────────────────────────
    if 'df' in st.session_state:
        st.markdown("---")
        with st.expander("PEEK PROCESSED DATASET"):
            st.dataframe(st.session_state['df'])

        df = st.session_state['df']

        # SQLite info
        if os.path.exists('pipeline.db'):
            conn = sqlite3.connect('pipeline.db')
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
            conn.close()
            with st.expander("SQLITE DATABASE"):
                st.caption(f"File: pipeline.db")
                st.write("Tables:", tables['name'].tolist())

        # Download
        download_format = st.selectbox("Download Format", ['csv', 'parquet', 'json'])

        if download_format == 'csv':
            data      = df.to_csv(index=False)
            mime      = 'text/csv'
            file_name = 'processed.csv'
        elif download_format == 'parquet':
            data      = df.to_parquet(index=False)
            mime      = 'application/octet-stream'
            file_name = 'processed.parquet'
        elif download_format == 'json':
            data      = df.to_json(orient='records', indent=2)
            mime      = 'application/json'
            file_name = 'processed.json'

        st.download_button(
            label=f"Download as {download_format.upper()}",
            data=data,
            file_name=file_name,
            mime=mime
        )

    # ── Log viewer ─────────────────────────────────────────────────────────────
    st.markdown("---")
    with st.expander("PIPELINE LOGS"):
        if os.path.exists('pipeline.log'):
            with open('pipeline.log', 'r') as f:
                lines = f.readlines()
            last_lines = lines[-50:] if len(lines) > 50 else lines
            st.code(''.join(last_lines), language=None)
        else:
            st.caption("No logs yet. Run the pipeline first.")
