import streamlit as st
import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(".."))
from etl import ETL_pipeline


# st.markdown("""
# <style>
#     /* Main background */
#     .stApp { background-color: #0f1117; color: #e0e0e0; }

#     /* Cards */
#     .metric-card {
#         background: #1e2130;
#         border-radius: 12px;
#         padding: 1.2rem;
#         border: 1px solid #2d3250;
#         box-shadow: 0 4px 20px rgba(0,0,0,0.3);
#     }

#     /* Hide Streamlit branding */
#     #MainMenu, footer, header { visibility: hidden; }
# </style>
# """, unsafe_allow_html=True)


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

    null_thresh = st.sidebar.number_input("Null Threshold (default=0.5)", value=0.5)
    z_thresh   = st.sidebar.number_input("Z Threshold (default=3.29)",   value=3.29)
    skew_thresh    = st.sidebar.number_input("Skew Threshold (default=1)",   value=1)
    remove_outliers = st.sidebar.checkbox("Remove Outliers", value=True)

    if not remove_outliers:
        clip = st.sidebar.selectbox("Outlier Clipper", ['iqr', 'sigma', 'percentile'])
        if clip == 'sigma': 
            sclip_thresh = st.sidebar.number_input("Sigma Value (default=3)", value=3)

    scaler         = st.sidebar.selectbox("Scaler",  ['standard', 'minmax'])
    encoder        = st.sidebar.selectbox("Encoder", ['label', 'onehot'])
    scale_exclude  = st.sidebar.multiselect("Exclude from Scaling",  options=df_raw.columns.tolist())
    encode_exclude = st.sidebar.multiselect("Exclude from Encoding", options=df_raw.columns.tolist())

    if st.button("Execute Pipeline"):
        with open(file.name, 'wb') as f:
            f.write(file.getbuffer())

        e = ETL_pipeline(
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
        )
        st.session_state['df'] = e.pipeline(file.name)
        st.success('Pipeline Executed')


    if 'df' in st.session_state:
        with st.expander("PEEK PROCESSED DATASET"):
            st.dataframe(st.session_state['df'])

        df = st.session_state['df']

        download_format = st.selectbox("Download Format", ['csv', 'parquet', 'json', 'xlsx']) 

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
