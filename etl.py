import pandas as pd
import numpy as np
import os
import sqlite3
import logging
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder

logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ETL_pipeline:
    def __init__(self, numeric_fill='mean', categorical_fill='mode', date_columns=None,
                 z_thresh=3.29, null_thresh=0.5, remove_outliers=True, skew_thresh=1,
                 clip='iqr', sclip_thresh=3, scaler='standard', encoder='label',
                 scale_exclude=None, encode_exclude=None, load_data='csv',
                 db_path='pipeline.db', db_table='processed_data'):
        self.numeric_fill = numeric_fill
        self.categorical_fill = categorical_fill
        self.date_columns = date_columns
        self.z_thresh = z_thresh
        self.null_thresh = null_thresh
        self.scaler = scaler
        self.clip = clip
        self.sclip_thresh = sclip_thresh
        self.encoder = encoder
        self.skew_thresh = skew_thresh
        self.remove_outliers = remove_outliers
        self.load_data = load_data
        self.scale_exclude = scale_exclude or []
        self.encode_exclude = encode_exclude or []
        self.db_path = db_path
        self.db_table = db_table

    def pipeline(self, file_path: str) -> pd.DataFrame:
        try:
            logger.info("Pipeline started")
            df = self._extract(file_path)
            logger.info(f"Extract complete — {df.shape[0]} rows, {df.shape[1]} columns")
            df = self._transform(df)
            logger.info(f"Transform complete — {df.shape[0]} rows, {df.shape[1]} columns")
            df = self._load(df)
            logger.info("Load complete — pipeline finished successfully")
            return df
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise

    def _extract(self, file_path: str) -> pd.DataFrame:
        file_name = os.path.basename(file_path)
        file_extension = file_name.split('.')[-1]
        if file_extension == 'csv':
            return pd.read_csv(file_path)
        elif file_extension == 'parquet':
            return pd.read_parquet(file_path)
        elif file_extension == 'json':
            return pd.read_json(file_path)
        elif file_extension == 'sql':
            return pd.read_sql(file_path)
        elif file_extension == 'xlsx':
            return pd.read_excel(file_path)

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        def fill_numeric(df):
            for col in df.select_dtypes(include=np.number).columns:
                if self.numeric_fill == "mean":
                    df[col] = df[col].fillna(df[col].mean())
                elif self.numeric_fill == "median":
                    df[col] = df[col].fillna(df[col].median())
                elif isinstance(self.numeric_fill, (int, float)):
                    df[col] = df[col].fillna(self.numeric_fill)
            return df

        def fill_categorical(df):
            for col in df.select_dtypes(include='object').columns:
                if self.categorical_fill == "mode":
                    df[col] = df[col].fillna(df[col].mode()[0])
                elif isinstance(self.categorical_fill, (int, float)):
                    df[col] = df[col].fillna(self.categorical_fill)
            return df

        def remove_duplicates(df):
            before = len(df)
            df = df.drop_duplicates(keep='last').reset_index(drop=True)
            logger.info(f"Duplicates removed: {before - len(df)} rows dropped")
            return df

        def remove_outliers(df):
            before = len(df)
            numeric_cols = df.select_dtypes(include=np.number).columns
            for col in numeric_cols:
                mean, std = df[col].mean(), df[col].std()
                z_score = (df[col] - mean) / std
                df = df.loc[z_score.abs() <= self.z_thresh, :]
            logger.info(f"Outlier removal: {before - len(df)} rows dropped")
            return df.reset_index(drop=True)

        def drop_null_columns(df):
            before = df.shape[1]
            null_mean = df.isnull().mean()
            df = df.loc[:, null_mean < self.null_thresh]
            dropped = before - df.shape[1]
            if dropped:
                logger.info(f"Null column drop: {dropped} columns removed (threshold={self.null_thresh})")
            return df

        def scale_features(df):
            numeric_cols = [c for c in df.select_dtypes(include=np.number).columns
                            if c not in self.scale_exclude]
            if not numeric_cols:
                return df
            if self.scaler == 'standard':
                df[numeric_cols] = StandardScaler().fit_transform(df[numeric_cols])
            elif self.scaler == 'minmax':
                df[numeric_cols] = MinMaxScaler().fit_transform(df[numeric_cols])
            logger.info(f"Scaling ({self.scaler}) applied to {len(numeric_cols)} columns")
            return df

        def encode_categorical(df):
            categorical_cols = [c for c in df.select_dtypes(include='object').columns
                                if c not in self.encode_exclude]
            if self.encoder == 'label':
                le = LabelEncoder()
                for col in categorical_cols:
                    df[col] = le.fit_transform(df[col].astype('object'))
            elif self.encoder == 'onehot':
                df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)
            logger.info(f"Encoding ({self.encoder}) applied to {len(categorical_cols)} columns")
            return df

        def fix_skew(df):
            fixed = []
            for col in df.select_dtypes(include=np.number).columns:
                if df[col].skew() > self.skew_thresh and (df[col] > 0).all():
                    df[col] = np.log1p(df[col])
                    fixed.append(col)
            if fixed:
                logger.info(f"Skew fix (log1p) applied to: {fixed}")
            return df

        def clip_outliers(df):
            numeric_cols = df.select_dtypes(include=np.number).columns
            if self.clip == 'iqr':
                for col in numeric_cols:
                    Q1, Q3 = df[col].quantile(0.25), df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    df[col] = df[col].clip(Q1 - 1.5 * IQR, Q3 + 1.5 * IQR)
            elif self.clip == 'sigma':
                for col in numeric_cols:
                    mean, std = df[col].mean(), df[col].std()
                    df[col] = df[col].clip(mean - self.sclip_thresh * std,
                                           mean + self.sclip_thresh * std)
            elif self.clip == 'percentile':
                for col in numeric_cols:
                    df[col] = df[col].clip(df[col].quantile(0.01), df[col].quantile(0.99))
            logger.info(f"Outlier clipping ({self.clip}) applied")
            return df

        def datetime_features(df):
            for col in df.select_dtypes(include=["datetime64"]).columns:
                df[f"{col}_year"] = df[col].dt.year
                df[f"{col}_month"] = df[col].dt.month
                df[f"{col}_day"] = df[col].dt.day
                df[f"{col}_dayofweek"] = df[col].dt.dayofweek
                df = df.drop(columns=[col])
            return df

        df = drop_null_columns(df).reset_index(drop=True)
        df = fill_numeric(df).reset_index(drop=True)
        df = fill_categorical(df).reset_index(drop=True)
        df = remove_duplicates(df).reset_index(drop=True)

        if self.remove_outliers:
            df = remove_outliers(df).reset_index(drop=True)
        else:
            df = clip_outliers(df).reset_index(drop=True)

        df = fix_skew(df).reset_index(drop=True)
        df = scale_features(df).reset_index(drop=True)
        df = encode_categorical(df).reset_index(drop=True)
        df = datetime_features(df).reset_index(drop=True)
        return df

    def _load(self, df: pd.DataFrame) -> pd.DataFrame:
        # Save to flat file
        if self.load_data == 'csv':
            df.to_csv('etl.csv', index=False)
        elif self.load_data == 'parquet':
            df.to_parquet('etl.parquet', index=False)
        elif self.load_data == 'json':
            df.to_json('etl.json', orient='records', indent=2)

        # Save to SQLite
        try:
            conn = sqlite3.connect(self.db_path)
            df.to_sql(self.db_table, conn, if_exists='replace', index=False)
            conn.close()
            logger.info(f"Data saved to SQLite — db='{self.db_path}', table='{self.db_table}', rows={len(df)}")
        except Exception as e:
            logger.error(f"SQLite save failed: {e}")

        return df
