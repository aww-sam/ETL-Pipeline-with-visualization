import pandas as pd
import numpy as np
import types
import os
from sklearn.preprocessing import StandardScaler,MinMaxScaler,LabelEncoder

class ETL_pipeline:
    def __init__(self,numeric_fill='mean',categorical_fill='mode',date_columns=None,z_thresh=3.29,null_thresh=0.5,remove_outliers=True,skew_thresh=1,clip='iqr',sclip_thresh=3,scaler='standard',encoder='label', scale_exclude=None,encode_exclude=None, load_data='csv'):
        self.numeric_fill=numeric_fill
        self.categorical_fill=categorical_fill
        self.date_columns=date_columns
        self.z_thresh=z_thresh
        self.null_thresh=null_thresh
        self.scaler=scaler
        self.clip=clip
        self.sclip_thresh=sclip_thresh
        self.encoder = encoder
        self.skew_thresh = skew_thresh
        self.remove_outliers=remove_outliers
        self.load_data = load_data
        self.scale_exclude = scale_exclude or []
        self.encode_exclude = encode_exclude or []
        
    def pipeline(self,df:pd.DataFrame)->pd.DataFrame:
        df=self._extract(df)
        df=self._transform(df)
        df=self._load(df)

        return df
    def _extract(self,df:pd.DataFrame)->pd.DataFrame:
        file_name=os.path.basename(df)
        file_extension=file_name.split('.')[1]
        if file_extension =='csv':
            return pd.read_csv(file_name)
        elif file_extension =='parquet':
            return pd.read_parquet(file_name)
        elif file_extension =='json':
            return pd.read_json(file_name)
        elif file_extension =='sql':
            return pd.read_sql(file_name)
        elif file_extension =='xlsx' :
            return pd.read_excel(file_name)
    def _transform(self,df:pd.DataFrame)->pd.DataFrame:
        def fill_numeric(df:pd.DataFrame)->pd.DataFrame:
            for col in df.select_dtypes(include=np.number).columns:
                if self.numeric_fill=="mean":
                    df[col]=df[col].fillna(df[col].mean())
                elif self.numeric_fill=="median":
                    df[col]=df[col].fillna(df[col].median())
                elif isinstance(self.numeric_fill,(int,float)):
                    df[col]=df[col].fillna(self.numeric_fill)
                # elif isinstance(self.numeric_fill,types.FunctionType):
                #     df[col].apply(self.numeric_fill)
            return df
        def fill_categorical(df:pd.DataFrame)->pd.DataFrame:
            for col in df.select_dtypes(include='object').columns:
                if self.categorical_fill=="mode":
                    df[col]=df[col].fillna(df[col].mode()[0])
                elif isinstance(self.categorical_fill,(int,float)):
                    df[col]=df[col].fillna(self.categorical_fill)
                # elif isinstance(self.categorical_fill,types.FunctionType):
                #     df[col].apply(self.categorical_fill)
            return df
        def remove_duplicates(df:pd.DataFrame)->pd.DataFrame:
            return df.drop_duplicates(keep='last').reset_index(drop=True)

        def remove_outliers(df:pd.DataFrame)->pd.DataFrame:
            numeric_cols=df.select_dtypes(include=np.number).columns
            for col in numeric_cols:
                mean,std=df[col].mean(),df[col].std()
                z_score=(df[col]-mean)/std
                df = df.loc[z_score.abs() <= self.z_thresh,:]
            return df.reset_index(drop=True)
        def drop_null_columns(df:pd.DataFrame)->pd.DataFrame:
            null_mean=df.isnull().mean()
            return df.loc[:,null_mean < self.null_thresh]
        def scale_features(df: pd.DataFrame) -> pd.DataFrame:
            numeric_cols = df.select_dtypes(include=np.number).columns
            numeric_cols = [col for col in numeric_cols if col not in self.scale_exclude]
            
            if not len(numeric_cols):
                return df
            if self.scaler == 'standard':
                df[numeric_cols] = StandardScaler().fit_transform(df[numeric_cols])
            elif self.scaler == 'minmax':
                df[numeric_cols] = MinMaxScaler().fit_transform(df[numeric_cols])
            return df
            
        def encode_catgorical(df: pd.DataFrame) -> pd.DataFrame:
            categorical_cols = [col for col in df.select_dtypes(include='object').columns 
                        if col not in self.encode_exclude] 
            if self.encoder == 'label':
                le = LabelEncoder()
                for col in categorical_cols:
                    df[col] = le.fit_transform(df[col].astype('object'))
            elif self.encoder == 'onehot':
                df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)
            return df
        def fix_skew(df:pd.DataFrame)->pd.DataFrame:
            for cols in df.select_dtypes(include=np.number).columns:
                if df[cols].skew()>self.skew_thresh and (df[cols]>0).all():
                    df[cols]=np.log1p(df[cols])
            return df
        def clip_outliers(df:pd.DataFrame)->pd.DataFrame:
            numeric_cols=df.select_dtypes(include=np.number).columns
            if self.clip=='iqr':
                for cols in numeric_cols:
                    Q3=df[cols].quantile(0.75)
                    Q1=df[cols].quantile(0.25)
                    IQR=Q3-Q1
                    lower_bound=Q1-1.5*IQR
                    upper_bound=Q3+1.5*IQR
                    df[cols]=df[cols].clip(lower_bound,upper_bound)
                return df
            elif self.clip=='sigma':
                for cols in numeric_cols:
                    mean=df[cols].mean()
                    std=df[cols].std()
                    lower_bound=mean-(self.sclip_thresh*std)
                    upper_bound=mean+(self.sclip_thresh*std)
                    df[cols]=df[cols].clip(lower_bound,upper_bound)
                return df
            elif self.clip=='percentile':
                for cols in numeric_cols:
                    lower_bound=df[cols].quantile(0.01)
                    upper_bound=df[cols].quantile(0.99)
                    df[cols]=df[cols].clip(lower_bound,upper_bound)
                return df
        def datetime_features(df: pd.DataFrame) -> pd.DataFrame:
            for cols in df.select_dtypes(include=["datetime64"]).columns:
                df[f"{cols}_year"]      = df[cols].dt.year
                df[f"{cols}_month"]     = df[cols].dt.month
                df[f"{cols}_day"]       = df[cols].dt.day
                df[f"{cols}_dayofweek"] = df[cols].dt.dayofweek
                df = df.drop(columns=[cols])
            return df

        df = drop_null_columns(df).reset_index(drop=True)
        df = fill_numeric(df).reset_index(drop=True)
        df = fill_categorical(df).reset_index(drop=True)
        df = remove_duplicates(df).reset_index(drop=True)

        if self.remove_outliers == True:
            df = remove_outliers(df).reset_index(drop=True)
        else:
            df = clip_outliers(df).reset_index(drop=True)

        df = scale_features(df).reset_index(drop=True)
        df = fix_skew(df).reset_index(drop=True)
        df = encode_catgorical(df).reset_index(drop=True)
        df = datetime_features(df).reset_index(drop=True)
        return df
    def _load(self,df:pd.DataFrame)->pd.DataFrame:
        if self.load_data=='csv':
            df.to_csv('etl.csv',index=False)
        elif self.load_data=='parquet':
            data = df.to_parquet(index=False)
        elif self.load_data=='json':
            data = df.to_json(orient='records', indent=2)
        return df