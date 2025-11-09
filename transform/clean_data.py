import pandas as pd
import numpy as np

def normalize_text(s: pd.Series) -> pd.Series:
    return (s.astype(str)
              .str.strip()
              .str.replace(r"\s+", " ", regex=True))

def parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=False).dt.tz_localize(None)

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ","_") for c in df.columns]
    return df

def build_dim_fecha(dates: pd.Series) -> pd.DataFrame:
    d = pd.to_datetime(dates.dropna().unique())
    if len(d)==0:
        return pd.DataFrame(columns=["fecha_key","fecha","anio","mes","dia","trimestre","mes_nombre","dia_semana"])
    dim = pd.DataFrame({"fecha": pd.to_datetime(d)})
    dim["fecha_key"] = dim["fecha"].dt.strftime("%Y%m%d").astype(int)
    dim["anio"] = dim["fecha"].dt.year
    dim["mes"] = dim["fecha"].dt.month
    dim["dia"] = dim["fecha"].dt.day
    dim["trimestre"] = dim["fecha"].dt.quarter
    dim["mes_nombre"] = dim["fecha"].dt.month_name()
    dim["dia_semana"] = dim["fecha"].dt.day_name()
    return dim[["fecha_key","fecha","anio","mes","dia","trimestre","mes_nombre","dia_semana"]]