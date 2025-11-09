import pandas as pd
from .base_extractor import IExtractor

class CsvExtractor(IExtractor):
    def __init__(self, path: str, **read_csv_kwargs):
        self.path = path
        self.kw = {"encoding":"utf-8","na_filter":False} | read_csv_kwargs

    def extract(self) -> pd.DataFrame:
        return pd.read_csv(self.path, **self.kw)