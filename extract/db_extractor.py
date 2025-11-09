# extract/db_extractor.py
import pandas as pd
from .base_extractor import IExtractor
from core.db_engine import get_engine

class DatabaseExtractor(IExtractor):
    def __init__(self, query: str):
        self.query = query
        self.engine = get_engine()

    def extract(self) -> pd.DataFrame:
        with self.engine.connect() as conn:
            df = pd.read_sql(self.query, conn)
        return df
