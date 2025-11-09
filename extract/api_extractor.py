# extract/api_extractor.py
import requests
import pandas as pd
from .base_extractor import IExtractor

class ApiExtractor(IExtractor):
    def __init__(self, url: str, headers=None, timeout: int = 30):
        self.url = url
        self.headers = headers or {}
        self.timeout = timeout

    def extract(self) -> pd.DataFrame:
        try:
            resp = requests.get(self.url, headers=self.headers, timeout=self.timeout)
            resp.raise_for_status()

            ctype = resp.headers.get("Content-Type", "")
            if "application/json" not in ctype.lower():
                return pd.DataFrame()

            data = resp.json()
            if isinstance(data, dict) and "data" in data:
                data = data["data"]
            if isinstance(data, list):
                return pd.DataFrame(data)
            return pd.json_normalize(data)
        except Exception:
            return pd.DataFrame()
