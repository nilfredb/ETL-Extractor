# core/db_engine.py
import os
import urllib
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    server = os.getenv("DB_SERVER", "localhost")
    database = os.getenv("DB_NAME", "SistemaOpiniones")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    
    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes"
    
    conn_url = "mssql+pyodbc:///?odbc_connect=%s" % urllib.parse.quote_plus(conn_str)
    return create_engine(conn_url, fast_executemany=True)
