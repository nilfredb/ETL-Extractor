import pandas as pd
import sqlite3

def upsert_table(df: pd.DataFrame, conn: sqlite3.Connection, table: str):
    df.to_sql(table, conn, if_exists="replace", index=False)

def ensure_indexes(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.executescript('''
    CREATE INDEX IF NOT EXISTS ix_dim_cliente_id ON dim_cliente(cliente_id);
    CREATE INDEX IF NOT EXISTS ix_dim_producto_id ON dim_producto(producto_id);
    CREATE INDEX IF NOT EXISTS ix_dim_fuente_id ON dim_fuente(fuente_id);
    CREATE INDEX IF NOT EXISTS ix_dim_fecha_key ON dim_fecha(fecha_key);
    CREATE INDEX IF NOT EXISTS ix_fact_fecha_key ON fact_opiniones(fecha_key);
    ''')
    conn.commit()