# main.py
import os, json, sqlite3
import pandas as pd
from core.logger import get_logger
from extract.csv_extractor import CsvExtractor
from extract.db_extractor import DatabaseExtractor
from extract.api_extractor import ApiExtractor
from transform.clean_data import (
    standardize_columns, normalize_text, parse_date, build_dim_fecha
)
from load.load_to_staging import upsert_table, ensure_indexes

BASE = os.path.dirname(__file__)
with open(os.path.join(BASE, "config", "settings.json"), "r", encoding="utf-8") as f:
    cfg = json.load(f)

log = get_logger("etl", cfg["log_path"])

# ------------------------------
# Lectura de fuentes (BD + API + CSV)
# ------------------------------
def read_sources():
    dfs = {}

    # 1) BD relacional
    try:
        log.info("Consultando base de datos relacional...")
        db_query = (
            "SELECT IdOpinion, IdCliente, IdProducto, Comentario, "
            "PuntajeSatisfaccion, Fecha, Fuente FROM Opiniones"
        )
        df_db = DatabaseExtractor(db_query).extract()
        df_db = standardize_columns(df_db)
        if df_db is None or df_db.empty:
            log.warning("BD: consulta vacía o sin filas.")
        else:
            dfs["db_opiniones"] = df_db
            log.info(f"BD: {len(df_db)} filas")
    except Exception as e:
        log.warning(f"No se pudo consultar la BD: {e}")

    # 2) API REST
    try:
        log.info("Consultando API de opiniones...")
        api_url = cfg.get("api_url", "https://api.miempresa.com/opiniones")
        df_api = ApiExtractor(api_url).extract()
        df_api = standardize_columns(df_api)
        if df_api is None or df_api.empty:
            log.warning("API: respuesta vacía/no JSON o sin filas.")
        else:
            dfs["api_opiniones"] = df_api
            log.info(f"API: {len(df_api)} filas")
    except Exception as e:
        log.warning(f"No se pudo consultar la API: {e}")

    # 3) CSVs
    paths = cfg["paths"]
    for key, path in paths.items():
        if key.endswith("_csv"):
            try:
                log.info(f"Leyendo {key} desde {path}")
                df = CsvExtractor(path).extract()
                df = standardize_columns(df)
                dfs[key] = df
                log.info(f"CSV {key}: {len(df)} filas")
            except Exception as e:
                log.warning(f"CSV {key}: error leyendo {path}: {e}")

    return dfs

# ------------------------------
# Staging
# ------------------------------
def stage(conn, dfs):
    for k, df in dfs.items():
        if not isinstance(df, pd.DataFrame):
            log.warning(f"Staging -> {k}: fuente no es DataFrame, se omite.")
            continue
        if df.empty:
            log.info(f"Staging -> {k}: DataFrame vacío, se omite.")
            continue
        table = f"stg_{k.replace('_csv','')}"
        upsert_table(df, conn, table)
        log.info(f"Staging -> {table}: {len(df)} filas")

# ------------------------------
# Dimensiones
# ------------------------------
def build_dimensions(conn):
    # Dim Cliente (clients.csv -> stg_clients: idcliente, nombre, email)
    try:
        clients = pd.read_sql("SELECT * FROM stg_clients", conn)
        clients["cliente_id"] = "C" + clients["idcliente"].astype(str).str.zfill(3)
        dim_cliente = clients[["cliente_id","nombre","email"]].drop_duplicates().copy()
        dim_cliente["nombre"] = normalize_text(dim_cliente["nombre"])
        dim_cliente["email"] = normalize_text(dim_cliente["email"])
        upsert_table(dim_cliente, conn, "dim_cliente")
        log.info(f"Dim Cliente: {len(dim_cliente)}")
    except Exception as e:
        log.warning(f"Dim Cliente: no se pudo construir: {e}")

    # Dim Producto (products.csv -> stg_products: idproducto, nombre, categoría/categoria)
    try:
        products = pd.read_sql("SELECT * FROM stg_products", conn)
        products["producto_id"] = "P" + products["idproducto"].astype(str).str.zfill(3)
        if "categoría" in products.columns and "categoria" not in products.columns:
            products = products.rename(columns={"categoría":"categoria"})
        keep_cols = [c for c in ["producto_id","nombre","categoria"] if c in products.columns]
        dim_producto = products[keep_cols].drop_duplicates().copy()
        dim_producto["nombre"] = normalize_text(dim_producto["nombre"])
        upsert_table(dim_producto, conn, "dim_producto")
        log.info(f"Dim Producto: {len(dim_producto)}")
    except Exception as e:
        log.warning(f"Dim Producto: no se pudo construir: {e}")

    # Dim Fuente (fuente_datos.csv -> stg_fuente: idfuente, tipofuente, fechacarga)
    try:
        fuentes = pd.read_sql("SELECT * FROM stg_fuente", conn)
        dim_fuente = fuentes.rename(columns={"idfuente":"fuente_id","tipofuente":"tipo_fuente"}).copy()
        if "nombre" not in dim_fuente.columns:
            dim_fuente["nombre"] = dim_fuente["tipo_fuente"]
        dim_fuente = dim_fuente[["fuente_id","nombre","tipo_fuente","fechacarga"]].drop_duplicates()
        upsert_table(dim_fuente, conn, "dim_fuente")
        log.info(f"Dim Fuente: {len(dim_fuente)}")
    except Exception as e:
        log.warning(f"Dim Fuente: no se pudo construir: {e}")

    # Dim Fecha (usa 'fecha' en social, surveys, web, BD y API si existen)
    try:
        frames = []
        for table, col in [
            ("stg_social_comments","fecha"),
            ("stg_surveys","fecha"),
            ("stg_web_reviews","fecha"),
            ("stg_db_opiniones","fecha"),
            ("stg_api_opiniones","fecha"),
        ]:
            try:
                df = pd.read_sql(f"SELECT {col} FROM {table}", conn)
                if col in df.columns:
                    df[col] = parse_date(df[col])
                    frames.append(df[col])
            except Exception:
                # Tabla/columna puede no existir: continuar
                pass

        all_dates = pd.concat(frames, ignore_index=True) if frames else pd.Series([], dtype="datetime64[ns]")
        dim_fecha = build_dim_fecha(all_dates)
        upsert_table(dim_fecha, conn, "dim_fecha")
        log.info(f"Dim Fecha: {len(dim_fecha)}")
    except Exception as e:
        log.warning(f"Dim Fecha: no se pudo construir: {e}")

# ------------------------------
# Hechos
# ------------------------------
def build_fact(conn):
    try:
        dim_fuente  = pd.read_sql("SELECT * FROM dim_fuente", conn)
    except Exception:
        dim_fuente = pd.DataFrame(columns=["fuente_id","nombre"])

    frames = []

    def fecha_key_from(series):
        s = pd.to_datetime(series, errors="coerce").dt.strftime("%Y%m%d")
        return pd.to_numeric(s, errors="coerce").fillna(-1).astype("int64")

    def add_block(df, mapping):
        if df is None or df.empty:
            return None
        d = df.copy()

        # Normalización de nombres comunes
        if "comentario" in d.columns and "texto_opinion" not in d.columns:
            d = d.rename(columns={"comentario": "texto_opinion"})
        if "rating" in d.columns and "puntaje" not in d.columns:
            d["puntaje"] = pd.to_numeric(d["rating"], errors="coerce").fillna(0)
        if "puntajesatisfacción" in d.columns and "puntaje" not in d.columns:
            d["puntaje"] = pd.to_numeric(d["puntajesatisfacción"], errors="coerce").fillna(0)
        if "fecha" in d.columns and "fecha_key" not in d.columns:
            d["fecha_key"] = fecha_key_from(d["fecha"])

        # Map de IDs canónicos (C### / P### si ya vienen así)
        for src, dst in mapping.items():
            if src in d.columns and dst not in d.columns:
                d[dst] = d[src].astype(str)

        # fuente_id desde 'fuente' (si existe)
        if "fuente" in d.columns and "fuente_id" not in d.columns and not dim_fuente.empty:
            tmp = d.merge(dim_fuente[["fuente_id","nombre"]], how="left",
                          left_on="fuente", right_on="nombre")
            d["fuente_id"] = tmp["fuente_id"].fillna("-1").astype(str)

        # Columnas finales con defaults
        out_cols = ["cliente_id","producto_id","fuente_id","fecha_key","puntaje","texto_opinion"]
        for c in out_cols:
            if c not in d.columns:
                d[c] = "" if c == "texto_opinion" else ("-1" if c in {"cliente_id","producto_id","fuente_id"} else 0)

        d["cliente_id"]    = d["cliente_id"].astype(str).fillna("-1")
        d["producto_id"]   = d["producto_id"].astype(str).fillna("-1")
        d["fuente_id"]     = d["fuente_id"].astype(str).fillna("-1")
        d["fecha_key"]     = pd.to_numeric(d["fecha_key"], errors="coerce").fillna(-1).astype("int64")
        d["puntaje"]       = pd.to_numeric(d["puntaje"], errors="coerce").fillna(0)
        d["texto_opinion"] = d["texto_opinion"].astype(str).str.strip().str[:2000]

        return d[out_cols]

    # Carga por bloques con manejo de errores por tabla
    for table, mapping in [
        ("stg_social_comments", {"idcliente":"cliente_id","idproducto":"producto_id"}),
        ("stg_surveys",         {"idcliente":"cliente_id","idproducto":"producto_id"}),
        ("stg_web_reviews",     {"idcliente":"cliente_id","idproducto":"producto_id"}),
        ("stg_db_opiniones",    {"idcliente":"cliente_id","idproducto":"producto_id"}),
        ("stg_api_opiniones",   {"idcliente":"cliente_id","idproducto":"producto_id"}),
    ]:
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            blk = add_block(df, mapping)
            if blk is not None and not blk.empty:
                frames.append(blk)
            else:
                log.info(f"FACT: {table} sin filas útiles (vacío o no mapeable).")
        except Exception as e:
            log.warning(f"FACT: no se pudo procesar {table}: {e}")

    valid_frames = [f for f in frames if f is not None and not f.empty]
    fact = (
        pd.concat(valid_frames, ignore_index=True)
        if len(valid_frames) > 0
        else pd.DataFrame(columns=["cliente_id","producto_id","fuente_id","fecha_key","puntaje","texto_opinion"])
    )

    upsert_table(fact, conn, "fact_opiniones")
    log.info(f"FACT: fact_opiniones = {len(fact)} filas")

# ------------------------------
# Orquestación
# ------------------------------
def main():
    log.info("=== ETL Opiniones (Python) ===")
    dfs = read_sources()
    conn = sqlite3.connect(cfg["staging_db"])
    try:
        stage(conn, dfs)
        build_dimensions(conn)
        build_fact(conn)
        ensure_indexes(conn)
        log.info("ETL finalizado OK")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
