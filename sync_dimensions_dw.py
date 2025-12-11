# sync_dimensions_dw.py
import os
import json
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from core.logger import get_logger
from core.db_engine import get_engine  # o el que uses para SQL Server

BASE = os.path.dirname(__file__)
with open(os.path.join(BASE, "config", "settings.json"), "r", encoding="utf-8") as f:
    cfg = json.load(f)

log = get_logger("sync_dims", cfg["log_path"])

def main():
    # Conexión a la BD de staging (SQLite)
    conn_stg = sqlite3.connect(cfg["staging_db"])

    # Conexión al DW (SQL Server)
    engine_dw = get_engine()

    try:
        # ------------------------
        # DIMENSION.CLIENTE
        # stg_clients: idcliente, nombre, email
        # Dimension.Cliente: IdCliente (IDENTITY o PK), Nombre, Email, Edad, Pais
        # ------------------------
        try:
            df_cli = pd.read_sql("SELECT idcliente, nombre, email FROM stg_clients", conn_stg)

            # Ordenamos para que, si IdCliente es IDENTITY, se genere en el mismo orden 1..500
            df_cli = df_cli.sort_values("idcliente")

            dim_cli = pd.DataFrame({
                "Nombre": df_cli["nombre"],
                "Email": df_cli["email"],
                # Puedes rellenar estos más adelante si quieres
                "Edad": None,
                "Pais": None,
            })

            dim_cli.to_sql(
                "Cliente",
                engine_dw,
                schema="Dimension",
                if_exists="append",
                index=False,
            )
            log.info(f"Dimension.Cliente poblada: {len(dim_cli)} filas")
        except Exception as e:
            log.warning(f"Error poblando Dimension.Cliente: {e}")

        # ------------------------
        # DIMENSION.PRODUCTO
        # stg_products: idproducto, nombre, categoría/categoria
        # Dimension.Producto: IdProducto, Nombre, Categoria, Marca
        # ------------------------
        try:
            df_prod = pd.read_sql("SELECT * FROM stg_products", conn_stg)

            # Normalizar nombre de columna categoría -> categoria
            if "categoría" in df_prod.columns and "categoria" not in df_prod.columns:
                df_prod = df_prod.rename(columns={"categoría": "categoria"})

            df_prod = df_prod.sort_values("idproducto")

            dim_prod = pd.DataFrame({
                "Nombre": df_prod["nombre"],
                "Categoria": df_prod.get("categoria"),
                "Marca": None,  # Por ahora no hay marca en el CSV
            })

            dim_prod.to_sql(
                "Producto",
                engine_dw,
                schema="Dimension",
                if_exists="append",
                index=False,
            )
            log.info(f"Dimension.Producto poblada: {len(dim_prod)} filas")
        except Exception as e:
            log.warning(f"Error poblando Dimension.Producto: {e}")

          # ============================
        # Dimension.Fuente
        # stg_fuente: idfuente, tipofuente, fechacarga
        # Dimension.Fuente: IdFuente (IDENTITY), Nombre, Tipo, FechaCarga
        # ============================
        try:
            df_fte = pd.read_sql(
                "SELECT idfuente, tipofuente, fechacarga FROM stg_fuente",
                conn_stg
            )
            df_fte = df_fte.sort_values("idfuente")

            dim_fte = pd.DataFrame({
                "Nombre": df_fte["tipofuente"],
                "Tipo":   df_fte["tipofuente"],  # 👈 esta columna EXISTE en SQL Server y es NOT NULL
                "FechaCarga": pd.to_datetime(df_fte["fechacarga"], errors="coerce"),
            })

            dim_fte.to_sql(
                "Fuente",
                engine_dw,
                schema="Dimension",
                if_exists="append",
                index=False,
            )
            log.info(f"Dimension.Fuente poblada: {len(dim_fte)} filas")
        except Exception as e:
            log.warning(f"Error poblando Dimension.Fuente: {e}")

         # ============================
        # Dimension.Fecha
        # dim_fecha (SQLite): fecha_key, fecha, anio, mes, dia
        # Dimension.Fecha: IdFecha (IDENTITY), Fecha, Anio, Mes, Dia
        # ============================
        try:
            df_fecha = pd.read_sql("SELECT * FROM dim_fecha", conn_stg)

            # Evitar fechas duplicadas
            df_fecha = df_fecha.drop_duplicates(subset=["fecha"])

            dim_fecha = pd.DataFrame({
                # ❌ NO mandamos IdFecha porque es IDENTITY en SQL Server
                "Fecha": pd.to_datetime(df_fecha["fecha"], errors="coerce").dt.date,
                "Anio": df_fecha["anio"],
                "Mes": df_fecha["mes"],
                "Dia": df_fecha["dia"],
            })

            dim_fecha.to_sql(
                "Fecha",
                engine_dw,
                schema="Dimension",
                if_exists="append",
                index=False,
            )
            log.info(f"Dimension.Fecha poblada: {len(dim_fecha)} filas")
        except Exception as e:
            log.warning(f"Error poblando Dimension.Fecha: {e}")

    finally:
        conn_stg.close()

if __name__ == "__main__":
    main()
