# core/dw_models.py
from sqlalchemy import MetaData, Table
from .db_engine import engine  # importa tu engine ya configurado

metadata = MetaData()

# Dimensiones (schema Dimension)
dim_cliente = Table(
    "Cliente", metadata,
    schema="Dimension",
    autoload_with=engine
)

dim_producto = Table(
    "Producto", metadata,
    schema="Dimension",
    autoload_with=engine
)

dim_fuente = Table(
    "Fuente", metadata,
    schema="Dimension",
    autoload_with=engine
)

dim_fecha = Table(
    "Fecha", metadata,
    schema="Dimension",
    autoload_with=engine
)

# Tabla de hechos (schema Fact)
fact_opinion = Table(
    "Opinion", metadata,
    schema="Fact",
    autoload_with=engine
)
