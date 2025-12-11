# core/dw_repository.py
from typing import List, Dict
from sqlalchemy import insert
from .db_engine import engine
from .dw_models import fact_opinion  # tabla Fact.Opinion reflejada


def insert_opiniones(rows: List[Dict]) -> None:
    """
    Inserta un lote de filas en Fact.Opinion (DWopiniones).
    Cada dict debe tener las columnas:
    IdCliente, IdProducto, IdFuente, IdFecha,
    Calificacion, Sentimiento, Comentario, Satisfaccion.
    """
    if not rows:
        return

    stmt = insert(fact_opinion)

    # Usamos transacción automática
    with engine.begin() as conn:
        conn.execute(stmt, rows)
