# test_dw_query.py
from sqlalchemy import select
from core.db_engine import engine
from core.dw_models import (
    fact_opinion,
    dim_cliente,
    dim_producto,
    dim_fuente,
    dim_fecha,
)
import pandas as pd

def ejemplo_consulta():
    stmt = (
        select(
            fact_opinion.c.IdOpinion,
            dim_cliente.c.Nombre.label("Cliente"),
            dim_producto.c.Nombre.label("Producto"),
            dim_fuente.c.Tipo.label("TipoFuente"),
            dim_fecha.c.Fecha,
            fact_opinion.c.Calificacion,
            fact_opinion.c.Sentimiento,
            fact_opinion.c.Satisfaccion,
            fact_opinion.c.Comentario,
        )
        .select_from(
            fact_opinion
            .join(dim_cliente, fact_opinion.c.IdCliente == dim_cliente.c.IdCliente)
            .join(dim_producto, fact_opinion.c.IdProducto == dim_producto.c.IdProducto)
            .join(dim_fuente,  fact_opinion.c.IdFuente  == dim_fuente.c.IdFuente)
            .join(dim_fecha,   fact_opinion.c.IdFecha   == dim_fecha.c.IdFecha)
        )
    )

    with engine.connect() as conn:
        result = conn.execute(stmt)
        rows = result.fetchall()
        cols = result.keys()

    df = pd.DataFrame(rows, columns=cols)
    print(df.head())

if __name__ == "__main__":
    ejemplo_consulta()
