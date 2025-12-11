# db.py
from sqlalchemy import create_engine, text

# Ajusta el nombre del driver si usas 18 en vez de 17
DRIVER = "ODBC Driver 17 for SQL Server"

# Si usas autenticación de Windows (Trusted_Connection)
CONN_STR = (
    "mssql+pyodbc://@localhost/DWopiniones"
    f"?driver={DRIVER.replace(' ', '+')}"
    "&trusted_connection=yes"
)

engine = create_engine(CONN_STR, echo=False, future=True)
def get_engine():
    return engine

def test_connection():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("Conectado OK, resultado:", result.scalar())

if __name__ == "__main__":
    test_connection()
