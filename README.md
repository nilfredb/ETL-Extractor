
# ETL Opiniones (DWOpiniones)

Pipeline ETL en Python que cumple la rúbrica de la práctica de arquitectura y extracción (adaptado de .NET a Python).

## Estructura
- `config/settings.json`: rutas de CSV y salida SQLite
- `core/logger.py`: logging rotativo
- `extract/`: extractores (CSV implementado)
- `transform/clean_data.py`: normalización y Dim Fecha
- `load/load_to_staging.py`: *upsert* a SQLite e índices
- `main.py`: orquestación completa

## Ejecutar
```bash
python main.py
```
La salida se genera en: `/mnt/data/etl_opiniones/output/staging_dwopiniones.sqlite` y logs en `/mnt/data/etl_opiniones/logs/etl.log`.

## Fuentes usadas
- clients.csv
- products.csv
- fuente_datos.csv
- social_comments.csv
- surveys_part1.csv
- web_reviews.csv

## Tablas generadas
- `stg_*`: staging crudo
- `dim_cliente`, `dim_producto`, `dim_fuente`, `dim_fecha`
- `fact_opiniones`

## Atributos de calidad
- **Rendimiento**: pandas vectorizado; IO a SQLite
- **Escalabilidad**: extractores modulares
- **Seguridad**: configuración centralizada en JSON (credenciales no incluidas aquí)
- **Mantenibilidad**: separación por capas
