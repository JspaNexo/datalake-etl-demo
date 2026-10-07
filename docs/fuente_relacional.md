# Fuente relacional

Ejemplo de ETL desde PostgreSQL operacional hacia MinIO y PostgreSQL Gold, junto al CSV.

## Funcionalidad

- Usa tablas relacionadas de ciudades, clientes, productos, ventas y detalles.
- Carga inicialmente 300 ventas y 600 detalles; extrae los 570 detalles de ventas confirmadas.
- Ejecuta el job `etl_ventas_db` automáticamente mediante `ventas_db_sensor`.
- Guarda las capas en `bronze/ventas_db/`, `silver/ventas_db/` y `gold/ventas_db/`, con subcarpetas `runs/<execution_id>/` en Silver y Gold.

Cada fila del lake representa un detalle de venta. Los resultados del CSV y de PostgreSQL se guardan por separado.

## Ejecución manual

```powershell
docker compose up -d --build
docker compose exec dagster datalake-sync --source-type postgres
```

## Conexión desde pgAdmin

| Campo | Valor |
|---|---|
| Host / puerto | `127.0.0.1` / `5433` |
| Maintenance database | `ventas_origen` |
| Usuario | `operacion` |
| Contraseña | `operacion_demo_2026` |
| Esquema | `operacion` |

Son valores de la demo, configurables mediante `SOURCE_POSTGRES_*` en `.env`. Dentro de Docker, el origen es `source_postgres:5432`. Los datos iniciales se cargan una vez y el volumen conserva las operaciones agregadas.

## Consultas en SQLPad

- **Origen - Operacion:** consulta las tablas o inserta una venta con [operacion.sql](../sql/operacion.sql).
- **Gold - Ventas:** consulta `gold.ventas_db_diarias` y `gold.calidad_db` con [consultas.sql](../sql/consultas.sql).

La versión vigente está en `etl_control.publicaciones`. Si ya no hay ventas confirmadas, se publica Gold vacío. [Detalles](arquitectura.md).
