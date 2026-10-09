# Data Lake de ventas: guía del caso de uso

Demo de un ETL en Python que procesa ventas desde CSV y PostgreSQL, guarda las capas Bronze, Silver y Gold en MinIO y publica resultados para consultas SQL.

## Pasos realizados

1. **Preparar las fuentes.** Se creó [ventas.csv](data/ventas.csv) y una base PostgreSQL operacional con ciudades, clientes, productos, ventas y detalles. Del origen relacional se extraen los detalles de ventas confirmadas.
2. **Organizar el código.** Se separaron las reglas de negocio (`domain`), el ETL (`etl`), los adaptadores (`infra`), la ejecución (`jobs`) y la configuración de dependencias (`bootstrap`).
3. **Implementar las capas.** Bronze conserva el origen; Silver valida y separa registros rechazados; Gold agrega ventas, unidades e ingresos por fecha, ciudad y producto. Cada fuente mantiene resultados independientes.
4. **Publicar y conservar versiones.** Se guardaron resultados por ejecución en MinIO. Gold, calidad y la referencia vigente se publican juntos en PostgreSQL; una fuente inválida conserva la última publicación válida.
5. **Automatizar con Dagster.** Se crearon los jobs `etl_ventas` y `etl_ventas_db`, con sensores que revisan cambios aproximadamente cada 30 segundos. El CSV requiere dos lecturas iguales; PostgreSQL, una.
6. **Incorporar consultas y logs.** Se configuraron SQLPad para consultar origen y Gold, Seq para los eventos del ETL y un worker que registra actividad cada 60 segundos en Seq y `logs/worker_log.txt`.
7. **Agregar pruebas e integración continua.** Se incluyeron pruebas de datos, arquitectura, versiones y fallos. El pipeline de [TeamCity](.teamcity/settings.kts) ejecuta pruebas, construye Docker, verifica ambas fuentes y genera archivos de entrega. La publicación de imagen está desactivada y el despliegue automático queda pendiente.

## Cómo ejecutar y comprobar la demo

Con Docker Desktop en modo de contenedores Linux, ejecutar desde la raíz del proyecto:

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose exec dagster datalake-sync
docker compose exec dagster datalake-sync --source-type postgres
```

En Dagster también se pueden materializar los grupos `ventas` y `ventas_db`. Para probar la automatización, editar `data/ventas.csv` o insertar una venta con [operacion.sql](sql/operacion.sql) en la conexión **Origen - Operacion** de SQLPad. Revisar **Automation / Sensors** y **Runs** en Dagster.

Consultar los resultados con [consultas.sql](sql/consultas.sql) en **Gold - Ventas**, revisar las capas en el bucket `datalake` de MinIO y los eventos en Seq. La versión publicada se consulta en `etl_control.publicaciones`.

Para ejecutar las pruebas:

```powershell
docker compose exec dagster python -m unittest discover -s tests -v
```

## Accesos de la demo

Valores predeterminados, configurables en `.env`:

| Servicio | Dirección | Usuario / contraseña |
|---|---|---|
| Dagster | http://localhost:3000 | Sin autenticación |
| MinIO | http://localhost:9001 | `minio_demo` / `minio_demo_2026` |
| SQLPad | http://localhost:3001 | `demo@example.com` / `sqlpad_demo_2026` |
| Seq | http://localhost:5341 | Sin autenticación |
| PostgreSQL Gold | `127.0.0.1:5432`, base `ventas` | `demo` / `postgres_demo_2026` |
| PostgreSQL origen | `127.0.0.1:5433`, base `ventas_origen` | `operacion` / `operacion_demo_2026` |
