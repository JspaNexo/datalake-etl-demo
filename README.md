# datalake-etl-demo

Proyecto simple de análisis de ventas con arquitectura limpia para ETL en Python.

## Funcionalidad

- Lee un CSV de ventas y valida sus datos.
- Extrae tambien ventas confirmadas de PostgreSQL de origen con clientes, productos y detalles.
- Genera Bronze, Silver y Gold en MinIO, con resultados por ejecución y publicación transaccional.
- Ejecuta el ETL con Dagster y consulta los resultados con SQLPad/PostgreSQL.
- Detecta cambios en las fuentes y ejecuta automáticamente el ETL con sensores de Dagster.
- Guarda los logs en archivo y envía los eventos a Seq vía HTTP.

## Requisitos

- Docker Desktop con contenedores Linux.

## Instalación

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose exec dagster datalake-sync
docker compose exec dagster datalake-sync --source-type postgres
```

Para ejecutar desde Dagster, selecciona el grupo `ventas` o `ventas_db` y pulsa **Materialize**.

El sensor `ventas_csv_sensor` se activa al iniciar Dagster y revisa el CSV cada 30 segundos. Edita y guarda `data/ventas.csv`; tras dos lecturas iguales ejecuta el ETL. Estado e historial en **Automation / Sensors** y **Runs**. [Detalles](docs/automatizacion.md).

La base de origen tiene su propio job `etl_ventas_db` y sensor `ventas_db_sensor`. En SQLPad, elige **Origen - Operacion** para consultar o insertar ventas. [Caso relacional](docs/fuente_relacional.md).

## Accesos y credenciales

Valores predeterminados de la demo, configurables en `.env`.

| Servicio | Dirección | Usuario | Contraseña |
|---|---|---|---|
| Dagster | http://localhost:3000 | Sin autenticación | — |
| MinIO | http://localhost:9001 | `minio_demo` | `minio_demo_2026` |
| SQLPad | http://localhost:3001 | `demo@example.com` | `sqlpad_demo_2026` |
| Seq | http://localhost:5341 | Sin autenticación | — |
| PostgreSQL | `postgres:5432` dentro de Docker; base `ventas` | `demo` | `postgres_demo_2026` |
| PostgreSQL de origen | `localhost:5433`; base `ventas_origen` | `operacion` | `operacion_demo_2026` |

## Pruebas

```powershell
docker compose exec dagster python -m unittest discover -s tests -v
```

Consultas de ejemplo: [sql/consultas.sql](sql/consultas.sql).

CI/CD con TeamCity: [configuracion y pasos](docs/teamcity.md).

Arquitectura, versiones y tratamiento de fallos: [guía breve](docs/arquitectura.md).
