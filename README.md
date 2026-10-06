# datalake-etl-demo

Proyecto simple de análisis de ventas con estructura clean code para ETL en Python.

## Funcionalidad

- Lee un CSV de ventas y valida sus datos.
- Genera las capas bronze, silver y gold en MinIO.
- Ejecuta el ETL con Dagster y consulta los resultados con SQLPad/PostgreSQL.
- Guarda los logs en archivo y envía los eventos a Seq vía HTTP.

## Requisitos

- Docker Desktop con contenedores Linux.

## Instalación

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose exec dagster datalake-sync
```

Para ejecutar desde Dagster, selecciona los cuatro assets y pulsa **Materialize**.

## Accesos y credenciales

Valores predeterminados de la demo, configurables en `.env`.

| Servicio | Dirección | Usuario | Contraseña |
|---|---|---|---|
| Dagster | http://localhost:3000 | Sin autenticación | — |
| MinIO | http://localhost:9001 | `minio_demo` | `minio_demo_2026` |
| SQLPad | http://localhost:3001 | `demo@example.com` | `sqlpad_demo_2026` |
| Seq | http://localhost:5341 | Sin autenticación | — |
| PostgreSQL | `postgres:5432` dentro de Docker; base `ventas` | `demo` | `postgres_demo_2026` |

## Pruebas

```powershell
docker compose exec dagster python -m unittest discover -s tests -v
```

Consultas de ejemplo: [sql/consultas.sql](sql/consultas.sql).

CI/CD con TeamCity: [configuracion y pasos](docs/teamcity.md).
