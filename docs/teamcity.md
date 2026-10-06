# TeamCity: DataLake Build Pack Docker

Configuracion para el flujo **Python Tests -> Docker Build -> Integration Tests -> Docker Push -> Prepare Release Files**. El paso push esta desactivado por defecto y `env.PUBLISH_IMAGE=false`.

El proyecto local es [datalake-etl-demo](http://localhost:8111/buildConfiguration/DatalakeEtlDemo_Build). Sus pasos estan configurados desde TeamCity; los cambios al archivo Kotlin no se aplican automaticamente mientras Versioned Settings este desactivado.

El agente Windows local utiliza `C:/TeamCity/tools/python312/tools/python.exe`. Docker y Git estan en su PATH, y `env.DOCKER_CONFIG` apunta a `C:/TeamCity/buildAgent/conf/docker-cli` para localizar los plugins de Docker Desktop. Esta configuracion pertenece al agente, no al contenedor ETL.

El runner Docker nativo local encuentra una copia del CLI en `C:/TeamCity/buildAgent/bin/docker.exe`, verificada contra la instalacion de Docker Desktop. Si actualizas Docker Desktop, actualiza tambien esta copia. El checkout actual se realiza en el servidor.

## Requisitos del agente

- Windows o Linux, Python 3.12 con `venv` y `pip`.
- Docker con contenedores Linux y el plugin `docker compose` V2.
- Acceso a GitHub y a los repositorios de dependencias e imagenes publicas.

## Configurar desde la interfaz

Conecta el proyecto a su repositorio GitHub. Los siguientes pasos usan la raiz del checkout como directorio de trabajo y **Only if build status is successful** como politica de ejecucion.

| Step | Runner | Configuracion |
|---|---|---|
| Python Tests | Python | Python 3.12; Unittest; argumentos `discover -s tests -v`; Test reporting activado; `PYTHONPATH` apunta a `src`. Usa adaptadores en memoria. |
| Docker Build ETL | Docker | Build; archivo `Dockerfile`; contexto `.`; plataforma Linux; imagen `%env.APP_IMAGE_BUILD_NUMBER%`. |
| Integration Tests | Python | File `scripts/ci/run_integration.py`; Python 3.12; sin entorno virtual ni dependencias adicionales. |
| Docker Push Staging | Docker | Push de `%env.APP_IMAGE_BUILD_NUMBER%`; conservar la imagen en el agente; condiciones `env.PUBLISH_IMAGE=true` y `teamcity.build.branch.is_default=true`. |
| Prepare Release Files | Python | File `scripts/ci/prepare_release.py`; Python 3.12; sin entorno virtual ni dependencias adicionales. |

Parametros de la build configuration:

| Parametro | Valor inicial |
|---|---|
| `env.PYTHON_EXECUTABLE` | `python3.12` o ruta del interprete en el agente |
| `env.PYTHONPATH` | `%teamcity.build.checkoutDir%/src` |
| `env.APP_IMAGE_BUILD_NUMBER` | `datalake-etl-demo:%build.number%` |
| `env.CI_PROJECT_NAME` | `datalake-ci-%build.counter%` |
| `env.CI_GIT_REVISION` | `%build.vcs.number%` |
| `env.PUBLISH_IMAGE` | `false` |

Artifact paths:

```text
artifacts/integration/** => integration
artifacts/release/** => release
```

Configura un VCS trigger para ejecutar el flujo con cada cambio. El repositorio utiliza actualmente `main`; las pruebas y el build pueden ejecutarse en todas las ramas, y el push solo en la rama predeterminada. Para publicar desde `develop`, cambia la condicion del push a `teamcity.build.branch=develop` y configura esa rama en el VCS root.

Las pruebas unitarias usan la biblioteca estandar y adaptadores en memoria. Las dependencias de la aplicacion se instalan en el Dockerfile y se comprueban al ejecutar la integracion con la imagen construida.

## Usar configuracion como codigo

[`.teamcity/settings.kts`](../.teamcity/settings.kts) contiene la misma configuracion en Kotlin DSL y reutiliza el VCS root conectado al proyecto mediante `DslContext.settingsRoot`.

Para importarla, activa **Versioned Settings -> Kotlin** en un proyecto dedicado a esta demo y selecciona el repositorio de este proyecto como origen. Conserva la version y el `pom.xml` exportados por tu servidor si su version difiere de `2026.2`. El ID portable `Build` corresponde al build type `DatalakeEtlDemo_Build` del proyecto local; su aplicacion en el servidor queda a cargo del administrador del proyecto.

## Publicar la imagen

1. Cambia `env.APP_IMAGE_BUILD_NUMBER` por la imagen completa del registry real, por ejemplo `registry.example.com/datalake-etl-demo:%build.number%`.
2. En TeamCity configura **Project Settings -> Connections -> Docker Registry** con las credenciales del registry y agrega el build feature **Docker Registry Connections** al build type. Las credenciales se almacenan en TeamCity.
3. Habilita el paso Docker Push Staging y activa `env.PUBLISH_IMAGE=true` cuando la conexion y la rama de publicacion esten definidas. Agrega sus condiciones de ejecucion indicadas en la tabla. En configuracion Kotlin, cambia `enabled` a `true` y agrega el build feature de conexion usando el ID exportado por tu servidor.

Se publica la misma imagen que paso la integracion. `Prepare Release Files` guarda `image.txt` y `release.json`, con su ID, digests disponibles y revision Git. Estos son descriptores de entrega; el flujo todavia no realiza un despliegue a Compose o Swarm.

## Integracion aislada

El script utiliza `compose.ci.yaml`, con un nombre unico `datalake-ci-...`, credenciales ficticias y volumenes propios. La imagen ETL no monta el codigo ni el CSV del host: verifica los archivos incluidos en la imagen construida. No publica puertos.

Ejecuta el job Dagster dos veces, verifica MinIO/PostgreSQL y comprueba que Seq haya recibido las cuatro etapas de ambas ejecuciones. Guarda logs e informe en `artifacts/integration/`. Al finalizar, tambien en caso de error, elimina exclusivamente los recursos de ese proyecto de pruebas. El identificador no puede ser el de la demo y se rechazan volumenes externos, montajes del host y puertos publicados.

Para probarlo localmente despues de construir la imagen:

```powershell
docker build -t datalake-etl-demo:ci-test .
python scripts/ci/run_integration.py --image datalake-etl-demo:ci-test
python scripts/ci/prepare_release.py --image datalake-etl-demo:ci-test
```

El primer arranque de CI puede compilar MinIO; el cache de Docker se reutiliza en las siguientes ejecuciones.

Referencias: [Python runner](https://www.jetbrains.com/help/teamcity/python.html), [Docker runner](https://www.jetbrains.com/help/teamcity/docker.html), [Kotlin DSL](https://www.jetbrains.com/help/teamcity/kotlin-dsl.html).
