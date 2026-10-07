# TeamCity

Pipeline de integración continua del proyecto: prueba el código, construye Docker y verifica el ETL de CSV y PostgreSQL.

## Requisitos

- Agente con Python 3.12, Docker con contenedores Linux y Docker Compose.
- Acceso al repositorio GitHub y a las dependencias del proyecto.

## Pasos

| Paso | Runner | Función |
|---|---|---|
| Python Tests | Python | Ejecuta `unittest discover -s tests -v`, incluidas las pruebas de arquitectura, con `PYTHONPATH` apuntando a `src`. |
| Docker Build ETL | Docker | Construye el `Dockerfile` con la etiqueta `%env.APP_IMAGE_BUILD_NUMBER%`. |
| Integration Tests | Python | Ejecuta `scripts/ci/run_integration.py`. |
| Docker Push Staging | Docker | Publica la imagen; está desactivado hasta configurar un registry. |
| Prepare Release Files | Python | Ejecuta `scripts/ci/prepare_release.py` y genera los archivos de entrega. |

El VCS trigger inicia el flujo al recibir cambios en GitHub. Los pasos posteriores se omiten si falla uno anterior.

## Resultados

La integración ejecuta dos veces cada fuente, verifica MinIO/PostgreSQL y comprueba 16 eventos de etapas en Seq. Usa el CSV fijo de `tests/fixtures/` y añade pruebas de concurrencia, fallos y reejecución parcial. Los contenedores y volúmenes temporales se eliminan al finalizar.

Configura estos **Artifact paths** para consultar los informes:

```text
artifacts/integration/** => integration
artifacts/release/** => release
```

## Configuración

Proyecto local: [DataLake - Build Pack Docker](http://localhost:8111/buildConfiguration/DatalakeEtlDemo_Build).

La referencia de pasos y parámetros está en [settings.kts](../.teamcity/settings.kts). Sus cambios se sincronizan con el servidor al habilitar **Versioned Settings → Kotlin**.

El agente Windows local utiliza `C:/TeamCity/tools/python312/tools/python.exe` y el CLI Docker en `C:/TeamCity/buildAgent/bin/docker.exe`.

Para publicar, configura la conexión al registry, usa su nombre completo en `env.APP_IMAGE_BUILD_NUMBER`, habilita **Docker Push Staging** y cambia `env.PUBLISH_IMAGE=true`. Conserva las condiciones de publicación de `settings.kts`. El despliegue automático queda pendiente.
