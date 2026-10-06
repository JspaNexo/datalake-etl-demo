"""Prueba la imagen del ETL en un Compose aislado y elimina sus recursos al terminar."""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "compose.ci.yaml"


def validate_project_name(project: str) -> None:
    if not re.fullmatch(r"datalake-ci-[a-z0-9][a-z0-9_-]{0,63}", project):
        raise ValueError("El proyecto de pruebas debe empezar por datalake-ci- y tener un identificador propio")


def validate_compose_config(config: dict, project: str) -> None:
    validate_project_name(project)
    if config.get("name") != project:
        raise ValueError("Compose no esta usando el proyecto de CI solicitado")
    for service in config.get("services", {}).values():
        if service.get("ports"):
            raise ValueError("El Compose de CI no debe publicar puertos del host")
        if any(mount.get("type") == "bind" for mount in service.get("volumes", [])):
            raise ValueError("El Compose de CI no debe montar archivos del host")
    for category in ("volumes", "networks"):
        for resource in config.get(category, {}).values():
            if resource.get("external") or not resource.get("name", "").startswith(project + "_"):
                raise ValueError(f"Los recursos de {category} deben pertenecer exclusivamente al proyecto de CI")


def validate_seq_events(events: list[dict]) -> dict:
    groups = {}
    for event in events:
        if event.get("Application") != "datalake_demo_ci":
            continue
        correlation = event.get("CorrelationId")
        if not correlation:
            raise ValueError("Un evento del ETL no tiene CorrelationId")
        if event.get("@l") in {"Error", "Fatal"}:
            raise ValueError("Seq recibio un error durante la integracion")
        groups.setdefault(correlation, {})[event.get("Stage")] = event
    expected_stages = {"bronze", "silver", "gold", "sql"}
    if len(groups) != 2 or any(set(stages) != expected_stages for stages in groups.values()):
        raise ValueError("Seq debe contener las cuatro etapas de las dos ejecuciones de Dagster")
    for stages in groups.values():
        if stages["silver"].get("ValidRows") != 8 or stages["silver"].get("RejectedRows") != 6:
            raise ValueError("Las metricas silver de Seq no coinciden con los datos de prueba")
        if stages["gold"].get("RevenueBs") != "356.00":
            raise ValueError("Los ingresos gold de Seq no coinciden con los datos de prueba")
    return {"runs": sorted(groups), "stages_per_run": 4, "seq_events": len(events)}


def execute(command: list[str], env: dict, *, log_file: Path | None = None, check: bool = True):
    result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if log_file:
        log_file.write_text(result.stdout + result.stderr, encoding="utf-8")
        print(result.stdout + result.stderr, end="", flush=True)
    if check and result.returncode:
        raise RuntimeError(f"Fallo el comando {command[0]} (codigo {result.returncode}):\n{result.stdout}{result.stderr}")
    return result


def assert_project_is_unused(project: str, env: dict) -> None:
    selector = f"label=com.docker.compose.project={project}"
    commands = [
        ["docker", "ps", "-aq", "--filter", selector],
        ["docker", "volume", "ls", "-q", "--filter", selector],
        ["docker", "network", "ls", "-q", "--filter", selector],
    ]
    if any(execute(command, env).stdout.strip() for command in commands):
        raise ValueError("El identificador de CI ya tiene recursos; utiliza otro para no reutilizar datos existentes")


def run_integration(image: str, project: str, artifacts: Path) -> None:
    validate_project_name(project)
    if not image or image.startswith("-") or any(char.isspace() for char in image):
        raise ValueError("Indica un nombre de imagen Docker valido")
    artifacts.mkdir(parents=True, exist_ok=True)
    report_path = artifacts / "summary.json"
    report = {"status": "running", "image": image, "project": project}
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    env = dict(os.environ, CI_ETL_IMAGE=image)
    compose = ["docker", "compose", "--project-name", project, "--file", str(COMPOSE_FILE)]
    config = json.loads(execute([*compose, "config", "--format", "json"], env).stdout)
    validate_compose_config(config, project)
    if config["services"]["etl"].get("image") != image or config["services"]["etl"].get("build"):
        raise ValueError("El contenedor ETL debe utilizar la imagen construida, sin volver a construirla")
    assert_project_is_unused(project, env)
    image_id = execute(["docker", "image", "inspect", image, "--format", "{{.Id}}"], env).stdout.strip()
    failure = None
    summary = None
    try:
        execute([*compose, "up", "-d", "--build", "--wait", "--wait-timeout", "180"], env,
                log_file=artifacts / "startup.log")
        container_id = execute([*compose, "ps", "-q", "etl"], env).stdout.strip()
        actual_image_id = execute(["docker", "inspect", container_id, "--format", "{{.Image}}"], env).stdout.strip()
        if actual_image_id != image_id:
            raise ValueError("El contenedor no esta ejecutando la imagen que se pretende verificar")
        package_version = execute([*compose, "exec", "-T", "etl", "python", "-c",
                             "import importlib.metadata; print(importlib.metadata.version('datalake-demo'))"], env).stdout.strip()
        print(f"Verificando paquete datalake-demo {package_version}; imagen {image_id}", flush=True)
        execute([*compose, "exec", "-T", "etl", "datalake-verify"], env, log_file=artifacts / "etl.log")
        deadline = time.monotonic() + 20
        while True:
            result = execute([*compose, "exec", "-T", "seq", "/seqsvr/Client/seqcli", "search",
                              "-s", "http://localhost:80", "-f", "Application = 'datalake_demo_ci'", "-c", "30", "--json"], env)
            events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
            try:
                summary = validate_seq_events(events)
                break
            except ValueError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        (artifacts / "seq-events.json").write_text(json.dumps(events, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        execute([*compose, "cp", "etl:/app/logs", str(artifacts / "app-logs")], env)
    except (Exception, KeyboardInterrupt) as error:
        failure = error
    finally:
        # La configuracion y el nombre se validaron antes de crear cualquier recurso.
        for command, filename in (
            ([*compose, "logs", "--no-color"], "services.log"),
            ([*compose, "down", "--volumes", "--remove-orphans"], "cleanup.log"),
        ):
            try:
                result = execute(command, env, log_file=artifacts / filename, check=False)
                if result.returncode and failure is None:
                    failure = RuntimeError(f"Fallo la recopilacion o limpieza de CI: {filename}")
            except Exception as error:
                if failure is None:
                    failure = error
    if failure is not None:
        report.update({"status": "failed", "error": str(failure)[:4000]})
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        raise RuntimeError(f"La integracion no se completo: {failure}") from failure
    summary.update({"image": image, "image_id": image_id, "project": project, "status": "passed"})
    report_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print("Integracion completada; recursos temporales eliminados.", flush=True)


def main() -> None:
    # La salida capturada de Docker puede incluir caracteres fuera de cp1252 en Windows.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=os.getenv("APP_IMAGE_BUILD_NUMBER"))
    parser.add_argument("--project", default=os.getenv("CI_PROJECT_NAME") or f"datalake-ci-{uuid4().hex}")
    parser.add_argument("--artifacts", type=Path, default=ROOT / "artifacts" / "integration")
    args = parser.parse_args()
    if not args.image:
        parser.error("Indica --image o APP_IMAGE_BUILD_NUMBER")
    run_integration(args.image, args.project, args.artifacts.resolve())


if __name__ == "__main__":
    main()
