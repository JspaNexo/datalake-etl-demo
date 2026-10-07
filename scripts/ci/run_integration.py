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


def validate_seq_events(events: list[dict], expected_by_source: dict[str, dict]) -> dict:
    groups = {}
    for event in events:
        if event.get("Application") != "datalake_demo_ci":
            continue
        correlation = event.get("CorrelationId")
        if not correlation or event.get("@l") in {"Error", "Fatal"}:
            raise ValueError("Seq recibio un error o un evento sin CorrelationId")
        stages = groups.setdefault(correlation, {})
        stage = event.get("Stage")
        if stage in stages:
            raise ValueError("Evento de etapa duplicado")
        stages[stage] = event
    expected_stages = {"bronze", "silver", "gold", "sql"}
    if len(groups) != 2 * len(expected_by_source) or any(set(stages) != expected_stages for stages in groups.values()):
        raise ValueError("Seq debe contener las cuatro etapas de dos ejecuciones por fuente")
    source_runs = dict.fromkeys(expected_by_source, 0)
    for correlation, stages in groups.items():
        first = stages["bronze"]
        source_type = first.get("SourceType")
        if source_type not in expected_by_source:
            raise ValueError("Fuente inesperada en Seq")
        identity = (source_type, first.get("Dataset"), first.get("ExecutionId"), first.get("Revision"))
        if not identity[2] or not isinstance(identity[3], int) or identity[3] < 1:
            raise ValueError("Falta identidad de la version")
        if identity[1] != ("ventas" if source_type == "csv" else "ventas_db"):
            raise ValueError("Dataset incorrecto en Seq")
        if any((e.get("SourceType"), e.get("Dataset"), e.get("ExecutionId"), e.get("Revision")) != identity
               for e in stages.values()):
            raise ValueError("Las etapas deben identificar la misma version y fuente")
        source_runs[source_type] += 1
        expected = expected_by_source[source_type]
        if expected.get("runs") and correlation not in expected["runs"]:
            raise ValueError("El evento no pertenece a las ejecuciones verificadas")
        if stages["silver"].get("ValidRows") != expected["valid_rows"] or stages["silver"].get("RejectedRows") != expected["rejected_rows"]:
            raise ValueError("Las metricas Silver no coinciden con el informe de integracion")
        if stages["gold"].get("RevenueBs") != expected["revenue_bs"]:
            raise ValueError("Los ingresos Gold no coinciden con el informe de integracion")
        if stages["sql"].get("PublicationStatus") != "published" or not stages["sql"].get("ManifestKey"):
            raise ValueError("La publicacion no fue confirmada")
    if any(count != 2 for count in source_runs.values()):
        raise ValueError("Se necesitan dos ejecuciones de cada fuente")
    return {"runs": sorted(groups), "stages_per_run": 4, "seq_events": len(events), "source_runs": source_runs}


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
        execute([*compose, 'exec', '-T', 'etl', 'python', '-m', 'datalake_demo.jobs.verify_ventas_db'], env,
                log_file=artifacts / 'etl-db.log')
        execute([*compose, 'cp', 'etl:/app/artifacts/db-summary.json', str(artifacts / 'db-summary.json')], env)
        database_expected = json.loads((artifacts / 'db-summary.json').read_text(encoding='utf-8'))
        execute([*compose, 'cp', 'etl:/app/artifacts/csv-summary.json', str(artifacts / 'csv-summary.json')], env)
        csv_expected = json.loads((artifacts / 'csv-summary.json').read_text(encoding='utf-8'))

        deadline = time.monotonic() + 20
        while True:
            result = execute([*compose, "exec", "-T", "seq", "/seqsvr/Client/seqcli", "search",
                              "-s", "http://localhost:80", "-f", "Application = 'datalake_demo_ci'", "-c", "30", "--json"], env)
            events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
            try:
                summary = validate_seq_events(events, {"csv": csv_expected, "postgres": database_expected})
                break
            except ValueError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        (artifacts / "seq-events.json").write_text(json.dumps(events, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

        execute([*compose, "exec", "-T", "etl", "python", "-B", "-m", "unittest", "discover",
                 "-s", "tests/integration", "-v"], env, log_file=artifacts / "regression.log")
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
