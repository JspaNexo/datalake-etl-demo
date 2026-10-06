"""Guarda la identidad de la imagen validada como artefacto de TeamCity."""

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def prepare_release(image: str, directory: Path) -> None:
    result = subprocess.run(["docker", "image", "inspect", image, "--format", "{{json .}}"],
                            check=True, capture_output=True, text=True, encoding="utf-8")
    details = json.loads(result.stdout)
    revision = os.getenv("CI_GIT_REVISION")
    if not revision:
        revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
                                  capture_output=True, text=True).stdout.strip()
    try:
        dirty = bool(subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, check=True,
                                   capture_output=True, text=True).stdout.strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        # TeamCity puede entregar el checkout sin el directorio .git.
        dirty = None
    summary_path = ROOT / "artifacts" / "integration" / "summary.json"
    integration = json.loads(summary_path.read_text(encoding="utf-8"))
    if integration.get("status") != "passed" or integration.get("image_id") != details["Id"]:
        raise ValueError("La imagen de entrega debe ser exactamente la que paso la integracion")
    directory.mkdir(parents=True, exist_ok=True)
    metadata = {
        "image": image, "image_id": details["Id"], "repo_digests": details.get("RepoDigests", []),
        "git_revision": revision, "working_tree_dirty": dirty, "build_number": os.getenv("BUILD_NUMBER", "local"),
        "created_utc": datetime.now(timezone.utc).isoformat(), "integration": integration,
    }
    (directory / "image.txt").write_text(image + "\n", encoding="utf-8")
    (directory / "release.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"Descriptor de entrega generado: {directory / 'release.json'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=os.getenv("APP_IMAGE_BUILD_NUMBER"))
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts" / "release")
    args = parser.parse_args()
    if not args.image:
        parser.error("Indica --image o APP_IMAGE_BUILD_NUMBER")
    prepare_release(args.image, args.output.resolve())


if __name__ == "__main__":
    main()
