from pathlib import Path


def ensure_directory(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def read_file(path: str | Path) -> bytes:
    return Path(path).read_bytes()

