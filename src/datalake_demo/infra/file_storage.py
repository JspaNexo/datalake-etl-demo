import hashlib
from pathlib import Path


def ensure_directory(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def read_file(path: str | Path) -> bytes:
    return Path(path).read_bytes()


def stable_file_sha256(path: str | Path) -> str:
    """Detecta un archivo modificado mientras se calcula su huella."""
    source = Path(path)
    before = source.stat()
    payload = source.read_bytes()
    after = source.stat()
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
        raise OSError("El archivo cambio durante la lectura")
    return hashlib.sha256(payload).hexdigest()
