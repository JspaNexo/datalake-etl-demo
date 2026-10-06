"""Todas las variables de entorno se resuelven al iniciar un job."""

import os
from dataclasses import dataclass, field
from pathlib import Path

SILVER_KEY = "silver/ventas/ventas_limpias.parquet"
REJECTED_KEY = "silver/ventas/rechazados.json"
QUALITY_KEY = "silver/ventas/calidad.json"
GOLD_KEY = "gold/ventas/ventas_diarias.parquet"


@dataclass(frozen=True)
class Settings:
    app_name: str = "datalake_demo"
    app_env: str = "demo"
    source_path: Path = Path("data/ventas.csv")
    log_dir: Path = Path("logs")
    log_file_name: str = "ventas_etl.log"
    log_level: str = "INFO"
    seq_url: str = "http://localhost:5341"
    seq_api_key: str = field(default="", repr=False)
    seq_timeout_seconds: float = 5.0
    minio_endpoint: str = "http://localhost:9000"
    minio_user: str = "minio_demo"
    minio_password: str = field(default="minio_demo_2026", repr=False)
    minio_bucket: str = "datalake"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "ventas"
    postgres_user: str = "demo"
    postgres_password: str = field(default="postgres_demo_2026", repr=False)

    @property
    def log_file_path(self) -> Path:
        return self.log_dir / self.log_file_name

    @staticmethod
    def from_env() -> "Settings":
        from dotenv import load_dotenv

        load_dotenv(Path.cwd() / ".env", override=False)
        return Settings(
            app_name=os.getenv("APP_NAME", "datalake_demo"),
            app_env=os.getenv("APP_ENV", "demo"),
            source_path=Path(os.getenv("SOURCE_PATH", "data/ventas.csv")),
            log_dir=Path(os.getenv("LOG_DIR", "logs")),
            log_file_name=os.getenv("LOG_FILE_NAME", "ventas_etl.log"),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            seq_url=os.getenv("SEQ_URL", "http://localhost:5341"),
            seq_api_key=os.getenv("SEQ_API_KEY", ""),
            seq_timeout_seconds=float(os.getenv("SEQ_TIMEOUT_SECONDS", "5")),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_user=os.getenv("MINIO_ROOT_USER", "minio_demo"),
            minio_password=os.getenv("MINIO_ROOT_PASSWORD", "minio_demo_2026"),
            minio_bucket=os.getenv("MINIO_BUCKET", "datalake"),
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "ventas"),
            postgres_user=os.getenv("POSTGRES_USER", "demo"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "postgres_demo_2026"),
        )
