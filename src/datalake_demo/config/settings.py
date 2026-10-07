"""Todas las variables de entorno se resuelven al iniciar un job."""

import os
from dataclasses import dataclass, field, replace
from pathlib import Path

@dataclass(frozen=True)
class Settings:
    app_name: str = "datalake_demo"
    app_env: str = "demo"
    source_path: Path = Path("data/ventas.csv")
    source_type: str = "csv"
    dataset: str = "ventas"
    source_postgres_host: str = "localhost"
    source_postgres_port: int = 5433
    source_postgres_db: str = "ventas_origen"
    source_postgres_user: str = "operacion"
    source_postgres_password: str = field(default="operacion_demo_2026", repr=False)
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

    def __post_init__(self):
        if (self.source_type, self.dataset) not in {("csv", "ventas"), ("postgres", "ventas_db")}:
            raise ValueError("La fuente y el dataset deben corresponder al perfil CSV o PostgreSQL")

    def for_database_source(self) -> "Settings":
        return replace(self, source_type="postgres", dataset="ventas_db")

    @property
    def gold_table(self) -> str:
        return "ventas_db_diarias" if self.dataset == "ventas_db" else "ventas_diarias"

    @property
    def quality_table(self) -> str:
        return "calidad_db" if self.dataset == "ventas_db" else "calidad"

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
            source_postgres_host=os.getenv("SOURCE_POSTGRES_HOST", "localhost"),
            source_postgres_port=int(os.getenv("SOURCE_POSTGRES_PORT", "5433")),
            source_postgres_db=os.getenv("SOURCE_POSTGRES_DB", "ventas_origen"),
            source_postgres_user=os.getenv("SOURCE_POSTGRES_USER", "operacion"),
            source_postgres_password=os.getenv("SOURCE_POSTGRES_PASSWORD", "operacion_demo_2026"),
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
