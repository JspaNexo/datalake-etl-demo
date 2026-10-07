"""Artefactos inmutables en S3 y serialización de las etapas."""

import hashlib
import json
import re
from dataclasses import asdict
from decimal import Decimal

from datalake_demo.config.settings import Settings
from datalake_demo.etl.models import (
    Artifact, BronzeResult, GoldResult, PreparedPublication, SilverResult, gold_from_dict,
)
from datalake_demo.infra.csv_source import read_sales


class MinioStorage:
    def __init__(self, settings: Settings):
        import boto3
        from botocore.config import Config

        self.bucket = settings.minio_bucket
        self.client = boto3.client(
            "s3", endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_user, aws_secret_access_key=settings.minio_password,
            region_name="us-east-1", config=Config(s3={"addressing_style": "path"}),
        )

    def ensure_bucket(self):
        from botocore.exceptions import ClientError
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"404", "NoSuchBucket", "NotFound"}:
                raise
            try:
                self.client.create_bucket(Bucket=self.bucket)
            except ClientError as race:
                if race.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                    raise

    def get_bytes(self, key: str) -> bytes:
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
        try:
            return body.read()
        finally:
            body.close()

    def _put(self, key: str, payload: bytes, content_type: str) -> Artifact:
        from botocore.exceptions import ClientError
        digest = hashlib.sha256(payload).hexdigest()
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=payload,
                                   ContentType=content_type, IfNoneMatch="*")
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
                raise
            if self.get_bytes(key) != payload:
                raise ValueError(f"Conflicto con artefacto inmutable: {key}") from error
        return Artifact(key, digest)

    def _verified(self, artifact: Artifact) -> bytes:
        payload = self.get_bytes(artifact.key)
        if hashlib.sha256(payload).hexdigest() != artifact.sha256:
            raise ValueError(f"Hash incorrecto: {artifact.key}")
        return payload

    @staticmethod
    def _key(execution, layer, filename):
        for value in (execution.dataset, execution.execution_id):
            if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
                raise ValueError("Identificador de artefacto invalido")
        return f"{layer}/{execution.dataset}/runs/{execution.execution_id}/{filename}"

    def _json(self, key, value):
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return self._put(key, payload, "application/json")

    def get_json(self, key):
        return json.loads(self.get_bytes(key))

    @staticmethod
    def parquet_schema(layer):
        import pyarrow as pa
        dimensions = [("fecha", pa.date32()), ("ciudad", pa.string()), ("producto", pa.string())]
        if layer == "silver":
            return pa.schema([("venta_id", pa.int32()), *dimensions, ("cantidad", pa.int32()),
                              ("precio_unitario", pa.decimal128(18, 2)), ("importe", pa.decimal128(18, 2))])
        if layer == "gold":
            return pa.schema([*dimensions, ("ventas", pa.int64()), ("unidades", pa.int64()),
                              ("ingresos", pa.decimal128(18, 2))])
        raise ValueError(f"Capa desconocida: {layer}")

    def _parquet(self, key, rows, layer):
        import pyarrow as pa
        import pyarrow.parquet as pq
        sink = pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pylist(rows, schema=self.parquet_schema(layer)), sink, compression="snappy")
        return self._put(key, sink.getvalue().to_pybytes(), "application/octet-stream")

    @staticmethod
    def _decode_parquet(payload):
        import pyarrow as pa
        import pyarrow.parquet as pq
        return pq.read_table(pa.BufferReader(payload)).to_pylist()

    def get_parquet(self, key):
        return self._decode_parquet(self.get_bytes(key))

    def write_bronze(self, execution, snapshot):
        self.ensure_bucket()
        key = f"bronze/{execution.dataset}/{snapshot.sha256}.csv"
        artifact = self._put(key, snapshot.payload, "text/csv; charset=utf-8")
        return BronzeResult(execution, artifact, len(snapshot.rows))

    def read_bronze(self, bronze):
        return read_sales(self._verified(bronze.artifact))

    def write_silver(self, bronze, sales, rejected, quality):
        execution = bronze.execution
        a = self._parquet(self._key(execution, "silver", "ventas_limpias.parquet"), sales, "silver")
        serialized_rejected = [{"linea_csv": r["registro"] + 1, "datos": r["datos"], "motivo": r["motivo"]}
                               for r in rejected]
        b = self._json(self._key(execution, "silver", "rechazados.json"), serialized_rejected)
        c = self._json(self._key(execution, "silver", "calidad.json"), quality)
        return SilverResult(bronze, a, b, c, len(sales), len(rejected))

    def read_silver(self, silver):
        return self._decode_parquet(self._verified(silver.sales))

    def write_gold(self, silver, rows):
        artifact = self._parquet(self._key(silver.bronze.execution, "gold", "ventas_diarias.parquet"), rows, "gold")
        revenue = str(sum((row["ingresos"] for row in rows), Decimal("0.00")))
        return GoldResult(silver, artifact, len(rows), revenue)

    def prepare(self, gold):
        # Un manifiesto solo existe después de haber comprobado todos sus artefactos.
        for artifact in (gold.silver.bronze.artifact, gold.silver.sales, gold.silver.rejected,
                         gold.silver.quality, gold.artifact):
            self._verified(artifact)
        manifest = self._json(self._key(gold.execution, "gold", "manifest.json"),
                              {"schema_version": 1, "result": asdict(gold)})
        return PreparedPublication(gold, manifest)

    def read_prepared(self, manifest):
        value = json.loads(self._verified(manifest))
        if value["schema_version"] != 1:
            raise ValueError("Version de manifiesto desconocida")
        return PreparedPublication(gold_from_dict(value["result"]), manifest)

    def read_publication(self, prepared):
        if self.read_prepared(prepared.manifest) != prepared:
            raise ValueError("El manifiesto no corresponde al resultado solicitado")
        gold = prepared.gold
        self._verified(gold.silver.bronze.artifact)
        self._verified(gold.silver.sales)
        self._verified(gold.silver.rejected)
        rows = self._decode_parquet(self._verified(gold.artifact))
        quality = json.loads(self._verified(gold.silver.quality))
        if len(rows) != gold.groups or str(sum((r["ingresos"] for r in rows), Decimal("0.00"))) != gold.revenue_bs:
            raise ValueError("El manifiesto no concilia con Gold")
        if (quality["archivo_bronze"] != gold.silver.bronze.artifact.key
                or quality["filas_bronze"] != gold.silver.bronze.rows
                or quality["filas_silver"] != gold.silver.rows
                or quality["filas_rechazadas"] != gold.silver.rejected_rows
                or quality["filas_bronze"] != quality["filas_silver"] + quality["filas_rechazadas"]):
            raise ValueError("La calidad no concilia con el snapshot")
        return rows, quality
