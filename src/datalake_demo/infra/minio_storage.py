"""Detalles S3 y serializacion de archivos; no contiene reglas de ventas."""

import json

from datalake_demo.config.settings import Settings


class MinioStorage:
    def __init__(self, settings: Settings):
        import boto3
        from botocore.config import Config

        self.bucket = settings.minio_bucket
        self.client = boto3.client(
            "s3", endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_user,
            aws_secret_access_key=settings.minio_password,
            region_name="us-east-1", config=Config(s3={"addressing_style": "path"}),
        )

    def ensure_bucket(self):
        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"404", "NoSuchBucket", "NotFound"}:
                raise
            self.client.create_bucket(Bucket=self.bucket)

    def put_bytes(self, key: str, payload: bytes, content_type: str):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=payload, ContentType=content_type)

    def put_original(self, key: str, payload: bytes):
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"404", "NoSuchKey", "NotFound"}:
                raise
            self.put_bytes(key, payload, "text/csv; charset=utf-8")

    def get_bytes(self, key: str) -> bytes:
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
        try:
            return body.read()
        finally:
            body.close()

    def put_json(self, key: str, value):
        payload = json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
        self.put_bytes(key, payload, "application/json")

    def get_json(self, key: str):
        return json.loads(self.get_bytes(key))

    @staticmethod
    def parquet_schema(layer: str):
        import pyarrow as pa

        dimensions = [("fecha", pa.date32()), ("ciudad", pa.string()), ("producto", pa.string())]
        if layer == "silver":
            return pa.schema([("venta_id", pa.int32()), *dimensions, ("cantidad", pa.int32()),
                              ("precio_unitario", pa.decimal128(18, 2)), ("importe", pa.decimal128(18, 2))])
        if layer == "gold":
            return pa.schema([*dimensions, ("ventas", pa.int64()), ("unidades", pa.int64()),
                              ("ingresos", pa.decimal128(18, 2))])
        raise ValueError(f"Capa Parquet desconocida: {layer}")

    def put_parquet(self, key: str, rows: list[dict], layer: str):
        import pyarrow as pa
        import pyarrow.parquet as pq

        sink = pa.BufferOutputStream()
        table = pa.Table.from_pylist(rows, schema=self.parquet_schema(layer))
        pq.write_table(table, sink, compression="snappy")
        self.put_bytes(key, sink.getvalue().to_pybytes(), "application/octet-stream")

    def get_parquet(self, key: str) -> list[dict]:
        import pyarrow as pa
        import pyarrow.parquet as pq

        return pq.read_table(pa.BufferReader(self.get_bytes(key))).to_pylist()

