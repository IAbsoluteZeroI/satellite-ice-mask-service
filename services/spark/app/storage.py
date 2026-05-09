import os
import json
import re
from datetime import datetime
from typing import List

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from config import AppConfig


def build_s3_client(config: AppConfig) -> BaseClient:
    """Создаёт клиент для работы с MinIO через S3-совместимый API."""
    return boto3.client(
        "s3",
        endpoint_url=config.minio_endpoint,
        aws_access_key_id=config.minio_access_key,
        aws_secret_access_key=config.minio_secret_key,
    )


def configure_spark_for_minio(spark, config: AppConfig) -> None:
    """Передаёт Spark настройки доступа к MinIO через S3A."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", config.minio_endpoint)
    hadoop_conf.set("fs.s3a.access.key", config.minio_access_key)
    hadoop_conf.set("fs.s3a.secret.key", config.minio_secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")


def ensure_bucket_exists(client: BaseClient, bucket_name: str) -> None:
    """Создаёт бакет для снимков, если он ещё не был создан."""
    existing_buckets = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    if bucket_name not in existing_buckets:
        client.create_bucket(Bucket=bucket_name)


def list_raw_images(client: BaseClient, config: AppConfig) -> List[str]:
    """Возвращает список снимков из каталога raw/."""
    response = client.list_objects_v2(Bucket=config.minio_bucket, Prefix=config.raw_prefix)
    contents = response.get("Contents", [])

    object_keys = []
    for item in contents:
        object_key = item["Key"]
        if object_key.endswith("/"):
            continue
        object_keys.append(object_key)

    return object_keys


def download_object(client: BaseClient, bucket_name: str, object_key: str, target_path: str) -> None:
    """Скачивает объект из MinIO во временный локальный файл."""
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    client.download_file(bucket_name, object_key, target_path)


def upload_file(client: BaseClient, bucket_name: str, object_key: str, source_path: str) -> None:
    """Загружает локальный файл в MinIO."""
    client.upload_file(source_path, bucket_name, object_key)


def safe_object_name(object_key: str) -> str:
    """Преобразует ключ объекта в безопасное имя для служебных файлов."""
    name = os.path.splitext(os.path.basename(object_key))[0]
    return re.sub(r"[^A-Za-z0-9_-]+", "_", name).strip("_") or "source"


def processed_marker_key(config: AppConfig, object_key: str) -> str:
    """Возвращает путь маркера обработки снимка в MinIO."""
    return f"{config.status_prefix}{safe_object_name(object_key)}.json"


def has_processed_marker(client: BaseClient, config: AppConfig, object_key: str) -> bool:
    """Проверяет, есть ли в MinIO маркер уже обработанного снимка."""
    try:
        client.head_object(Bucket=config.minio_bucket, Key=processed_marker_key(config, object_key))
        return True
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def save_processed_marker(
    client: BaseClient,
    config: AppConfig,
    object_key: str,
    status: str,
    shapefile_keys: List[str],
) -> None:
    """Сохраняет в MinIO служебный маркер результата обработки."""
    payload = {
        "object_key": object_key,
        "status": status,
        "shapefile_keys": shapefile_keys,
        "processed_at": datetime.utcnow().isoformat() + "Z",
    }
    client.put_object(
        Bucket=config.minio_bucket,
        Key=processed_marker_key(config, object_key),
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def sync_local_data_to_minio(client: BaseClient, config: AppConfig) -> int:
    """
    Загружает локальные GeoTIFF-файлы из каталога data в MinIO.

    Уже существующие объекты повторно не загружает.
    """
    if not os.path.isdir(config.local_input_dir):
        return 0

    existing_keys = set(list_raw_images(client, config))
    uploaded_count = 0

    for root, _, files in os.walk(config.local_input_dir):
        for filename in files:
            if not filename.lower().endswith((".tif", ".tiff")):
                continue

            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, config.local_input_dir).replace("\\", "/")
            object_key = f"{config.raw_prefix}{relative_path}"

            if object_key in existing_keys:
                continue

            upload_file(client, config.minio_bucket, object_key, local_path)
            existing_keys.add(object_key)
            uploaded_count += 1

    return uploaded_count
