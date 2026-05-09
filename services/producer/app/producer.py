import os
import time
import json

import boto3
from botocore.client import Config
from kafka import KafkaProducer


FOLDER = os.getenv("LOCAL_INPUT_DIR", "/data")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "2"))
BUCKET_NAME = os.getenv("MINIO_BUCKET", "satellite-images")
RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/")
REMOVE_AFTER_UPLOAD = os.getenv("REMOVE_AFTER_UPLOAD", "true").lower() == "true"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-images")


def build_s3_client():
    """Создаёт клиент для загрузки файлов в MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def build_kafka_producer():
    """Создаёт Kafka producer для уведомлений о новых снимках."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )


def send_image_event(producer, object_key: str, filename: str) -> None:
    """Отправляет в Kafka событие о новом объекте MinIO."""
    producer.send(
        KAFKA_TOPIC,
        {
            "bucket": BUCKET_NAME,
            "object_key": object_key,
            "filename": filename,
        },
    )
    producer.flush()
    print(f"[producer] Событие отправлено в Kafka: topic={KAFKA_TOPIC}, object_key={object_key}")


def ensure_bucket(client, bucket_name):
    """Создаёт бакет в MinIO, если его ещё нет."""
    buckets = [bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])]

    if bucket_name not in buckets:
        print(f"Бакет '{bucket_name}' не найден. Создаю...")
        client.create_bucket(Bucket=bucket_name)
        print(f"Бакет '{bucket_name}' создан.")
    else:
        print(f"Бакет '{bucket_name}' уже существует.")


def is_geotiff(filename):
    """Пропускает к загрузке только GeoTIFF-файлы."""
    return filename.lower().endswith((".tif", ".tiff"))


def list_existing_keys(client, bucket_name, prefix):
    """Читает уже загруженные объекты, чтобы не отправлять дубликаты."""
    response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return {item["Key"] for item in response.get("Contents", [])}


def upload_new_files():
    """Следит за локальной папкой и загружает новые файлы в MinIO."""
    os.makedirs(FOLDER, exist_ok=True)
    print("Producer запущен.")
    print(f"Каталог наблюдения: {FOLDER}")
    print(f"Интервал проверки: {CHECK_INTERVAL} сек.")
    print(f"Бакет MinIO: {BUCKET_NAME}")
    print(f"Префикс загрузки: {RAW_PREFIX}")
    print(f"Удалять локальный файл после загрузки: {REMOVE_AFTER_UPLOAD}")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")

    client = build_s3_client()
    kafka_producer = build_kafka_producer()
    ensure_bucket(client, BUCKET_NAME)

    processed = set()
    cycle_number = 0

    while True:
        try:
            cycle_number += 1
            existing_keys = list_existing_keys(client, BUCKET_NAME, RAW_PREFIX)
            filenames = os.listdir(FOLDER)

            print(
                f"[producer] Цикл {cycle_number}: "
                f"файлов в каталоге = {len(filenames)}, "
                f"объектов в raw = {len(existing_keys)}"
            )

            for filename in filenames:
                local_path = os.path.join(FOLDER, filename)

                if not is_geotiff(filename):
                    print(f"[producer] Пропуск {filename}: не GeoTIFF.")
                    continue
                if not os.path.isfile(local_path):
                    print(f"[producer] Пропуск {filename}: это не обычный файл.")
                    continue
                if local_path in processed:
                    print(f"[producer] Пропуск {filename}: уже был обработан ранее.")
                    continue

                object_key = f"{RAW_PREFIX}{filename}"
                if object_key in existing_keys:
                    print(f"[producer] Пропуск {filename}: уже есть в MinIO как {object_key}.")
                    send_image_event(kafka_producer, object_key, filename)
                    if REMOVE_AFTER_UPLOAD:
                        os.remove(local_path)
                        print(f"[producer] Локальный дубликат удалён: {filename}")
                    processed.add(local_path)
                    continue

                try:
                    print(f"[producer] Загружаю {filename} в {object_key}...")
                    client.upload_file(local_path, BUCKET_NAME, object_key)
                    print(f"[producer] Файл загружен в MinIO: {filename}")
                    send_image_event(kafka_producer, object_key, filename)

                    if REMOVE_AFTER_UPLOAD:
                        os.remove(local_path)
                        print(f"[producer] Локальный файл удалён после загрузки: {filename}")

                    processed.add(local_path)
                except Exception as error:
                    print(f"[producer] Ошибка загрузки {filename}: {error}")

            time.sleep(CHECK_INTERVAL)
        except Exception as error:
            print(f"[producer] Ошибка watcher-сервиса: {error}")
            time.sleep(5)


if __name__ == "__main__":
    upload_new_files()
