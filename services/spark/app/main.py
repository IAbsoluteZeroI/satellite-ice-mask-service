import os
import time
import zipfile

from pyspark.sql import SparkSession

from config import load_config
from db import (
    ensure_schema,
    get_already_processed_keys,
    get_sources_with_tiles,
    get_worker_name,
    log_processing_event,
    save_processing_result,
    save_tile_metadata,
)
from geotiff_processor import slice_geotiff
from kafka_client import poll_image_keys
from mask_assembler import create_mask_shapefile, safe_source_name, shapefile_exists
from storage import (
    build_s3_client,
    configure_spark_for_minio,
    download_object,
    ensure_bucket_exists,
    has_processed_marker,
    list_raw_images,
    save_processed_marker,
)


def build_spark_session():
    """Создаёт SparkSession для пакетной обработки снимков."""
    return SparkSession.builder.appName("SatelliteImageProcessingService").getOrCreate()


def register_app_modules(spark) -> None:
    """Передаёт Python-модули приложения Spark worker'ам."""
    app_dir = os.path.dirname(os.path.abspath(__file__))
    zip_path = os.path.join("/tmp", "satellite_app.zip")

    with zipfile.ZipFile(zip_path, "w") as archive:
        for filename in os.listdir(app_dir):
            if filename.endswith(".py"):
                archive.write(os.path.join(app_dir, filename), arcname=filename)

    spark.sparkContext.addPyFile(zip_path)


def report_processing_state(
    config,
    source_object_key: str,
    status: str,
    stage: str,
    details: str,
    processor_name: str = "geotiff_processor",
    result_path=None,
    progress_current=None,
    progress_total=None,
    level: str = "INFO",
) -> None:
    """Единая точка обновления статуса задачи и записи события обработки."""
    worker_name = get_worker_name()
    try:
        save_processing_result(
            config=config,
            object_key=source_object_key,
            status=status,
            processor_name=processor_name,
            result_path=result_path,
            details=details,
            stage=stage,
            worker_name=worker_name,
            progress_current=progress_current,
            progress_total=progress_total,
        )
        log_processing_event(
            config=config,
            object_key=source_object_key,
            stage=stage,
            message=details,
            level=level,
            progress_current=progress_current,
            progress_total=progress_total,
            worker_name=worker_name,
        )
    except Exception as error:
        # Статус нужен для мониторинга, но временная недоступность БД не должна
        # ломать саму обработку GeoTIFF и загрузку результатов в MinIO.
        print(
            f"Не удалось записать статус {stage} для {source_object_key}: {error}"
        )


def process_source_image(config, source_object_key: str) -> bool:
    """Обрабатывает один GeoTIFF и собирает shapefile только для него."""
    s3_client = build_s3_client(config)
    source_dir = os.path.join(config.work_dir, "source")
    os.makedirs(source_dir, exist_ok=True)

    if not source_object_key.lower().endswith((".tif", ".tiff")):
        report_processing_state(
            config=config,
            source_object_key=source_object_key,
            status="skipped",
            stage="validate_input",
            details="Файл пропущен: поддерживаются только GeoTIFF.",
        )
        print(f"Файл {source_object_key} пропущен: это не GeoTIFF.")
        return False

    source_name = safe_source_name(source_object_key)
    extension = os.path.splitext(source_object_key)[1] or ".tif"
    local_tif_path = os.path.join(source_dir, f"{source_name}{extension}")

    report_processing_state(
        config=config,
        source_object_key=source_object_key,
        status="processing",
        stage="queued",
        details="Снимок взят в обработку.",
    )

    try:
        print(f"Начата обработка снимка: {source_object_key}")
        report_processing_state(
            config=config,
            source_object_key=source_object_key,
            status="processing",
            stage="download_source",
            details="Скачивание исходного GeoTIFF из MinIO.",
        )
        download_object(s3_client, config.minio_bucket, source_object_key, local_tif_path)

        report_processing_state(
            config=config,
            source_object_key=source_object_key,
            status="processing",
            stage="slice_geotiff",
            details="Нарезка снимка на тайлы и построение масок.",
        )
        tile_metadata = slice_geotiff(local_tif_path, source_object_key, config, s3_client)

        report_processing_state(
            config=config,
            source_object_key=source_object_key,
            status="processing",
            stage="save_tile_metadata",
            details=f"Запись метаданных тайлов в PostgreSQL. Тайлов: {len(tile_metadata)}.",
            progress_current=len(tile_metadata),
            progress_total=len(tile_metadata),
        )
        save_tile_metadata(config, source_object_key, tile_metadata)

        shapefile_keys = []
        if tile_metadata:
            report_processing_state(
                config=config,
                source_object_key=source_object_key,
                status="processing",
                stage="assemble_shapefile",
                details="Сборка shapefile по маскам этого снимка.",
            )
            shapefile_keys = create_mask_shapefile(config, s3_client, source_object_key)

        status = "processed" if tile_metadata else "empty"
        save_processed_marker(s3_client, config, source_object_key, status, shapefile_keys)

        report_processing_state(
            config=config,
            source_object_key=source_object_key,
            status=status,
            stage="completed",
            result_path=",".join(shapefile_keys) if shapefile_keys else config.processed_prefix,
            details=(
                f"Создано тайлов: {len(tile_metadata)}. "
                f"Shapefile-файлов: {len(shapefile_keys)}."
            ),
            progress_current=len(tile_metadata),
            progress_total=len(tile_metadata),
        )
        print(
            f"Обработан снимок {source_object_key}: "
            f"тайлов {len(tile_metadata)}, shapefile-файлов {len(shapefile_keys)}."
        )
        return bool(tile_metadata)
    except Exception as error:
        report_processing_state(
            config=config,
            source_object_key=source_object_key,
            status="failed",
            stage="failed",
            details=f"Ошибка обработки: {error}",
            level="ERROR",
        )
        print(f"Ошибка обработки {source_object_key}: {error}")
        return False


def rebuild_missing_shapefiles(config, s3_client) -> None:
    """Дособирает shapefile для снимков, у которых уже есть тайлы, но нет shapefile."""
    sources = get_sources_with_tiles(config)
    rebuilt_count = 0

    for source_object_key in sources:
        if shapefile_exists(config, s3_client, source_object_key):
            continue

        print(
            f"Для {source_object_key} найден отсутствующий shapefile. "
            "Пересобираю по готовым маскам."
        )
        report_processing_state(
            config=config,
            source_object_key=source_object_key,
            status="processing",
            stage="rebuild_shapefile",
            details="Пересборка отсутствующего shapefile по уже готовым маскам.",
            processor_name="mask_assembler",
        )
        shapefile_keys = create_mask_shapefile(config, s3_client, source_object_key)
        if shapefile_keys:
            report_processing_state(
                config=config,
                source_object_key=source_object_key,
                status="processed",
                stage="rebuild_shapefile",
                details="Shapefile пересобран по уже существующим тайлам и маскам.",
                processor_name="mask_assembler",
                result_path=",".join(shapefile_keys),
            )
            rebuilt_count += 1

    if rebuilt_count:
        print(f"Пересобрано отсутствующих shapefile: {rebuilt_count}")


def main() -> None:
    config = load_config()
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    register_app_modules(spark)

    try:
        configure_spark_for_minio(spark, config)

        s3_client = build_s3_client(config)
        ensure_bucket_exists(s3_client, config.minio_bucket)
        ensure_schema(config)
        print(
            "Spark-процессор запущен в циклическом режиме и ожидает новые GeoTIFF "
            f"в MinIO с интервалом {config.poll_interval} сек. "
            f"Параллельных обработчиков: {config.processor_workers}. "
            f"Kafka topic: {config.kafka_topic}."
        )

        while True:
            try:
                kafka_keys = poll_image_keys(config)
                raw_keys = list_raw_images(s3_client, config)
                object_keys = list(dict.fromkeys(kafka_keys + raw_keys))
                processed_keys = get_already_processed_keys(config)
                pending_keys = [
                    object_key
                    for object_key in object_keys
                    if object_key not in processed_keys
                    and not has_processed_marker(s3_client, config, object_key)
                ]

                if not pending_keys:
                    print("Новых файлов для обработки пока нет.")
                    rebuild_missing_shapefiles(config, s3_client)
                    time.sleep(config.poll_interval)
                    continue

                parallelism = max(1, min(config.processor_workers, len(pending_keys)))
                print(
                    f"Найдено новых файлов: {len(pending_keys)}. "
                    f"Запускаю обработку в {parallelism} Spark-задачах."
                )

                results = (
                    spark.sparkContext.parallelize(pending_keys, parallelism)
                    .map(lambda object_key: process_source_image(config, object_key))
                    .collect()
                )
                print(
                    "Цикл обработки завершён. "
                    f"Успешно обработано снимков: {sum(1 for item in results if item)}"
                )
                rebuild_missing_shapefiles(config, s3_client)
                time.sleep(config.poll_interval)
            except Exception as error:
                print(f"Ошибка цикла обработки: {error}")
                time.sleep(config.poll_interval)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
