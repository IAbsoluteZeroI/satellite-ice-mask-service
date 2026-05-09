import os
import socket
from typing import List, Optional, Set

import psycopg2
from pyspark import TaskContext

from config import AppConfig


def get_connection(config: AppConfig):
    """Открывает соединение с PostgreSQL."""
    return psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
        connect_timeout=10,
    )


def ensure_schema(config: AppConfig) -> None:
    """Гарантирует наличие таблиц для задач обработки и метаданных тайлов."""
    query = """
        CREATE TABLE IF NOT EXISTS image_processing_tasks (
            id BIGSERIAL PRIMARY KEY,
            object_key TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            processor_name TEXT NOT NULL,
            stage TEXT,
            worker_name TEXT,
            progress_current INTEGER,
            progress_total INTEGER,
            result_path TEXT,
            details TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS tiles (
            id BIGSERIAL PRIMARY KEY,
            filename VARCHAR(255) NOT NULL UNIQUE,
            source_object_key TEXT NOT NULL,
            top_left_lon DOUBLE PRECISION,
            top_left_lat DOUBLE PRECISION,
            top_right_lon DOUBLE PRECISION,
            top_right_lat DOUBLE PRECISION,
            bottom_right_lon DOUBLE PRECISION,
            bottom_right_lat DOUBLE PRECISION,
            bottom_left_lon DOUBLE PRECISION,
            bottom_left_lat DOUBLE PRECISION,
            tile_x INTEGER,
            tile_y INTEGER,
            tile_width INTEGER,
            tile_height INTEGER,
            mask_name VARCHAR(255),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_tiles_coordinates
        ON tiles (top_left_lat, top_left_lon, bottom_right_lat, bottom_right_lon);

        CREATE TABLE IF NOT EXISTS processing_logs (
            id BIGSERIAL PRIMARY KEY,
            object_key TEXT NOT NULL,
            worker_name TEXT,
            stage TEXT,
            level TEXT NOT NULL DEFAULT 'INFO',
            message TEXT NOT NULL,
            progress_current INTEGER,
            progress_total INTEGER,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_processing_logs_object_created_at
        ON processing_logs (object_key, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_processing_logs_created_at
        ON processing_logs (created_at DESC);

        ALTER TABLE image_processing_tasks ADD COLUMN IF NOT EXISTS stage TEXT;
        ALTER TABLE image_processing_tasks ADD COLUMN IF NOT EXISTS worker_name TEXT;
        ALTER TABLE image_processing_tasks ADD COLUMN IF NOT EXISTS progress_current INTEGER;
        ALTER TABLE image_processing_tasks ADD COLUMN IF NOT EXISTS progress_total INTEGER;
        ALTER TABLE tiles ADD COLUMN IF NOT EXISTS top_right_lon DOUBLE PRECISION;
        ALTER TABLE tiles ADD COLUMN IF NOT EXISTS top_right_lat DOUBLE PRECISION;
        ALTER TABLE tiles ADD COLUMN IF NOT EXISTS bottom_left_lon DOUBLE PRECISION;
        ALTER TABLE tiles ADD COLUMN IF NOT EXISTS bottom_left_lat DOUBLE PRECISION;
        ALTER TABLE tiles ADD COLUMN IF NOT EXISTS tile_x INTEGER;
        ALTER TABLE tiles ADD COLUMN IF NOT EXISTS tile_y INTEGER;
        ALTER TABLE tiles ADD COLUMN IF NOT EXISTS tile_width INTEGER;
        ALTER TABLE tiles ADD COLUMN IF NOT EXISTS tile_height INTEGER;
    """

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()


def save_tile_metadata(config: AppConfig, source_object_key: str, tile_metadata: List[dict]) -> None:
    """Сохраняет метаданные по сгенерированным тайлам."""
    if not tile_metadata:
        return

    query = """
        INSERT INTO tiles (
            filename,
            source_object_key,
            top_left_lon,
            top_left_lat,
            top_right_lon,
            top_right_lat,
            bottom_right_lon,
            bottom_right_lat,
            bottom_left_lon,
            bottom_left_lat,
            tile_x,
            tile_y,
            tile_width,
            tile_height,
            mask_name
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (filename) DO UPDATE SET
            source_object_key = EXCLUDED.source_object_key,
            top_left_lon = EXCLUDED.top_left_lon,
            top_left_lat = EXCLUDED.top_left_lat,
            top_right_lon = EXCLUDED.top_right_lon,
            top_right_lat = EXCLUDED.top_right_lat,
            bottom_right_lon = EXCLUDED.bottom_right_lon,
            bottom_right_lat = EXCLUDED.bottom_right_lat,
            bottom_left_lon = EXCLUDED.bottom_left_lon,
            bottom_left_lat = EXCLUDED.bottom_left_lat,
            tile_x = EXCLUDED.tile_x,
            tile_y = EXCLUDED.tile_y,
            tile_width = EXCLUDED.tile_width,
            tile_height = EXCLUDED.tile_height,
            mask_name = EXCLUDED.mask_name;
    """

    rows = [
        (
            item["tile_id"],
            source_object_key,
            item["top_left_lon"],
            item["top_left_lat"],
            item["top_right_lon"],
            item["top_right_lat"],
            item["bottom_right_lon"],
            item["bottom_right_lat"],
            item["bottom_left_lon"],
            item["bottom_left_lat"],
            item["tile_x"],
            item["tile_y"],
            item["tile_width"],
            item["tile_height"],
            item["mask_name"],
        )
        for item in tile_metadata
    ]

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(query, rows)
        connection.commit()


def save_processing_result(
    config: AppConfig,
    object_key: str,
    status: str,
    processor_name: str,
    result_path,
    details: str,
    stage: Optional[str] = None,
    worker_name: Optional[str] = None,
    progress_current: Optional[int] = None,
    progress_total: Optional[int] = None,
) -> None:
    """Сохраняет или обновляет итог обработки конкретного снимка."""
    query = """
        INSERT INTO image_processing_tasks (
            object_key,
            status,
            processor_name,
            stage,
            worker_name,
            progress_current,
            progress_total,
            result_path,
            details,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (object_key) DO UPDATE SET
            status = EXCLUDED.status,
            processor_name = EXCLUDED.processor_name,
            stage = EXCLUDED.stage,
            worker_name = EXCLUDED.worker_name,
            progress_current = EXCLUDED.progress_current,
            progress_total = EXCLUDED.progress_total,
            result_path = EXCLUDED.result_path,
            details = EXCLUDED.details,
            updated_at = CURRENT_TIMESTAMP;
    """

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    object_key,
                    status,
                    processor_name,
                    stage,
                    worker_name,
                    progress_current,
                    progress_total,
                    result_path,
                    details,
                ),
            )
        connection.commit()


def get_already_processed_keys(config: AppConfig) -> Set[str]:
    """Возвращает ключи файлов, которые уже успешно обработаны или осознанно пропущены."""
    query = """
        SELECT object_key
        FROM image_processing_tasks
        WHERE status IN ('processed', 'empty', 'skipped');
    """

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    return {row[0] for row in rows}


def get_tiles(config: AppConfig) -> List[dict]:
    """Возвращает все тайлы с координатами и именами масок."""
    query = """
        SELECT
            filename,
            source_object_key,
            top_left_lon,
            top_left_lat,
            top_right_lon,
            top_right_lat,
            bottom_right_lon,
            bottom_right_lat,
            bottom_left_lon,
            bottom_left_lat,
            tile_x,
            tile_y,
            tile_width,
            tile_height,
            mask_name
        FROM tiles
        ORDER BY id;
    """

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    return [
        {
            "filename": row[0],
            "source_object_key": row[1],
            "top_left_lon": row[2],
            "top_left_lat": row[3],
            "top_right_lon": row[4],
            "top_right_lat": row[5],
            "bottom_right_lon": row[6],
            "bottom_right_lat": row[7],
            "bottom_left_lon": row[8],
            "bottom_left_lat": row[9],
            "tile_x": row[10],
            "tile_y": row[11],
            "tile_width": row[12],
            "tile_height": row[13],
            "mask_name": row[14],
        }
        for row in rows
    ]


def get_tiles_by_source(config: AppConfig, source_object_key: str) -> List[dict]:
    """Возвращает тайлы только для одного исходного снимка."""
    query = """
        SELECT
            filename,
            source_object_key,
            top_left_lon,
            top_left_lat,
            top_right_lon,
            top_right_lat,
            bottom_right_lon,
            bottom_right_lat,
            bottom_left_lon,
            bottom_left_lat,
            tile_x,
            tile_y,
            tile_width,
            tile_height,
            mask_name
        FROM tiles
        WHERE source_object_key = %s
        ORDER BY id;
    """

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (source_object_key,))
            rows = cursor.fetchall()

    return [
        {
            "filename": row[0],
            "source_object_key": row[1],
            "top_left_lon": row[2],
            "top_left_lat": row[3],
            "top_right_lon": row[4],
            "top_right_lat": row[5],
            "bottom_right_lon": row[6],
            "bottom_right_lat": row[7],
            "bottom_left_lon": row[8],
            "bottom_left_lat": row[9],
            "tile_x": row[10],
            "tile_y": row[11],
            "tile_width": row[12],
            "tile_height": row[13],
            "mask_name": row[14],
        }
        for row in rows
    ]


def get_sources_with_tiles(config: AppConfig) -> List[str]:
    """Возвращает исходные снимки, для которых уже есть тайлы."""
    query = """
        SELECT DISTINCT source_object_key
        FROM tiles
        ORDER BY source_object_key;
    """

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    return [row[0] for row in rows]


def get_worker_name() -> str:
    """Формирует читаемое имя текущего Spark-воркера с процессом и partition."""
    hostname = socket.gethostname()
    process_id = os.getpid()
    task_context = TaskContext.get()
    if task_context is None:
        return f"{hostname}:{process_id}"
    return (
        f"{hostname}:{process_id}:p{task_context.partitionId()}:a{task_context.attemptNumber()}"
    )


def log_processing_event(
    config: AppConfig,
    object_key: str,
    stage: str,
    message: str,
    level: str = "INFO",
    progress_current: Optional[int] = None,
    progress_total: Optional[int] = None,
    worker_name: Optional[str] = None,
) -> None:
    """Сохраняет событие обработки для последующего мониторинга через API."""
    query = """
        INSERT INTO processing_logs (
            object_key,
            worker_name,
            stage,
            level,
            message,
            progress_current,
            progress_total
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    object_key,
                    worker_name or get_worker_name(),
                    stage,
                    level,
                    message,
                    progress_current,
                    progress_total,
                ),
            )
        connection.commit()


def get_recent_processing_tasks(config: AppConfig, limit: int = 20) -> List[dict]:
    """Возвращает последние задачи обработки с текущим состоянием и прогрессом."""
    query = """
        SELECT
            object_key,
            status,
            processor_name,
            stage,
            worker_name,
            progress_current,
            progress_total,
            details,
            updated_at
        FROM image_processing_tasks
        ORDER BY
            CASE WHEN status = 'processing' THEN 0 ELSE 1 END,
            updated_at DESC
        LIMIT %s;
    """

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()

    return [
        {
            "object_key": row[0],
            "status": row[1],
            "processor_name": row[2],
            "stage": row[3],
            "worker_name": row[4],
            "progress_current": row[5],
            "progress_total": row[6],
            "details": row[7],
            "updated_at": row[8].isoformat() if row[8] else None,
        }
        for row in rows
    ]


def get_recent_processing_logs(config: AppConfig, limit: int = 60) -> List[dict]:
    """Возвращает журнал последних событий обработки по всем снимкам."""
    query = """
        SELECT
            object_key,
            worker_name,
            stage,
            level,
            message,
            progress_current,
            progress_total,
            created_at
        FROM processing_logs
        ORDER BY created_at DESC
        LIMIT %s;
    """

    with get_connection(config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()

    return [
        {
            "object_key": row[0],
            "worker_name": row[1],
            "stage": row[2],
            "level": row[3],
            "message": row[4],
            "progress_current": row[5],
            "progress_total": row[6],
            "created_at": row[7].isoformat() if row[7] else None,
        }
        for row in rows
    ]
