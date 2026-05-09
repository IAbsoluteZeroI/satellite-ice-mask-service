import io
import mimetypes
import os
import re
import tempfile
import zipfile
from datetime import datetime
from typing import List, Optional
from urllib.parse import quote, urlsplit, urlunsplit

import boto3
import ee
import geemap
import geopandas as gpd
import psycopg2
from psycopg2 import errors
from botocore.exceptions import ClientError
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from PIL import Image
from pydantic import BaseModel
from shapely.geometry import Polygon, mapping
from shapely.ops import unary_union


app = FastAPI(title="Satellite Image Service")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1024)


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "satellite-images")
MINIO_PUBLIC_ENDPOINT = os.getenv("MINIO_PUBLIC_ENDPOINT", "http://localhost:9000")
MINIO_PRESIGNED_TTL_SECONDS = int(os.getenv("MINIO_PRESIGNED_TTL_SECONDS", "86400"))
TILE_PREFIX = os.getenv("TILE_PREFIX", "processed/tiles/")
MASK_PREFIX = os.getenv("MASK_PREFIX", "processed/masks/")
SHAPEFILE_PREFIX = os.getenv("SHAPEFILE_PREFIX", "processed/shapefiles/")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/data")
EE_PROJECT = os.getenv("EE_PROJECT", "small-talk-xfnc")
TILE_RENDER_PADDING_RATIO = float(os.getenv("TILE_RENDER_PADDING_RATIO", "0.0015"))
HTTP_CACHE_SECONDS = int(os.getenv("HTTP_CACHE_SECONDS", "86400"))
MASK_ALPHA_MULTIPLIER = float(os.getenv("MASK_ALPHA_MULTIPLIER", "0.72"))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "satellite")
POSTGRES_USER = os.getenv("POSTGRES_USER", "satellite")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "satellite")


class DownloadAreaRequest(BaseModel):
    west: float
    south: float
    east: float
    north: float
    start_date: str = "2021-11-01"
    end_date: str = "2022-03-30"
    limit: int = 3


def get_connection():
    """Открывает соединение с PostgreSQL."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


@app.on_event("startup")
def ensure_tile_corner_columns():
    """Добавляет новые колонки углов тайлов, если БД была создана старой версией сервиса."""
    query = """
        ALTER TABLE IF EXISTS tiles ADD COLUMN IF NOT EXISTS top_right_lon DOUBLE PRECISION;
        ALTER TABLE IF EXISTS tiles ADD COLUMN IF NOT EXISTS top_right_lat DOUBLE PRECISION;
        ALTER TABLE IF EXISTS tiles ADD COLUMN IF NOT EXISTS bottom_left_lon DOUBLE PRECISION;
        ALTER TABLE IF EXISTS tiles ADD COLUMN IF NOT EXISTS bottom_left_lat DOUBLE PRECISION;
        ALTER TABLE IF EXISTS tiles ADD COLUMN IF NOT EXISTS tile_x INTEGER;
        ALTER TABLE IF EXISTS tiles ADD COLUMN IF NOT EXISTS tile_y INTEGER;
        ALTER TABLE IF EXISTS tiles ADD COLUMN IF NOT EXISTS tile_width INTEGER;
        ALTER TABLE IF EXISTS tiles ADD COLUMN IF NOT EXISTS tile_height INTEGER;
    """
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
        connection.commit()


def get_recent_processing_tasks(limit: int = 12):
    """Возвращает последние задачи обработки с текущими стадиями и воркерами."""
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

    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (limit,))
                rows = cursor.fetchall()
    except (errors.UndefinedTable, psycopg2.errors.UndefinedColumn):
        return []

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


def get_recent_processing_logs(limit: int = 40):
    """Возвращает свежие события обработки для панели мониторинга."""
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

    try:
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (limit,))
                rows = cursor.fetchall()
    except (errors.UndefinedTable, psycopg2.errors.UndefinedColumn):
        return []

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


def build_s3_client():
    """Создаёт клиент MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def safe_source_name(source_object_key: str) -> str:
    """Преобразует ключ исходного снимка в безопасный идентификатор."""
    name = os.path.splitext(os.path.basename(source_object_key))[0]
    return re.sub(r"[^A-Za-z0-9_-]+", "_", name).strip("_") or "source"


def extract_date(source_object_key: str):
    """Пытается извлечь дату снимка из имени файла."""
    match = re.search(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})", source_object_key)
    if not match:
        return None
    return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"


def list_shapefile_keys(s3_client, source_object_key: str) -> List[str]:
    """Возвращает список файлов shapefile для одного снимка."""
    source_name = safe_source_name(source_object_key)
    prefix = f"{SHAPEFILE_PREFIX}{source_name}/"
    try:
        response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
    except ClientError as error:
        print(f"[web] Не удалось получить список shapefile из MinIO для {source_object_key}: {error}")
        return []
    return [item["Key"] for item in response.get("Contents", [])]


def empty_feature_collection(message: str = ""):
    """Возвращает пустой GeoJSON, чтобы карта не падала при отсутствии слоя."""
    payload = {"type": "FeatureCollection", "features": []}
    if message:
        payload["properties"] = {"warning": message}
    return payload


def json_safe_value(value):
    """Преобразует значения из GeoDataFrame в JSON-совместимые типы."""
    if value is None:
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, datetime):
        return value.isoformat()
    try:
        if value != value:
            return None
    except (TypeError, ValueError):
        return None
    return value


def geodataframe_to_geojson(gdf):
    """Собирает GeoJSON без gdf.to_json(), чтобы обойти проблемы GeoPandas/NumPy."""
    geometry_column = gdf.geometry.name
    features = []

    for _, row in gdf.iterrows():
        geometry = row[geometry_column]
        if geometry is None or geometry.is_empty:
            continue

        properties = {}
        for column in gdf.columns:
            if column == geometry_column:
                continue
            properties[column] = json_safe_value(row[column])

        features.append(
            {
                "type": "Feature",
                "properties": properties,
                "geometry": mapping(geometry),
            }
        )

    return {"type": "FeatureCollection", "features": features}


def build_bounds_polygon(west: float, south: float, east: float, north: float):
    """Строит прямоугольный полигон по границам снимка."""
    return Polygon(
        [
            (west, south),
            (east, south),
            (east, north),
            (west, north),
            (west, south),
        ]
    )


def sanitize_hex_color(color: str) -> str:
    """Нормализует цвет маски в шестнадцатеричном формате."""
    normalized = (color or "").strip().lstrip("#")
    if len(normalized) == 3:
        normalized = "".join(channel * 2 for channel in normalized)
    if len(normalized) != 6 or not re.fullmatch(r"[0-9a-fA-F]{6}", normalized):
        normalized = "2d6a4f"
    return normalized.lower()


def http_cache_headers():
    """Общие cache-control заголовки для статических слоёв."""
    return {"Cache-Control": f"public, max-age={HTTP_CACHE_SECONDS}"}


def no_cache_headers():
    """Заголовки для динамических слоёв, которые могут измениться после переобработки."""
    return {"Cache-Control": "no-store"}


def normalize_bbox_value(value: Optional[float]) -> Optional[float]:
    """Округляет границы bbox, чтобы одинаковые запросы лучше переиспользовали кэш."""
    if value is None:
        return None
    return round(float(value), 4)


def build_object_url(key: str) -> str:
    """
    Возвращает URL объекта для браузера.

    Тайлы выгоднее отдавать сразу из MinIO через подписанную ссылку: так FastAPI
    не тратит время на проксирование каждого изображения.
    """
    if MINIO_PUBLIC_ENDPOINT:
        try:
            presigned_url = build_s3_client().generate_presigned_url(
                "get_object",
                Params={"Bucket": MINIO_BUCKET, "Key": key},
                ExpiresIn=MINIO_PRESIGNED_TTL_SECONDS,
            )
            public_parts = urlsplit(MINIO_PUBLIC_ENDPOINT)
            presigned_parts = urlsplit(presigned_url)
            return urlunsplit(
                (
                    public_parts.scheme or presigned_parts.scheme,
                    public_parts.netloc or presigned_parts.netloc,
                    presigned_parts.path,
                    presigned_parts.query,
                    presigned_parts.fragment,
                )
            )
        except Exception as error:
            print(f"[web] Не удалось построить public URL для {key}: {error}")

    return f"/api/object?key={quote(key, safe='')}"


def get_tiles_for_source_cached(source: str):
    """Кеширует метаданные тайлов по одному снимку, чтобы не дёргать БД на каждый pan."""
    query = """
        SELECT
            filename,
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

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (source,))
            rows = cursor.fetchall()

    return tuple(
        {
            "filename": row[0],
            "top_left_lon": row[1],
            "top_left_lat": row[2],
            "top_right_lon": row[3] if row[3] is not None else row[5],
            "top_right_lat": row[4] if row[4] is not None else row[2],
            "bottom_right_lon": row[5],
            "bottom_right_lat": row[6],
            "bottom_left_lon": row[7] if row[7] is not None else row[1],
            "bottom_left_lat": row[8] if row[8] is not None else row[6],
            "tile_x": row[9],
            "tile_y": row[10],
            "tile_width": row[11],
            "tile_height": row[12],
            "mask_name": row[13],
        }
        for row in rows
    )


def tile_corners(row):
    """Возвращает четыре угла тайла в формате Leaflet: [lat, lon]."""
    return {
        "top_left": [row["top_left_lat"], row["top_left_lon"]],
        "top_right": [row["top_right_lat"], row["top_right_lon"]],
        "bottom_right": [row["bottom_right_lat"], row["bottom_right_lon"]],
        "bottom_left": [row["bottom_left_lat"], row["bottom_left_lon"]],
    }


def tile_bbox(row):
    """Считает bbox по четырем углам тайла только для фильтрации текущего экрана."""
    lon_values = [
        row["top_left_lon"],
        row["top_right_lon"],
        row["bottom_right_lon"],
        row["bottom_left_lon"],
    ]
    lat_values = [
        row["top_left_lat"],
        row["top_right_lat"],
        row["bottom_right_lat"],
        row["bottom_left_lat"],
    ]
    return min(lon_values), min(lat_values), max(lon_values), max(lat_values)


def bounds_intersect(
    tile_west: float,
    tile_south: float,
    tile_east: float,
    tile_north: float,
    west: float,
    south: float,
    east: float,
    north: float,
) -> bool:
    """Проверяет пересечение тайла с текущим bbox карты."""
    return tile_east >= west and tile_west <= east and tile_north >= south and tile_south <= north


def filter_tiles_by_bbox(rows, west: Optional[float], south: Optional[float], east: Optional[float], north: Optional[float]):
    """Оставляет только тайлы, пересекающие текущую область карты."""
    if None in (west, south, east, north):
        return list(rows)

    filtered_rows = []
    for row in rows:
        tile_west, tile_south, tile_east, tile_north = tile_bbox(row)
        if bounds_intersect(tile_west, tile_south, tile_east, tile_north, west, south, east, north):
            filtered_rows.append(row)
    return filtered_rows


def build_tiles_geojson_cached(
    source: str,
    west: Optional[float],
    south: Optional[float],
    east: Optional[float],
    north: Optional[float],
    sanitized_color: str,
):
    """
    Кеширует готовый GeoJSON по снимку и округлённому bbox.

    Карта перемещается часто, поэтому повторная сборка одинаковых ответов
    только замедляет API.
    """
    rows = get_tiles_for_source_cached(source)
    filtered_rows = filter_tiles_by_bbox(rows, west, south, east, north)

    features = []
    for row in filtered_rows:
        filename = row["filename"]
        mask_name = row["mask_name"]

        corners = tile_corners(row)
        tile_west, tile_south, tile_east, tile_north = tile_bbox(row)
        pad_lon = (tile_east - tile_west) * TILE_RENDER_PADDING_RATIO
        pad_lat = (tile_north - tile_south) * TILE_RENDER_PADDING_RATIO

        tile_key = f"{TILE_PREFIX}{filename}"
        mask_key = f"{MASK_PREFIX}{mask_name}"

        features.append(
            {
                "type": "Feature",
                "properties": {
                    "filename": filename,
                    "mask_name": mask_name,
                    "image_url": build_object_url(tile_key),
                    "mask_image_url": f"/api/mask-object?key={quote(mask_key, safe='')}&color={sanitized_color}",
                    "corners": corners,
                    "bounds": [[tile_south, tile_west], [tile_north, tile_east]],
                    "image_bounds": [
                        [tile_south - pad_lat, tile_west - pad_lon],
                        [tile_north + pad_lat, tile_east + pad_lon],
                    ],
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [row["top_left_lon"], row["top_left_lat"]],
                            [row["top_right_lon"], row["top_right_lat"]],
                            [row["bottom_right_lon"], row["bottom_right_lat"]],
                            [row["bottom_left_lon"], row["bottom_left_lat"]],
                            [row["top_left_lon"], row["top_left_lat"]],
                        ]
                    ],
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {
            "source": source,
            "requested_bbox": {
                "west": west,
                "south": south,
                "east": east,
                "north": north,
            },
        },
    }


def build_outline_from_tiles(source: str):
    """Создаёт контур footprint снимка по внешней границе всех его тайлов."""
    rows = get_tiles_for_source_cached(source)
    if not rows:
        return empty_feature_collection("Контур снимка пока недоступен.")

    tile_polygons = []
    for row in rows:
        tile_polygons.append(
            Polygon(
                [
                    (row["top_left_lon"], row["top_left_lat"]),
                    (row["top_right_lon"], row["top_right_lat"]),
                    (row["bottom_right_lon"], row["bottom_right_lat"]),
                    (row["bottom_left_lon"], row["bottom_left_lat"]),
                    (row["top_left_lon"], row["top_left_lat"]),
                ]
            )
        )

    footprint = unary_union(tile_polygons)
    if not footprint.is_valid:
        footprint = footprint.buffer(0)

    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"source": source, "outline": "tile_footprint"},
                "geometry": mapping(footprint),
            }
        ],
    }


def load_outline_geojson_cached(source: str):
    """Кеширует контур footprint снимка по внешним границам тайлов."""
    return build_outline_from_tiles(source)


def build_mask_overlay_png(mask_object_key: str, color_hex: str) -> bytes:
    """Преобразует бинарную маску в прозрачный цветной PNG и кеширует результат."""
    s3_client = build_s3_client()
    response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=mask_object_key)
    body = response["Body"].read()

    with Image.open(io.BytesIO(body)).convert("RGBA") as image:
        alpha = image.getchannel("A") if "A" in image.getbands() else image.convert("L")
        alpha = alpha.point(lambda value: 0 if value < 8 else min(255, int(value * MASK_ALPHA_MULTIPLIER)))
        red = int(color_hex[0:2], 16)
        green = int(color_hex[2:4], 16)
        blue = int(color_hex[4:6], 16)
        overlay = Image.new("RGBA", image.size, (red, green, blue, 0))
        overlay.putalpha(alpha)

        buffer = io.BytesIO()
        overlay.save(buffer, format="PNG")
        return buffer.getvalue()


@app.get("/")
def index():
    """Открывает страницу карты."""
    return RedirectResponse(url="/map")


@app.get("/map")
def map_page(request: Request):
    """Возвращает веб-карту."""
    return templates.TemplateResponse(request=request, name="map.html", context={})


@app.get("/api/images")
def get_images():
    """Возвращает список снимков с базовыми метаданными и текущим статусом обработки."""
    query = """
        WITH sources AS (
            SELECT object_key AS source_object_key
            FROM image_processing_tasks
            UNION
            SELECT source_object_key
            FROM tiles
        )
        SELECT
            s.source_object_key,
            COUNT(t.id) AS tile_count,
            MIN(LEAST(
                t.top_left_lon,
                COALESCE(t.top_right_lon, t.bottom_right_lon),
                t.bottom_right_lon,
                COALESCE(t.bottom_left_lon, t.top_left_lon)
            )) AS west,
            MIN(LEAST(
                t.top_left_lat,
                COALESCE(t.top_right_lat, t.top_left_lat),
                t.bottom_right_lat,
                COALESCE(t.bottom_left_lat, t.bottom_right_lat)
            )) AS south,
            MAX(GREATEST(
                t.top_left_lon,
                COALESCE(t.top_right_lon, t.bottom_right_lon),
                t.bottom_right_lon,
                COALESCE(t.bottom_left_lon, t.top_left_lon)
            )) AS east,
            MAX(GREATEST(
                t.top_left_lat,
                COALESCE(t.top_right_lat, t.top_left_lat),
                t.bottom_right_lat,
                COALESCE(t.bottom_left_lat, t.bottom_right_lat)
            )) AS north,
            COALESCE(MAX(p.status), 'unknown') AS status,
            MAX(p.updated_at) AS updated_at,
            MAX(p.stage) AS stage,
            MAX(p.progress_current) AS progress_current,
            MAX(p.progress_total) AS progress_total,
            MAX(p.details) AS details
        FROM sources s
        LEFT JOIN tiles t ON t.source_object_key = s.source_object_key
        LEFT JOIN image_processing_tasks p ON p.object_key = s.source_object_key
        GROUP BY s.source_object_key
        ORDER BY MAX(p.updated_at) DESC NULLS LAST;
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    images = []
    for row in rows:
        source_key = row[0]
        date_value = extract_date(source_key)
        images.append(
            {
                "id": safe_source_name(source_key),
                "source_object_key": source_key,
                "source_label": os.path.basename(source_key),
                "acquisition_date": date_value,
                "tile_count": row[1],
                "bounds": {
                    "west": row[2],
                    "south": row[3],
                    "east": row[4],
                    "north": row[5],
                },
                "status": row[6],
                "updated_at": row[7].isoformat() if row[7] else None,
                "stage": row[8],
                "progress_current": row[9],
                "progress_total": row[10],
                "details": row[11],
                "tiles_url": f"/api/tiles-geojson?source={quote(source_key, safe='')}",
                "outline_url": f"/api/image-outline-geojson?source={quote(source_key, safe='')}",
                "download_url": f"/api/download-shapefile?source={quote(source_key, safe='')}",
            }
        )

    images.sort(key=lambda item: item["acquisition_date"] or item["updated_at"] or "", reverse=True)
    return JSONResponse(content={"images": images}, headers=no_cache_headers())


@app.get("/api/processing-overview")
def get_processing_overview(task_limit: int = 12, log_limit: int = 40):
    """Возвращает состояние текущих задач и свежие события обработки воркеров."""
    task_limit = max(1, min(task_limit, 50))
    log_limit = max(1, min(log_limit, 200))
    return JSONResponse(
        content={
            "tasks": get_recent_processing_tasks(task_limit),
            "logs": get_recent_processing_logs(log_limit),
        },
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/tiles-geojson")
def get_tiles_geojson(
    source: str,
    west: Optional[float] = None,
    south: Optional[float] = None,
    east: Optional[float] = None,
    north: Optional[float] = None,
    mask_color: str = "2d6a4f",
):
    """Возвращает только те тайлы снимка, которые попадают в текущий bbox карты."""
    sanitized_color = sanitize_hex_color(mask_color)
    west = normalize_bbox_value(west)
    south = normalize_bbox_value(south)
    east = normalize_bbox_value(east)
    north = normalize_bbox_value(north)

    return JSONResponse(
        content=build_tiles_geojson_cached(source, west, south, east, north, sanitized_color),
        headers=no_cache_headers(),
    )


@app.get("/api/image-outline-geojson")
def get_image_outline_geojson(source: str):
    """Возвращает внешний footprint снимка по границам его тайлов."""
    return JSONResponse(content=load_outline_geojson_cached(source), headers=no_cache_headers())


@app.get("/api/mask-geojson")
def get_mask_geojson(source: str):
    """
    Оставлен для совместимости со старым фронтом.

    Теперь используется как footprint снимка, а не как основной способ
    отображения масок: сама карта накладывает реальные маски тайлов.
    """
    return JSONResponse(content=load_outline_geojson_cached(source), headers=no_cache_headers())


@app.get("/api/mask-object")
def get_mask_object(key: str, color: str = "2d6a4f"):
    """Возвращает прозрачный PNG-оверлей маски без чёрного фона."""
    sanitized_color = sanitize_hex_color(color)
    try:
        png_bytes = build_mask_overlay_png(key, sanitized_color)
    except ClientError:
        return JSONResponse(content={"error": "Маска не найдена"}, status_code=404)
    except Exception as error:
        return JSONResponse(content={"error": f"Не удалось подготовить маску: {error}"}, status_code=500)

    return StreamingResponse(
        io.BytesIO(png_bytes),
        media_type="image/png",
        headers=no_cache_headers(),
    )


@app.get("/api/object")
def get_object(key: str):
    """Проксирует объект из MinIO в браузер."""
    s3_client = build_s3_client()
    try:
        response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)
    except ClientError:
        return JSONResponse(content={"error": "Объект не найден"}, status_code=404)

    return StreamingResponse(
        response["Body"],
        media_type=mimetypes.guess_type(key)[0] or response.get("ContentType") or "application/octet-stream",
        headers=http_cache_headers(),
    )


@app.get("/api/download-shapefile")
def download_shapefile(source: str):
    """Скачивает shapefile одного снимка архивом."""
    s3_client = build_s3_client()
    keys = list_shapefile_keys(s3_client, source)
    if not keys:
        return JSONResponse(content={"error": "Shapefile для снимка не найден"}, status_code=404)

    source_name = safe_source_name(source)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        for key in keys:
            object_body = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)["Body"].read()
            zip_file.writestr(os.path.basename(key), object_body)

    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={source_name}_masks.zip"},
    )


@app.post("/api/download-area")
def download_area(request: DownloadAreaRequest):
    """
    Скачивает Sentinel-1 GeoTIFF по текущей области карты.

    Файлы сохраняются в общую папку data, откуда их заберёт producer.
    """
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    try:
        ee.Initialize(project=EE_PROJECT)
    except Exception as error:
        return JSONResponse(
            content={
                "error": "Earth Engine не инициализирован. Нужна авторизация или сервисный аккаунт.",
                "details": str(error),
            },
            status_code=500,
        )

    region = ee.Geometry.Rectangle([request.west, request.south, request.east, request.north])
    sentinel1 = (
        ee.ImageCollection("COPERNICUS/S1_GRD")
        .filterBounds(region)
        .filterDate(request.start_date, request.end_date)
        .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
        .filter(ee.Filter.eq("instrumentMode", "IW"))
        .select("VV")
        .sort("system:time_start", False)
    )

    count = min(int(sentinel1.size().getInfo()), request.limit)
    image_list = sentinel1.toList(count)
    downloaded_files = []

    for index in range(count):
        image = ee.Image(image_list.get(index))
        date_str = image.date().format("YYYYMMdd").getInfo()
        filename = f"sentinel1_{date_str}_{index}.tif"
        local_path = os.path.join(DOWNLOAD_DIR, filename)

        geemap.download_ee_image(
            image=image.clip(region),
            filename=local_path,
            scale=10,
            region=region,
        )
        downloaded_files.append(filename)

    return {
        "downloaded_count": len(downloaded_files),
        "downloaded_files": downloaded_files,
        "message": "Файлы сохранены в data. Producer загрузит их в MinIO.",
    }
