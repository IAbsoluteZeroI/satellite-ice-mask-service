import glob
import os
import re
import shutil
from typing import Iterable, List

import cv2
import geopandas as gpd
import numpy as np
from affine import Affine
from botocore.exceptions import ClientError
from rasterio.features import shapes
from shapely.geometry import shape
from shapely.ops import unary_union

from config import AppConfig
from db import get_tiles_by_source, get_worker_name, log_processing_event
from land_mask import clip_geometry_to_water
from storage import download_object, upload_file


def reset_directory(path: str) -> None:
    """Очищает временную директорию для сборки shapefile."""
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def _download_mask(s3_client, config: AppConfig, mask_name: str, mask_dir: str):
    """Скачивает маску из MinIO и возвращает локальный путь или None."""
    mask_object_key = f"{config.mask_prefix}{mask_name}"
    mask_path = os.path.join(mask_dir, mask_name)

    try:
        download_object(s3_client, config.minio_bucket, mask_object_key, mask_path)
        return mask_path
    except ClientError as error:
        print(f"Маска {mask_object_key} не скачана из MinIO: {error}")
        return None


def _upload_shapefile_parts(s3_client, config: AppConfig, shapefile_path: str) -> List[str]:
    """Загружает все служебные файлы shapefile в MinIO."""
    shapefile_base = os.path.splitext(shapefile_path)[0]
    uploaded_keys = []
    source_dir = os.path.basename(os.path.dirname(shapefile_path))

    for local_path in glob.glob(f"{shapefile_base}.*"):
        filename = os.path.basename(local_path)
        object_key = f"{config.shapefile_prefix}{source_dir}/{filename}"
        upload_file(s3_client, config.minio_bucket, object_key, local_path)
        uploaded_keys.append(object_key)

    return uploaded_keys


def _read_mask_as_binary(mask_path: str):
    """
    Читает маску как бинарный растр.

    Новые маски сохраняются как PNG с alpha-каналом. Если читать такой PNG как grayscale,
    прозрачный фон может превратиться в белый цвет, и shapefile станет сплошным прямоугольником.
    Поэтому для RGBA берём именно alpha-канал.
    """
    image = cv2.imread(mask_path, cv2.IMREAD_UNCHANGED)
    if image is None:
        return None

    if image.ndim == 3 and image.shape[2] == 4:
        mask = image[:, :, 3]
    elif image.ndim == 3:
        mask = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    else:
        mask = image

    _, binary = cv2.threshold(mask, 127, 1, cv2.THRESH_BINARY)
    return binary.astype(np.uint8)


def _iter_polygon_parts(geometry) -> Iterable:
    """Возвращает отдельные полигоны из Polygon или MultiPolygon."""
    if geometry.is_empty:
        return []
    if geometry.geom_type == "MultiPolygon":
        return [item for item in geometry.geoms if not item.is_empty]
    if geometry.geom_type == "Polygon":
        return [geometry]
    return []


def _tile_affine_transform(tile: dict, width: int, height: int) -> Affine:
    """Строит преобразование пикселей маски по реальным углам тайла.

    Это точнее, чем from_bounds: наклонный SAR-тайл не превращается в обычный
    прямоугольник широта/долгота, поэтому маска не уезжает относительно тайла.
    """
    top_left_lon = tile["top_left_lon"]
    top_left_lat = tile["top_left_lat"]
    top_right_lon = tile.get("top_right_lon") or tile["bottom_right_lon"]
    top_right_lat = tile.get("top_right_lat") or tile["top_left_lat"]
    bottom_left_lon = tile.get("bottom_left_lon") or tile["top_left_lon"]
    bottom_left_lat = tile.get("bottom_left_lat") or tile["bottom_right_lat"]

    lon_per_x = (top_right_lon - top_left_lon) / width
    lon_per_y = (bottom_left_lon - top_left_lon) / height
    lat_per_x = (top_right_lat - top_left_lat) / width
    lat_per_y = (bottom_left_lat - top_left_lat) / height

    return Affine(lon_per_x, lon_per_y, top_left_lon, lat_per_x, lat_per_y, top_left_lat)


def safe_source_name(source_object_key: str) -> str:
    """Преобразует ключ исходного снимка в безопасное имя директории."""
    name = os.path.splitext(os.path.basename(source_object_key))[0]
    return re.sub(r"[^A-Za-z0-9_-]+", "_", name).strip("_") or "source"


def shapefile_exists(config: AppConfig, s3_client, source_object_key: str) -> bool:
    """Проверяет, есть ли shapefile для конкретного снимка в MinIO."""
    source_name = safe_source_name(source_object_key)
    prefix = f"{config.shapefile_prefix}{source_name}/"
    response = s3_client.list_objects_v2(Bucket=config.minio_bucket, Prefix=prefix)
    return any(item["Key"].endswith(".shp") for item in response.get("Contents", []))


def create_mask_shapefile(config: AppConfig, s3_client, source_object_key: str) -> List[str]:
    """
    Собирает итоговый shapefile из реальных пикселей тайловых масок.

    Полигонализация выполняется по бинарной маске, а не по границе тайла. Поэтому итоговый слой
    повторяет форму маски и сохраняет вырезанные области суши.
    """
    tiles = get_tiles_by_source(config, source_object_key)
    worker_name = get_worker_name()

    if not tiles:
        print(f"Сборка масок пропущена: для {source_object_key} в PostgreSQL нет тайлов.")
        log_processing_event(
            config,
            source_object_key,
            stage="assemble_shapefile",
            message="Сборка shapefile пропущена: тайлы в PostgreSQL не найдены.",
            level="WARNING",
            worker_name=worker_name,
        )
        return []

    source_name = safe_source_name(source_object_key)
    work_dir = os.path.join(config.work_dir, "mask_assembler", source_name)
    mask_dir = os.path.join(work_dir, "masks")
    output_dir = os.path.join(work_dir, "shapefile", source_name)
    shapefile_path = os.path.join(output_dir, f"{source_name}_masks.shp")
    reset_directory(work_dir)
    os.makedirs(mask_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    geometries = []
    attributes = []

    log_processing_event(
        config,
        source_object_key,
        stage="assemble_shapefile",
        message=f"Начата сборка shapefile из тайловых масок. Тайлов в PostgreSQL: {len(tiles)}.",
        worker_name=worker_name,
    )

    for index, tile in enumerate(tiles, start=1):
        mask_path = _download_mask(s3_client, config, tile["mask_name"], mask_dir)
        if not mask_path or not os.path.exists(mask_path):
            continue

        binary = _read_mask_as_binary(mask_path)
        if binary is None or int(binary.max()) == 0:
            continue

        filled_pixels = int(binary.sum())
        total_pixels = int(binary.size)
        filled_ratio = filled_pixels / total_pixels
        if filled_ratio < config.mask_filled_threshold:
            continue

        height, width = binary.shape
        transform = _tile_affine_transform(tile, width, height)

        for geometry_mapping, value in shapes(binary, mask=binary.astype(bool), transform=transform):
            if int(value) != 1:
                continue

            polygon = shape(geometry_mapping)
            if not polygon.is_valid:
                polygon = polygon.buffer(0)

            polygon = clip_geometry_to_water(polygon)
            if polygon.is_empty:
                continue

            for candidate_polygon in _iter_polygon_parts(polygon):
                if not candidate_polygon.is_valid:
                    candidate_polygon = candidate_polygon.buffer(0)
                if candidate_polygon.is_empty or candidate_polygon.area < config.mask_min_area:
                    continue

                geometries.append(candidate_polygon)
                attributes.append(
                    {
                        "tile_id": tile["filename"],
                        "mask_name": tile["mask_name"],
                        "fill_ratio": filled_ratio,
                    }
                )

        if index == 1 or index % 50 == 0 or index == len(tiles):
            log_processing_event(
                config,
                source_object_key,
                stage="assemble_shapefile",
                message=f"Обработано масок: {index} из {len(tiles)}.",
                progress_current=index,
                progress_total=len(tiles),
                worker_name=worker_name,
            )

    if not geometries:
        print(f"Сборка масок для {source_object_key} завершена: подходящие полигоны не найдены.")
        log_processing_event(
            config,
            source_object_key,
            stage="assemble_shapefile",
            message="Подходящие полигоны не найдены, shapefile не создан.",
            level="WARNING",
            worker_name=worker_name,
        )
        return []

    if config.mask_merge_gaps:
        merged_geometry = unary_union([geometry.buffer(0) for geometry in geometries])
        if config.mask_gap_buffer > 0:
            merged_geometry = merged_geometry.buffer(config.mask_gap_buffer).buffer(-config.mask_gap_buffer)
        gdf = gpd.GeoDataFrame(
            [{"source": source_name, "name": "mask"}],
            geometry=[merged_geometry],
            crs="EPSG:4326",
        )
    else:
        gdf = gpd.GeoDataFrame(attributes, geometry=geometries, crs="EPSG:4326")
        gdf["geometry"] = gdf.buffer(0)

    gdf.to_file(shapefile_path)
    uploaded_keys = _upload_shapefile_parts(s3_client, config, shapefile_path)
    print(f"Shapefile масок для {source_object_key} загружен в MinIO: {uploaded_keys}")
    log_processing_event(
        config,
        source_object_key,
        stage="assemble_shapefile",
        message=f"Shapefile собран из масок и загружен в MinIO. Файлов: {len(uploaded_keys)}.",
        progress_current=len(tiles),
        progress_total=len(tiles),
        worker_name=worker_name,
    )
    return uploaded_keys
