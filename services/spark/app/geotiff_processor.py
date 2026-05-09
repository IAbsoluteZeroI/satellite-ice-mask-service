import os
import re
import shutil
from pathlib import Path
from typing import List, Tuple

import numpy as np
import rasterio
from PIL import Image
from rasterio.enums import Resampling
from rasterio.warp import transform as reproject_coords
from rasterio.windows import Window

from config import AppConfig
from db import get_worker_name, log_processing_event
from land_mask import clip_mask_to_water
from segmenter import ImageSegmentation
from storage import upload_file

_segmenter_instance = None
MAX_PREVIEW_SIDE = 4096
MAX_CONTRAST_VALUES_PER_TILE = 2048
BACKGROUND_EPSILON = 1e-12
CORNER_MATCH_TOLERANCE = 1e-10


def get_segmenter(config: AppConfig) -> ImageSegmentation:
    """Ленивая инициализация сегментатора, чтобы модель загружалась один раз на Python worker."""
    global _segmenter_instance
    if _segmenter_instance is None:
        _segmenter_instance = ImageSegmentation(config.model_path)
        if _segmenter_instance.device_warning:
            print(_segmenter_instance.device_warning)
        if _segmenter_instance.ready:
            print(
                f"Загружена модель сегментации: {config.model_path}. "
                f"Устройство: {_segmenter_instance.device}"
            )
        else:
            print(
                "Сегментация работает в режиме заглушки. "
                f"Причина: {_segmenter_instance.fallback_reason}"
            )
    return _segmenter_instance


def reset_directory(path: str) -> None:
    """Полностью очищает временную директорию и создаёт её заново."""
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def _safe_source_name(source_object_key: str) -> str:
    """Преобразует ключ исходного снимка в безопасное имя временной директории."""
    name = os.path.splitext(os.path.basename(source_object_key))[0]
    return re.sub(r"[^A-Za-z0-9_-]+", "_", name).strip("_") or "source"


def _build_tile_directories(config: AppConfig, source_object_key: str) -> Tuple[str, str]:
    """Создаёт временные директории для тайлов и масок одного снимка."""
    source_name = _safe_source_name(source_object_key)
    source_work_dir = os.path.join(config.work_dir, "tiles", source_name)
    tiles_dir = os.path.join(source_work_dir, "images")
    masks_dir = os.path.join(source_work_dir, "masks")
    reset_directory(source_work_dir)
    os.makedirs(tiles_dir, exist_ok=True)
    os.makedirs(masks_dir, exist_ok=True)
    return tiles_dir, masks_dir


def _read_band_and_valid_mask(src, window: Window) -> Tuple[np.ndarray, np.ndarray]:
    """Читает окно растра и маску валидных пикселей для точной обработки краевых тайлов."""
    band = src.read(1, window=window, masked=True)
    valid_mask = src.read_masks(1, window=window) > 0
    data = band.filled(np.nan).astype(np.float32)
    valid_mask &= np.isfinite(data)
    # В выгрузках Sentinel-1 из Earth Engine пустая область часто хранится как 0,
    # хотя nodata задан как -inf. Такие пиксели нельзя отдавать в нарезку и маски.
    valid_mask &= np.abs(data) > BACKGROUND_EPSILON
    return data, valid_mask


def _is_valid_tile(band: np.ndarray, valid_mask: np.ndarray, min_valid_ratio: float) -> bool:
    """Проверяет, что в тайле достаточно полезных данных для дальнейшей обработки."""
    valid_pixels = int(valid_mask.sum())
    total_pixels = int(valid_mask.size)
    if valid_pixels == 0 or total_pixels == 0:
        return False

    if valid_pixels / total_pixels < min_valid_ratio:
        return False

    valid_values = band[valid_mask]
    if valid_values.size == 0:
        return False
    if np.isnan(valid_values).all():
        return False
    if np.isinf(valid_values).any():
        return False
    if np.nanmax(valid_values) - np.nanmin(valid_values) == 0:
        return False
    return True


def _estimate_data_window(src, tile_size: int) -> Tuple[int, int, int, int]:
    """Быстро находит область GeoTIFF, где есть валидные пиксели.

    У SAR-снимков часто огромный прямоугольный холст и небольшая наклонная
    область данных внутри него. Если сканировать весь холст, worker тратит
    минуты на пустые окна и держит лишнюю нагрузку на диск.
    """
    scale = max(1, int(np.ceil(max(src.width, src.height) / MAX_PREVIEW_SIDE)))
    out_width = max(1, int(np.ceil(src.width / scale)))
    out_height = max(1, int(np.ceil(src.height / scale)))

    try:
        preview_band = src.read(
            1,
            out_shape=(out_height, out_width),
            masked=True,
            resampling=Resampling.bilinear,
        )
        preview_data = preview_band.filled(np.nan).astype(np.float32)
        preview_valid = np.isfinite(preview_data) & (np.abs(preview_data) > BACKGROUND_EPSILON)
    except Exception:
        preview_valid = None

    if preview_valid is None or not np.any(preview_valid):
        try:
            preview_mask = src.read_masks(
                1,
                out_shape=(out_height, out_width),
                resampling=Resampling.max,
            )
        except Exception:
            preview_mask = src.read_masks(
                1,
                out_shape=(out_height, out_width),
                resampling=Resampling.nearest,
            )
        preview_valid = preview_mask > 0

    valid_rows, valid_cols = np.where(preview_valid)
    if valid_rows.size == 0 or valid_cols.size == 0:
        return 0, 0, src.width, src.height

    x_start = max(0, int(valid_cols.min() * scale) - scale)
    y_start = max(0, int(valid_rows.min() * scale) - scale)
    x_stop = min(src.width, int((valid_cols.max() + 1) * scale) + scale)
    y_stop = min(src.height, int((valid_rows.max() + 1) * scale) + scale)

    # Выравнивание по сетке тайлов сохраняет стабильные границы соседних окон.
    x_start = max(0, (x_start // tile_size) * tile_size)
    y_start = max(0, (y_start // tile_size) * tile_size)
    x_stop = min(src.width, int(np.ceil(x_stop / tile_size)) * tile_size)
    y_stop = min(src.height, int(np.ceil(y_stop / tile_size)) * tile_size)
    return x_start, y_start, x_stop, y_stop


def _sample_valid_values(values: np.ndarray) -> np.ndarray:
    """Берет равномерную выборку значений для расчета глобального контраста."""
    if values.size <= MAX_CONTRAST_VALUES_PER_TILE:
        return values

    step = max(1, values.size // MAX_CONTRAST_VALUES_PER_TILE)
    return values[::step][:MAX_CONTRAST_VALUES_PER_TILE]


def _save_tile_image(tile_path: str, band_uint8: np.ndarray, valid_mask: np.ndarray) -> None:
    """Сохраняет тайл; у частичных тайлов область вне footprint становится прозрачной."""
    os.makedirs(os.path.dirname(tile_path), exist_ok=True)

    if np.all(valid_mask):
        Image.fromarray(band_uint8).save(tile_path, "JPEG")
        return

    rgba = np.zeros((band_uint8.shape[0], band_uint8.shape[1], 4), dtype=np.uint8)
    rgba[..., 0] = band_uint8
    rgba[..., 1] = band_uint8
    rgba[..., 2] = band_uint8
    rgba[..., 3] = valid_mask.astype(np.uint8) * 255
    Image.fromarray(rgba, "RGBA").save(tile_path, "PNG")


def _pixel_corner_wgs84(pixel_x: int, pixel_y: int, transform, src_crs, corner_cache: dict) -> Tuple[float, float]:
    """Возвращает координаты угла пиксельной сетки в WGS84 с кешем общих узлов.

    Соседние тайлы используют один и тот же узел сетки, поэтому их общие
    углы получают буквально одинаковые lon/lat, без накопления расхождений.
    """
    key = (int(pixel_x), int(pixel_y))
    if key in corner_cache:
        return corner_cache[key]

    projected_x, projected_y = transform * key
    if src_crs and src_crs.to_epsg() == 4326:
        lon, lat = projected_x, projected_y
    else:
        lon_values, lat_values = reproject_coords(
            src_crs,
            "EPSG:4326",
            [projected_x],
            [projected_y],
        )
        lon, lat = lon_values[0], lat_values[0]

    corner_cache[key] = (float(lon), float(lat))
    return corner_cache[key]


def _tile_corners_wgs84(x: int, y: int, width: int, height: int, transform, src_crs, corner_cache: dict) -> dict:
    """Возвращает четыре угла тайла в WGS84 в порядке пиксельного окна."""
    return {
        "top_left": _pixel_corner_wgs84(x, y, transform, src_crs, corner_cache),
        "top_right": _pixel_corner_wgs84(x + width, y, transform, src_crs, corner_cache),
        "bottom_right": _pixel_corner_wgs84(x + width, y + height, transform, src_crs, corner_cache),
        "bottom_left": _pixel_corner_wgs84(x, y + height, transform, src_crs, corner_cache),
    }


def _bbox_from_corners(corners: dict) -> Tuple[float, float, float, float]:
    """Строит bbox только для фильтрации и грубого позиционирования карты."""
    lon_values = [point[0] for point in corners.values()]
    lat_values = [point[1] for point in corners.values()]
    return min(lon_values), min(lat_values), max(lon_values), max(lat_values)


def _corner_delta(first: Tuple[float, float], second: Tuple[float, float]) -> float:
    """Считает максимальное расхождение между двумя углами в градусах."""
    return max(abs(first[0] - second[0]), abs(first[1] - second[1]))


def validate_adjacent_tile_corners(tile_metadata: List[dict]) -> int:
    """Проверяет совпадение углов у смежных тайлов одной пиксельной сетки."""
    by_origin = {(item["tile_x"], item["tile_y"]): item for item in tile_metadata}
    checked_edges = 0
    mismatches = []

    for item in tile_metadata:
        right_neighbor = by_origin.get((item["tile_x"] + item["tile_width"], item["tile_y"]))
        if right_neighbor:
            checked_edges += 2
            top_delta = _corner_delta(
                (item["top_right_lon"], item["top_right_lat"]),
                (right_neighbor["top_left_lon"], right_neighbor["top_left_lat"]),
            )
            bottom_delta = _corner_delta(
                (item["bottom_right_lon"], item["bottom_right_lat"]),
                (right_neighbor["bottom_left_lon"], right_neighbor["bottom_left_lat"]),
            )
            if top_delta > CORNER_MATCH_TOLERANCE:
                mismatches.append((item["tile_id"], right_neighbor["tile_id"], "top", top_delta))
            if bottom_delta > CORNER_MATCH_TOLERANCE:
                mismatches.append((item["tile_id"], right_neighbor["tile_id"], "bottom", bottom_delta))

        bottom_neighbor = by_origin.get((item["tile_x"], item["tile_y"] + item["tile_height"]))
        if bottom_neighbor:
            checked_edges += 2
            left_delta = _corner_delta(
                (item["bottom_left_lon"], item["bottom_left_lat"]),
                (bottom_neighbor["top_left_lon"], bottom_neighbor["top_left_lat"]),
            )
            right_delta = _corner_delta(
                (item["bottom_right_lon"], item["bottom_right_lat"]),
                (bottom_neighbor["top_right_lon"], bottom_neighbor["top_right_lat"]),
            )
            if left_delta > CORNER_MATCH_TOLERANCE:
                mismatches.append((item["tile_id"], bottom_neighbor["tile_id"], "left", left_delta))
            if right_delta > CORNER_MATCH_TOLERANCE:
                mismatches.append((item["tile_id"], bottom_neighbor["tile_id"], "right", right_delta))

    if mismatches:
        preview = "; ".join(
            f"{first} -> {second}, грань {edge}, delta={delta:.12f}"
            for first, second, edge, delta in mismatches[:8]
        )
        raise ValueError(
            "Нарушена стыковка координат смежных тайлов. "
            f"Ошибок: {len(mismatches)}. Примеры: {preview}"
        )

    return checked_edges


def slice_geotiff(tif_path: str, source_object_key: str, config: AppConfig, s3_client) -> List[dict]:
    """
    Режет GeoTIFF на тайлы, создаёт маски и загружает результаты в MinIO.

    Нарезка идёт в исходной CRS снимка, чтобы не растягивать SAR-данные в градусной сетке
    на высоких широтах. Для карты сохраняются границы каждого окна в EPSG:4326.
    """
    tiles_dir, masks_dir = _build_tile_directories(config, source_object_key)
    segmenter = get_segmenter(config)
    worker_name = get_worker_name()

    with rasterio.open(tif_path) as src:
        width, height = src.width, src.height
        transform = src.transform
        src_crs = src.crs

        if src_crs is None:
            print(f"Источник {source_object_key} пропущен: не задана система координат.")
            return []

        tile_metadata = []
        tile_entries = []
        contrast_samples = []
        tile_id = 0
        scanned_tiles = 0
        x_start, y_start, x_stop, y_stop = _estimate_data_window(src, config.tile_size)
        corner_cache = {}

        log_processing_event(
            config,
            source_object_key,
            stage="scan_tiles",
            message=(
                f"Начато сканирование тайлов в исходной CRS. Размер снимка: {width}x{height} px, "
                f"размер тайла: {config.tile_size}. Область данных: "
                f"x={x_start}..{x_stop}, y={y_start}..{y_stop}."
            ),
            worker_name=worker_name,
        )

        for y in range(y_start, y_stop, config.tile_size):
            for x in range(x_start, x_stop, config.tile_size):
                scanned_tiles += 1
                tile_width = min(config.tile_size, x_stop - x, width - x)
                tile_height = min(config.tile_size, y_stop - y, height - y)
                window = Window(x, y, tile_width, tile_height)
                band, valid_mask = _read_band_and_valid_mask(src, window)

                if not _is_valid_tile(band, valid_mask, config.tile_min_valid_ratio):
                    continue

                tile_entries.append((x, y, tile_width, tile_height))
                contrast_samples.append(_sample_valid_values(band[valid_mask]))

                if scanned_tiles % 1000 == 0:
                    print(
                        f"Сканирование {source_object_key}: просмотрено {scanned_tiles} окон, "
                        f"валидных тайлов {len(tile_entries)}."
                    )

        if not tile_entries:
            print(f"Для снимка {source_object_key} не найдено валидных тайлов.")
            log_processing_event(
                config,
                source_object_key,
                stage="scan_tiles",
                message="Валидные тайлы не найдены.",
                level="WARNING",
                worker_name=worker_name,
            )
            return []

        all_data = np.concatenate(contrast_samples)
        if all_data.size == 0:
            print(f"Для снимка {source_object_key} все значения оказались NaN/Inf.")
            return []

        global_min, global_max = np.percentile(all_data, (2, 98))
        if global_max - global_min == 0:
            print(f"Для снимка {source_object_key} диапазон значений равен нулю.")
            return []

        print(
            f"Контрастное растяжение для {source_object_key}: "
            f"min = {global_min:.4f}, max = {global_max:.4f}"
        )
        log_processing_event(
            config,
            source_object_key,
            stage="scan_tiles",
            message=(
                f"Подготовлены тайлы: {len(tile_entries)} из {scanned_tiles}. "
                f"Контрастное окно: {global_min:.4f}..{global_max:.4f}."
            ),
            progress_current=len(tile_entries),
            progress_total=scanned_tiles,
            worker_name=worker_name,
        )

        source_name = Path(source_object_key).stem
        total_tiles = len(tile_entries)

        for index, (x, y, tile_width, tile_height) in enumerate(tile_entries, start=1):
            window = Window(x, y, tile_width, tile_height)
            band, valid_mask = _read_band_and_valid_mask(src, window)
            normalized_band = np.zeros_like(band, dtype=np.float32)
            normalized_band[valid_mask] = 255 * (
                (band[valid_mask] - global_min) / (global_max - global_min)
            )
            normalized_band = np.clip(normalized_band, 0, 255)
            band_uint8 = normalized_band.astype(np.uint8)

            if np.max(band_uint8[valid_mask]) == 0:
                continue

            is_partial_tile = not np.all(valid_mask)
            tile_extension = ".png" if is_partial_tile else ".jpg"
            tile_filename = f"tile_{source_name}_{tile_id:04d}{tile_extension}"
            mask_filename = f"mask_{source_name}_{tile_id:04d}.png"

            tile_path = os.path.join(tiles_dir, tile_filename)
            mask_path = os.path.join(masks_dir, mask_filename)

            _save_tile_image(tile_path, band_uint8, valid_mask)

            corners = _tile_corners_wgs84(
                x,
                y,
                tile_width,
                tile_height,
                transform,
                src_crs,
                corner_cache,
            )
            west, south, east, north = _bbox_from_corners(corners)

            mask = segmenter.predict(tile_path)
            mask = np.where(valid_mask, mask, 0)
            mask = clip_mask_to_water(mask, west, south, east, north)
            segmenter.save_prediction(mask, mask_path)

            tile_object_key = f"{config.tile_prefix}{tile_filename}"
            mask_object_key = f"{config.mask_prefix}{mask_filename}"
            upload_file(s3_client, config.minio_bucket, tile_object_key, tile_path)
            upload_file(s3_client, config.minio_bucket, mask_object_key, mask_path)

            tile_metadata.append(
                {
                    "tile_id": tile_filename,
                    "tile_x": x,
                    "tile_y": y,
                    "tile_width": tile_width,
                    "tile_height": tile_height,
                    "top_left_lon": corners["top_left"][0],
                    "top_left_lat": corners["top_left"][1],
                    "top_right_lon": corners["top_right"][0],
                    "top_right_lat": corners["top_right"][1],
                    "bottom_right_lon": corners["bottom_right"][0],
                    "bottom_right_lat": corners["bottom_right"][1],
                    "bottom_left_lon": corners["bottom_left"][0],
                    "bottom_left_lat": corners["bottom_left"][1],
                    "mask_name": mask_filename,
                }
            )
            tile_id += 1

            if index == 1 or index % 50 == 0 or index == total_tiles:
                log_processing_event(
                    config,
                    source_object_key,
                    stage="generate_tiles",
                    message=f"Подготовлено тайлов: {index} из {total_tiles}.",
                    progress_current=index,
                    progress_total=total_tiles,
                    worker_name=worker_name,
                )

        checked_edges = validate_adjacent_tile_corners(tile_metadata)
        log_processing_event(
            config,
            source_object_key,
            stage="validate_tile_corners",
            message=f"Проверены углы смежных тайлов. Совпавших реберных углов: {checked_edges}.",
            progress_current=checked_edges,
            progress_total=checked_edges,
            worker_name=worker_name,
        )

    return tile_metadata
