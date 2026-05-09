import os
from functools import lru_cache

import geopandas as gpd
import numpy as np
from rasterio.features import rasterize
from rasterio.transform import from_bounds
from shapely.geometry import box
from shapely.ops import unary_union

_DEFAULT_LAND_DATASET_NAME = "naturalearth_lowres"
_DEFAULT_HIGH_RES_PATH = "/opt/land-mask/ne_10m_land/ne_10m_land.shp"


def _resolve_land_mask_path() -> str:
    """Возвращает путь к детальной маске суши из переменной окружения или по умолчанию."""
    return os.getenv("LAND_MASK_PATH", _DEFAULT_HIGH_RES_PATH)


@lru_cache(maxsize=1)
def get_land_geodataframe():
    """Загружает полигоны суши в EPSG:4326 и кэширует их для всех тайлов worker-процесса."""
    custom_path = _resolve_land_mask_path()

    try:
        if custom_path and os.path.exists(custom_path):
            land_gdf = gpd.read_file(custom_path)
            if land_gdf.crs is None:
                land_gdf = land_gdf.set_crs("EPSG:4326")
            land_gdf = land_gdf.to_crs("EPSG:4326")
            land_gdf = land_gdf[~land_gdf.geometry.is_empty & land_gdf.geometry.notna()]
            print(f"Используется детальная маска суши: {custom_path}")
            return land_gdf
    except Exception as error:
        print(f"Не удалось загрузить детальную маску суши {custom_path}: {error}")

    try:
        dataset_path = gpd.datasets.get_path(_DEFAULT_LAND_DATASET_NAME)
        land_gdf = gpd.read_file(dataset_path).to_crs("EPSG:4326")
        land_gdf = land_gdf[~land_gdf.geometry.is_empty & land_gdf.geometry.notna()]
        print("Используется резервная маска суши naturalearth_lowres.")
        return land_gdf
    except Exception as error:
        print(f"Не удалось загрузить резервную маску суши Natural Earth: {error}")
        return None


@lru_cache(maxsize=1)
def get_land_geometry():
    """Возвращает объединённую геометрию суши для векторного вычитания."""
    land_gdf = get_land_geodataframe()
    if land_gdf is None or land_gdf.empty:
        return None
    return unary_union(land_gdf.geometry)


def _land_geometry_for_bounds(west: float, south: float, east: float, north: float):
    """Выбирает только те полигоны суши, которые пересекают конкретный тайл."""
    land_gdf = get_land_geodataframe()
    if land_gdf is None or land_gdf.empty:
        return None

    tile_bounds = box(west, south, east, north)
    try:
        candidate_indices = list(land_gdf.sindex.intersection(tile_bounds.bounds))
        candidates = land_gdf.iloc[candidate_indices]
    except Exception:
        candidates = land_gdf

    if candidates.empty:
        return None

    candidates = candidates[candidates.intersects(tile_bounds)]
    if candidates.empty:
        return None

    clipped = candidates.geometry.intersection(tile_bounds)
    clipped = clipped[~clipped.is_empty & clipped.notna()]
    if clipped.empty:
        return None

    return unary_union(clipped)


def clip_mask_to_water(mask: np.ndarray, west: float, south: float, east: float, north: float) -> np.ndarray:
    """
    Попиксельно обнуляет участки маски, которые попадают на сушу.

    all_touched=True выбран намеренно: если пиксель хотя бы касается полигона суши, он удаляется
    из ледовой маски. Так прибрежные ложные срабатывания отсекаются максимально строго.
    """
    if east <= west or north <= south:
        return mask

    land_within_tile = _land_geometry_for_bounds(west, south, east, north)
    if land_within_tile is None or land_within_tile.is_empty:
        return mask

    height, width = mask.shape
    transform = from_bounds(west, south, east, north, width, height)
    land_raster = rasterize(
        [(land_within_tile, 1)],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        default_value=1,
        dtype="uint8",
        all_touched=True,
    )

    clipped_mask = np.array(mask, copy=True)
    clipped_mask[land_raster == 1] = 0
    return clipped_mask


def clip_geometry_to_water(geometry):
    """Вырезает из полигона части, попадающие на сушу."""
    land_geometry = get_land_geometry()
    if land_geometry is None or geometry.is_empty:
        return geometry

    try:
        result = geometry.difference(land_geometry)
        if not result.is_valid:
            result = result.buffer(0)
        return result
    except Exception:
        return geometry
