import os
import cv2
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import unary_union
from sqlalchemy.orm import Session
from db.models import Tile
from db.database import SessionLocal

def create_mask_shapefile(
    mask_dir="static/masks",
    shapefile_path="mask/masks.shp",
    filled_threshold=0.05,
    min_area=1e-5,
    merge_gaps=True,
    gap_buffer=0.0085  # ~850 м при EPSG:4326
):
    os.makedirs(os.path.dirname(shapefile_path), exist_ok=True)

    db: Session = SessionLocal()
    tiles = db.query(Tile).all()

    geometries = []
    attributes = []

    kernel = np.ones((5, 5), np.uint8)

    for tile in tiles:
        mask_path = os.path.join(mask_dir, tile.mask_name)
        if not os.path.exists(mask_path):
            continue

        mask = cv2.imread(mask_path, cv2.IMREAD_GRAYSCALE)
        if mask is None or np.max(mask) == 0:
            continue

        _, binary = cv2.threshold(mask, 127, 255, cv2.THRESH_BINARY)

        filled_pixels = cv2.countNonZero(binary)
        total_pixels = binary.size
        filled_ratio = filled_pixels / total_pixels

        if filled_ratio < filled_threshold:
            continue

        dilated = cv2.dilate(binary, kernel, iterations=1)
        smoothed = cv2.erode(dilated, kernel, iterations=1)

        contours, _ = cv2.findContours(smoothed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        lon_left, lat_top = tile.top_left_lon, tile.top_left_lat
        lon_right, lat_bottom = tile.bottom_right_lon, tile.bottom_right_lat
        h, w = smoothed.shape
        pixel_width = (lon_right - lon_left) / w
        pixel_height = (lat_top - lat_bottom) / h

        for contour in contours:
            if len(contour) < 3:
                continue

            coords = []
            for x, y in contour.squeeze():
                lon = lon_left + x * pixel_width
                lat = lat_top - y * pixel_height
                coords.append((lon, lat))

            poly = Polygon(coords)
            if poly.is_valid and poly.area >= min_area:
                geometries.append(poly)
                attributes.append({
                    "tile_id": tile.filename,
                    "mask_name": tile.mask_name,
                    "filled_ratio": filled_ratio
                })

    if not geometries:
        return

    # === Объединение и заделка щелей ===
    if merge_gaps:
        union_poly = unary_union(geometries)

        # расширяем и сжимаем для устранения микропустот между тайлами
        merged_poly = union_poly.buffer(gap_buffer).buffer(-gap_buffer)

        gdf = gpd.GeoDataFrame([{}], geometry=[merged_poly], crs="EPSG:4326")
    else:
        gdf = gpd.GeoDataFrame(attributes, geometry=geometries, crs="EPSG:4326")
        gdf["geometry"] = gdf.buffer(0)

    gdf.to_file(shapefile_path)
