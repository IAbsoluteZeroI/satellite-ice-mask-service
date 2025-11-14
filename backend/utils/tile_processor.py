import os
import shutil
import numpy as np
from PIL import Image
import rasterio
from rasterio.windows import Window
from rasterio.warp import transform as reproject_coords
from rasterio.transform import Affine
from utils.segmenter import ImageSegmentation

def reset_directory(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)

segmenter = ImageSegmentation("utils/UnetPP.pth")

def slice_geotiff(tif_path, out_dir="static/tiles", tile_size=1000):
    reset_directory("static/masks")
    reset_directory(out_dir)

    with rasterio.open(tif_path) as src:
        width, height = src.width, src.height
        transform: Affine = src.transform
        src_crs = src.crs
        dst_crs = "EPSG:4326"

        tile_metadata = []
        tile_id = 0

        tile_data_list = []  # список массивов тайлов
        coords_list = []     # список координат тайлов

        # === Первый проход: собираем валидные тайлы ===
        for y in range(0, height, tile_size):
            for x in range(0, width, tile_size):
                w = min(tile_size, width - x)
                h = min(tile_size, height - y)
                window = Window(x, y, w, h)

                band = src.read(1, window=window)

                # Исключаем тайлы с NaN, Inf или одинаковыми значениями
                if (
                    np.isnan(band).all() or
                    np.isinf(band).any() or
                    np.all(band == band[0])
                ):
                    continue

                tile_data_list.append(band)
                coords_list.append((x, y, w, h))

        if not tile_data_list:
            print("Нет валидных тайлов для обработки.")
            return []

        # === Контрастное растяжение: глобальные min/max через percentiles ===
        all_data = np.stack(tile_data_list)
        all_data = all_data[np.isfinite(all_data)]

        if all_data.size == 0:
            print("Все данные содержат только NaN/Inf.")
            return []

        global_min, global_max = np.percentile(all_data, (2, 98))

        if global_max - global_min == 0:
            print("Диапазон значений равен 0. Обработка невозможна.")
            return []

        print(f"[+] Контрастное растяжение: min = {global_min:.4f}, max = {global_max:.4f}")

        # === Второй проход: нормализация, сохранение и сегментация ===
        for idx, (band, (x, y, w, h)) in enumerate(zip(tile_data_list, coords_list)):
            if not np.isfinite(band).all():
                continue

            # Контрастная нормализация
            norm_band = 255 * (band - global_min) / (global_max - global_min)
            norm_band = np.clip(norm_band, 0, 255)
            band_uint8 = norm_band.astype(np.uint8)

            if np.max(band_uint8) == 0:
                continue

            # Сохранение изображения
            img = Image.fromarray(band_uint8)
            tile_filename = f"tile_{os.path.basename(tif_path).replace('.tif','')}_{tile_id:04d}.jpg"
            tile_path = os.path.join(out_dir, tile_filename)
            img.save(tile_path, "JPEG")

            # Сегментация и сохранение маски
            mask = segmenter.predict(tile_path)
            mask_name = f"mask_{tile_id:04d}.jpg"
            mask_path = os.path.join("static/masks", mask_name)
            segmenter.save_prediction(mask, mask_path)

            # Географические координаты (в WGS84)
            top_left_x, top_left_y = transform * (x, y)
            bottom_right_x, bottom_right_y = transform * (x + w, y + h)

            lon_top_left, lat_top_left = reproject_coords("EPSG:32644", "EPSG:4326", [top_left_x], [top_left_y])
            lon_bottom_right, lat_bottom_right = reproject_coords("EPSG:32644", "EPSG:4326", [bottom_right_x], [bottom_right_y])

            tile_metadata.append({
                "tile_id": tile_filename,
                "top_left_lon": lon_top_left[0],
                "top_left_lat": lat_top_left[0],
                "bottom_right_lon": lon_bottom_right[0],
                "bottom_right_lat": lat_bottom_right[0],
                "mask_name": mask_name,
            })

            tile_id += 1
        return tile_metadata
