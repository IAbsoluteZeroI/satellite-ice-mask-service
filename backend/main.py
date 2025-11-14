from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import RedirectResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import shutil
import os
from utils.tile_processor import slice_geotiff
from utils.masks_collector import create_mask_shapefile
from db.database import init_db, SessionLocal, clear_database
from db.models import Tile
import geopandas as gpd
import zipfile
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # адрес фронта
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


init_db()
clear_database()

SHAPEFILE_NAME = "masks"
SHAPEFILE_DIR = "mask"
SHAPEFILE_PATH = os.path.join(SHAPEFILE_DIR, f"{SHAPEFILE_NAME}.shp")

UPLOAD_DIR = "uploaded"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def reset_directory(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)  # Удаляет всю папку и содержимое
    os.makedirs(path, exist_ok=True)

@app.get("/")
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload/")
def upload_tiff(request: Request, file: UploadFile = File(...)):
    reset_directory(UPLOAD_DIR)
    filepath = os.path.join(UPLOAD_DIR, file.filename)
    with open(filepath, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    tile_metadata = slice_geotiff(filepath)
    clear_database()
    db = SessionLocal()
    for tile in tile_metadata:
        db_tile = Tile(
            filename=tile["tile_id"],
            top_left_lon=tile["top_left_lon"],
            top_left_lat=tile["top_left_lat"],
            bottom_right_lon=tile["bottom_right_lon"],
            bottom_right_lat=tile["bottom_right_lat"],
            mask_name=tile["mask_name"],
        )
        db.add(db_tile)
    db.commit()
    db.close()

    create_mask_shapefile()

    if not os.path.exists(SHAPEFILE_PATH):
        return JSONResponse(content={"error": "Shapefile not found"}, status_code=404)

    gdf = gpd.read_file(SHAPEFILE_PATH)
    gdf = gdf.to_crs("EPSG:4326")
    geojson = gdf.__geo_interface__

    return JSONResponse(content=geojson)

@app.get("/map/")
def map_page(request: Request):
    return templates.TemplateResponse("map.html", {"request": request})


@app.get("/geojson")
def get_geojson():
    if not os.path.exists(SHAPEFILE_PATH):
        return JSONResponse(content={"error": "Shapefile not found"}, status_code=404)

    gdf = gpd.read_file(SHAPEFILE_PATH)
    gdf = gdf.to_crs("EPSG:4326")
    return JSONResponse(content=gdf.__geo_interface__)

@app.get("/download")
def download_shapefile():
    # Ищем все компоненты шейп-файла
    extensions = [".shp", ".shx", ".dbf", ".prj", ".cpg"]
    files = [os.path.join(SHAPEFILE_DIR, f"{SHAPEFILE_NAME}{ext}") for ext in extensions]
    existing_files = [f for f in files if os.path.exists(f)]

    if not existing_files:
        return JSONResponse(content={"error": "Shapefile components not found"}, status_code=404)

    # Упаковываем их во временный архив
    zip_path = os.path.join(SHAPEFILE_DIR, f"{SHAPEFILE_NAME}.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in existing_files:
            zipf.write(file, arcname=os.path.basename(file))

    return FileResponse(zip_path, filename=f"{SHAPEFILE_NAME}.zip", media_type='application/zip')
