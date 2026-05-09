import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    raw_prefix: str
    processed_prefix: str
    tile_prefix: str
    mask_prefix: str
    shapefile_prefix: str
    status_prefix: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_consumer_group: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    tile_size: int
    tile_min_valid_ratio: float
    work_dir: str
    local_input_dir: str
    model_path: str
    land_mask_path: str
    poll_interval: int
    processor_workers: int
    mask_filled_threshold: float
    mask_min_area: float
    mask_merge_gaps: bool
    mask_gap_buffer: float


def load_config() -> AppConfig:
    """Собирает конфигурацию сервиса из переменных окружения."""
    return AppConfig(
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        minio_bucket=os.getenv("MINIO_BUCKET", "satellite-images"),
        raw_prefix=os.getenv("RAW_PREFIX", "raw/"),
        processed_prefix=os.getenv("PROCESSED_PREFIX", "processed/"),
        tile_prefix=os.getenv("TILE_PREFIX", "processed/tiles/"),
        mask_prefix=os.getenv("MASK_PREFIX", "processed/masks/"),
        shapefile_prefix=os.getenv("SHAPEFILE_PREFIX", "processed/shapefiles/"),
        status_prefix=os.getenv("STATUS_PREFIX", "processed/status/"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "raw-images"),
        kafka_consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "spark-processor"),
        postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "satellite"),
        postgres_user=os.getenv("POSTGRES_USER", "satellite"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "satellite"),
        tile_size=int(os.getenv("TILE_SIZE", "1000")),
        tile_min_valid_ratio=float(os.getenv("TILE_MIN_VALID_RATIO", "0.08")),
        work_dir=os.getenv("WORK_DIR", "/tmp/satellite-processing"),
        local_input_dir=os.getenv("LOCAL_INPUT_DIR", "/data/input"),
        model_path=os.getenv("MODEL_PATH", "/app/models/UnetPP.pth"),
        land_mask_path=os.getenv("LAND_MASK_PATH", "/opt/land-mask/ne_10m_land/ne_10m_land.shp"),
        poll_interval=int(os.getenv("POLL_INTERVAL", "5")),
        processor_workers=int(os.getenv("PROCESSOR_WORKERS", "3")),
        mask_filled_threshold=float(os.getenv("MASK_FILLED_THRESHOLD", "0.05")),
        mask_min_area=float(os.getenv("MASK_MIN_AREA", "0.00001")),
        mask_merge_gaps=os.getenv("MASK_MERGE_GAPS", "false").lower() == "true",
        mask_gap_buffer=float(os.getenv("MASK_GAP_BUFFER", "0.0")),
    )
