CREATE TABLE IF NOT EXISTS image_processing_tasks (
    id BIGSERIAL PRIMARY KEY,
    object_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    processor_name TEXT NOT NULL,
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
    bottom_right_lon DOUBLE PRECISION,
    bottom_right_lat DOUBLE PRECISION,
    mask_name VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tiles_coordinates
ON tiles (top_left_lat, top_left_lon, bottom_right_lat, bottom_right_lon);
