# Сервис обработки спутниковых снимков

Проект обрабатывает спутниковые GeoTIFF-снимки через цепочку `producer -> MinIO -> Kafka -> Spark -> PostgreSQL/MinIO -> FastAPI map`.

## Что делает система

- `producer` следит за папкой `data/`, загружает новые GeoTIFF в `MinIO` и отправляет событие в `Kafka`.
- `Kafka` хранит события о новых объектах `MinIO` между загрузчиком и Spark.
- `spark-processor` читает события из Kafka, дополнительно сверяет `raw/` в MinIO и запускает обработку новых снимков.
- Для каждого снимка Spark режет GeoTIFF на тайлы, строит маски, сохраняет тайлы и маски в `MinIO`.
- Для каждого снимка создаётся отдельный shapefile маски, без перезаписи результатов других снимков.
- `PostgreSQL` хранит метаданные тайлов и статусы обработки.
- `MinIO` дополнительно хранит JSON-маркеры обработанных снимков в `processed/status/`.
- `FastAPI` показывает карту, тайлы, маски и позволяет скачать Sentinel-1 по текущей области карты.

## Запуск

```bash
docker compose up --build
```

После запуска доступны:

- веб-карта: `http://localhost:8000/map`
- MinIO Console: `http://localhost:9001`
- Spark Master UI: `http://localhost:8085`
- PostgreSQL: `localhost:5432`
- Kafka: `localhost:9092`

## Входные данные

Положите `*.tif` или `*.tiff` в папку [data](/D:/Desktop/Diplom/diplom2/data). `producer` загрузит файл в `satellite-images/raw/`, отправит событие в Kafka и удалит локальный файл после успешной загрузки.

## Параллельная обработка

Количество параллельных обработчиков задаётся переменной:

```text
PROCESSOR_WORKERS=3
```

В compose подняты три отдельных Spark worker-контейнера:

- `spark-worker-1`
- `spark-worker-2`
- `spark-worker-3`

Каждый worker получает выделенные ресурсы через `docker-compose`, а Spark запускает обработку снимков через `sparkContext.parallelize(..., PROCESSOR_WORKERS)`, поэтому при трёх новых снимках каждый может попасть на отдельный worker.

## Краевые тайлы и неровный footprint снимка

Сервис больше не опирается только на «идеальные» прямоугольные тайлы. При нарезке:

- для каждого окна читается маска валидных пикселей;
- краевые тайлы сохраняются, если доля валидных данных достаточна;
- неполные тайлы сохраняются с прозрачными областями вне реального footprint снимка;
- маска тайла дополнительно обнуляется вне валидной области.

Это уменьшает рваные края у снимков, которые лежат под углом или имеют неровную геометрию по краям.

Порог минимальной доли валидных пикселей задаётся переменной:

```text
TILE_MIN_VALID_RATIO=0.08
```

## Отделение акватории от суши

Для фильтрации суши используется детальная береговая маска Natural Earth 10m, смонтированная в Spark-контейнеры.

При необходимости можно подменить её своим более точным источником:

```text
LAND_MASK_PATH=/opt/land-mask/ne_10m_land.shp
```

По умолчанию эта маска лежит в рабочей директории проекта и монтируется в Spark-контейнеры из:

```text
data/land-mask/ne_10m_land/
```

Если детальная маска недоступна, сервис использует резервный источник `naturalearth_lowres`.

## Мониторинг обработки

Сервис сохраняет стадии и события обработки в PostgreSQL:

- текущий статус задачи, стадия, воркер и прогресс лежат в `image_processing_tasks`;
- журнал событий воркеров лежит в `processing_logs`.

Во фронтенде на карте появился блок мониторинга обработки. Дополнительно доступны API:

```text
/api/processing-overview
```

Этот endpoint возвращает:

- последние задачи обработки;
- последние события воркеров;
- текущие стадии: скачивание, нарезка, запись метаданных, сборка shapefile и завершение.

## Учёт обработанных снимков

Учёт ведётся в двух местах:

- `PostgreSQL`, таблица `image_processing_tasks`;
- `MinIO`, маркеры `satellite-images/processed/status/<имя_снимка>.json`.

Перед обработкой `spark-processor` проверяет оба источника и не запускает уже обработанные снимки повторно.

## Shapefile по каждому снимку

Shapefile сохраняется отдельно для каждого исходного снимка:

```text
satellite-images/processed/shapefiles/<имя_снимка>/
```

Пример:

```text
processed/shapefiles/sentinel1_20211101_0/sentinel1_20211101_0_masks.shp
processed/shapefiles/sentinel1_20211101_0/sentinel1_20211101_0_masks.shx
processed/shapefiles/sentinel1_20211101_0/sentinel1_20211101_0_masks.dbf
processed/shapefiles/sentinel1_20211101_0/sentinel1_20211101_0_masks.prj
```

## Веб-карта

Карта находится по адресу `http://localhost:8000/map`.

На карте можно:

- видеть несколько снимков одновременно;
- отображать слой тайлов конкретного снимка;
- отображать GeoJSON-слой маски конкретного снимка;
- скрывать тайлы конкретного снимка;
- скрывать маску конкретного снимка;
- скрывать весь снимок одной кнопкой;
- скачать shapefile конкретного снимка;
- скачать Sentinel-1 GeoTIFF по текущей области карты.

GeoJSON для маски отдаётся endpoint-ом:

```text
/api/mask-geojson?source=<ключ_снимка>
```

Тайлы отдаются endpoint-ом:

```text
/api/tiles-geojson?source=<ключ_снимка>
```

## Скачивание снимков с карты

На карте нажмите кнопку `Скачать снимки по текущей области карты`. FastAPI берёт текущие границы Leaflet-карты и скачивает Sentinel-1 VV за выбранный период.

Файлы сохраняются в `data/`, после чего автоматически проходят обычный путь:

```text
data -> producer -> MinIO raw/ -> Kafka -> Spark -> MinIO processed/ -> PostgreSQL -> web map
```

Для Earth Engine нужна авторизация внутри контейнера или сервисный аккаунт. Проект задаётся переменной `EE_PROJECT`.

## Основные файлы

- [services/producer/app/producer.py](/D:/Desktop/Diplom/diplom2/services/producer/app/producer.py) — загрузка GeoTIFF в MinIO и отправка событий в Kafka.
- [services/spark/app/main.py](/D:/Desktop/Diplom/diplom2/services/spark/app/main.py) — чтение Kafka, фильтрация уже обработанных снимков и запуск Spark-задач.
- [services/spark/app/geotiff_processor.py](/D:/Desktop/Diplom/diplom2/services/spark/app/geotiff_processor.py) — нарезка GeoTIFF и генерация тайлов/масок.
- [services/spark/app/mask_assembler.py](/D:/Desktop/Diplom/diplom2/services/spark/app/mask_assembler.py) — сборка shapefile отдельно для каждого снимка.
- [services/spark/app/storage.py](/D:/Desktop/Diplom/diplom2/services/spark/app/storage.py) — работа с MinIO и маркерами обработки.
- [services/web/app/main.py](/D:/Desktop/Diplom/diplom2/services/web/app/main.py) — API карты, GeoJSON и скачивание снимков.
- [services/web/app/templates/map.html](/D:/Desktop/Diplom/diplom2/services/web/app/templates/map.html) — интерфейс карты.

## Структура MinIO

- `satellite-images/raw/` — исходные GeoTIFF.
- `satellite-images/processed/tiles/` — JPEG-тайлы.
- `satellite-images/processed/masks/` — маски тайлов.
- `satellite-images/processed/shapefiles/<имя_снимка>/` — shapefile конкретного снимка.
- `satellite-images/processed/status/` — JSON-маркеры обработанных снимков.
