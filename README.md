# Сервис обработки спутниковых снимков

## Запуск проекта

Требуется установленный `Docker` и `Docker Compose`. Если нужна обработка на GPU, также должен быть настроен `NVIDIA Container Toolkit`.

1. Поместите исходные GeoTIFF-файлы (`*.tif` или `*.tiff`) в папку `data/`.

2. Запустите все сервисы:

```bash
docker compose up --build -d
```

3. Откройте веб-карту:

```text
http://localhost:8080/map
```

## Полезные адреса

- Веб-карта: `http://localhost:8080/map`
- MinIO Console: `http://localhost:9001`
- Spark Master UI: `http://localhost:8085`
- PostgreSQL: `localhost:5432`
- Kafka: `localhost:9092`

## Полезные команды

Посмотреть состояние контейнеров:

```bash
docker compose ps
```

Посмотреть логи обработки:

```bash
docker compose logs -f spark-processor
```

Посмотреть логи веб-приложения:

```bash
docker compose logs -f web
```

Остановить проект:

```bash
docker compose down
```
