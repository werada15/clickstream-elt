# Clickstream ELT: пакетная аналитика

Учебный проект ELT-пайплайна для clickstream-данных. Исходный CSV пакетируется
и сжимается, партии хранятся в Garage через S3 API, затем Spark очищает и
преобразует данные в Parquet. Jupyter читает аналитический слой из Garage и
строит сводки и графики.

## Архитектура

data/source/clickstream_full.csv
        |
        | DuckDB: разделение на партии примерно по 100 MiB, сжатие в Zstandard
        v
data/batches/*.csv.zst
        |
        | boto3 / Garage S3 API
        v
Garage: clickstream-raw/raw/*.csv.zst
        |
        | boto3 download + Zstandard decompression
        v
data/tmp/*.csv
        |
        | Apache Spark: очистка, типизация, аналитические поля
        v
data/analytics/sessions/  (Parquet, partitioned by session_date)
        |
        | boto3 upload; старый curated prefix заменяется
        v
Garage: clickstream-analytics/curated/sessions/
        |
        | PyArrow S3 filesystem
        v
Jupyter: jupyter/analytics.ipynb


Это пакетный запуск, а не постоянно работающий планировщик. `docker compose up`
последовательно выполняет одноразовые этапы загрузки, распаковки, Spark и
публикации Parquet, после чего оставляет запущенными Garage и Jupyter.

## Состав проекта

`docker-compose.yaml` - Сервисы и порядок запуска ELT 
`main/extract.py` - Чтение исходного CSV и создание Zstandard-партий через DuckDB 
`main/load.py` - Загрузка сжатых партий в raw-бакет Garage 
`main/decompress.py` - Скачивание партий из Garage и распаковка для Spark 
`main/transform.py` - Очистка данных и запись Parquet 
`main/analytics_upload.py` - Замена объектов аналитического префикса в Garage 
`main/s3_utils.py` - Создание S3-клиента, ожидание Garage и проверка бакетов 
`main/clear_bucket.py` - Удаление всех объектов из указанного бакета 
`jupyter/analytics.ipynb` - Аналитика и графики по данным в Parquet 
`garage/garage.toml.example` - Безопасный шаблон конфигурации Garage

Spark читает распакованные файлы с локального volume. Доступ Spark к S3A не
настраивается: загрузку из Garage и публикацию обратно выполняют Python-этапы
через S3 API.

## Требования

- Docker Desktop с Docker Compose v2.
- Исходный CSV объёмом не менее 500 MB, помещённый в
  `data/source/...`, в данном случае `clickstream_full.csv`. Датасет не включён в Git-репозиторий.
- Для полного цикла рекомендуется иметь около 20 GB свободного места:
  кроме исходного CSV создаются сжатые партии, распакованные CSV, локальные
  Parquet-файлы, копии объектов в Garage и Docker-образы. Фактический объём
  зависит от датасета и степени сжатия.
- Для Spark выделено 4 GiB heap и два локальных worker-потока, чтобы
  ограничить параллельное потребление памяти при обработке и shuffle.
  Если Docker Desktop выделяет контейнерам меньше 6 GiB памяти, увеличьте
  лимит в настройках ресурсов Docker Desktop; для большого CSV желательно
  выделить 8 GiB и более.
- Для настройки RPC-секрета в PowerShell используется встроенный .NET
  криптографический генератор; остальные этапы выполняются в контейнерах.
- S3-ключ Garage с доступом на чтение, запись и удаление объектов в двух
  проектных бакетах.

## Первый запуск на Windows

Все команды ниже выполняются в PowerShell из каталога проекта.

### 1. Подготовка локальной конфигурации

Copy-Item .env.example .env
Copy-Item garage/garage.toml.example garage/garage.toml
notepad garage/garage.toml


Создайте уникальный секрет для RPC Garage и впишите его вместо
`REPLACE_WITH_64_HEX_CHARACTERS` в `garage/garage.toml`:

$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$bytes = New-Object byte[] 32
$rng.GetBytes($bytes)
$rpcSecret = ([System.BitConverter]::ToString($bytes) -replace '-', '').ToLowerInvariant()
$rpcSecret

Файлы `.env` и `garage/garage.toml` игнорируются Git. Не публикуйте их и не
вставляйте выданные ключи в исходный код.

### 2. Запуск Garage и инициализация хранилища

docker compose up -d garage
docker compose exec garage /garage status

В выводе `status` найдите ID узла Garage. Подставьте его вместо
`NODE_ID_HERE` (без угловых скобок), чтобы назначить ёмкость и применить
начальную конфигурацию кластера:

docker compose exec garage /garage layout assign -z dc1 -c 10G NODE_ID_HERE
docker compose exec garage /garage layout apply --version 1

Создайте ключ и два бакета, затем разрешите ключу чтение и запись. Команда
создания ключа напечатает access key и secret key:

docker compose exec garage /garage key create elt-local
docker compose exec garage /garage bucket create clickstream-raw
docker compose exec garage /garage bucket allow --read --write --owner clickstream-raw --key elt-local
docker compose exec garage /garage bucket create clickstream-analytics
docker compose exec garage /garage bucket allow --read --write --owner clickstream-analytics --key elt-local


Скопируйте значения ключа в `.env`:

KEY=выданный-access-key
SECRET=выданный-secret-key

Не делитесь secret key. Если бакеты и S3-ключ уже настроены, повторно
инициализировать Garage не нужно.

### 3. Поместите датасет и запустите ELT

Проверьте, что CSV находится по точному пути:

Test-Path data/source/clickstream_full.csv

Запустите все этапы:

docker compose up --build

При первом запуске будут собраны образы и обработан весь датасет; это может
занять время и потребовать несколько гигабайт свободного места. После окончания
одноразовые сервисы `load`, `decompress`, `spark` и `upload-analytics` должны
завершиться успешно. Garage и Jupyter остаются запущенными.

Откройте Jupyter по адресу `http://localhost:8888/lab`. Токен доступа показан в
логах:

docker compose logs jupyter

Откройте `analytics.ipynb` и выполните **Run All Cells**. Ноутбук подключается к
S3 endpoint `http://garage:3900` внутри Docker-сети и читает Parquet из
`clickstream-analytics/curated/sessions/`.

Полезные команды проверки:

docker compose ps -a
docker compose logs --tail=100 load decompress spark upload-analytics

`docker compose down` останавливает и удаляет контейнеры, но оставляет данные
на диске и состояние Garage.

## Что делает трансформация

`main/transform.py`:

1. Читает распакованные CSV через Spark.
2. Удаляет дубликаты по `session_id`.
3. Приводит временные, целочисленные и числовые поля к типам Spark.
4. Отбрасывает записи без `session_id`/`session_start` и записи с отрицательной
   длительностью, глубиной, количеством событий или суммами.
5. Добавляет дату, час и день недели сессии, длительность в минутах и признак
   вовлечённой сессии.
6. Записывает Parquet с разбиением по `session_date`.

Parquet — колонночный формат сжатых типизированных данных; он уменьшает
объём чтения для аналитических запросов по отдельным полям. Разбиение по дате
помогает запросам, ограниченным временным периодом. Raw-слой сохраняет исходные
партии отдельно от преобразованного аналитического слоя.

Ноутбук показывает зависимость глубины и длительности сессии от поиска,
вовлечённость по типу устройства, распределение метрик по браузерам и
зависимость глубины сессии от часа начала.

## Повторный запуск и очистка

При обычном повторном `docker compose up --build` имеющиеся
`data/batches/*.csv.zst` используются повторно. Аналитическая выгрузка удаляет
старые объекты только под `curated/sessions/`, прежде чем отправить новые
Parquet-файлы, поэтому этот префикс не накапливает дубликаты. Raw-объекты
перезаписываются по тем же S3-ключам.

Для полностью чистого прогона — из CMD в каталоге проекта:

docker compose down
docker compose up -d garage
docker compose run --rm load python clear_bucket.py clickstream-raw
docker compose run --rm load python clear_bucket.py clickstream-analytics
docker compose down
if exist data\batches rmdir /s /q data\batches
if exist data\tmp rmdir /s /q data\tmp
if exist data\analytics rmdir /s /q data\analytics
mkdir data\batches
mkdir data\tmp
mkdir data\analytics


Команда `clear_bucket.py` удаляет все объекты именно в указанном бакете.
В приведённой последовательности указаны только бакеты этого проекта. Не
удаляйте `data/source/clickstream_full.csv`, `garage/meta` или `garage/data`,
если хотите сохранить исходник и настроенный кластер Garage. Для полного
сброса самого кластера потребуется отдельная переинициализация Garage.