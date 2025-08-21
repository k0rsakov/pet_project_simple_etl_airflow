# pet_project_simple_etl_airflow

Порядок работы над проектом:

1) Создание виртуального окружения
2) Получение `api_key` от [weatherapi](https://www.weatherapi.com/)
3) Поднятие инфраструктуры
4) Подключение к Minio
    - Создание `bucket` в Minio (в коде прописан `prod`)
    - Создание `access_key`
    - Создание `secret_key`
5) Создание `Variables` в Airflow через UI или [create_variable.py](handles/create_variable.py)
6) Создание `Connection` в Airflow через UI или [create_connection.py](handles/create_connection.py)
7) Подключение к PostgresSQL
8) Создание таблицы в PostgresSQL
9) Запуск DAG в Airflow

## Команды и скрипты

### Создание виртуального окружения

```bash
python3.12 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install poetry && \
poetry lock && \
poetry install
```

#### Добавление новых зависимостей в окружение

```bash
poetry lock && \
poetry install
```

### Поднятие инфраструктуры

```bash
docker compose up -d
```

Если отображаются исключения, то необходимо выполнить команду ниже, так как в проекте используется своя сборка Airflow:

```bash
docker compose build
```

### Подключение к Minio

Параметры подключения стандартные:

- `login`: `minioadmin`
- `password`: `minioadmin`

### Создание таблицы в PostgresSQL

```sql
DROP TABLE IF EXISTS public.weather_raw;

CREATE TABLE IF NOT EXISTS public.weather_raw (
	location_name text NULL,
	location_region text NULL,
	location_country text NULL,
	location_lat float8 NULL,
	location_lon float8 NULL,
	location_tz_id text NULL,
	location_localtime_epoch int8 NULL,
	location_localtime text NULL,
	forecastday_date text NULL,
	params jsonb NULL
);
```