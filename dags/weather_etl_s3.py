import json
from datetime import timedelta
from io import BytesIO
from typing import Any

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from minio import Minio

MINIO_ENDPOINT = "minio:9000"
MINIO_A_KEY = Variable.get("access_key")
MINIO_S_KEY = Variable.get("secret_key")
WEATHER_API_KEY = Variable.get("weather_api_key")

default_args = {
    "owner": "i.korsakov",
    "depends_on_past": True,
    "start_date": pendulum.datetime(2025, 8, 10),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "catchup": True,
    "retry_delay": timedelta(minutes=5),
}


def get_weather_history(date: str, api_key: str, city_name: str) -> dict:
    """
    Получает исторические данные о погоде для указанного города и даты.

    :param date: Дата в формате 'YYYY-MM-DD'.
    :param api_key: Ключ API для доступа к weatherapi.com.
    :param city_name: Название города (например, 'Irkutsk').
    :return: Словарь с данными о погоде.
    :raises requests.exceptions.HTTPError: При HTTP ошибках.
    :raises requests.exceptions.RequestException: При ошибках запроса.
    """
    url = "https://api.weatherapi.com/v1/history.json"

    params = {
        "key": api_key,
        "q": city_name,
        "dt": date,
    }

    try:
        response = requests.get(url=url, params=params, timeout=600)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP ошибка: {http_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        print(f"Ошибка запроса: {req_err}")
        raise
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        raise


def weather_dict_to_raw_df(data: dict) -> pd.DataFrame:
    """
    Преобразует словарь погоды в "полуплоский" DataFrame для RAW-слоя.

    :param data: Словарь, полученный от WeatherAPI.
    :return: pd.DataFrame с одной строкой, содержащей обработанные данные о погоде.
    """
    row = {}

    # 1. Извлекаем плоские поля из location
    location = data.get("location", {})
    row["location_name"] = location.get("name")
    row["location_region"] = location.get("region")
    row["location_country"] = location.get("country")
    row["location_lat"] = location.get("lat")
    row["location_lon"] = location.get("lon")
    row["location_tz_id"] = location.get("tz_id")
    row["location_localtime_epoch"] = location.get("localtime_epoch")
    row["location_localtime"] = location.get("localtime")

    # 2. Извлекаем дату прогноза (forecastday -> date)
    forecastday = None
    if data.get("forecast") and isinstance(data["forecast"].get("forecastday"), list) and len(data["forecast"]["forecastday"]) > 0:
        forecastday = data["forecast"]["forecastday"][0]

    row["forecastday_date"] = forecastday.get("date") if forecastday else None

    # 3. Всё содержимое forecastday сохраняем как JSON в колонке params
    if forecastday:
        params = {k: v for k, v in forecastday.items() if k != "date"}
        row["params"] = json.dumps(params, ensure_ascii=False, default=str)
    else:
        row["params"] = None

    return pd.DataFrame([row])


def get_minio_client():
    """
    Создает MinIO клиент с учетными данными из Variables.

    :return: Объект MinIO клиента.
    """
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_A_KEY,
        secret_key=MINIO_S_KEY,
        secure=False,  # Установите True если используете HTTPS
    )


def get_storage_options() -> dict:
    """
    Получает параметры подключения для pandas S3 operations.

    :return: Словарь с параметрами подключения для pandas.
    """
    protocol = "http"  # Измените на "https" если используете HTTPS
    endpoint_url = f"{protocol}://{MINIO_ENDPOINT}"

    return {
        "key": MINIO_A_KEY,
        "secret": MINIO_S_KEY,
        "client_kwargs": {"endpoint_url": endpoint_url},
    }


def extract_weather_data_s3(**context: dict[str, Any]) -> str:
    """
    Получение данных о погоде и сохранение в S3.

    :param context: Контекст DAG, содержащий информацию о выполнении задачи.
    :return: S3 ключ сохраненного JSON файла.
    """
    execution_date = context["data_interval_end"].format("YYYY-MM-DD")

    # Получаем данные о погоде
    weather_data = get_weather_history(
        date=execution_date,
        api_key=WEATHER_API_KEY,
        city_name="Irkutsk",
    )

    # Подключение к MinIO
    minio_client = get_minio_client()
    bucket_name = "prod"
    object_name = f"weather/raw/weather_{execution_date}.json"

    # Убеждаемся что bucket существует
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Преобразуем данные в JSON строку
    json_data = json.dumps(weather_data, ensure_ascii=False, indent=2)
    json_bytes = json_data.encode("utf-8")

    # Сохраняем в MinIO
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(json_bytes),
        length=len(json_bytes),
        content_type="application/json",
    )

    print(f"Weather data saved to s3://{bucket_name}/{object_name}")
    return object_name


def transform_weather_data_s3(**context: dict[str, Any]) -> str:
    """
    Преобразование JSON в CSV в S3.

    :param context: Контекст DAG, содержащий информацию о выполнении задачи.
    :return: S3 ключ сохраненного CSV файла.
    """
    execution_date = context["data_interval_end"].format("YYYY-MM-DD")
    bucket_name = "prod"
    json_object_name = f"weather/raw/weather_{execution_date}.json"
    csv_object_name = f"weather/processed/weather_{execution_date}.csv"

    # Подключение к MinIO
    minio_client = get_minio_client()

    # Читаем JSON файл из MinIO
    response = minio_client.get_object(bucket_name, json_object_name)
    weather_data = json.loads(response.data.decode("utf-8"))

    # Преобразуем в DataFrame
    df = weather_dict_to_raw_df(weather_data)

    # Сохраняем DataFrame как CSV в S3 используя pandas
    storage_options = get_storage_options()
    s3_path = f"s3://{bucket_name}/{csv_object_name}"

    df.to_csv(
        path_or_buf=s3_path,
        index=False,
        escapechar="\\",
        compression=None,  # Без компрессии для простоты
        storage_options=storage_options,
    )

    print(f"Transformed data saved to {s3_path}")
    return csv_object_name


def load_to_postgres_s3(**context: dict[str, Any]) -> None:
    """
    Загрузка данных в PostgreSQL из S3.

    :param context: Контекст DAG, содержащий информацию о выполнении задачи.
    :return: Функция ничего не возвращает, она производит загрузку данных в базу.
    """
    execution_date = context["data_interval_end"].format("YYYY-MM-DD")
    bucket_name = "prod"
    csv_object_name = f"weather/processed/weather_{execution_date}.csv"

    # Читаем CSV файл из S3 используя pandas
    storage_options = get_storage_options()
    s3_path = f"s3://{bucket_name}/{csv_object_name}"

    df = pd.read_csv(
        filepath_or_buffer=s3_path,
        storage_options=storage_options,
        compression=None,  # Без компрессии
    )

    # Подключение к PostgreSQL через PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="pg_con")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Загружаем данные в таблицу
    df.to_sql(name="weather_raw", con=engine, if_exists="append", index=False)

    print(f"Data loaded to PostgreSQL from {s3_path}")


def cleanup_s3_files(**context: dict[str, Any]) -> None:
    """
    Удаление файлов из S3.

    :param context: Контекст DAG, содержащий информацию о выполнении задачи.
    :return: Функция ничего не возвращает, она производит очистку файлов из S3.
    """
    execution_date = context["data_interval_end"].format("YYYY-MM-DD")
    bucket_name = "prod"

    # Подключение к MinIO
    minio_client = get_minio_client()

    # Список файлов для удаления
    objects_to_delete = [
        f"weather/raw/weather_{execution_date}.json",
        f"weather/processed/weather_{execution_date}.csv",
    ]

    # Удаляем файлы
    for object_name in objects_to_delete:
        try:
            minio_client.remove_object(bucket_name, object_name)
            print(f"Removed file: s3://{bucket_name}/{object_name}")
        except Exception as e:
            print(f"Error removing s3://{bucket_name}/{object_name}: {e}")


with DAG(
        dag_id="weather_etl_s3_minio",
        schedule_interval="0 10 * * *",
        default_args=default_args,
        tags=["weather", "etl", "s3", "minio"],
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    extract_task_s3 = PythonOperator(
        task_id="extract_weather_data_s3",
        python_callable=extract_weather_data_s3,
    )

    transform_task_s3 = PythonOperator(
        task_id="transform_weather_data_s3",
        python_callable=transform_weather_data_s3,
    )

    load_task_s3 = PythonOperator(
        task_id="load_to_postgres_s3",
        python_callable=load_to_postgres_s3,
    )

    cleanup_task_s3 = PythonOperator(
        task_id="cleanup_s3_files",
        python_callable=cleanup_s3_files,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> extract_task_s3 >> transform_task_s3 >> load_task_s3 >> cleanup_task_s3 >> end
