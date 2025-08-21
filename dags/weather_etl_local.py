import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

def extract_weather_data(**context: dict[str, Any]) -> str:
    """
    Получение данных о погоде и сохранение в JSON файл.

    :param context: Контекст DAG, содержащий информацию о выполнении задачи.
    :return: Путь к сохраненному JSON файлу.
    """
    execution_date = context["ds"]  # Дата выполнения в формате YYYY-MM-DD

    # Создаем директорию для файлов если её нет
    weather_data_path = Path("/tmp/weather_data")
    weather_data_path.mkdir(parents=True, exist_ok=True)

    # Получаем данные о погоде
    weather_data = get_weather_history(
        date=execution_date,
        api_key=WEATHER_API_KEY,
        city_name="Irkutsk",
    )

    # Сохраняем в JSON файл
    json_file_path = weather_data_path / f"weather_{execution_date}.json"
    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)

    print(f"Weather data saved to {json_file_path}")
    return str(json_file_path)

def transform_weather_data(**context: dict[str, Any]) -> str:
    """
    Преобразование JSON в CSV.

    :param context: Контекст DAG, содержащий информацию о выполнении задачи.
    :return: Путь к сохраненному CSV файлу.
    """
    execution_date = context["ds"]
    json_file_path = Path(f"/tmp/weather_data/weather_{execution_date}.json")

    # Читаем JSON файл
    with open(json_file_path, encoding="utf-8") as f:
        weather_data = json.load(f)

    # Преобразуем в DataFrame
    df = weather_dict_to_raw_df(weather_data)

    # Сохраняем в CSV файл
    csv_file_path = Path(f"/tmp/weather_data/weather_{execution_date}.csv")
    df.to_csv(csv_file_path, index=False, encoding="utf-8")

    print(f"Transformed data saved to {csv_file_path}")
    return str(csv_file_path)

def load_to_postgres(**context: dict[str, Any]) -> None:
    """
    Загрузка данных в PostgreSQL.

    :param context: Контекст DAG, содержащий информацию о выполнении задачи.
    :return: Функция ничего не возвращает, она производит загрузку данных в базу.
    """
    execution_date = context["ds"]
    csv_file_path = Path(f"/tmp/weather_data/weather_{execution_date}.csv")

    # Читаем CSV файл
    df = pd.read_csv(csv_file_path, encoding="utf-8")

    # Подключение к PostgreSQL через PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="pg_con")
    engine = postgres_hook.get_sqlalchemy_engine()

    # Загружаем данные в таблицу (идемпотентность через append)
    df.to_sql(name="weather_raw", con=engine, if_exists="append", index=False)

    print(f"Data loaded to PostgreSQL from {csv_file_path}")

def cleanup_files(**context: dict[str, Any]) -> None:
    """
    Удаление временных файлов.

    :param context: Контекст DAG, содержащий информацию о выполнении задачи.
    :return: Функция ничего не возвращает, она производит очистку временных файлов.
    """
    execution_date = context["ds"]
    json_file_path = Path(f"/tmp/weather_data/weather_{execution_date}.json")
    csv_file_path = Path(f"/tmp/weather_data/weather_{execution_date}.csv")

    # Удаляем файлы если они существуют
    for file_path in [json_file_path, csv_file_path]:
        if file_path.exists():
            file_path.unlink()
            print(f"Removed file: {file_path}")

with DAG(
        dag_id="weather_etl_local",
        schedule_interval="0 10 * * *",
        default_args=default_args,
        tags=["weather", "etl", "local"],
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
    )

    transform_task = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_files",
        python_callable=cleanup_files,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> extract_task >> transform_task >> load_task >> cleanup_task >> end
