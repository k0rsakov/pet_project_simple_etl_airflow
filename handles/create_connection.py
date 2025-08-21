import requests
import json


def create_airflow_connection(
    connection_id: str | None = "pg_con",
    conn_type: str | None = "postgres",
    description: str | None = None,
    host: str |None = "bi_postgres",
    login: str | None = "postgres",
    password: str | None = "postgres",  # noqa: S107
    port: int | None = 5432,
    schema: str | None = "postgres",
    extra: str | None = None,
    airflow_api_url: str = "http://localhost:8080/api/v1/connections",
    user_name: str | None = None,
    password_auth: str | None = None,
):
    """
    Создаёт подключение в Apache Airflow через REST API.

    :param connection_id: Уникальный идентификатор подключения
    :param conn_type: Тип подключения (например, 'postgres', 'http', 's3' и т.д.)
    :param description: Описание подключения (опционально)
    :param host: Хост (опционально)
    :param login: Логин для аутентификации (опционально)
    :param password: Пароль для аутентификации (опционально)
    :param port: Порт (опционально)
    :param schema: Название схемы/базы данных (опционально)
    :param extra: Дополнительные параметры в формате JSON-строки или строка с произвольными данными (опционально)
    :param airflow_api_url: Базовый URL Airflow API для подключений (по умолчанию: /api/v1/connections)
    :param user_name: Имя пользователя для аутентификации в Airflow API
    :param password_auth: Пароль пользователя для аутентификации в Airflow API
    """
    headers = {
        "Content-Type": "application/json",
    }

    # Формируем тело запроса
    data = {
        "connection_id": connection_id,
        "conn_type": conn_type,
    }

    # Добавляем необязательные поля, если они переданы
    if description is not None:
        data["description"] = description
    if host is not None:
        data["host"] = host
    if login is not None:
        data["login"] = login
    if password is not None:
        data["password"] = password
    if port is not None:
        data["port"] = port
    if schema is not None:
        data["schema"] = schema
    if extra is not None:
        data["extra"] = extra

    try:
        response = requests.post(
            url=airflow_api_url,
            auth=(user_name, password_auth),
            data=json.dumps(data),
            headers=headers,
            timeout=600,
        )

        if response.status_code in {200, 201}:
            print(f"Подключение '{connection_id}' успешно создано.")
        elif response.status_code == 409:  # Конфликт — уже существует  # noqa: PLR2004
            print(f"Подключение '{connection_id}' уже существует.")
        else:
            print(f"Ошибка при создании подключения '{connection_id}': {response.status_code} - {response.text}")
            response.raise_for_status()

    except requests.exceptions.RequestException as e:
        print(f"Ошибка соединения с Airflow API: {e}")
        raise

create_airflow_connection(
    user_name="airflow",
    password_auth="airflow",  # noqa: S106
)
