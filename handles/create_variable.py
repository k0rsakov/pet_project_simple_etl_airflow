import json

import requests


def create_airflow_variable(
    key: str | None= None,
    value: str |None= None,
    description: str | None = None,
    airflow_api_url: str = "http://localhost:8080/api/v1/variables",
    user_name: str | None = None,
    password_auth: str | None= None,
):
    """
    Создаёт переменную в Apache Airflow через REST API.

    :param key: Ключ переменной (должен быть уникальным)
    :param value: Значение переменной (строка)
    :param description: Описание переменной (опционально)
    :param airflow_api_url: URL Airflow API для переменных
    :param user_name: Имя пользователя Airflow API
    :param password_auth: Пароль пользователя Airflow API
    """
    headers = {
        "Content-Type": "application/json",
    }

    data = {
        "key": key,
        "value": value,
    }

    if description is not None:
        data["description"] = description

    try:
        response = requests.post(
            url=airflow_api_url,
            auth=(user_name, password_auth),
            data=json.dumps(data),
            headers=headers,
            timeout=600,
        )

        if response.status_code in {200, 201}:
            print(f"Переменная '{key}' успешно создана.")
        elif response.status_code == 409:  # noqa: PLR2004
            print(f"Переменная '{key}' уже существует.")
        else:
            print(f"Ошибка при создании переменной '{key}': {response.status_code} - {response.text}")
            response.raise_for_status()

    except requests.exceptions.RequestException as e:
        print(f"Ошибка соединения с Airflow API: {e}")
        raise


create_airflow_variable(
    key="access_key",
    value="4MevhvVdQClrOpIL37l8",
    description="access_key для S3 бакета",
    user_name="airflow",
    password_auth="airflow",  # noqa: S106
)

create_airflow_variable(
    key="secret_key",
    value="m0lpCnpOM3wtyloX7FeWskDOuZ9CRPZJBXhY475f",
    description="secret_key для S3 бакета",
    user_name="airflow",
    password_auth="airflow",  # noqa: S106
)

create_airflow_variable(
    key="weather_api_key",
    value="",
    description="API ключ для weatherapi.com",
    user_name="airflow",
    password_auth="airflow",  # noqa: S106
)
