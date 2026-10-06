# ETL Лучшие Практики: Полное руководство для начинающих и не только (ETL Best Practices)

> [!NOTE]
> **TL;DR** Заменил нерабочий Minio S3 на рабочий Silo S3. Проект снова актуален. Все изменения отображены в PR [#1](https://github.com/k0rsakov/pet_project_simple_etl_airflow/pull/1)

> [!IMPORTANT]
> [Minio](https://github.com/minio/minio) ушел из OpenSource, но Docker-образы были доступны.<br><br>
> 2026-09-14 на Reddit был опубликован
пост — [MinIO just removed their DockerHub image](https://www.reddit.com/r/minio/s/KxwCW1gKNE).<br><br>
> Я не сижу на Reddit 24/7 и поэтому увидел пост не сразу. Узнал об этом когда получил сообщение по типу: "_проект не
работает_".
> Проинформировал, что в курсе проблемы в своем tg-канале — [пост](https://t.me/DataLikeQWERTY/194).<br><br>
> Исследовал аналоги, думал взять совсем что-то другое, что описывал в этом [посте](https://t.me/DataLikeQWERTY/150)
>, но решил остановить свой выбор на fork Minio — [PGSTY Silo](https://github.com/pgsty/silo).
> На текущий момент (2026-10-05) Minio заменен на Silo. Более подробно описано в PR [#1](https://github.com/k0rsakov/pet_project_simple_etl_airflow/pull/1)

___

Ты только начинаешь создавать ETL процессы или ты хочешь улучшить свои навыки в создании ETL процессов? Тогда
это [видео](https://youtu.be/ZZp6W1KWRxM) для тебя. В этом [видео](https://youtu.be/ZZp6W1KWRxM) я покажу худшие и
лучшие практики при создании ETL-процессов. Также мы разберём что такое "плохо" и что такое "хорошо".

📌 Что вы узнаете:
- Как собирать RAW данные
- Как работать с API
- Лучшие практики для создания ETL-процессов
- Худшие практики для создания ETL-процессов
- Рекомендации при создании ETL-процессов

💻 Менторство/консультации по IT – https://korsak0v.notion.site/Data-Engineer-185c62fdf79345eb9da9928356884ea0

📂 Полный проект на GitHub: https://github.com/k0rsakov/pet_project_simple_etl_airflow

👨‍💻 Подходит для начального уровня, junior и middle дата-инженеров, ищущих реальный опыт и сильное портфолио.

🔔 Подписывайтесь и ставьте лайк, если хотите больше практических видео!

Ссылки:
- Менторство/консультации по IT – https://korsak0v.notion.site/Data-Engineer-185c62fdf79345eb9da9928356884ea0
- TG канал – https://t.me/DataLikeQWERTY
- Instagram – https://www.instagram.com/i__korsakov/
- Habr – https://habr.com/ru/users/k0rsakov/publications/articles/
- Git-репозиторий из видео – https://github.com/k0rsakov/pet_project_simple_etl_airflow
- Лучший пет-проект для дата-инженера (The best pet-project for a data-engineer) – https://youtu.be/MQPHgUQvKnI
- Как зайти в контейнер? / How do I enter the container? – https://youtu.be/bDY7M0YHakk
- Что такое контекст DAG и как его использовать? / What is a DAG context and how to use
it? – https://youtu.be/TiSFGmLV8l0
- Пишем первый простой DAG / Writing the first simple DAG – https://youtu.be/Wt3IwjsqzAI
- Как установить пакеты в Airflow с использованием Dockerfile? / How install packages in
Airflow? – https://youtu.be/Bazz6N6X8Ek
- Как запустить Airflow в Docker? / How do I run Airflow in Docker? – https://youtu.be/RCKeW35WY-o
- Что такое Docker network и как это работает? / What is the Docker network and how does it
work? – https://youtu.be/gn_dXV9HrIo
- Как загрузить данные в PostgreSQL при помощи Python? / How do upload data to PostgreSQL using
Python – https://youtu.be/I_PXPlVFmPw
- Как работает S3 и зачем он нужен в 2025 году? – https://youtu.be/Vfe5H3bD2SQ

Тайминги:
- 00:00 – Начало
- 00:13 – Что такое ETL
- 01:43 – Про источник данных
- 02:27 – Разбор структуры проекта
- 03:11 – Разбор "плохой" практики реализации ETL
- 08:00 – Запуск "плохой" практики реализации ETL
- 14:04 – Разбор почему работать через worker плохо
- 14:51 – Разбор "хорошей" практики реализации ETL
- 19:57 – Подведение итогов

#dataengineer #petproject #airflow #postgresql #minio #metabase #dwh #python #dataengineering #etl #docker #portfolio
#датаинженер #etl #elt #s3 #datalake

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