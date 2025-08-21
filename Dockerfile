# Python 3.12.8
FROM apache/airflow:2.10.4-python3.12

USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Установка основных пакетов
USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    apache-airflow==2.10.4 \
    minio==7.2.15 \
    s3fs==2024.12.0 \
    fpdf==1.7.2 \
