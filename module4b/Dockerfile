FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

ENV DBT_PROFILES_DIR=/dbt

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core==1.11.4 \
    dbt-duckdb==1.10.0 \
    dbt-bigquery==1.11.0

WORKDIR /dbt
