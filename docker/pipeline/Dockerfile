FROM python:3.12.7-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY etl ./etl
COPY ml ./ml
COPY sql ./sql
COPY tests ./tests
COPY pytest.ini .
COPY docker/pipeline/entrypoint.sh /usr/local/bin/pipeline

RUN chmod +x /usr/local/bin/pipeline && useradd --create-home --uid 1000 pipeline \
    && mkdir -p /app/artifacts && chown pipeline /app/artifacts
USER pipeline

ENTRYPOINT ["pipeline"]
