FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    CAL_HOST=0.0.0.0 \
    CAL_PORT=8080

WORKDIR /srv/app

# Install dependencies first so the layer is cached across code changes.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Directory holding the durable SQLite database (WAL); mounted from a volume
# by docker compose. Created here so the non-root runtime can write to it.
RUN mkdir -p /srv/data

EXPOSE 8080

# Default single-process command; docker compose overrides this with two
# worker processes sharing the same SQLite database file.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
