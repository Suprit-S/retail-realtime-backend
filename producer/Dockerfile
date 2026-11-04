# Custom ingestion image. Spark is the only other built image and only
# extends the official apache/spark image; Kafka, Postgres and Grafana run
# unmodified official images (see docker-compose.yml).
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Runs as a non-root user for defense in depth.
RUN useradd --create-home appuser
USER appuser

CMD ["python", "producer.py"]
