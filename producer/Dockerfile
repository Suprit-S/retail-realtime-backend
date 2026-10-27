# Only the producer is custom-built; every other service in this project
# uses an official, unmodified image (see docker-compose.yml).
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Runs as a non-root user for defense in depth.
RUN useradd --create-home appuser
USER appuser

CMD ["python", "producer.py"]
