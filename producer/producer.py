"""
Producer microservice.

Replays the Online Retail II dataset into Kafka, paced by each row's
InvoiceDate, to simulate a live stream of checkout events. This is the
ingestion microservice described in Portfolio Part 1.
"""
import csv
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime

from dateutil import parser as dateparser
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s producer %(levelname)s %(message)s",
)
log = logging.getLogger("producer")

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "retail-transactions")
DATA_FILE = os.environ.get("DATA_FILE", "/data/online_retail_ii.csv")
REPLAY_SPEEDUP = float(os.environ.get("REPLAY_SPEEDUP", "3600"))
# Never sleep longer than this between two events, no matter how big the
# real gap between them was (keeps the demo moving instead of stalling
# for a simulated overnight gap).
MAX_SLEEP_SECONDS = float(os.environ.get("MAX_SLEEP_SECONDS", "2.0"))
LOG_EVERY = int(os.environ.get("LOG_EVERY", "5000"))
CONNECT_RETRY_SECONDS = 5
CONNECT_MAX_RETRIES = 60  # ~5 minutes waiting for Kafka to become ready


def wait_for_port(host: str, port: int, attempts: int, delay_seconds: float) -> bool:
    """Plain TCP readiness probe. docker-compose already gates this
    container's startup on Kafka's own healthcheck, but a container can
    restart on its own (restart: unless-stopped), so this check also
    protects a manual `docker compose restart producer`."""
    for attempt in range(1, attempts + 1):
        try:
            with socket.create_connection((host, port), timeout=3):
                return True
        except OSError:
            log.warning(
                "Kafka port %s:%d not open yet (attempt %d/%d), retrying in %.0fs...",
                host, port, attempt, attempts, delay_seconds,
            )
            time.sleep(delay_seconds)
    return False


def connect_producer() -> KafkaProducer:
    """Wait for the Kafka port, then construct the client. Bootstrapping
    is retried a few times on top of that in case the broker's port is
    open before it is actually ready to serve metadata."""
    host, _, port_str = BOOTSTRAP_SERVERS.partition(":")
    port = int(port_str or 9092)

    if not wait_for_port(host, port, CONNECT_MAX_RETRIES, CONNECT_RETRY_SECONDS):
        log.error("Kafka never became reachable at %s, giving up.", BOOTSTRAP_SERVERS)
        sys.exit(1)

    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                linger_ms=50,
                request_timeout_ms=15000,
            )
            log.info("Connected to Kafka at %s (attempt %d)", BOOTSTRAP_SERVERS, attempt)
            return producer
        except Exception as exc:  # kafka-python's exception types vary by version
            last_error = exc
            log.warning("KafkaProducer setup failed (attempt %d/5): %s", attempt, exc)
            time.sleep(CONNECT_RETRY_SECONDS)

    log.error("Could not create a KafkaProducer after 5 attempts: %s", last_error)
    sys.exit(1)


def parse_row(row: dict) -> dict | None:
    """Turn one CSV row into a JSON-ready event, or None if it's unusable."""
    invoice_no = (row.get("Invoice") or "").strip()
    raw_date = (row.get("InvoiceDate") or "").strip()
    if not invoice_no or not raw_date:
        return None
    try:
        event_time = dateparser.parse(raw_date, dayfirst=True)
    except (ValueError, OverflowError):
        return None
    quantity_raw = (row.get("Quantity") or "").strip()
    price_raw = (row.get("Price") or "").strip()
    if not quantity_raw or not price_raw:
        return None
    try:
        quantity = int(float(quantity_raw))
        unit_price = float(price_raw)
    except ValueError:
        return None

    return {
        "invoice_no": invoice_no,
        "stock_code": (row.get("StockCode") or "").strip(),
        "description": (row.get("Description") or "").strip(),
        "quantity": quantity,
        "invoice_date": event_time.isoformat(),
        "unit_price": unit_price,
        "customer_id": (row.get("Customer ID") or "").strip() or None,
        "country": (row.get("Country") or "Unknown").strip(),
    }, event_time


def wait_for_file(path: str) -> None:
    if os.path.exists(path):
        return
    log.error(
        "Dataset not found at %s. Place the combined Online Retail II CSV "
        "there (see data/README.md) and restart this container.",
        path,
    )
    sys.exit(1)


def main() -> None:
    wait_for_file(DATA_FILE)
    producer = connect_producer()

    sent = 0
    skipped = 0
    prev_event_time: datetime | None = None
    started = time.time()

    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = parse_row(row)
            if parsed is None:
                skipped += 1
                continue
            event, event_time = parsed

            if prev_event_time is not None:
                delta_seconds = (event_time - prev_event_time).total_seconds()
                sleep_for = max(0.0, delta_seconds) / REPLAY_SPEEDUP
                sleep_for = min(sleep_for, MAX_SLEEP_SECONDS)
                if sleep_for > 0:
                    time.sleep(sleep_for)
            prev_event_time = event_time

            producer.send(TOPIC, key=event["invoice_no"], value=event)
            sent += 1
            if sent % LOG_EVERY == 0:
                elapsed = time.time() - started
                log.info("Sent %d events (%d skipped) in %.1fs", sent, skipped, elapsed)

    producer.flush()
    producer.close()
    log.info("Done. Sent %d events, skipped %d malformed rows.", sent, skipped)


if __name__ == "__main__":
    main()
