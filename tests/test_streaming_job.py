"""Tests for the Spark cleaning + windowing logic, using a local Spark
session and an in-memory batch instead of Kafka.

Run from the repo root:  pip install -r tests/requirements.txt && pytest tests
"""
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark"))

# collect() converts timestamps to the Python process's local zone; the
# Spark container runs in UTC, so the tests do too.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):  # not available on Windows
    time.tzset()

from streaming_job import aggregate, clean, parse_events  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def kafka_like(spark, events):
    """Build a DataFrame shaped like the Kafka source (a binary `value`)."""
    rows = [(json.dumps(e).encode("utf-8"),) for e in events]
    return spark.createDataFrame(rows, "value binary")


def event(invoice, minute, qty, price, country="United Kingdom"):
    return {
        "invoice_no": invoice,
        "stock_code": "X",
        "description": "item",
        "quantity": qty,
        "invoice_date": f"2010-03-05T10:{minute:02d}:00",
        "unit_price": price,
        "customer_id": "1",
        "country": country,
    }


def test_window_aggregation_and_cleaning(spark):
    events = [
        event("1001", 1, 2, 10.0),             # window 10:00-10:05
        event("1002", 4, 1, 5.0),              # same window
        event("1003", 6, 3, 1.0),              # window 10:05-10:10
        event("1004", 2, 1, 8.0, "France"),    # window 10:00-10:05, France
        event("C1005", 2, -2, 10.0),           # cancellation: dropped
        event("1006", 3, 5, 0.0),              # price 0: dropped (no 0.00 rows)
        event("1007", 3, -1, 4.0),             # negative quantity: dropped
    ]
    result = aggregate(clean(parse_events(kafka_like(spark, events))))
    got = {
        (r["window_start"], r["country"]): (round(r["revenue"], 2), r["items"])
        for r in result.collect()
    }
    assert got == {
        (datetime(2010, 3, 5, 10, 0), "United Kingdom"): (25.0, 3),
        (datetime(2010, 3, 5, 10, 5), "United Kingdom"): (3.0, 3),
        (datetime(2010, 3, 5, 10, 0), "France"): (8.0, 1),
    }


def test_no_window_has_zero_revenue(spark):
    events = [event("2001", 1, 4, 0.0), event("2002", 1, 1, 2.5)]
    result = aggregate(clean(parse_events(kafka_like(spark, events)))).collect()
    assert all(r["revenue"] > 0 for r in result)
