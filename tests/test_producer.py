"""Unit tests for the producer's row parsing (no Kafka needed).

Run from the repo root:  pip install -r tests/requirements.txt && pytest tests
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "producer"))

from producer import parse_event_time, parse_row  # noqa: E402


def make_row(**overrides):
    row = {
        "Invoice": "489434",
        "StockCode": "85048",
        "Description": "15CM CHRISTMAS GLASS BALL 20 LIGHTS",
        "Quantity": "12",
        "InvoiceDate": "2009-12-01 07:45:00",
        "Price": "6.95",
        "Customer ID": "13085.0",
        "Country": "United Kingdom",
    }
    row.update(overrides)
    return row


def test_iso_date_keeps_month_and_day():
    # Regression test: dateutil(dayfirst=True) read this as 12 January.
    assert parse_event_time("2009-12-01 07:45:00") == datetime(2009, 12, 1, 7, 45)
    assert parse_event_time("2011-12-09 12:50:00") == datetime(2011, 12, 9, 12, 50)


def test_uk_excel_format_is_day_first():
    assert parse_event_time("01/12/2009 07:45") == datetime(2009, 12, 1, 7, 45)


def test_unparseable_date_is_rejected():
    assert parse_event_time("not a date") is None


def test_parse_row_builds_event():
    event, event_time = parse_row(make_row())
    assert event_time == datetime(2009, 12, 1, 7, 45)
    assert event["invoice_date"] == "2009-12-01T07:45:00"
    assert event["quantity"] == 12
    assert event["unit_price"] == 6.95
    assert event["country"] == "United Kingdom"


def test_parse_row_skips_incomplete_rows():
    assert parse_row(make_row(Invoice="")) is None
    assert parse_row(make_row(Price="")) is None
    assert parse_row(make_row(InvoiceDate="garbage")) is None
