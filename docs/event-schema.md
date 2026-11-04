# Data contract and lineage

## Kafka topic `retail-transactions`

One JSON message per dataset row. Key = `invoice_no` (all lines of one
invoice land in the same partition, so their order is kept).

| Field          | Type            | Source column (Online Retail II) | Notes                                 |
|----------------|-----------------|----------------------------------|---------------------------------------|
| `invoice_no`   | string          | `Invoice`                        | starts with `C` = cancellation        |
| `stock_code`   | string          | `StockCode`                      |                                       |
| `description`  | string          | `Description`                    |                                       |
| `quantity`     | integer         | `Quantity`                       | negative for returns                  |
| `invoice_date` | ISO 8601 string | `InvoiceDate`                    | event time, no time zone (UK, UTC)    |
| `unit_price`   | number (GBP)    | `Price`                          | 0 for free items / adjustments        |
| `customer_id`  | string or null  | `Customer ID`                    | null when the customer is unknown     |
| `country`      | string          | `Country`                        |                                       |

Retention: 7 days or 2 GB per partition (`docker-compose.yml`).
Rows the producer cannot parse are skipped and counted in its log.

## Serving table `country_revenue_5min` (PostgreSQL)

| Column         | Type          | Meaning                                         |
|----------------|---------------|-------------------------------------------------|
| `window_start` | timestamp     | start of the 5-minute event-time window (UTC)   |
| `window_end`   | timestamp     | end of the window                               |
| `country`      | text          |                                                 |
| `revenue`      | numeric(14,2) | `SUM(quantity * unit_price)`                    |
| `items`        | bigint        | `SUM(quantity)`                                 |
| `updated_at`   | timestamp     | last time Spark upserted this row (freshness)   |

Primary key: `(window_start, window_end, country)`; writes are idempotent
upserts, so a replayed micro-batch overwrites instead of duplicating.

## Lineage

```
UCI Online Retail II (.xlsx, CC BY 4.0)
  -> data/online_retail_ii.csv          (both sheets combined, data/README.md)
  -> producer                           (parse, skip malformed rows)
  -> Kafka retail-transactions          (raw events, 7-day retention)
  -> Spark Structured Streaming         (drop cancellations, quantity <= 0,
                                         price <= 0; 5-min window, 2-min watermark)
  -> PostgreSQL country_revenue_5min    (written by retail_app)
  -> Grafana "Retail real-time report"  (read by report_reader, read-only)
```

## Access

| Role / user     | Rights                                              |
|-----------------|-----------------------------------------------------|
| `postgres`      | superuser, only used by the init scripts            |
| `retail_app`    | SELECT, INSERT, UPDATE on `country_revenue_5min`    |
| `report_reader` | SELECT on `country_revenue_5min`, read-only sessions |
| Grafana `admin` | dashboard login; sign-up and anonymous access off   |
