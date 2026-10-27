# Real-Time Data Backend for a Data-Intensive Retail Application

DLMDSEDE02 &middot; Portfolio &middot; Task 2 (real-time)

A streaming pipeline that replays the UCI **Online Retail II** dataset as a
checkout-style event stream, ingests it through **Apache Kafka**, cleans and
aggregates it with **Spark Structured Streaming** (5-minute tumbling
windows, 2-minute watermark), and serves the result from **PostgreSQL**.
Every component is its own isolated Docker container; the whole stack is
defined as code in `docker-compose.yml`.

```
producer (Python) --> Kafka --> Spark Structured Streaming --> PostgreSQL
```

## 1. Prerequisites

- Docker Desktop (or Docker Engine + the Compose plugin) installed and running
- ~4 GB of free RAM for Spark and Kafka together
- The dataset (see below)

## 2. Get the dataset

Download **Online Retail II** from UCI:
https://archive.ics.uci.edu/dataset/502/online+retail+ii

It ships as one `.xlsx` file with two sheets. Combine both into a single CSV
named `online_retail_ii.csv` and place it in `data/`. Full instructions and
a ready-to-paste pandas snippet are in `data/README.md`.

## 3. Configure

```bash
cp .env.example .env
```

Open `.env` and set your own Postgres passwords. `.env` is gitignored — it
never gets committed.

## 4. Run it

```bash
docker compose up --build
```

First run builds the producer and Spark images and pulls Kafka/Postgres —
give it a few minutes. Startup order is enforced by healthchecks: Kafka and
Postgres must report healthy before the producer and Spark containers start.

You should see, in order:
1. `kafka` and `postgres` become healthy
2. `producer` starts replaying events, logging every 5,000 rows sent
3. `spark` starts consuming, and every 30 seconds logs `upserted N rows into country_revenue_5min`

## 5. Check it's actually working

From another terminal, query the serving table directly:

```bash
docker compose exec postgres psql -U postgres -d retail \
  -c "SELECT * FROM country_revenue_5min ORDER BY window_start DESC LIMIT 10;"
```

You should see rows accumulating and updating as the producer keeps
replaying events. That query *is* your proof that data is ingested,
pre-processed, and aggregated as expected — the requirement this phase asks
you to demonstrate.

## 6. Stop it

```bash
docker compose down          # stop containers, keep data
docker compose down -v       # also wipe Kafka/Postgres/Spark volumes, start clean
```

## Project layout

```
docker-compose.yml     # the whole stack, as Infrastructure as Code
.env.example            # config template (copy to .env, never commit .env)
producer/                # ingestion microservice (custom image) 
  producer.py
  Dockerfile
  requirements.txt
spark/                   # pre-processing + aggregation microservice
  streaming_job.py
  Dockerfile
postgres/init/           # schema + least-privilege app role, run on first start
  001-schema.sql
  002-app-role.sh
data/                     # put online_retail_ii.csv here (gitignored)
  README.md
```

## Design notes (why it's built this way)

- **Reliability / scalability / maintainability** &mdash; each stage is an
  isolated container with its own restart policy and healthcheck; Kafka's 3
  partitions and Spark's checkpointing (`spark/checkpoints`, a named volume)
  allow independent scaling and crash recovery without data loss.
- **Security / governance / protection** &mdash; Spark and the producer
  connect to Postgres as `retail_app`, a role with `SELECT/INSERT/UPDATE` on
  exactly one table, not the Postgres superuser; secrets live only in the
  gitignored `.env`; Kafka has no host port published at all, and Postgres
  is bound to `127.0.0.1` only.
- **Why `kafka-python==3.0.11` and not an older pin** &mdash; kafka-python's
  2.x line predates recent Kafka protocol versions; 3.0.11 is the current
  maintained release and is what this producer was built and tested
  against.
- **Why `psycopg2` in `foreachBatch` instead of the Spark JDBC sink** &mdash;
  each micro-batch of aggregates is small (a handful of country/window
  rows), so a plain upsert is simpler and avoids bundling a JDBC driver jar;
  a higher-throughput deployment would switch to the JDBC writer or a
  connection pool.

## Known limitation
This is a single-broker, single-executor local setup, by design (the brief
asks for local development, not a cloud deployment). It demonstrates the
architecture correctly but is not the same as a multi-broker production
cluster — see the presentation's "disadvantages" slide for the same point.