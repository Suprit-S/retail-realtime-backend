# Based on the official, unmodified Apache Spark (PySpark) image — we only
# add the one Python driver dependency (psycopg2) our aggregation job needs
# to write into Postgres.
FROM apache/spark:3.5.3-python3

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.9

COPY streaming_job.py /opt/spark-app/streaming_job.py

RUN mkdir -p /opt/spark-app/checkpoints && chown -R 185:185 /opt/spark-app

# Non-root user 185 needs write access to Ivy's package cache, or
# `spark-submit --packages` fails to resolve the Kafka connector jar.
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && chown -R 185:185 /home/spark/.ivy2

USER 185

WORKDIR /opt/spark-app