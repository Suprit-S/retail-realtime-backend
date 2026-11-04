#!/usr/bin/env bash
# Creates the read-only role the Grafana reporting layer connects as.
# It can only SELECT from the serving table, and every session it opens is
# read-only by default, so a dashboard query can never change data.
# Runs once, on first container start (empty postgres_data volume).
set -euo pipefail

: "${POSTGRES_REPORT_USER:?POSTGRES_REPORT_USER must be set}"
: "${POSTGRES_REPORT_PASSWORD:?POSTGRES_REPORT_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${POSTGRES_REPORT_USER}') THEN
            CREATE ROLE ${POSTGRES_REPORT_USER} LOGIN PASSWORD '${POSTGRES_REPORT_PASSWORD}';
        END IF;
    END
    \$\$;

    ALTER ROLE ${POSTGRES_REPORT_USER} SET default_transaction_read_only = on;
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${POSTGRES_REPORT_USER};
    GRANT USAGE ON SCHEMA public TO ${POSTGRES_REPORT_USER};
    GRANT SELECT ON country_revenue_5min TO ${POSTGRES_REPORT_USER};
SQL
