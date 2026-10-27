-- Serving table for the real-time streaming report. Spark upserts 5-minute aggregates here.
CREATE TABLE IF NOT EXISTS country_revenue_5min (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    country      TEXT      NOT NULL,
    revenue      NUMERIC(14, 2) NOT NULL,
    items        BIGINT    NOT NULL,
    updated_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, window_end, country)
);

CREATE INDEX IF NOT EXISTS idx_country_revenue_5min_window
    ON country_revenue_5min (window_start DESC);
