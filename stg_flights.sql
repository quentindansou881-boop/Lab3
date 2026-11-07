-- STG: типизация, очистка, нормализация. Идемпотентно (TRUNCATE + INSERT).
-- Каждая отброшенная строка попадает в stg_rejected_log с причиной.

CREATE TABLE IF NOT EXISTS stg_flights (
    raw_id                bigint   PRIMARY KEY,
    flight_year           smallint,
    month                 smallint NOT NULL CHECK (month BETWEEN 1 AND 12),
    airline_code          text     NOT NULL,
    airline               text     NOT NULL,
    origin_airport        text     NOT NULL,
    origin_code_format    text     NOT NULL,   -- IATA | NUMERIC_ID | OTHER
    arrival_delay_minutes integer  NOT NULL,   -- отрицательное = прибыл раньше срока
    is_delayed            smallint NOT NULL CHECK (is_delayed IN (0, 1))
);

CREATE TABLE IF NOT EXISTS stg_rejected_log (
    raw_id      bigint      PRIMARY KEY,
    reason      text        NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now()
);

TRUNCATE stg_flights, stg_rejected_log;

CREATE TEMP TABLE tmp_flights_classified AS
WITH parsed AS (
    SELECT
        f.raw_id,
        CASE WHEN trim(f.year)  ~ '^\d{4}$'  THEN trim(f.year)::int  END AS year_num,
        CASE WHEN trim(f.month) ~ '^\d{1,2}$' THEN trim(f.month)::int END AS month_num,
        upper(trim(f.airline))                                          AS airline_code,
        upper(trim(f.origin_airport))                                   AS origin_airport,
        CASE WHEN trim(f.arrival_delay) ~ '^-?\d+(\.\d+)?$'
             THEN round(trim(f.arrival_delay)::numeric)::int END        AS delay_num,
        coalesce(trim(f.arrival_delay), '')                             AS delay_raw,
        trim(f.cancelled) IN ('1', '1.0')                               AS is_cancelled,
        trim(f.diverted)  IN ('1', '1.0')                               AS is_diverted
    FROM raw_flights f
)
SELECT
    p.*,
    CASE
        WHEN p.is_cancelled                                        THEN 'cancelled'
        WHEN p.is_diverted                                         THEN 'diverted'
        WHEN p.month_num IS NULL OR p.month_num NOT BETWEEN 1 AND 12 THEN 'invalid_month'
        WHEN coalesce(p.airline_code, '') = ''
          OR coalesce(p.origin_airport, '') = ''                   THEN 'missing_airline_or_origin'
        WHEN p.delay_raw = ''                                      THEN 'missing_arrival_delay'
        WHEN p.delay_num IS NULL                                   THEN 'non_numeric_arrival_delay'
    END AS reject_reason
FROM parsed p;

INSERT INTO stg_rejected_log (raw_id, reason)
SELECT raw_id, reject_reason
FROM tmp_flights_classified
WHERE reject_reason IS NOT NULL;

INSERT INTO stg_flights (
    raw_id, flight_year, month, airline_code, airline, origin_airport,
    origin_code_format, arrival_delay_minutes, is_delayed
)
SELECT
    c.raw_id,
    c.year_num,
    c.month_num,
    c.airline_code,
    -- нормализация названия: пробелы, сноски вида [1] и *, при отсутствии в справочнике - код
    coalesce(
        nullif(
            regexp_replace(
                regexp_replace(trim(a.airline), '\s+', ' ', 'g'),
                '\s*(\[\d+\]|\*+)\s*$', ''
            ), ''
        ),
        c.airline_code
    ),
    c.origin_airport,
    CASE
        WHEN c.origin_airport ~ '^[A-Z]{3}$' THEN 'IATA'
        WHEN c.origin_airport ~ '^\d+$'      THEN 'NUMERIC_ID'
        ELSE 'OTHER'
    END,
    c.delay_num,
    CASE WHEN c.delay_num >= 15 THEN 1 ELSE 0 END   -- порог DOT: 15 минут и более
FROM tmp_flights_classified c
LEFT JOIN raw_airlines a ON upper(trim(a.iata_code)) = c.airline_code
WHERE c.reject_reason IS NULL;

CREATE INDEX IF NOT EXISTS idx_stg_flights_month   ON stg_flights (month);
CREATE INDEX IF NOT EXISTS idx_stg_flights_airline ON stg_flights (airline);

CREATE OR REPLACE VIEW stg_rejected_summary AS
SELECT reason, count(*) AS rows_rejected
FROM stg_rejected_log
GROUP BY reason;

ANALYZE stg_flights;
