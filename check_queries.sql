-- Контрольные запросы для pgAdmin: числа для раздела «Выводы» и проверки сходимости.

-- 1. Баланс записей: RAW = STG + отбраковано
SELECT
    (SELECT count(*) FROM raw_flights)        AS raw_rows,
    (SELECT count(*) FROM stg_flights)        AS stg_rows,
    (SELECT count(*) FROM stg_rejected_log)   AS rejected_rows,
    (SELECT count(*) FROM flight_delays_datamart) AS mart_rows;

-- 2. Причины отбраковки
SELECT * FROM stg_rejected_summary ORDER BY rows_rejected DESC;

-- 3. Общий % задержек и средняя задержка
SELECT round(100.0 * avg(is_delayed), 2) AS pct_delayed,
       round(avg(arrival_delay_minutes), 2) AS avg_arrival_delay_min,
       count(*) AS flights
FROM flight_delays_datamart;

-- 4. % задержек и число рейсов по авиакомпаниям
SELECT airline, count(*) AS flights,
       round(100.0 * avg(is_delayed), 2) AS pct_delayed
FROM flight_delays_datamart
GROUP BY airline
ORDER BY pct_delayed DESC;

-- 5. Динамика по месяцам
SELECT month, count(*) AS flights,
       round(100.0 * avg(is_delayed), 2) AS pct_delayed,
       round(avg(arrival_delay_minutes), 2) AS avg_arrival_delay_min
FROM flight_delays_datamart
GROUP BY month
ORDER BY month;

-- 6. Аэропорты вылета с числовыми кодами вместо IATA (ограничение данных)
SELECT origin_code_format, count(*) AS flights
FROM stg_flights
GROUP BY origin_code_format;
