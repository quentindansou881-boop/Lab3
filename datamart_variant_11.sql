-- Витрина варианта 11: анализ задержек рейсов.
-- Superset подключается к этому VIEW, а не к stg_flights.
-- Одна строка = один выполненный рейс (отменённые и перенаправленные исключены на слое STG).

CREATE OR REPLACE VIEW flight_delays_datamart AS
SELECT
    month,                              -- месяц вылета, 1..12
    airline,                            -- название авиакомпании (нормализовано)
    origin_airport,                     -- аэропорт вылета
    is_delayed,                         -- 1, если опоздание прибытия >= 15 минут, иначе 0
    arrival_delay_minutes               -- минуты (отрицательное значение = прибытие раньше)
FROM stg_flights;

COMMENT ON VIEW flight_delays_datamart IS
    'Витрина ЛР3 вар.11: выполненные рейсы 2015 г., is_delayed = arrival_delay >= 15 мин';
