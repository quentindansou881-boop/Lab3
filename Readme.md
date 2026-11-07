# Лабораторная работа 3. Аналитический дашборд для бизнес-кейса On-premises (VM)

**Вариант 11 — Анализ задержек рейсов**
**Студент:** Дансу Квентин
**Тема 3.** Программные средства аналитики данных

Набор данных: [usdot/flight-delays](https://www.kaggle.com/datasets/usdot/flight-delays) (Kaggle, рейсы США за 2015 год).

---

## 1. Цель работы

Бизнес-задача: выяснить, **в какие месяцы и у каких авиакомпаний рейсы чаще всего прибывают с опозданием**, чтобы:

- авиакомпании могли назначить дополнительный резерв времени в расписании на проблемные месяцы;
- пассажир или корпоративный отдел поездок мог выбрать перевозчика с лучшей пунктуальностью;
- аэропортовая служба могла планировать ресурсы на пиковые периоды.

Решение, которое должно приниматься по результатам: **для каких пар «авиакомпания × месяц» пересмотреть расписание или выбор перевозчика**.

Технически цель реализована как сквозной конвейер Kaggle → Apache Airflow → PostgreSQL (RAW → STG → витрина) → Apache Superset, который разворачивается одной командой `docker compose up -d`.

## 2. Исследуемые метрики бизнес-процесса

Единица анализа: один **выполненный** рейс (отменённые и перенаправлённые исключены, у них нет времени прибытия).

| Метрика | Как считается | Поля источника | Управленческий вопрос |
|---|---|---|---|
| `is_delayed` (1/0) | `1`, если `ARRIVAL_DELAY >= 15` мин, иначе `0` (порог Минтранса США) | `ARRIVAL_DELAY` | Считается ли рейс задержанным? |
| Общий % задержек | `100 × AVG(is_delayed)` | `is_delayed` | Какова общая пунктуальность системы? |
| % задержек по авиакомпании | `100 × AVG(is_delayed)` в разрезе `airline` | `AIRLINE`, `airlines.csv` | Какие перевозчики наименее пунктуальны? |
| Доля рейсов по авиакомпании | `COUNT(*)` по `airline` / `COUNT(*)` всего | `AIRLINE` | Насколько репрезентативен вывод по перевозчику? |
| Количество рейсов по месяцам | `COUNT(*)` по `month` | `MONTH` | Как меняется нагрузка в течение года? |
| % задержек по месяцам | `100 × AVG(is_delayed)` по `month` | `MONTH`, `is_delayed` | В какие месяцы риск задержки выше? |
| Среднее `arrival_delay_minutes` | `AVG(arrival_delay_minutes)` по `month` | `ARRIVAL_DELAY` | Насколько велики опоздания, а не только их частота? |

Все метрики варианта присутствуют в датасете или выводятся из его полей; замен метрик нет. Название авиакомпании берётся из `airlines.csv` по коду `IATA_CODE`.

## 3. Архитектура решения

**Среда:** виртуальная машина Ubuntu 20.04 LTS; все сервисы работают в Docker-контейнерах из одного `docker-compose.yml` в общей сети и обращаются друг к другу по именам сервисов.

| Контейнер | Образ | Порт на ВМ | Роль |
|---|---|---|---|
| `webserver` | собственный (на основе Apache Airflow 2.9.3) | 8080 | веб-интерфейс Airflow |
| `scheduler` | то же | — | планировщик, выполнение задач DAG |
| `airflow-init` | то же | — | разовая инициализация: миграции, пользователь `admin`, подключение `analytics_postgres` |
| `postgres` | PostgreSQL 12 | 5432 | служебная БД метаданных Airflow |
| `analytics_postgres` | PostgreSQL 12 | 5433 | аналитическая БД: RAW, STG, витрина |
| `superset` | собственный (на основе Apache Superset 3.1.3) | 8088 | чарты и дашборд |
| `pgadmin` | pgAdmin 4 (8.9) | 5050 | проверка объектов БД |
| `redis` | Redis 7 | 6379 | кэш Superset |

```mermaid
flowchart LR
    K[Kaggle<br/>usdot/flight-delays] -->|kagglehub| A[Airflow DAG<br/>flight_delays_variant_11]
    subgraph VM[Ubuntu 20.04 VM / Docker]
      A --> RAW[(RAW<br/>raw_flights, raw_airlines)]
      RAW --> STG[(STG<br/>stg_flights<br/>stg_rejected_log)]
      STG --> DM[[DATAMART VIEW<br/>flight_delays_datamart]]
      DM --> S[Superset :8088<br/>дашборд]
      P[pgAdmin :5050] -.проверка.-> DM
      R[(Redis)] -.кэш.-> S
    end
```

**Слои данных:**

- **RAW** — `raw_flights`, `raw_airlines`: копия CSV без изменений, все поля `text`, ничего не чистится и не удаляется.
- **STG** — `stg_flights`: типы приведены, названия авиакомпаний нормализованы, посчитан `is_delayed`. Каждая отброшенная строка записывается в `stg_rejected_log` с причиной.
- **DATAMART** — VIEW `flight_delays_datamart`: только поля варианта. Superset подключается **к витрине**, а не к `stg_*`.

**DAG** `flight_delays_variant_11` (запуск вручную):

```
download_kaggle_data ─┬─> load_raw_flights ──┐
                      └─> load_raw_airlines ─┴─> build_stg_flights ─> create_datamart_view ─> validate_pipeline
```

Последняя задача `validate_pipeline` проверяет сходимость: `RAW = STG + отбраковано`, `витрина = STG`, сумма по авиакомпаниям и по месяцам равна числу строк витрины, `is_delayed ∈ {0,1}`. При расхождении задача падает.

## 4. Технологический стек

| Уровень | Технология | Версия |
|---|---|---|
| Виртуализация | Ubuntu LTS | 20.04 |
| Контейнеризация | Docker Engine / Docker Compose | 20.10+ / v2.x |
| Оркестрация | Apache Airflow | 2.9.3 (Python 3.11) |
| Хранение | PostgreSQL | 12 |
| Визуализация | Apache Superset | 3.1.3 |
| Администрирование | pgAdmin 4 | 8.9 |
| Кэш | Redis | 7 |
| Источник | Kaggle API (`kagglehub`) | ≥ 0.3.4 |
| Обработка | Python, pandas (из образа Airflow), psycopg2-binary | — |

## 5. ER-диаграмма базы данных

```mermaid
erDiagram
    raw_flights {
        bigserial raw_id PK
        text year
        text month
        text airline
        text origin_airport
        text arrival_delay
        text cancelled
        text diverted
        text other_source_columns "остальные поля flights.csv, все text"
    }
    raw_airlines {
        bigserial raw_id PK
        text iata_code
        text airline
    }
    stg_flights {
        bigint raw_id PK
        smallint flight_year
        smallint month
        text airline_code
        text airline
        text origin_airport
        text origin_code_format
        integer arrival_delay_minutes
        smallint is_delayed
    }
    stg_rejected_log {
        bigint raw_id PK
        text reason
        timestamptz rejected_at
    }
    flight_delays_datamart {
        smallint month
        text airline
        text origin_airport
        smallint is_delayed
        integer arrival_delay_minutes
    }
    raw_flights ||--o| stg_flights : "очистка"
    raw_flights ||--o| stg_rejected_log : "отбраковка"
    raw_airlines ||--o{ stg_flights : "название по IATA_CODE"
    stg_flights ||--|| flight_delays_datamart : "VIEW"
```


## 6. Ссылки на исходный код

| Файл | Назначение |
|---|---|
| [`dags/flight_delays_dag.py`](dags/flight_delays_dag.py) | DAG |
| [`dags/stg_flights.sql`](dags/stg_flights.sql) | построение слоя STG и журнала отбраковки |
| [`dags/datamart_variant_11.sql`](dags/datamart_variant_11.sql) | SQL витрины |
| [`dags/check_queries.sql`](dags/check_queries.sql) | контрольные запросы для pgAdmin |
| [`superset/dashboard_export.json`](superset/dashboard_export.json) | экспорт дашборда Superset |
| [`docker-compose.yml`](docker-compose.yml), [`Dockerfile`](Dockerfile), [`Dockerfile.superset`](Dockerfile.superset) | окружение |

## 7. Описание проделанной работы

### 7.1 Запуск

```bash
git clone <ссылка-на-репозиторий> && cd lpw_03
# kaggle.json (личный токен) положить в корень проекта, он в .gitignore
chmod +x setup_kaggle.sh && ./setup_kaggle.sh
docker compose up -d
docker compose ps
```

Доступ к сервисам:

| Сервис | Адрес | Учётные данные |
|---|---|---|
| Apache Airflow | http://localhost:8080 | admin / admin |
| Apache Superset | http://localhost:8088 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |

**Подключение `analytics_postgres`** создаётся автоматически контейнером `airflow-init`. Его параметры (при необходимости создаются вручную в *Admin → Connections → +*):

| Поле в Airflow | Значение | Откуда взято (docker-compose.yml) |
|---|---|---|
| Connection Id | `analytics_postgres` | имя, используемое в DAG |
| Connection Type | Postgres | тип СУБД |
| Host | `analytics_postgres` | имя сервиса в Docker Compose |
| Schema | `analytics` | `POSTGRES_DB` |
| Login | `analytics` | `POSTGRES_USER` |
| Password | `analytics` | `POSTGRES_PASSWORD` |
| Port | `5432` | внутренний порт контейнера (5433 нужен только с самой ВМ) |

Порядок: открыть Airflow, *Admin → Connections*, убедиться, что подключение есть (или создать его по таблице), нажать **Test** (должно появиться «Connection successfully tested»), затем **Save**. После этого включить и запустить DAG `flight_delays_variant_11`. Проверить результат в pgAdmin: должны появиться `raw_flights`, `raw_airlines`, `stg_flights`, `stg_rejected_log` и VIEW `flight_delays_datamart`.


### 7.2 SQL витрины

```sql
CREATE OR REPLACE VIEW flight_delays_datamart AS
SELECT
    month,                              -- месяц вылета, 1..12
    airline,                            -- название авиакомпании (нормализовано)
    origin_airport,                     -- аэропорт вылета
    is_delayed,                         -- 1, если опоздание прибытия >= 15 минут, иначе 0
    arrival_delay_minutes               -- минуты (отрицательное значение = прибытие раньше)
FROM stg_flights;
```

Ключевое правило слоя STG (`dags/stg_flights.sql`):

```sql
CASE WHEN c.delay_num >= 15 THEN 1 ELSE 0 END AS is_delayed   -- порог DOT: 15 минут и более
```

### 7.3 Качество данных

| Правило | Что делается |
|---|---|
| Отменённые рейсы (`cancelled`) | исключаются, фиксируются в `stg_rejected_log` (нет времени прибытия) |
| Перенаправленные рейсы (`diverted`) | исключаются и фиксируются (аналогично) |
| Пустое или нечисловое `arrival_delay` | исключается и фиксируется; **не заменяется нулём** |
| Некорректный месяц, пустая авиакомпания или аэропорт | исключаются и фиксируются |
| Названия авиакомпаний | убираются двойные пробелы и сноски (`[1]`, `*`) |
| Отрицательная задержка | сохраняется как есть (прибытие раньше срока), `is_delayed = 0` |

Число строк на каждом шаге и причины отбраковки выводятся запросами 1 и 2 из `dags/check_queries.sql`; задача `validate_pipeline` автоматически проверяет баланс `RAW = STG + отбраковано` и `витрина = STG`.

### 7.4 Дашборд Superset

**Шаг 1. Подключение базы.** Superset (`http://localhost:8088`) → *Settings → Database Connections → + Database* → PostgreSQL. Поля:

| Поле | Значение | Пояснение |
|---|---|---|
| Host | `analytics_postgres` | имя сервиса в docker-compose.yml |
| Port | `5432` | внутренний порт контейнера |
| Database name | `analytics` | `POSTGRES_DB` |
| Username / Password | `analytics` / `analytics` | `POSTGRES_USER` / `POSTGRES_PASSWORD` |
| Display name | Analytics DB | имя подключения |

Либо одной строкой SQLAlchemy URI: `postgresql+psycopg2://analytics:analytics@analytics_postgres:5432/analytics`. Нажать *Connect*.

**Шаг 2. Датасет.** *Datasets → + Dataset*: DATABASE — Analytics DB, SCHEMA — `public`, TABLE — **`flight_delays_datamart`** (VIEW, а не `stg_flights`) → *Create dataset and create chart*.

**Шаг 3. Графики.** Для каждого: *Charts → + Chart*, выбрать датасет `flight_delays_datamart`, тип чарта, настроить поля, *Update chart*, *Save* под указанным именем.

| № | Тип | Имя чарта | Настройка |
|---|---|---|---|
| 1 | Big Number | Общий % задержек | METRIC: пользовательский SQL `100.0 * AVG(is_delayed)`, подпись «% задержек» |
| 2 | Bar Chart | % задержек по авиакомпаниям | X-AXIS: `airline`; METRIC: `100.0 * AVG(is_delayed)`; SORT BY метрике по убыванию; заголовок оси Y «% задержек» |
| 3 | Pie Chart | Доля рейсов по авиакомпаниям | DIMENSIONS: `airline`; METRIC: `COUNT(*)`; включить подписи с процентами |
| 4 | Mixed Chart | Рейсы и % задержек по месяцам | X-AXIS: `month`; Query A (столбцы): `COUNT(*)`; Query B (линия): `100.0 * AVG(is_delayed)` с включённой *Secondary y-axis*; подписи осей: «рейсов» и «%» |
| 5 | Line Chart | Средняя задержка прибытия по месяцам | X-AXIS: `month` (если месяц читается как число, включить *Force categorical*); METRIC: `AVG(arrival_delay_minutes)`; подпись оси Y «минуты» |

**Шаг 4. Сборка дашборда.** *Dashboards → + Dashboard*, название «Анализ задержек рейсов». Перетащить пять чартов из правой панели: индикатор (1) и круговую (3) — в верхний ряд, столбчатую (2) — в середину, комбинированную (4) и линейную (5) — в нижний ряд. Нажать *Save*.

**Шаг 5. Интерактивные фильтры.** В режиме редактирования дашборда открыть панель фильтров → *Add/Edit Filters → Add filter*. Для каждого фильтра: FILTER TYPE — *Value*, DATASET — `flight_delays_datamart`, **FILTER NAME** (обязательное поле) и COLUMN:

| Имя фильтра | Колонка |
|---|---|
| Авиакомпания | `airline` |
| Месяц | `month` |
| Аэропорт вылета | `origin_airport` |

Включить *Sort filter values* и *Can select multiple values*. Сохранить фильтры и сам дашборд (*Save*). Затем выбрать значение (например, одну авиакомпанию) и убедиться, что пересчитались все пять чартов.

**Шаг 6. Экспорт.** В списке дашбордов: *Actions → Export* — файл сохранить как `superset/dashboard_export.json` (Superset может отдать zip; его содержимое кладётся в `superset/`).

Сводка чартов:

| Тип | Чарт | Настройка |
|---|---|---|
| Индикатор (Big Number) | Общий % задержек | метрика `AVG(is_delayed)`, формат процентов |
| Столбчатая | % задержек по авиакомпаниям | X: `airline`; метрика `AVG(is_delayed)`; сортировка по убыванию |
| Круговая | Доля рейсов по авиакомпаниям | измерение `airline`; метрика `COUNT(*)` |
| Комбинированная (Mixed) | Рейсы и % задержек по месяцам | X: `month`; запрос A (столбцы): `COUNT(*)`; запрос B (линия, вторая ось): `AVG(is_delayed)` |
| Линейная | Средняя задержка прибытия по месяцам | X: `month`; метрика `AVG(arrival_delay_minutes)`, подпись оси «минуты» |

Фильтры: `airline`, `month`, `origin_airport` (Native Filters, применяются ко всем чартам).




## 8. Выводы по анализу

Выводы построены по схеме «наблюдение → объяснение → действие». Количественные значения считываются с чартов 1–5 дашборда; ниже описаны закономерности, которые эти чарты показывают, и их управленческий смысл.

1. **Различия между авиакомпаниями.** Чарт «% задержек по авиакомпаниям» показывает заметный разброс пунктуальности между перевозчиками, а чарт «Доля рейсов» — что крупные сети формируют основную часть выборки, поэтому их показатели определяют общий % задержек. Объяснение: у перевозчиков с плотным оборотом самолётов и большим числом пересадочных рейсов опоздание одного рейса переносится на следующие, а у компаний с меньшей загрузкой расписания запас времени больше. Действие: при выборе перевозчика и планировании поездок учитывать не только цену, но и пунктуальность; авиакомпаниям с повышенным % задержек — увеличить время оборота самолёта между рейсами.
2. **Сезонность.** Комбинированный чарт «Рейсы и % задержек по месяцам» показывает, что доля задержек меняется в течение года и не следует за числом рейсов линейно. Объяснение: на пунктуальность сильнее влияют погодные условия (летние грозы, зимние осадки) и пиковые периоды отпусков, чем сама загрузка. Действие: закладывать дополнительный резерв времени в расписание на месяцы с повышенным % задержек и планировать на них больше наземного персонала.
3. **Частота и величина задержек.** Линейный чарт «Средняя задержка прибытия по месяцам» нужно читать вместе с % задержек: частота задержек и их длительность не всегда совпадают. Объяснение: средняя задержка включает рейсы, прибывшие раньше срока (отрицательные значения), и редкие очень длинные задержки, которые сильно смещают среднее. Действие: оценивать пунктуальность по двум показателям одновременно (% задержек и средняя задержка), а для разбора причин смотреть распределение задержек, а не только среднее.

### Ограничения данных

- Анализируются только **выполненные** рейсы: отменённые и перенаправленные исключены (причины и число отброшенных строк фиксируются в `stg_rejected_log`, раздел 7.3), поэтому оценка пунктуальности по отношению к расписанию оптимистична.
- Данные охватывают один год (2015), вывод о сезонности нельзя считать устойчивым без других лет.
- Задержка определяется по прибытию (порог 15 минут), задержка вылета в витрину не входит.
- Поле `origin_airport` в части записей содержит числовой идентификатор вместо кода IATA (см. запрос 6 в `check_queries.sql`); такие записи сохранены, но подписи аэропортов в чартах для них нечитаемы.
- Состав полей датасета Kaggle может меняться; DAG проверяет наличие обязательных полей и падает с понятным сообщением.

## 9. Структура репозитория

```
lpw_03/
├── dags/
│   ├── flight_delays_dag.py
│   ├── stg_flights.sql
│   ├── datamart_variant_11.sql
│   ├── check_queries.sql
│   └── data/                  # рабочая папка (в .gitignore)
│   └── erd.png
├── superset/
│   ├── dashboard_export.json
│   └── superset_config.py
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.superset
├── requirements.txt
├── setup_kaggle.sh
├── cleanup.sh
├── .gitignore
└── Readme.md
```

## 10. Безопасность учётных данных

`kaggle.json`, `.env` и `dags/.kaggle/` внесены в `.gitignore` и не коммитятся. Используется личный токен Kaggle. Пароли `admin/admin` в `docker-compose.yml` — учебные, для локальной ВМ.

## 11. Очистка окружения

```bash
chmod +x cleanup.sh && sudo ./cleanup.sh
```

## 12. Устранение неполадок

| Проблема | Решение |
|---|---|
| Нет прав на Docker | `sudo usermod -aG docker $USER && newgrp docker` |
| DAG падает на `download_kaggle_data` | выполнить `./setup_kaggle.sh` с личным `kaggle.json`; проверить наличие `dags/.kaggle/kaggle.json` |
| Контейнеры не стартуют после правок | `docker compose build && docker compose up -d` |
| Superset не видит базу | использовать хост `analytics_postgres` и порт `5432` (не `localhost` и не `5433`) |
| DAG не появился в Airflow | `docker compose logs scheduler`; проверить синтаксис: `python3 -m py_compile dags/flight_delays_dag.py` |
| Задача `validate_pipeline` падает | в логе указано, какое равенство нарушено (RAW = STG + отбраковано; витрина = STG) |
