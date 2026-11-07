"""DAG ЛР3, вариант 11: анализ задержек рейсов (usdot/flight-delays).

Kaggle -> RAW (PostgreSQL) -> STG (очистка + журнал отбракованных строк)
-> DATAMART (VIEW flight_delays_datamart) -> проверка сходимости.
"""
from __future__ import annotations

import io
import json
import logging
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

CONN_ID = "analytics_postgres"
DAGS_DIR = Path(__file__).resolve().parent
DATA_DIR = DAGS_DIR / "data"
KAGGLE_SLUG = "usdot/flight-delays"
SOURCE_FILES = {"raw_flights": "flights.csv", "raw_airlines": "airlines.csv"}
CHUNK_ROWS = 250_000

# Поля flights.csv, без которых STG построить нельзя (имена в нижнем регистре).
REQUIRED_FLIGHT_COLUMNS = {
    "year", "month", "airline", "origin_airport",
    "arrival_delay", "cancelled", "diverted",
}
REQUIRED_AIRLINE_COLUMNS = {"iata_code", "airline"}


def _read_sql(name: str) -> str:
    return (DAGS_DIR / name).read_text(encoding="utf-8")


def _ensure_kaggle_credentials() -> None:
    """Берём ключ из переменных окружения, иначе из kaggle.json (вне Git)."""
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        return
    cfg_dir = Path(os.environ.get("KAGGLE_CONFIG_DIR", DAGS_DIR / ".kaggle"))
    cfg = cfg_dir / "kaggle.json"
    if not cfg.exists():
        raise FileNotFoundError(
            "Не найден токен Kaggle: выполните ./setup_kaggle.sh "
            f"(ожидался файл {cfg} или переменные KAGGLE_USERNAME/KAGGLE_KEY)."
        )
    creds = json.loads(cfg.read_text(encoding="utf-8"))
    os.environ["KAGGLE_USERNAME"] = creds["username"]
    os.environ["KAGGLE_KEY"] = creds["key"]


def download_data() -> None:
    _ensure_kaggle_credentials()
    import kagglehub  # импорт здесь, чтобы не тормозить парсинг DAG

    os.environ.setdefault("KAGGLEHUB_CACHE", str(DATA_DIR / ".kagglehub"))
    src = Path(kagglehub.dataset_download(KAGGLE_SLUG))
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for filename in SOURCE_FILES.values():
        found = list(src.rglob(filename))
        if not found:
            raise FileNotFoundError(f"В наборе {KAGGLE_SLUG} нет файла {filename}")
        shutil.copy2(found[0], DATA_DIR / filename)
        log.info("Файл %s скопирован в %s", filename, DATA_DIR)


def _load_csv_to_raw(table: str, filename: str, required: set[str]) -> None:
    """RAW: копия источника как есть, все поля text, ничего не чистим."""
    path = DATA_DIR / filename
    header = pd.read_csv(path, nrows=0, dtype=str).columns.tolist()
    cols = [c.strip().lower() for c in header]
    missing = required - set(cols)
    if missing:
        raise ValueError(f"В {filename} отсутствуют обязательные поля: {sorted(missing)}")

    col_defs = ", ".join(f'"{c}" text' for c in cols)
    col_list = ", ".join(f'"{c}"' for c in cols)

    conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"CREATE TABLE {table} (raw_id bigserial PRIMARY KEY, {col_defs})")
            loaded = 0
            for chunk in pd.read_csv(
                path, dtype=str, keep_default_na=False, chunksize=CHUNK_ROWS
            ):
                buf = io.StringIO()
                chunk.to_csv(buf, index=False, header=False)
                buf.seek(0)
                cur.copy_expert(
                    f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT csv)", buf
                )
                loaded += len(chunk)
                log.info("%s: загружено %d строк", table, loaded)
            cur.execute(f"SELECT count(*) FROM {table}")
            in_db = cur.fetchone()[0]
        if in_db != loaded:
            raise RuntimeError(f"{table}: прочитано {loaded}, в БД {in_db}")
        conn.commit()
        log.info("%s: итого %d строк (источник = RAW)", table, in_db)
    finally:
        conn.close()


def load_raw_flights() -> None:
    _load_csv_to_raw("raw_flights", SOURCE_FILES["raw_flights"], REQUIRED_FLIGHT_COLUMNS)


def load_raw_airlines() -> None:
    _load_csv_to_raw("raw_airlines", SOURCE_FILES["raw_airlines"], REQUIRED_AIRLINE_COLUMNS)


def build_stg() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(_read_sql("stg_flights.sql"), autocommit=True)
    for reason, cnt in hook.get_records(
        "SELECT reason, rows_rejected FROM stg_rejected_summary ORDER BY 2 DESC"
    ):
        log.info("Отбраковано (%s): %d строк", reason, cnt)


def create_datamart() -> None:
    PostgresHook(postgres_conn_id=CONN_ID).run(
        _read_sql("datamart_variant_11.sql"), autocommit=True
    )


def validate_pipeline() -> None:
    """Проверка сходимости: число записей и суммы по разрезам."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    one = lambda q: hook.get_first(q)[0]  # noqa: E731

    raw_cnt = one("SELECT count(*) FROM raw_flights")
    stg_cnt = one("SELECT count(*) FROM stg_flights")
    rej_cnt = one("SELECT count(*) FROM stg_rejected_log")
    mart_cnt = one("SELECT count(*) FROM flight_delays_datamart")
    by_airline = one(
        "SELECT sum(c) FROM (SELECT count(*) c FROM flight_delays_datamart GROUP BY airline) t"
    )
    by_month = one(
        "SELECT sum(c) FROM (SELECT count(*) c FROM flight_delays_datamart GROUP BY month) t"
    )
    bad_flag = one(
        "SELECT count(*) FROM flight_delays_datamart WHERE is_delayed NOT IN (0,1) OR is_delayed IS NULL"
    )
    null_keys = one(
        "SELECT count(*) FROM flight_delays_datamart WHERE month IS NULL OR airline IS NULL "
        "OR origin_airport IS NULL OR arrival_delay_minutes IS NULL"
    )

    log.info(
        "RAW=%d STG=%d REJECTED=%d MART=%d by_airline=%d by_month=%d",
        raw_cnt, stg_cnt, rej_cnt, mart_cnt, by_airline, by_month,
    )
    problems = []
    if raw_cnt != stg_cnt + rej_cnt:
        problems.append(f"RAW {raw_cnt} != STG {stg_cnt} + отбраковано {rej_cnt}")
    if mart_cnt != stg_cnt:
        problems.append(f"витрина {mart_cnt} != STG {stg_cnt}")
    if by_airline != mart_cnt or by_month != mart_cnt:
        problems.append("сумма по разрезам не равна итогу")
    if bad_flag or null_keys:
        problems.append(f"некорректный is_delayed: {bad_flag}, NULL в ключевых полях: {null_keys}")
    if problems:
        raise AssertionError("; ".join(problems))
    log.info("Проверка сходимости пройдена")


default_args = {
    "owner": "dansu_quentin",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="flight_delays_variant_11",
    description="ЛР3 вариант 11: задержки рейсов (Kaggle -> RAW -> STG -> витрина)",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["lab3", "variant_11", "flights"],
) as dag:
    t_download = PythonOperator(task_id="download_kaggle_data", python_callable=download_data)
    t_raw_flights = PythonOperator(task_id="load_raw_flights", python_callable=load_raw_flights)
    t_raw_airlines = PythonOperator(task_id="load_raw_airlines", python_callable=load_raw_airlines)
    t_stg = PythonOperator(task_id="build_stg_flights", python_callable=build_stg)
    t_mart = PythonOperator(task_id="create_datamart_view", python_callable=create_datamart)
    t_check = PythonOperator(task_id="validate_pipeline", python_callable=validate_pipeline)

    t_download >> [t_raw_flights, t_raw_airlines] >> t_stg >> t_mart >> t_check
