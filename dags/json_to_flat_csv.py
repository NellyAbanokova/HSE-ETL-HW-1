import json
import os
from datetime import datetime
from urllib.request import Request, urlopen

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

JSON_URL = "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"

OUTPUT_DIR = "/opt/airflow/output"
RESULT_CSV_PATH = os.path.join(OUTPUT_DIR, "result.csv")


def _http_get_text(url: str, timeout: int = 30) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


def extract(**context):
    raw = _http_get_text(JSON_URL)
    context["ti"].xcom_push(key="json_raw", value=raw)
    return "ok"


def transform(**context):
    raw = context["ti"].xcom_pull(task_ids="extract", key="json_raw")
    if not raw:
        raise ValueError("No JSON in XCom")

    data = json.loads(raw)
    pets = data.get("pets", [])

    df = pd.json_normalize(pets)

    if "favFoods" in df.columns:
        df["favFoods"] = df["favFoods"].apply(lambda x: x if isinstance(x, list) else [])
        df = df.explode("favFoods").rename(columns={"favFoods": "favFood"})
    else:
        df["favFood"] = None

    wanted = ["name", "species", "birthYear", "photo", "favFood"]
    for col in wanted:
        if col not in df.columns:
            df[col] = None
    df = df[wanted]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(RESULT_CSV_PATH, index=False, encoding="utf-8")

    return RESULT_CSV_PATH


def load(ti, **context):
    path = ti.xcom_pull(task_ids="transform")
    if not path:
        raise ValueError("No result path from transform")
    return str(path)


with DAG(
    dag_id="json_to_flat_csv",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hw", "etl", "json"],
) as dag:
    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load = PythonOperator(task_id="load", python_callable=load)

    t_extract >> t_transform >> t_load