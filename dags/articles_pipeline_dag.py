from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.main import fetch_rss_articles, scrape_site, scrape_orange_actu, process_articles
from pymongo import MongoClient
from config.config import MONGO_URI

default_args = {
    "owner": "henintsoa",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_rss():
    client = MongoClient(MONGO_URI)
    db = client["veille_media"]
    collection = db["articles"]
    rss_articles = fetch_rss_articles()
    process_articles(collection, rss_articles, source_label="RSS")

def run_scrap():
    client = MongoClient(MONGO_URI)
    db = client["veille_media"]
    collection = db["articles"]
    scrap_articles = scrape_site()
    process_articles(collection, scrap_articles, source_label="SCRAP")

def run_orange():
    client = MongoClient(MONGO_URI)
    db = client["veille_media"]
    collection = db["articles"]
    orange_articles = scrape_orange_actu(max_pages=3)
    process_articles(collection, orange_articles, source_label="ORANGE")

with DAG(
    "articles_pipeline",
    default_args=default_args,
    description="Pipeline ETL d'articles",
    schedule_interval="0 * * * *",  # toutes les heures
    start_date=datetime(2026, 2, 13),
    catchup=False,
) as dag:

    task_rss = PythonOperator(
        task_id="fetch_rss_articles",
        python_callable=run_rss
    )

    task_scrap = PythonOperator(
        task_id="scrape_html",
        python_callable=run_scrap
    )

    task_orange = PythonOperator(
        task_id="scrape_orange_actu",
        python_callable=run_orange
    )

    # Définir l’ordre d’exécution si nécessaire
    task_rss >> task_scrap >> task_orange
