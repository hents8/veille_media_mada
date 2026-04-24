#!/usr/bin/env python3
"""
MongoDB → BigQuery Sync TOTAL (veille_media.articles → articles_clean)
Amélioré : Workload Identity + TIMESTAMP natif + Robust float
"""
import sys
import os
import tempfile
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from google.cloud import bigquery
from datetime import datetime, timezone
import logging

# ETL standard
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))
sys.path.append(BASE_DIR)

# Config (pas de JSON key - ADC auto)
MONGO_URI = os.getenv("MONGO_URI")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "veille-media-mada-sync")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "veille_mada")

# Logs (Windows + rotation)
log_file = os.path.join(BASE_DIR, f"sync_logs_{datetime.now().strftime('%Y%m%d_%H%M')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def safe_float(value):
    """Safe float sans crash NaN/Infinity."""
    if value is None or value == "NaN" or value == "Infinity" or value == "-Infinity":
        return None
    try:
        if isinstance(value, dict) and "$numberDouble" in value:
            return float(value["$numberDouble"])
        return float(value)
    except (ValueError, TypeError):
        return None

def parse_mongo_date(field):
    """$date MongoDB → datetime natif."""
    if field is None:
        return None
    if isinstance(field, dict) and "$date" in field:
        return datetime.fromisoformat(field["$date"].replace('Z', '+00:00'))
    if isinstance(field, str):
        try:
            return datetime.fromisoformat(field.replace('Z', '+00:00'))
        except ValueError:
            return None
    if isinstance(field, datetime):
        return field
    return None

def create_table_if_not_exists(client_bq, dataset_id, table_id):
    table_ref = client_bq.dataset(dataset_id).table(table_id)
    try:
        client_bq.get_table(table_ref)
        logger.info(f"Table {table_id} exists.")
    except Exception:
        schema = [
            bigquery.SchemaField("id_article", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("titre", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("contenu", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("categorie", "STRING", mode="REPEATED"),
            bigquery.SchemaField("langue", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sentiment", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sentiment_score", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("origin", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("date_publication", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("sync_timestamp", "TIMESTAMP", mode="NULLABLE"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client_bq.create_table(table)
        logger.info(f"Table {table_id} created.")

def main():
    client_mongo = None
    tmp_name = None
    try:
        if not MONGO_URI or not GCP_PROJECT_ID or not BIGQUERY_DATASET:
            logger.error("MONGO_URI/GCP_PROJECT_ID/BIGQUERY_DATASET manquants")
            sys.exit(1)

        logger.info(f"Sync: {GCP_PROJECT_ID}.{BIGQUERY_DATASET}.articles_clean")

        # MongoDB
        client_mongo = MongoClient(MONGO_URI)
        collection = client_mongo["veille_media"]["articles"]
        total = collection.count_documents({})
        logger.info(f"TOTAL MongoDB: {total}")

        if total == 0:
            logger.info("Collection vide.")
            return

        # Cursor batché
        articles = list(collection.find({}).sort("created_at", -1).batch_size(100))

        # BigQuery
        client_bq = bigquery.Client(project=GCP_PROJECT_ID)
        table_id = "articles_clean"
        create_table_if_not_exists(client_bq, BIGQUERY_DATASET, table_id)
        table_ref = client_bq.dataset(BIGQUERY_DATASET).table(table_id)

        # Rows
        rows = []
        sync_time = datetime.now(timezone.utc)
        for i, article in enumerate(articles, 1):
            categories = article.get("categorie", [])
            if not isinstance(categories, list):
                categories = [str(categories)] if categories else []

            row = {
                "id_article": str(article.get("_id", "")),
                "titre": str(article.get("titre", "")),
                "url": str(article.get("url", "")),
                "contenu": str(article.get("contenu", "")),
                "source": str(article.get("source", "")),
                "source_type": str(article.get("source_type", "")),
                "categorie": categories,
                "langue": str(article.get("langue", "")),
                "sentiment": str(article.get("sentiment", "")),
                "sentiment_score": safe_float(article.get("sentiment_score")),
                "origin": str(article.get("origin", "")),
                "created_at": parse_mongo_date(article.get("created_at")),
                "date_publication": parse_mongo_date(article.get("date_publication")),
                "sync_timestamp": sync_time
            }
            rows.append(row)

            if i % 100 == 0 or i == total:
                logger.info(f"Préparé: {i}/{total} ({i/total*100:.1f}%)")

        # JSONL temp
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", encoding="utf-8", delete=False)
        tmp_name = tmp.name
        for row in rows:
            json.dump(row, tmp, ensure_ascii=False)
            tmp.write("\n")
        tmp.close()

        # Load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Remplace tout
            schema=client_bq.get_table(table_ref).schema
        )
        with open(tmp_name, "rb") as f:
            job = client_bq.load_table_from_file(f, table_ref, job_config=job_config)
        job.result()

        logger.info(f"SUCCESS: {job.output_rows} rows loaded.")
        logger.info(f"Logs: {log_file}")

    except Exception as e:
        logger.error(f"ERREUR: {e}")
        sys.exit(1)
    finally:
        if client_mongo:
            client_mongo.close()
        if tmp_name and os.path.exists(tmp_name):
            os.unlink(tmp_name)

if __name__ == "__main__":
    main()
