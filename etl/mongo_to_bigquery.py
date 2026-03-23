#!/usr/bin/env python3
"""
MongoDB → BigQuery Sync TOTAL (TOUS les articles, en respectant le JSON Schema)
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

# Configuration ETL standard
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))
sys.path.append(BASE_DIR)

# Config
MONGO_URI = os.getenv("MONGO_URI")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials", "service_account.json")

# Forcer l’utilisation de ce fichier de credentials pour Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

# Logging (Windows fix)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(BASE_DIR, f"sync_logs_{datetime.now().strftime('%Y%m%d_%H%M')}.log"),
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_table_if_not_exists(client_bq, dataset_id, table_id):
    """Créer la table articles_clean avec le schéma respectant le JSON Schema MongoDB."""
    table_ref = client_bq.dataset(dataset_id).table(table_id)
    try:
        client_bq.get_table(table_ref)
        logger.info(f"Table {table_id} already exists. Using existing schema.")
    except Exception:
        # Ne crée la table que si elle n’existe pas
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
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client_bq.create_table(table)
        logger.info(f"Table {table_id} created in {dataset_id}.")


def extract_date(field):
    """Extrait la valeur de date MongoDB { $date: "..." } → chaîne ISO."""
    val = field
    if val is None:
        return None
    if isinstance(val, dict) and "$date" in val:
        return val["$date"]  # ex: "2026-03-23T18:00:00Z"
    if isinstance(val, str):
        return val
    if isinstance(val, datetime):
        return val.isoformat()
    return None


def main():
    """Sync TOTAL MongoDB → BigQuery (mise à jour de la table existante)."""
    try:
        # Vérif config
        if not all([MONGO_URI, GCP_PROJECT_ID, BIGQUERY_DATASET]):
            logger.error("Variables manquantes")
            sys.exit(1)

        logger.info(f"Sync TOTAL: {GCP_PROJECT_ID}.{BIGQUERY_DATASET}")

        # MongoDB
        client_mongo = MongoClient(MONGO_URI)
        db = client_mongo["veille_media"]
        collection = db["articles"]

        # DEBUG: compter total
        total_articles = collection.count_documents({})
        logger.info(f"TOTAL articles MongoDB: {total_articles}")

        if total_articles == 0:
            logger.info("Collection vide - lance tes scrapers d'abord")
            return

        # TOUS les articles (pas de filtre 24h)
        logger.info("Extraction de TOUS les articles...")
        articles = list(collection.find({}).sort("created_at", -1))

        logger.info(f"{len(articles)} articles à synchroniser")

        # BigQuery
        client_bq = bigquery.Client(project=GCP_PROJECT_ID)
        table_id = "articles_clean"
        table_ref = client_bq.dataset(BIGQUERY_DATASET).table(table_id)

        # On utilise la table existante (on ne la recrée pas si elle existe déjà)
        create_table_if_not_exists(client_bq, BIGQUERY_DATASET, table_id)

        # Construire les lignes BigQuery (1 ligne = 1 document, déflatté)
        rows = []
        sync_time = datetime.now(timezone.utc).isoformat()

        for i, article in enumerate(articles, 1):
            # Dates MongoDB → chaînes ISO pour BigQuery
            created_at = extract_date(article.get("created_at"))
            date_publication = extract_date(article.get("date_publication"))

            # Catégories
            categories = article.get("categorie", [])
            if not isinstance(categories, list):
                categories = [str(categories)] if categories is not None else []

            # Sentiment score
            sentiment_score = article.get("sentiment_score")
            if isinstance(sentiment_score, dict) and "$numberDouble" in sentiment_score:
                sentiment_score = sentiment_score["$numberDouble"]

            row = {
                "id_article": str(article.get("_id")) or article.get("id_article"),
                "titre": str(article.get("titre", "")),
                "url": str(article.get("url", "")),
                "contenu": str(article.get("contenu", "")),
                "source": str(article.get("source", "")),
                "source_type": str(article.get("source_type", "")),
                "categorie": categories,  # array de strings
                "langue": str(article.get("langue", "")),
                "sentiment": str(article.get("sentiment", "")),
                "sentiment_score": float(sentiment_score) if sentiment_score not in (None, "NaN", "Infinity", "-Infinity") else None,
                "origin": str(article.get("origin", "")),
                "created_at": created_at,        # TIMESTAMP via ISO string
                "date_publication": date_publication,  # TIMESTAMP via ISO string
            }
            rows.append(row)

            # Progress
            if i % 50 == 0:
                logger.info(f"Préparé: {i}/{len(articles)}")

        # Écrire dans un fichier temporaire JSONL
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".jsonl",
            encoding="utf-8",
            delete=False
        ) as tmpfile:
            for row in rows:
                json.dump(row, tmpfile, ensure_ascii=False)
                tmpfile.write("\n")
            tmpfile.flush()
            tmpfile_name = tmpfile.name

        logger.info("Upload BigQuery via batch job (append/overwrite)...")

        # Config du job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
            # tu peux choisir :
            # write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # écrase tout
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,  # ou WRITE_APPEND
        )

        # Link au schéma existant
        job_config.schema = client_bq.get_table(table_ref).schema

        # Charger le fichier JSONL dans la table existante
        with open(tmpfile_name, "rb") as source_file:
            job = client_bq.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config,
            )

        job.result()  # Attente de la fin du job

        logger.info(f"SUCCESS: {job.output_rows} rows → {GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_id}")
        logger.info("Table mise à jour dans BigQuery.")

    except Exception as e:
        logger.error(f"ERREUR: {e}")
        sys.exit(1)
    finally:
        if 'client_mongo' in locals():
            client_mongo.close()


if __name__ == "__main__":
    main()
