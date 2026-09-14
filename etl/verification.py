import os
from pymongo import MongoClient
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "veille-media-mada-sync")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "veille_mada")

# 1. MongoDB
client_mongo = MongoClient(MONGO_URI)
mongo_count = client_mongo["veille_media"]["articles"].count_documents({})

# 2. BigQuery (Total de lignes vs Articles Uniques)
client_bq = bigquery.Client(project=GCP_PROJECT_ID)

query = f"""
SELECT 
  COUNT(1) as total_lignes,
  COUNT(DISTINCT id_article) as articles_uniques
FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.articles_clean`
"""
job = client_bq.query(query)
results = list(job.result())[0]

print(f"📊 MongoDB total : {mongo_count}")
print(f"📦 BigQuery total lignes : {results['total_lignes']}")
print(f"🔑 BigQuery articles uniques : {results['articles_uniques']}")