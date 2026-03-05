import sys
import os
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))
sys.path.append(BASE_DIR)

from etl.rss_loader import fetch_rss_articles
from etl.scraper_loader import scrape_site
from etl.selenium_loader import scrape_orange_actu
from etl.transform import clean_text, analyze_sentiment, categorize_text, analyze_sentiment_score, detect_language, extract_origin

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("La variable d'environnement MONGO_URI n'est pas définie !")
 
MIN_CONTENT_LENGTH = 30
MIN_TITLE_LENGTH = 5 
BATCH_SIZE = 100

def process_articles(collection, articles, source_label="RSS"):
    operations = []
    added_count = 0
    existing_count = 0
    skipped_count = 0

    # 🔹 Récupère les ID existants
    existing_ids = set(collection.distinct("id_article"))

    for article in articles:
        try:
            article_id = article.get("id_article")

            if not article_id:
                skipped_count += 1
                continue

            if article_id in existing_ids:
                existing_count += 1
                print(f"⚠️ [{source_label}] - {article.get('date_publication').date()} Déjà présent : {article.get('titre')}")
                continue

            # 🔹 Validation titre + contenu
            titre = article.get("titre")
            contenu = article.get("contenu")

            if (
                not titre
                or not contenu
                or pd.isna(titre)
                or pd.isna(contenu)
                or len(str(titre).strip()) < MIN_TITLE_LENGTH
                or len(str(contenu).strip()) < MIN_CONTENT_LENGTH
            ):
                skipped_count += 1
                print(f"⚠️ [{source_label}] Article ignoré (titre/contenu vide ou trop court)")
                continue

            # 🔹 Nettoyage
            cleaned_content = clean_text(contenu)
            article["contenu"] = cleaned_content

            # 🔹 NLP sécurisé
            try:
                article["langue"] = detect_language(cleaned_content)
            except:
                article["langue"] = None

            try:
                article["sentiment"] = analyze_sentiment(cleaned_content)
                article["sentiment_score"] = analyze_sentiment_score(cleaned_content)
            except:
                article["sentiment"] = None
                article["sentiment_score"] = None

            try:
                article["categorie"] = categorize_text(cleaned_content)
            except:
                article["categorie"] = None

            source_val = article.get("source", "")
            article["origin"] = extract_origin(source_val)
            
            operations.append(InsertOne(article))
            added_count += 1

            print(f"✅ [{source_label}] - {article.get('date_publication').date()} Ajouté : {titre}")

            # 🔹 Batch insert
            if len(operations) >= BATCH_SIZE:
                collection.bulk_write(operations)
                operations = []

        except Exception as e:
            print(f"❌ [{source_label}] Erreur sur article {article.get('titre', '')}: {e}")

    # 🔹 Insert restant
    if operations:
        collection.bulk_write(operations)
    
    return added_count, existing_count
    
def main():
    client = MongoClient(MONGO_URI)
    db = client["veille_media"]
    collection = db["articles"]

    print("📌 Démarrage pipeline veille média")
    
    # 1️⃣ RSS
    rss_articles = fetch_rss_articles()
    total_rss_added, total_rss_existing = process_articles(collection, rss_articles, "RSS")

    # 2️⃣ Scraping HTML
    scrap_articles = scrape_site()
    total_scrap_added, total_scrap_existing = process_articles(collection, scrap_articles, "SCRAP")

    # 3️⃣ Selenium Orange Actu
    #orange_articles = scrape_orange_actu(max_pages=3)
    #total_orange_added, total_orange_existing = process_articles(collection, orange_articles, "ORANGE")

    # 4️⃣ Résumé final
    print("\n📊 Résumé final du pipeline :")
    print(f"RSS     - Articles ajoutés : {total_rss_added}, déjà présents : {total_rss_existing}")
    print(f"SCRAP   - Articles ajoutés : {total_scrap_added}, déjà présents : {total_scrap_existing}")
    #print(f"ORANGE  - Articles ajoutés : {total_orange_added}, déjà présents : {total_orange_existing}")
    print("✅ Pipeline terminé, MongoDB mis à jour")

if __name__ == "__main__":
    main()
