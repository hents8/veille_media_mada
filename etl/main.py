import sys
import os
from dotenv import load_dotenv

load_dotenv()


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
from config.config import MONGO_URI
from rss_loader import fetch_rss_articles
from scraper_loader import scrape_site
from selenium_loader import scrape_orange_actu
from transform import clean_text, analyze_sentiment, categorize_text

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("La variable d'environnement MONGO_URI n'est pas définie !")

def upsert_article(collection, article) -> bool:
    """Ajoute un article si non présent, retourne True si ajouté, False sinon."""
    if collection.count_documents({"id_article": article["id_article"]}, limit=1) == 0:
        collection.insert_one(article)
        return True
    return False

def process_articles(collection, articles, source_label="RSS"):
    """Transforme et insère une liste d'articles dans MongoDB."""
    added = 0
    existing = 0
    for article in articles:
        try:
            article["contenu"] = clean_text(article.get("contenu", ""))
            sentiment_data = analyze_sentiment(article["contenu"])
            article["sentiment"] = sentiment_data
            article["sentiment_score"] = None
            article["categorie"] = categorize_text(article["contenu"])

            if upsert_article(collection, article):
                added += 1
                print(f"✅ [{source_label}] Article ajouté : {article['titre']}")
            else:
                existing += 1
                print(f"⚠️ [{source_label}] Article déjà présent : {article['titre']}")
        except Exception as e:
            print(f"❌ [{source_label}] Erreur sur l'article {article.get('titre', '')}: {e}")
    return added, existing

def main():
    # Connexion MongoDB
    client = MongoClient(MONGO_URI)
    db = client["veille_media"]
    collection = db["articles"]

    # 1️⃣ RSS
    rss_articles = fetch_rss_articles()
    total_rss_added, total_rss_existing = process_articles(collection, rss_articles, "RSS")

    # 2️⃣ Scraping HTML
    scrap_articles = scrape_site()
    total_scrap_added, total_scrap_existing = process_articles(collection, scrap_articles, "SCRAP")

    # 3️⃣ Selenium Orange Actu
    orange_articles = scrape_orange_actu(max_pages=3)
    total_orange_added, total_orange_existing = process_articles(collection, orange_articles, "ORANGE")

    # 4️⃣ Résumé final
    print("\n📊 Résumé final du pipeline :")
    print(f"RSS     - Articles ajoutés : {total_rss_added}, déjà présents : {total_rss_existing}")
    print(f"SCRAP   - Articles ajoutés : {total_scrap_added}, déjà présents : {total_scrap_existing}")
    print(f"ORANGE  - Articles ajoutés : {total_orange_added}, déjà présents : {total_orange_existing}")
    print("✅ Pipeline terminé, MongoDB mis à jour")

if __name__ == "__main__":
    main()
