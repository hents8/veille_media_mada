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
    
    print(f"📊 [{source_label}] Résumé: +{added_count} ajoutés, {existing_count} existants, {skipped_count} ignorés")
    return added_count, existing_count

def main():
    client = MongoClient(MONGO_URI)
    db = client["veille_media"]
    collection = db["articles"]

    print("🚀 PIPELINE VEILLE MÉDIA - 13/04/2026")
    
    # 🔍 1️⃣ DEBUG RSS (CAUSE PRINCIPALE)
    print("\n🔍 DEBUG RSS...")
    rss_articles = fetch_rss_articles()
    print(f"  📊 Articles RSS totaux: {len(rss_articles)}")
    
    if rss_articles:
        for i, art in enumerate(rss_articles[:3]):
            print(f"  {i+1}: '{art.get('titre', 'NO TITLE')[:60]}'")
            print(f"     URL: {art.get('url', 'NO URL')[:70]}")
            print(f"     Contenu: {len(str(art.get('contenu', '')))} chars")
        print(f"  ... + {len(rss_articles)-3} autres")
    else:
        print("  ❌ RSS VIDE = BUG rss_loader.py !")
        client.close()
        return

    # 2️⃣ PROCESS RSS
    print("\n🔄 Processing RSS...")
    total_rss_added, total_rss_existing = process_articles(collection, rss_articles, "RSS")

    # 3️⃣ SCRAPING (optionnel)
    # print("\n🕷️  Scraping...")
    # scrap_articles = scrape_site()
    # total_scrap_added, total_scrap_existing = process_articles(collection, scrap_articles, "SCRAP")

    # 4️⃣ ORANGE (optionnel)
    # print("\n🌊 Orange Actu...")
    # orange_articles = scrape_orange_actu(max_pages=3)
    # total_orange_added, total_orange_existing = process_articles(collection, orange_articles, "ORANGE")

    # 📊
    print("\n" + "="*60)
    print("✅ PIPELINE TERMINÉ")
    print(f"RSS:     +{total_rss_added} AJOUTÉS, {total_rss_existing} DÉJÀ PRÉSENTS")
    print("MongoDB mis à jour ! Vérifie: db.articles.find({origin: 'Midi Madagasikara'})")
    print("="*60)
    
    client.close()

if __name__ == "__main__":
    main()