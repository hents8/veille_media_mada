import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
from rss_loader import fetch_rss_articles
from scraper_loader import scrape_site
from transform import clean_text, analyze_sentiment, categorize_text
from config.config import MONGO_URI
from selenium_loader import scrape_orange_actu

# Connexion MongoDB
client = MongoClient(MONGO_URI)
db = client["veille_media"]
collection = db["articles"]

# Compteurs
rss_ajoutes = 0
rss_deja_present = 0
scrap_ajoutes = 0
scrap_deja_present = 0

# 1️⃣ Articles RSS
rss_articles = fetch_rss_articles()
for article in rss_articles:
    article["contenu"] = clean_text(article["contenu"])
    article["sentiment"] = analyze_sentiment(article["contenu"])
    article["categorie"] = categorize_text(article["contenu"])
    
    if collection.count_documents({"id_article": article["id_article"]}, limit=1) == 0:
        collection.insert_one(article)
        print(f"✅ [RSS] Article ajouté : {article['titre']}")
        rss_ajoutes += 1
    else:
        print(f"⚠️ [RSS] Article déjà présent : {article['titre']}")
        rss_deja_present += 1

# 2️⃣ Articles scrape sites sans RSS
scrap_ajoutes, scrap_deja_present = scrape_site(collection)

# 3️⃣ Orange Actu (Selenium)
scrape_orange_actu(collection, max_pages=3)

# 3️⃣ Résumé final
print("\n📊 Résumé du pipeline :")
print(f"RSS   - Articles ajoutés : {rss_ajoutes}, déjà présents : {rss_deja_present}")
print(f"SCRAP - Articles ajoutés : {scrap_ajoutes}, déjà présents : {scrap_deja_present}")
print("✅ Pipeline terminé, MongoDB mis à jour")
