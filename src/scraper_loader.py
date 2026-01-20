import requests
from bs4 import BeautifulSoup
from dateutil import parser
from pymongo import MongoClient
from config.config import SCRAP_SITES
from transform import clean_text, analyze_sentiment, categorize_text

HEADERS = {"User-Agent": "Mozilla/5.0"}

def scrape_site(collection):
    scrap_ajoutes = 0
    scrap_deja_present = 0

    for site in SCRAP_SITES:
        try:
            res = requests.get(site["url"], headers=HEADERS, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            articles_blocks = soup.select(site["article_selector"])
            print(f"🔍 [{site['name']}] Articles trouvés : {len(articles_blocks)}")


            for a in articles_blocks:
                titre = a.get_text(strip=True)
                url = a.get("href")
                if not url.startswith("http"):
                    url = site["url"].rstrip("/") + url

                if collection.count_documents({"id_article": url}, limit=1) > 0:
                    print(f"⚠️ [SCRAP] Article déjà présent : {titre}")
                    scrap_deja_present += 1
                    continue

                try:
                    res_article = requests.get(url, headers=HEADERS, timeout=10)
                    res_article.raise_for_status()
                    soup_article = BeautifulSoup(res_article.text, "html.parser")
                    contenu_block = soup_article.select_one(site["content_selector"])
                    contenu = contenu_block.get_text(" ", strip=True) if contenu_block else ""

                    date_block = soup_article.select_one(site["date_selector"])
                    date_pub = None
                    if date_block:
                        date_text = date_block.get_text(strip=True)
                        if date_text:
                            try:
                                date_pub = parser.parse(date_text, fuzzy=True)
                            except Exception:
                                pass

                    # Transformations
                    contenu_clean = clean_text(contenu)
                    sentiment = analyze_sentiment(contenu_clean)
                    categorie = categorize_text(contenu_clean)

                    doc = {
                        "id_article": url,
                        "source": site["name"],
                        "titre": titre,
                        "date_publication": date_pub.isoformat() if date_pub else None,
                        "contenu": contenu_clean,
                        "url": url,
                        "categorie": categorie,
                        "sentiment": sentiment
                    }

                    collection.insert_one(doc)
                    print(f"✅ [SCRAP] Article ajouté : {titre}")
                    scrap_ajoutes += 1

                except Exception as e:
                    print(f"Erreur site {site['name']} article {url} : {e}")

        except Exception as e:
            print(f"Erreur connexion site {site['name']} : {e}")

    return scrap_ajoutes, scrap_deja_present

