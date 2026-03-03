import hashlib
import time
from typing import List, Dict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from dateutil import parser as date_parser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from etl.transform import clean_text, analyze_sentiment, categorize_text


SCRAP_SITES = [
    {
        "name": "Malagasy News",
        "url": "https://www.malagasynews.com/actualites/",
        "article_selector": "h2.post-title a",
        "content_selector": "div.entry-content",
        "date_selector": "span.date"
    }
]


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com"
}


# 🔹 Session avec retry automatique
def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)

    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def generate_article_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def parse_date(text: str) -> datetime:
    try:
        dt = date_parser.parse(text, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return datetime.now(timezone.utc)


def scrape_site() -> List[Dict]:
    articles_list = []
    session = create_session()

    for site in SCRAP_SITES:
        url_base = site["url"]
        name = site["name"]

        print(f"\n🔎 Scraping {name}...")

        try:
            res = session.get(url_base, timeout=15)
            print("Status page liste:", res.status_code)

            if res.status_code != 200:
                print("❌ Impossible d'accéder à la page liste")
                continue

        except Exception as e:
            print("❌ Erreur requête page liste:", e)
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        articles_blocks = soup.select(site["article_selector"])

        print("Articles trouvés page liste:", len(articles_blocks))

        if not articles_blocks:
            print("⚠️ Aucun article détecté - vérifier selector ou blocage")
            continue

        for a in articles_blocks:
            titre = a.get_text(strip=True)
            url = a.get("href")

            if not url:
                continue

            url = urljoin(url_base, url)
            article_id = generate_article_id(url)

            try:
                res_article = session.get(url, timeout=15)

                if res_article.status_code != 200:
                    print("⚠️ Article inaccessible:", url)
                    continue

                soup_article = BeautifulSoup(res_article.text, "html.parser")

            except Exception as e:
                print("❌ Erreur article:", url, e)
                continue

            contenu_block = soup_article.select_one(site["content_selector"])
            contenu = contenu_block.get_text(" ", strip=True) if contenu_block else ""

            if not contenu:
                print("⚠️ Contenu vide pour:", url)
                continue

            date_block = soup_article.select_one(site["date_selector"])
            date_pub_text = date_block.get_text(strip=True) if date_block else ""
            date_pub = parse_date(date_pub_text) if date_pub_text else datetime.now(timezone.utc)

            contenu_clean = clean_text(contenu)

            doc = {
                "id_article": article_id,
                "source": name,
                "source_type": "scrap_html",
                "titre": titre,
                "date_publication": date_pub,
                "contenu": contenu_clean,
                "url": url,
                "categorie": categorize_text(contenu_clean),
                "sentiment": analyze_sentiment(contenu_clean),
                "created_at": datetime.now(timezone.utc)
            }

            articles_list.append(doc)

            # Petite pause anti-blocage
            time.sleep(1)

        print(f"✅ {len(articles_list)} articles extraits pour {name}")

    return articles_list