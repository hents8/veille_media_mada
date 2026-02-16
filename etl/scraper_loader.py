import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from transform import clean_text, analyze_sentiment, categorize_text
import datetime
import hashlib

SCRAP_SITES = [
    {
        "name": "Malagasy News",
        "url": "https://www.malagasynews.com/actualites/",
        "article_selector": "h2.post-title a",
        "content_selector": "div.entry-content",
        "date_selector": "span.date"
    }
]

HEADERS = {"User-Agent": "Mozilla/5.0"}

def generate_article_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def parse_date(text: str) -> str:
    try:
        dt = date_parser.parse(text, fuzzy=True)
        return dt.astimezone(datetime.timezone.utc).isoformat()
    except Exception:
        return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

def scrape_site() -> list[dict]:
    """Retourne la liste des articles extraits des sites HTML"""
    articles_list = []

    for site in SCRAP_SITES:
        url_base = site.get("url")
        name = site.get("name", url_base)

        try:
            res = requests.get(url_base, headers=HEADERS, timeout=10)
            res.raise_for_status()
        except Exception:
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        articles_blocks = soup.select(site.get("article_selector", ""))
        if not articles_blocks:
            continue

        for a in articles_blocks:
            titre = a.get_text(strip=True)
            url = a.get("href")
            if not url:
                continue
            if not url.startswith("http"):
                url = url_base.rstrip("/") + url

            article_id = generate_article_id(url)

            try:
                res_article = requests.get(url, headers=HEADERS, timeout=10)
                res_article.raise_for_status()
                soup_article = BeautifulSoup(res_article.text, "html.parser")
            except Exception:
                continue

            contenu_block = soup_article.select_one(site.get("content_selector", ""))
            contenu = contenu_block.get_text(" ", strip=True) if contenu_block else ""

            date_block = soup_article.select_one(site.get("date_selector", ""))
            date_pub_text = date_block.get_text(strip=True) if date_block else ""
            date_pub = parse_date(date_pub_text) if date_pub_text else datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

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
                "created_at": datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
            }
            articles_list.append(doc)

    return articles_list
