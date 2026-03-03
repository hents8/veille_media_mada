import feedparser
import datetime
import hashlib
from dateutil import parser
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import unicodedata

HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 15
MAX_RETRIES = 3

RSS_FEEDS = [
    "https://www.madagascar-tribune.com/spip.php?page=backend",
    "https://www.lexpress.mg/feeds/posts/default",
    "https://newsmada.com/feed/",
    "https://midi-madagasikara.mg/feed/",  
    "https://2424.mg/feed/",
    "https://rsf.org/fr/rss/afrique/madagascar/feed.xml",
    "https://lgdi-madagascar.com/feed/",
    "https://midi-madagasikara.mg/category/politique/feed/",
    "https://midi-madagasikara.mg/category/economie/feed/",
    "https://www.lexpress.mg/feeds/posts/default/-/Politique",
    "https://www.lexpress.mg/feeds/posts/default/-/%C3%89conomie",
    "https://newsmada.com/category/les-nouvelles/feed/",
    "https://newsmada.com/category/les-nouvelles/politique/feed/",
    "https://2424.mg/category/actualite/politique/feed/",
    "https://2424.mg/category/actualite/economie/feed/",
    "https://www.lemonde.fr/madagascar/rss_full.xml",
    "https://www.courrierinternational.com/feed/rubrique/madagascar/rss.xml",
    "https://namana-studio.fr/feed/",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UCK84qSI2bEMWkX9vUptkAlA"
]

def generate_article_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def parse_date(entry) -> datetime:
    """
    Retourne un objet datetime UTC propre pour MongoDB.
    Priorité : published → updated → fallback UTC now.
    """

    date_fields = ["published", "updated", "created"]

    for field in date_fields:
        if hasattr(entry, field):
            value = getattr(entry, field)
            if value:
                try:
                    dt = parser.parse(value)

                    # Si pas de timezone → forcer UTC
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)

                    return dt
                except Exception:
                    continue

    # Fallback propre en UTC
    return datetime.now(timezone.utc)

def fetch_feed_content(feed_url: str) -> str | None:
    for _ in range(MAX_RETRIES):
        try:
            response = requests.get(feed_url, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            return response.content
        except requests.RequestException:
            continue
    return None

def clean_summary(summary: str) -> str:
    if not summary:
        return ""
    soup = BeautifulSoup(summary, "html.parser")
    text = soup.get_text(separator=" ")
    text = unicodedata.normalize("NFKC", text)
    return " ".join(text.split())

def fetch_rss_articles():
    articles = []
    for feed_url in RSS_FEEDS:
        content = fetch_feed_content(feed_url)
        if not content:
            continue

        parsed_feed = feedparser.parse(content)
        if parsed_feed.bozo:
            continue

        for entry in parsed_feed.entries:
            if not hasattr(entry, "link") or not hasattr(entry, "title"):
                continue

            url = entry.link.strip()
            summary = clean_summary(entry.summary) if hasattr(entry, "summary") else ""

            date_pub = parse_date(entry)
            created_at = datetime.now(timezone.utc)

            # Si date_publication est vide, fallback sur created_at
            if not date_pub:
                date_pub = created_at

            article = {
                "id_article": generate_article_id(url),
                "source": feed_url,
                "source_type": "rss",
                "titre": entry.title.strip(),
                "date_publication": date_pub,
                "contenu": summary,
                "url": url,
                "created_at": created_at,
            }
            articles.append(article)
    return articles
