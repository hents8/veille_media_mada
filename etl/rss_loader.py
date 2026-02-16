import feedparser
import datetime
import hashlib
from dateutil import parser
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
    "https://www.madagate.org/index.php?format=feed&type=rss",
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
    "https://news.google.com/rss/search?q=Madagascar&hl=fr&gl=FR&ceid=FR:fr",
    "https://tanikomadagascar.com/feed/",
    "https://namana-studio.fr/feed/",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UCK84qSI2bEMWkX9vUptkAlA"  
]

def generate_article_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def parse_date(entry) -> str:
    if hasattr(entry, "published"):
        try:
            return parser.parse(entry.published).isoformat()
        except Exception:
            pass
    return datetime.datetime.utcnow().isoformat()

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

            article = {
                "id_article": generate_article_id(url),
                "source": feed_url,
                "source_type": "rss",
                "titre": entry.title.strip(),
                "date_publication": parse_date(entry),
                "contenu": summary,
                "url": url,
                "created_at": datetime.datetime.utcnow().isoformat(),
            }
            articles.append(article)
    return articles
