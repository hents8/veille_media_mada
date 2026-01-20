import feedparser
from dateutil import parser
from config.config import RSS_FEEDS
import datetime

def fetch_rss_articles():
    """
    Parcourt les flux RSS et renvoie une liste de dictionnaires
    avec les champs de base : titre, lien, date, résumé
    """
    articles = []
    for feed_url in RSS_FEEDS:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            article_id = entry.id if 'id' in entry else entry.link
            title = entry.title
            url = entry.link
            date_pub = parser.parse(entry.published) if 'published' in entry else datetime.datetime.now().isoformat()
            summary = entry.summary if 'summary' in entry else ""

            articles.append({
                "id_article": article_id,
                "source": feed_url,
                "titre": title,
                "date_publication": date_pub,
                "contenu": summary,
                "url": url
            })
    return articles
