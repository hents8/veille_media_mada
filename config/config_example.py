# Copier ce fichier et le renommer en config.py
MONGO_URI = "mongodb://localhost:27017/"
RSS_FEEDS = [
    "https://www.lexpress.mg/feed/",
    "https://www.madagascar-tribune.com/rss.xml"
]
SCRAP_SITES = [
    {
        "name": "Orange Actu",
        "url": "https://actu.orange.mg/depeches/",
        "article_selector": "article a",
        "content_selector": "div.content-article",
        "date_selector": "time"
    }
]
