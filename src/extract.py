import feedparser
from bs4 import BeautifulSoup

def fetch_rss(feed_url):
    return feedparser.parse(feed_url).entries

def clean_text(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text()
    return " ".join(text.split())
