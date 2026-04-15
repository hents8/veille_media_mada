import feedparser
import requests
from datetime import datetime, timezone
import hashlib
import re
from bs4 import BeautifulSoup

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
    # "https://www.lemonde.fr/madagascar/rss_full.xml",  # SSL bug → commente
]

def fetch_feed_content(url, timeout=30):
    """FIX BOM + MIDI MADA"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout, verify=False)
        resp.raise_for_status()
        content = resp.text
        
        # 🔥 SUPPRIME BOM UTF-8
        content = content.lstrip('\ufeff\xef\xbb\xbf\r\n\t ')
        
        # 🔥 SUPPRIME TOUT AVANT <?xml (MIDI MADA)
        if 'midi-madagasikara' in url.lower():
            content = re.sub(r'^.*?(\<\?xml)', r'\1', content, flags=re.DOTALL | re.IGNORECASE)
            print(f"   🎯 MIDI MADA CLEAN: {len(content)} chars")
        else:
            print(f"   📏 {len(content)} chars")
        
        return content
    except Exception as e:
        print(f"❌ HTTP {url}: {e}")
        return None

def parse_date(entry):
    """Parse date RSS"""
    for date_field in ['published', 'updated', 'pubDate', 'created']:
        if hasattr(entry, date_field):
            try:
                return feedparser._getTimeStruct(getattr(entry, date_field))
            except:
                pass
    return datetime.now(timezone.utc)

def clean_summary(summary):
    """Nettoie résumé RSS"""
    if not summary:
        return ""
    soup = BeautifulSoup(str(summary), 'html.parser')
    text = soup.get_text()
    paras = [p.strip() for p in text.split('\n') if p.strip()]
    result = paras[0][:500] + "..." if paras else text[:500]
    return re.sub(r'\s+', ' ', result).strip()

def generate_article_id(url):
    return hashlib.md5(url.encode()).hexdigest()

def fetch_rss_articles():
    """Pipeline RSS principal"""
    articles = []
    print("🌐 Test des 15 RSS feeds...")
    
    for i, feed_url in enumerate(RSS_FEEDS, 1):
        print(f"\n{i}. {feed_url}")
        content = fetch_feed_content(feed_url)
        if not content:
            continue

        feed = feedparser.parse(content)
        print(f"   📊 Bozo={feed.bozo}, Entries={len(feed.entries)}")
        
        if feed.bozo or not feed.entries:
            print(f"   ❌ Skip: {feed.bozo_exception if feed.bozo else 'No entries'}")
            continue

        count_ok = 0
        for entry in feed.entries[:15]:  # 15 max/feed
            try:
                link = getattr(entry, 'link', getattr(entry, 'id', None))
                if not link: continue
                
                title = getattr(entry, 'title', 'NO TITLE').strip()
                if len(title) < 5: continue

                summary = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
                content_clean = clean_summary(summary)
                if len(content_clean) < 20: 
                    print(f"     ⚠️ Skip court: {len(content_clean)} chars")
                    continue

                article = {
                    "id_article": generate_article_id(link),
                    "source": feed_url,
                    "titre": title,
                    "date_publication": parse_date(entry),
                    "contenu": content_clean,
                    "url": link.strip(),
                    "created_at": datetime.now(timezone.utc),
                    "source_type": "RSS",
                }
                
                articles.append(article)
                count_ok += 1
                print(f"     ✅ '{title[:60]}...'\n       📝 {content_clean[:60]}...\n       🔗 {link[:60]}...")
                print()  # Ligne vide

            except Exception as e:
                print(f"     ❌ Erreur: {e}")
                continue

        print(f"   ➕ {count_ok} articles OK")
    
    print(f"\n🏁 TOTAL: {len(articles)} articles RSS")
    return articles