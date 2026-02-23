from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timezone
import hashlib
import time
import logging
import os
os.environ["WDM_SSL_VERIFY"] = "0"

from etl.transform import clean_text, analyze_sentiment, categorize_text

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def generate_article_id(title: str, date: str) -> str:
    """ID unique basé sur le titre et la date."""
    key = f"{title}-{date}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def parse_date_now() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def extract_article_data(block) -> dict | None:
    """Extrait les informations d’un bloc d’article."""
    try:
        titre_elem = block.find_element(By.CSS_SELECTOR, "strong, .titled")
        titre = titre_elem.text.strip()

        try:
            date_elem = block.find_element(By.CSS_SELECTOR, "em, .italique")
            date_pub = date_elem.text.strip()
        except:
            date_pub = parse_date_now()

        contenu_raw = block.text.strip()
        contenu = contenu_raw.replace(titre, "").replace(date_pub, "").strip()
        contenu_clean = clean_text(contenu)

        article_id = generate_article_id(titre, date_pub)

        return {
            "id_article": article_id,
            "source": "Orange Actu",
            "source_type": "scrap_selenium",
            "titre": titre,
            "contenu": contenu_clean,
            "date_publication": date_pub,
            "sentiment": analyze_sentiment(contenu_clean),
            "categorie": categorize_text(contenu_clean),
            "created_at": parse_date_now()
        }
    except Exception as e:
        logging.warning(f"Erreur extraction article: {e}")
        return None

def scrape_orange_actu(max_pages=5) -> list[dict]:
    """Scrape les dépêches Orange Actu en naviguant correctement les pages via JS."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 15)

    articles_list = []
    articles_seen = set()  # pour éviter les doublons
    base_url = "https://actu.orange.mg/depeches/"

    logging.info(f"Démarrage du scraping jusqu'à {max_pages} pages...")
    driver.get(base_url)
    time.sleep(2)  # laisser JS charger la première page

    for page_num in range(1, max_pages + 1):
        logging.info(f"Scraping page {page_num}...")

        # attendre que les articles soient présents
        try:
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.oma-depeches-widget")))
        except:
            logging.warning("Aucun article trouvé sur cette page")
            continue

        blocks = driver.find_elements(By.CSS_SELECTOR, "div.oma-depeches-widget")
        for b in blocks:
            data = extract_article_data(b)
            if data and data["id_article"] not in articles_seen:
                data["url"] = base_url + f"#page-{page_num}"
                articles_list.append(data)
                articles_seen.add(data["id_article"])
                logging.info(f"✅ [ORANGE] Article ajouté : - {data['titre']}")
            elif data:
                logging.warning(f"⚠️ [ORANGE] Article déjà présent : - {data['titre']}")

        # Cliquer sur le lien de la page suivante
        try:
            next_page_selector = f"a.page-link[href='#page-{page_num+1}']"
            next_link = driver.find_element(By.CSS_SELECTOR, next_page_selector)
            next_link.click()
            time.sleep(1.5)  # laisser JS charger
        except:
            logging.info("Plus de pages disponibles ou impossible de cliquer sur la page suivante.")
            break

    driver.quit()
    logging.info(f"Scraping terminé. {len(articles_list)} articles uniques récupérés.")
    return articles_list
