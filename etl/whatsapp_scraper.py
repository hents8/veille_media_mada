from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timezone
import hashlib
import time
import logging
import os
import json

os.environ["WDM_SSL_VERIFY"] = "0"

from transform import clean_text, analyze_sentiment, categorize_text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def generate_article_id(title: str, date: str) -> str:
    """ID unique basé sur le titre et la date."""
    key = f"{title}-{date}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def parse_date_now() -> str:
    """Retourne la date/heure actuelle en UTC au format ISO."""
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def extract_whatsapp_message(msg_elem) -> dict | None:
    """
    Extrait un message depuis un bloc de message WhatsApp.
    TODO: affiner les sélecteurs une fois l’UI stabilisée.
    """
    try:
        # Texte principal
        try:
            text_elem = msg_elem.find_element(
                By.CSS_SELECTOR, "span[data-testid='selectable-text']"
            )
            full_text = text_elem.text.strip()
        except Exception:
            return None

        if not full_text or len(full_text) < 5:
            return None

        # Meta info (date / auteur) – best effort
        date_pub = parse_date_now()
        try:
            meta_elem = msg_elem.find_element(
                By.CSS_SELECTOR,
                "div.copyable-text[data-pre-plain-text]"
            )
            meta = meta_elem.get_attribute("data-pre-plain-text") or ""
            if meta:
                date_pub = meta
        except Exception:
            pass

        # Titre = première ligne, contenu = reste
        lines = full_text.split("\n")
        titre = lines[0].strip()
        contenu = "\n".join(lines[1:]).strip() or full_text

        contenu_clean = clean_text(contenu)
        article_id = generate_article_id(titre, date_pub)

        return {
            "id_article": article_id,
            "source": "Orange Actu WhatsApp",
            "source_type": "whatsapp_channel_selenium",
            "titre": titre[:200],
            "contenu": contenu_clean,
            "date_publication": date_pub,
            "sentiment": analyze_sentiment(contenu_clean),
            "categorie": categorize_text(contenu_clean),
            "created_at": parse_date_now(),
        }
    except Exception as e:
        logging.debug(f"Skip message: {e}")
        return None


def scrape_whatsapp_channel(
    channel_name: str = "Orange actu Madagascar",
    max_articles: int = 30
) -> list[dict]:
    """
    Scrape une chaîne WhatsApp (onglet Chaînes).
    TODO: stabiliser les sélecteurs de l’onglet Chaînes et de la recherche.
    """

    options = Options()

    # Pour debug: laisser visible
    # options.add_argument("--headless=new")

    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--user-data-dir=./whatsapp_session")
    options.add_argument("--disable-web-security")

    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--log-level=3")

    service = Service(ChromeDriverManager().install())

    driver = None
    try:
        driver = webdriver.Chrome(service=service, options=options)
        wait = WebDriverWait(driver, 30)

        articles_list: list[dict] = []
        articles_seen: set[str] = set()

        logging.info("🚀 Ouverture WhatsApp Web...")
        driver.get("https://web.whatsapp.com")

        logging.info("⏳ Attendre login (scan QR si première fois)...")
        time.sleep(10)

        # Vérifier qu’on est bien connecté
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[@contenteditable='true']")
            )
        )
        logging.info("✅ WhatsApp connecté")

        # --- OUVERTURE ONGLET CHAÎNES ---
        # NOTE: on garde ça simple pour l’instant, à ajuster plus tard si besoin.
        logging.info("📺 Tentative d’ouverture de l’onglet Chaînes...")
        try:
            channels_button = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[@aria-label='Chaînes']")
                )
            )
            channels_button.click()
            time.sleep(3)
        except Exception as e:
            logging.warning(
                f"Onglet Chaînes non cliquable (à ajuster plus tard) : {e}"
            )

        # --- RECHERCHE DE LA CHAÎNE ---
        logging.info(f"🔍 Tentative de recherche de la chaîne: {channel_name}")
        try:
            search_candidates = driver.find_elements(
                By.XPATH, "//div[@contenteditable='true']"
            )
            logging.info(
                f"{len(search_candidates)} champs contenteditable détectés"
            )

            channel_search = None
            for el in search_candidates:
                try:
                    size = el.size
                    if size["width"] > 0 and size["height"] > 0:
                        channel_search = el
                        break
                except Exception:
                    continue

            if channel_search:
                channel_search.clear()
                channel_search.send_keys(channel_name)
                time.sleep(3)
            else:
                logging.warning(
                    "Aucun champ de recherche interactif trouvé (à revoir plus tard)."
                )
        except Exception as e:
            logging.warning(
                f"Erreur pendant la recherche de la chaîne (à revoir plus tard) : {e}"
            )

        # --- OUVERTURE DE LA CHAÎNE ---
        # TODO: on pourra raffiner ce XPATH plus tard
        try:
            channel_card = wait.until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        f"//span[contains(., '{channel_name}')]"
                        f" | //div[contains(., '{channel_name}')]",
                    )
                )
            )
            channel_card.click()
            logging.info("✅ Chaîne ouverte")
            time.sleep(5)
        except Exception as e:
            logging.warning(
                f"Impossible de cliquer sur la chaîne (à ajuster plus tard) : {e}"
            )

        # --- SCROLL ET EXTRACTION ---
        logging.info("📜 Début du chargement des messages...")
        try:
            body = driver.find_element(By.TAG_NAME, "body")
        except Exception as e:
            logging.error(f"Impossible de trouver <body> : {e}")
            return []

        scroll_count = 0
        while len(articles_list) < max_articles and scroll_count < 50:
            body.send_keys(Keys.PAGE_UP)
            time.sleep(1.2)
            scroll_count += 1

            msg_selectors = [
                "div._anz0.message-in",
                "div[role='row']",
            ]

            msgs = []
            for selector in msg_selectors:
                found = driver.find_elements(By.CSS_SELECTOR, selector)
                if found:
                    msgs = found
                    break

            if not msgs:
                continue

            for msg in msgs[-15:]:
                data = extract_whatsapp_message(msg)
                if data and data["id_article"] not in articles_seen:
                    articles_list.append(data)
                    articles_seen.add(data["id_article"])
                    logging.info(
                        f"✅ [{len(articles_list)}] {data['titre'][:60]}..."
                    )

        logging.info(f"🎉 Terminé: {len(articles_list)} articles WhatsApp")
        return articles_list

    except Exception as e:
        logging.error(f"❌ Erreur: {e}")
        return []
    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    logging.info("=== WHATSAPP SCRAPER ===")
    articles = scrape_whatsapp_channel("Orange actu Madagascar", max_articles=20)

    if articles:
        filename = (
            f"orange_whatsapp_articles_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        )
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)
        logging.info(f"💾 Sauvegardé: {filename} ({len(articles)} articles)")
    else:
        logging.warning(
            "Aucun article pour l’instant – on ajustera les sélecteurs plus tard."
        )
