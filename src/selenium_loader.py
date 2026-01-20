from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import time

from transform import clean_text, analyze_sentiment, categorize_text


def scrape_orange_actu(collection, max_pages=10):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    # ✅ webdriver-manager auto-download
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 20)

    total_added = 0
    total_existing = 0

    for page in range(1, max_pages + 1):
        url = "https://actu.orange.mg/depeches/"
        if page > 1:
            url += f"?page={page}"

        print(f"\n📄 [ORANGE ACTU] Page {page} → {url}")
        driver.get(url)

        try:
            wait.until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "a[href*='/depeches/']")
                )
            )
        except:
            print("⛔ Aucun article détecté → arrêt pagination")
            break

        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/depeches/']")
        print(f"🔍 Articles détectés : {len(links)}")

        page_added = 0

        for a in links:
            try:
                titre = a.text.strip()
                article_url = a.get_attribute("href")

                if not titre or not article_url:
                    continue

                if collection.count_documents({"id_article": article_url}, limit=1):
                    total_existing += 1
                    continue

                driver.execute_script("window.open(arguments[0]);", article_url)
                driver.switch_to.window(driver.window_handles[1])
                time.sleep(2)

                try:
                    contenu = driver.find_element(
                        By.CSS_SELECTOR, "div.content-article"
                    ).text
                except:
                    contenu = ""

                contenu = clean_text(contenu)

                doc = {
                    "id_article": article_url,
                    "source": "Orange Actu",
                    "source_type": "scrap_selenium",
                    "titre": titre,
                    "contenu": contenu,
                    "url": article_url,
                    "date_publication": datetime.utcnow().isoformat(),
                    "sentiment": analyze_sentiment(contenu),
                    "categorie": categorize_text(contenu),
                }

                collection.insert_one(doc)
                total_added += 1
                page_added += 1

                print(f"✅ Ajouté : {titre}")

                driver.close()
                driver.switch_to.window(driver.window_handles[0])

            except Exception as e:
                print(f"❌ Erreur article : {e}")

        if page_added == 0:
            print("🛑 Aucun nouvel article → arrêt")
            break

    driver.quit()

    print("\n📊 RÉSULTAT ORANGE ACTU")
    print(f"Articles ajoutés : {total_added}")
    print(f"Articles déjà présents : {total_existing}")
