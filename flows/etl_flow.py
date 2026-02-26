from prefect import flow, task
from pymongo import MongoClient
from config.settings import MONGO_URI
from etl.main import process_articles
from etl.rss_loader import fetch_rss_articles
from etl.scraper_loader import scrape_site
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if os.getenv("CI") == "true":
    from etl.orange_loader_ci import scrape_orange_actu
else:
    from etl.selenium_loader import scrape_orange_actu


def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client["veille_media"]
    return db["articles"]


@task
def rss_task():
    collection = get_mongo_collection()
    articles = fetch_rss_articles()
    return process_articles(collection, articles, "RSS")


@task
def scrap_task():
    collection = get_mongo_collection()
    articles = scrape_site()
    return process_articles(collection, articles, "SCRAP")


@task
def orange_task():
    collection = get_mongo_collection()
    articles = scrape_orange_actu(max_pages=3)
    return process_articles(collection, articles, "ORANGE")


@flow(name="Articles ETL Flow")
def articles_etl_flow():
    rss_result = rss_task()
    scrap_result = scrap_task()
    orange_result = orange_task()

    print("Pipeline terminé !")
    print(rss_result, scrap_result, orange_result)


if __name__ == "__main__":
    articles_etl_flow()