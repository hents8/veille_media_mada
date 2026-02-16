from pymongo import MongoClient

def connect_mongo(uri, db_name="veille_media"):
    client = MongoClient(uri)
    return client[db_name]

def insert_article(collection, article):
    if collection.count_documents({"id_article": article["id_article"]}, limit=1) == 0:
        collection.insert_one(article)
        return True
    return False
