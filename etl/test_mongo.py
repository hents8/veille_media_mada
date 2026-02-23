import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv

# 📌 Déterminer la racine du projet (2 niveaux au-dessus si main est dans /scripts par ex)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 📌 Charger le .env depuis la racine
load_dotenv(os.path.join(BASE_DIR, ".env"))

# 📌 Ajouter la racine au PYTHONPATH
sys.path.append(BASE_DIR)

# 📌 Récupérer URI
MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    print("❌ MONGO_URI non trouvé dans le .env")
else:
    try:
        # Timeout court pour éviter attente longue
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

        # Test ping serveur
        client.admin.command("ping")

        print("✅ Connexion MongoDB réussie !")

        # Afficher les bases disponibles
        print("📂 Bases disponibles :", client.list_database_names())

    except Exception as e:
        print("❌ Erreur connexion :", e)
