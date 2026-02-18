from textblob import TextBlob
from bs4 import BeautifulSoup
import unicodedata

def clean_text(html_text: str) -> str:
    """Nettoie le HTML pour ne garder que le texte brut."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator=" ")
    text = unicodedata.normalize("NFKC", text)
    return " ".join(text.split())


def analyze_sentiment(text: str) -> str:
    """Analyse le sentiment du texte : positif, neutre, négatif."""
    if not text:
        return "neutre"
    
    score = TextBlob(text).sentiment.polarity
    if score > 0.1:
        return "positif"
    elif score < -0.1:
        return "negatif"
    else:
        return "neutre"
        

def analyze_sentiment_score(text: str) -> float:
    """Retourne le score numérique de sentiment entre -1 et +1"""
    if not text:
        return 0.0
    return TextBlob(text).sentiment.polarity


def categorize_text(text: str) -> str:
    """Catégorisation étendue par mots-clés pour articles de journaux."""
    if not text:
        return "autre"

    keywords_map = {
        "politique": [
            "manifestation", "coup d'État", "président", "gouvernement",
            "élection", "parlement", "ministre", "vote", "politique"
        ],
        "économie": [
            "économie", "exportation", "industrie", "commerce",
            "finance", "banque", "bourse", "investissement", "entreprise", "marché"
        ],
        "société": [
            "social", "communauté", "population", "culture", "démographie", "association"
        ],
        "santé": [
            "santé", "médecin", "hôpital", "vaccin", "pandémie", "maladie", "traitement"
        ],
        "éducation": [
            "éducation", "école", "université", "enseignement", "formation", "étudiant"
        ],
        "technologie": [
            "technologie", "innovation", "science", "robotique", "informatique",
            "internet", "intelligence artificielle", "IA", "recherche"
        ],
        "sport": [
            "football", "rugby", "match", "sport", "athlète", "compétition", "championnat"
        ],
        "culture": [
            "art", "cinéma", "théâtre", "musique", "livre", "festival", "exposition"
        ],
        "environnement": [
            "environnement", "climat", "écologie", "pollution", "déforestation",
            "biodiversité", "changement climatique"
        ],
        "international": [
            "international", "ONU", "guerre", "conflit", "diplomatie",
            "pays", "monde", "relations internationales"
        ],
        "justice": [
            "crime", "justice", "tribunal", "avocat", "police", "procès", "jugement"
        ],
        "transport": [
            "transport", "route", "train", "aéroport", "trafic", "infrastructure", "mobilité"
        ]
    }

    text_lower = text.lower()
    for category, keywords in keywords_map.items():
        if any(k.lower() in text_lower for k in keywords):
            return category

    return "autre"
