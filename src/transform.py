from textblob import TextBlob
from bs4 import BeautifulSoup

def clean_text(html_text):
    """
    Nettoie le HTML pour ne garder que le texte brut.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text()
    return " ".join(text.split())  # supprime espaces multiples

def analyze_sentiment(text):
    """
    Analyse le sentiment du texte : positif, neutre, négatif.
    """
    score = TextBlob(text).sentiment.polarity
    if score > 0.1:
        return "positif"
    elif score < -0.1:
        return "negatif"
    else:
        return "neutre"

def categorize_text(text):
    """
    Catégorisation simple par mots-clés.
    """
    keywords_politique = ["manifestation", "coup d'État", "président", "gouvernement"]
    keywords_economie = ["économie", "exportation", "industrie", "commerce"]
    text_lower = text.lower()
    
    if any(k.lower() in text_lower for k in keywords_politique):
        return "politique"
    elif any(k.lower() in text_lower for k in keywords_economie):
        return "économie"
    else:
        return "autre"
