import re
import unicodedata
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory
from textblob import Blobber
from textblob_fr import PatternTagger, PatternAnalyzer

DetectorFactory.seed = 0

tb_fr = Blobber(pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())

malagasy_words = {
    # ===== PRONOMS & MOTS COURANTS =====
    "izy", "anao", "aho", "isika", "ianao", "izy ireo", "izany", "ity", "ireo",
    "eto", "any", "amin", "amin'ny", "ao", "ary", "na", "fa", "raha", "satria",

    # ===== SOCIETE / ADMINISTRATION =====
    "firenena", "tanora", "mponina", "sekoly", "mpianatra",
    "governemanta", "ministera", "ben'ny tanàna", "ben ny tanana",
    "depiote", "depute", "filoha", "antoko politika",
    "lalàm-panorenana", "lalampanorenana",
    "repoblika", "kaominina", "prefe", "distrika",
    "mpitondra", "fitondrana", "biraom-panjakana",

    # ===== VIE QUOTIDIENNE =====
    "trano", "làlana", "lalana", "fianakaviana",
    "asa", "vidim-piainana", "vidimpiainana",
    "fampandrosoana", "fiainana", "vahoaka",
    "zaza", "reny", "ray",

    # ===== ECONOMIE / COMMERCE =====
    "toekarena", "varotra", "orinasa", "tsena",
    "banky", "fampiasam-bola", "fampiasambola",
    "hetra", "karama", "vola", "tetibola",
    "fividianana", "fanondranana", "fanondranana entana",

    # ===== SANTE =====
    "fahasalamana", "hopitaly", "hôpitaly",
    "dokoterà", "dokotera", "aretina",
    "vaksiny", "fanafody", "marary",
    "valanaretina", "hopital",

    # ===== EDUCATION =====
    "fanabeazana", "oniversite", "universite",
    "mpampianatra", "kilasy", "sekoly ambony",
    "mpianatra", "fampianarana",

    # ===== SPORT =====
    "baolina kitra", "lalao", "ekipa",
    "fifaninanana", "mpilalao", "stadiona",

    # ===== TECHNOLOGIE =====
    "haitao", "siansa", "fikajiana",
    "rindranasa", "aterineto", "tambajotra",
    "finday", "solosaina",

    # ===== ENVIRONNEMENT =====
    "tontolo iainana", "toetrandro",
    "fandotoana", "ala", "rano",
    "rivodoza", "hain-tany",

    # ===== INTERNATIONAL =====
    "iraisam-pirenena", "ady",
    "fifandraisana iraisam-pirenena",
    "firaisankina", "diplomasia",

    # ===== JUSTICE =====
    "heloka", "fitsarana",
    "mpisolovava", "polisy",
    "fonja", "lalàna", "lalana",

    # ===== TRANSPORT =====
    "fitaterana", "seranam-piaramanidina",
    "taxi-be", "taxibe",
    "bus", "fiara", "sambo",

    # ===== MEDIAS / ACTUALITE =====
    "vaovao", "gazety", "fampitam-baovao",
    "mpanao gazety", "tatitra",
    "fanambarana", "lahateny"
}

def clean_text(html_text: str) -> str:
    """Nettoie le HTML pour ne garder que le texte brut."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator=" ")
    text = unicodedata.normalize("NFKC", text)
    return " ".join(text.split())

def normalize_text(text: str) -> str:
    if not text:
        return ""

    soup = BeautifulSoup(text, "html.parser")
    text = soup.get_text(separator=" ")

    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")

    text = re.sub(r"[^\w\s]", " ", text)
    text = " ".join(text.split())

    return text
    
def detect_language(text: str, min_mg_words: int = 3) -> str:
    try:
        if not text:
            return "unknown"

        clean = normalize_text(text)
        words = clean.split()

        # 🔹 Texte trop court = détection instable
        if len(words) < 4:
            return "unknown"

        mg_count = 0

        # 🔹 1) Matching mots simples
        for w in words:
            if w in malagasy_words:
                mg_count += 1

        # 🔹 2) Matching expressions multi-mots
        for expr in malagasy_words:
            if " " in expr:
                expr_clean = normalize_text(expr)
                if expr_clean in clean:
                    mg_count += 1

        mg_ratio = mg_count / len(words)

        # 🔹 Override si signal fort MG
        if mg_count >= min_mg_words or mg_ratio > 0.25:
            return "mg"

        # 🔹 Sinon fallback langdetect
        lang = detect(text)

        if lang in ["fr", "mg"]:
            return lang

        return "other"

    except Exception:
        return "unknown"

def analyze_sentiment_score(text: str) -> float:
    if not text:
        return 0.0

    lang = detect_language(text)

    if lang == "fr":
        score = tb_fr(text).sentiment[0]
        return float(score)

    # MG ou autre -> neutre (pas de modèle fiable)
    return 0.0


def analyze_sentiment(text: str) -> str:
    score = analyze_sentiment_score(text)

    if score > 0.1:
        return "positif"
    elif score < -0.1:
        return "negatif"
    return "neutre"

keywords_map = {

    "politique": [
        # FR fort
        "président", "premier ministre", "gouvernement", "député",
        "sénat", "assemblée nationale", "élection", "campagne",
        "parti", "opposition", "majorité", "constitution",
        "république", "mandat", "candidat", "ministre",
        "manifestation", "coup d etat",

        # MG fort
        "filoha", "governemanta", "ministera", "depiote",
        "fifidianana", "antoko politika", "lalampanorenana",
        "repoblika"
    ],

    "justice": [
        "tribunal", "juge", "procureur", "cour",
        "justice", "avocat", "condamnation", "détention",
        "prison", "garde a vue", "incarcération",
        "crime", "enquête", "police", "gendarmerie",

        "fitsarana", "mpisolovava", "heloka",
        "fanadihadiana", "polisy"
    ],

    "économie": [
        "économie", "inflation", "croissance",
        "budget", "finance", "banque", "investissement",
        "marché", "entreprise", "industrie", "exportation",

        "toekarena", "varotra", "orinasa",
        "tsena", "fampiasambola"
    ],

    "société": [
        "population", "citoyen", "communauté",
        "social", "pauvreté", "emploi",
        "sécurité", "migration",

        "mponina", "fiarahamonina",
        "tanora", "fianakaviana"
    ],

    "santé": [
        "hôpital", "médecin", "maladie",
        "vaccin", "urgence", "épidémie",
        "santé publique",

        "hopitaly", "dokoterà",
        "aretina", "fahasalamana"
    ],

    "éducation": [
        "école", "université", "enseignement",
        "étudiant", "professeur", "examen",
        "réforme scolaire",

        "sekoly", "oniversite",
        "fanabeazana", "mpianatra"
    ],

    "technologie": [
        "technologie", "innovation",
        "numérique", "internet",
        "intelligence artificielle",
        "cybersécurité",

        "haitao", "rindranasa"
    ],

    "sport": [
        "football", "rugby", "basket",
        "match", "championnat",
        "équipe", "tournoi",

        "baolina", "lalao",
        "ekipa", "fifaninanana"
    ],

    "culture": [
        # seulement mots très spécifiques
        "festival", "concert",
        "exposition", "cinéma",
        "théâtre", "album",
        "mozika", "kolontsaina"
    ],

    "environnement": [
        "climat", "écologie",
        "déforestation", "pollution",
        "biodiversité",

        "tontolo", "toetrandro",
        "fandotoana"
    ],

    "international": [
        "onu", "union européenne",
        "relations internationales",
        "conflit international",
        "diplomatie",

        "iraisampirenena",
        "ady iraisampirenena"
    ],

    "transport": [
        "route", "aéroport",
        "transport public",
        "trafic", "infrastructure",

        "fitaterana",
        "seranampiaramanidina"
    ]
}

normalized_keywords_map = {
    cat: [normalize_text(kw) for kw in kws]
    for cat, kws in keywords_map.items()
}

def categorize_text(text: str, min_score: int = 1):
    if not text or not text.strip():
        return ["autre"]

    clean = normalize_text(text)
    words = clean.split()
    scores = {}

    for cat, keywords in normalized_keywords_map.items():
        score = 0

        for kw in keywords:
            # Pondération plus forte pour expression exacte
            if " " in kw:
                if kw in clean:
                    score += 3
            else:
                if kw in words:
                    score += 1

        if score > 0:
            scores[cat] = score

    # Si aucune catégorie détectée
    if not scores:
        return ["autre"]

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    max_score = sorted_scores[0][1]

    # Si score trop faible
    if max_score < min_score:
        return ["autre"]

    # Garde les catégories proches du meilleur score
    categories = [
        cat for cat, score in sorted_scores
        if score >= max_score * 0.6
    ]

    return categories if categories else ["autre"]
