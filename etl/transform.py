import re
import unicodedata
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory
from textblob import Blobber
from textblob_fr import PatternTagger, PatternAnalyzer
from sentence_transformers import SentenceTransformer, util
import torch
from collections import Counter

DetectorFactory.seed = 0
tb_fr = Blobber(pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())

embedding_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

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
    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^\w\s]", " ", text)
    return " ".join(text.split())

malagasy_words_set = set(w for w in malagasy_words if " " not in w)
malagasy_expr = [normalize_text(expr) for expr in malagasy_words if " " in expr]

_lang_cache = {}
    
def detect_language(text: str, min_mg_words: int = 3) -> str:
    if not text:
        return "unknown"
    if text in _lang_cache:
        return _lang_cache[text]

    clean = normalize_text(text)
    words = clean.split()
    if len(words) < 4:
        _lang_cache[text] = "unknown"
        return "unknown"

    # mots simples
    mg_count = sum(1 for w in words if w in malagasy_words_set)
    # expressions multi-mots
    for expr in malagasy_expr:
        if expr in clean:
            mg_count += 1

    if mg_count >= min_mg_words or mg_count / len(words) > 0.25:
        _lang_cache[text] = "mg"
        return "mg"

    try:
        lang = detect(text)
        _lang_cache[text] = lang if lang in ["fr", "mg"] else "other"
        return _lang_cache[text]
    except:
        _lang_cache[text] = "unknown"
        return "unknown"
        
_sentiment_cache = {}

def analyze_sentiment_score(text: str) -> float:
    if not text:
        return 0.0

    if text in _sentiment_cache:
        return _sentiment_cache[text]

    lang = detect_language(text)

    if lang == "fr":
        score = float(tb_fr(text).sentiment[0])
    else:
        score = 0.0

    _sentiment_cache[text] = score
    return score

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
        "manifestation", "coup d etat", "accord politique",
        "assises des partis politiques",
        "dissolution des institutions",
        "haute autorité",
        "transition",
        "concertation nationale"

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

CANDIDATE_LABELS = list(normalized_keywords_map.keys())

_category_cache = {}

def categorize_text(text: str, use_zero_shot=True):
    """
    Catégorise un texte en utilisant d'abord le scoring par mots-clés,
    puis, si nécessaire, un zero-shot classifier pour améliorer la précision.
    """
    if not text or not text.strip():
        return ["autre"]
    
    if text in _category_cache:
        return _category_cache[text]

    clean = normalize_text(text)
    words = clean.split()
    
    if len(words) < 8:
        _category_cache[text] = ["autre"]
        return ["autre"]

    words_counter = Counter(words)
    words_set = set(words)

    # 1️⃣ Keyword scoring rapide
    keyword_scores = {}
    for cat, kws in normalized_keywords_map.items():
        hits = sum(1 for kw in kws if kw in words_set)
        count = sum(words_counter.get(kw, 0) for kw in kws)
        if count > 0:
            density = hits / len(words)
            keyword_scores[cat] = round(count + density * 5, 2)

    # Classement initial
    sorted_kw = sorted(keyword_scores.items(), key=lambda x: x[1], reverse=True)
    best_kw_score = sorted_kw[0][1] if sorted_kw else 0

    # Si score keyword élevé, on se fie aux mots-clés (rapide)
    if best_kw_score >= 3:
        categories = [cat for cat, score in sorted_kw if score >= best_kw_score * 0.7]
        _category_cache[text] = categories[:2]
        return _category_cache[text]

    # 2️⃣ Zero-shot classifier si faible score ou texte > 50 mots
    transformer_scores = {}
    if use_zero_shot and len(words) > 50:
        try:
            result = zero_shot_classifier(text[:1000], CANDIDATE_LABELS, multi_label=True)
            transformer_scores = dict(zip(result["labels"], result["scores"]))
        except Exception as e:
            transformer_scores = {}

    # 3️⃣ Fusion des scores keywords + zero-shot
    final_scores = {}
    for cat in CANDIDATE_LABELS:
        kw_score = keyword_scores.get(cat, 0)
        tr_score = transformer_scores.get(cat, 0)
        # Pondération : keywords rapides + 0.6, zero-shot + 4 pour booster
        score = (kw_score * 0.6) + (tr_score * 4)
        if score > 0:
            final_scores[cat] = round(score, 3)

    if not final_scores:
        _category_cache[text] = ["autre"]
        return ["autre"]

    sorted_scores = sorted(final_scores.items(), key=lambda x: x[1], reverse=True)
    best_score = sorted_scores[0][1]
    categories = [cat for cat, score in sorted_scores if score >= best_score * 0.7]

    _category_cache[text] = categories[:2] if categories else ["autre"]
    return _category_cache[text]
