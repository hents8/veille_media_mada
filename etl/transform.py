import re
import unicodedata
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from textblob import Blobber
from textblob_fr import PatternTagger, PatternAnalyzer
from sentence_transformers import SentenceTransformer, util
import torch
from collections import Counter
from transformers import pipeline

DetectorFactory.seed = 0
tb_fr = Blobber(pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())

embedding_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

SIMILARITY_THRESHOLD = 0.30

fr_sentiment_model = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-xlm-roberta-base-sentiment"
)

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

malagasy_words_set = set(malagasy_words)
_lang_cache = {}
    
def detect_language(text: str, min_mg_ratio: float = 0.15) -> str:
    """
    Détecte la langue : soit 'mg' pour malgache, soit 'fr' pour français.
    Priorité au malgache via dictionnaire.
    """

    if not text or len(text.strip()) < 5:
        return "fr"  # par défaut, considérer FR si texte trop court

    # cache
    if text in _lang_cache:
        return _lang_cache[text]

    clean = normalize_text(text)
    words = clean.split()

    if not words:
        _lang_cache[text] = "fr"
        return "fr"

    # ✅ Vérifier ratio de mots malgaches
    mg_count = sum(1 for w in words if w in malagasy_words_set)
    mg_ratio = mg_count / len(words)

    if mg_ratio >= min_mg_ratio:
        _lang_cache[text] = "mg"
        return "mg"

    # sinon fallback langdetect
    try:
        lang = detect(text)
        result = "fr" if lang == "fr" else "mg"  # on force à l'un ou l'autre
    except LangDetectException:
        result = "fr"  # par défaut

    _lang_cache[text] = result
    return result
        
_sentiment_cache = {}

mg_positive_words = {
    # émotions positives
    "faly", "hafaliana", "ravoravo", "mientanentana", "fiononana", "tsapako ny hafaliana",
    "finaritra", "tahamaina",

    # qualité / appréciation
    "tsara", "tena tsara", "mahafinaritra", "mendrika",
    "mahafa-po", "mety", "ara-dalàna", "tsara tarehy", "mahatalanjona",
    "miavaka", "mahasoa", "mankasitraka",

    # réussite / progrès
    "fandrosoana", "fampandrosoana", "nahomby", "fahombiazana",
    "fanatsarana", "fanarenana", "fitomboana", "tombony",
    "fanavaozana", "fampivoarana", "fahavitrihana", "voavaha ny olana",

    # soutien / accord
    "mankasitraka", "fanohanana", "mirary soa",
    "firaisankina", "fandriampahalemana", "miara-miasa", "fampanantenana",

    # stabilité / sécurité
    "milamina", "filaminana", "voavaha", "vahaolana",
    "tsara fitantanana", "tsy misy olana", "fandriampahalemana"
}

mg_negative_words = {
    # émotions négatives
    "alahelo", "malahelo", "fahadisoam-panantenana", "fahoriana", "mampalahelo",
    "menatra", "harerahana", "mitsiriritra",

    # qualité négative
    "ratsy", "tena ratsy", "manahirana",
    "sarotra", "olana", "miteraka olana", "tsy mety", "manimba",
    "manahirana be", "maharary", "mampanahy",

    # crise / conflit
    "krizy", "korontana", "fifandirana",
    "disadisa", "ady", "herisetra",
    "fanafintohina", "fikorontanana", "tsy fandriampahalemana",
    "korontana ara-politika", "fahavoazana",

    # échec
    "tsy nahomby", "tsy fahombiazana",
    "fatiantoka", "fahaverezana", "fikorontanana",
    "tsy fahombiazana ara-toekarena",

    # pauvreté / difficulté
    "mahantra", "fahantrana", "tsy fanjarian-tsakafo", "kely vola", "tsy fahafahana",
    "tsy fahazoana asa", "tsy fahombiazana ara-tsosialy",

    # corruption / illégalité
    "kolikoly", "hosoka", "halatra",
    "tsy ara-dalàna", "fanararaotana", "fanafintohina", "fanao ratsy"
}

def analyze_sentiment_score(text: str) -> float:
    if not text or not text.strip():
        return 0.0

    # Cache pour éviter les recalculs
    if text in _sentiment_cache:
        return _sentiment_cache[text]

    lang = detect_language(text)

    score = 0.0

    if lang == "fr":
        try:
            # transformer retourne 'LABEL_0', 'LABEL_1', ou 'LABEL_2' selon le modèle
            result = fr_sentiment_model(text[:512])[0]  # tronquer si texte trop long
            label = result['label'].lower()
            score = float(result['score'])
            if 'neg' in label:
                score = -score  # négatif
            # sinon score positif reste positif
        except Exception:
            score = 0.0

    elif lang == "mg":
        # tokenizer simple pour MG
        words = normalize_text(text).split()
        pos_count = sum(1 for w in words if w in mg_positive_words)
        neg_count = sum(1 for w in words if w in mg_negative_words)

        total = pos_count + neg_count
        if total > 0:
            score = (pos_count - neg_count) / total
        else:
            score = 0.0

    else:
        # autre langue
        score = 0.0

    # Stocker dans le cache
    _sentiment_cache[text] = score
    return score

def analyze_sentiment(text: str) -> str:
    score = analyze_sentiment_score(text)
    if score > 0.2:
        return "positif"
    elif score < -0.2:
        return "negatif"
    return "neutre"

CATEGORY_DESCRIPTIONS = {
    "politique": "actualité politique gouvernement état élections",
    "justice": "tribunal police justice prison enquête",
    "économie": "finance économie entreprise marché croissance",
    "société": "social population sécurité emploi",
    "santé": "hôpital maladie santé vaccin",
    "éducation": "école université enseignement étudiant",
    "technologie": "technologie numérique innovation internet",
    "sport": "football sport championnat match équipe",
    "culture": "festival concert cinéma théâtre",
    "environnement": "climat écologie pollution biodiversité",
    "international": "relations internationales diplomatie conflit",
    "transport": "transport infrastructure route aéroport"
}

category_names = list(CATEGORY_DESCRIPTIONS.keys())
category_texts = list(CATEGORY_DESCRIPTIONS.values())

category_embeddings = embedding_model.encode(
    category_texts,
    convert_to_tensor=True
)

keywords_map = {

    "politique": [
        # Institutions FR
        "président", "premier ministre", "gouvernement", "député",
        "sénat", "assemblée nationale", "élection", "campagne électorale",
        "parti", "opposition", "majorité", "constitution",
        "république", "mandat", "candidat", "ministre",
        "manifestation", "coup d etat", "accord politique",
        "transition", "concertation nationale", "coalition",
        "motion de censure", "révision constitutionnelle",
        "remaniement", "conseil des ministres",
        "scrutin", "bureau de vote", "liste électorale",

        # Spécifique Afrique / Madagascar
        "autorité de transition", "haute cour constitutionnelle",
        "hcc", "chef de l etat", "collectivité territoriale",
        "gouvernorat", "commune", "maire", "préfet",
        "décentralisation",

        # MG
        "filoha", "governemanta", "ministera", "depiote",
        "fifidianana", "antoko politika", "lalampanorenana",
        "repoblika", "fitondrana", "mpitondra",
        "fanovana governemanta"
    ],

    "justice": [
        "tribunal", "juge", "procureur", "cour suprême",
        "justice", "avocat", "condamnation", "détention",
        "prison", "garde a vue", "incarcération",
        "crime", "enquête", "police", "gendarmerie",
        "plainte", "audience", "mandat d arrêt",
        "fraude", "corruption", "dossier judiciaire",
        "escroquerie", "trafic", "violence",

        "fitsarana", "mpisolovava", "heloka",
        "fanadihadiana", "polisy", "fonja"
    ],

    "économie": [
        "économie", "inflation", "croissance",
        "budget", "loi de finances", "finance", "banque",
        "investissement", "marché", "entreprise",
        "industrie", "exportation", "importation",
        "pib", "développement économique",
        "secteur privé", "microfinance",
        "subvention", "prix du carburant",
        "pouvoir d achat", "coût de la vie",

        "toekarena", "varotra", "orinasa",
        "tsena", "fampiasambola",
        "vidimpiainana", "tetibola"
    ],

    "société": [
        "population", "citoyen", "communauté",
        "social", "pauvreté", "emploi",
        "sécurité", "migration",
        "inégalité", "droits humains",
        "protestation", "grève",
        "violence urbaine", "insécurité",

        "mponina", "fiarahamonina",
        "tanora", "fianakaviana",
        "vahoaka"
    ],

    "santé": [
        "hôpital", "centre de santé", "médecin",
        "maladie", "vaccin", "urgence",
        "épidémie", "pandémie",
        "santé publique", "covid",
        "paludisme", "choléra",
        "campagne de vaccination",

        "hopitaly", "dokoterà",
        "aretina", "fahasalamana",
        "valanaretina"
    ],

    "éducation": [
        "école", "université", "enseignement",
        "étudiant", "professeur", "examen",
        "réforme scolaire", "baccalauréat",
        "rentrée scolaire", "formation professionnelle",

        "sekoly", "oniversite",
        "fanabeazana", "mpianatra",
        "mpampianatra"
    ],

    "technologie": [
        "technologie", "innovation",
        "numérique", "internet",
        "intelligence artificielle",
        "cybersécurité", "start up",
        "transformation digitale",
        "réseau mobile", "télécommunication",

        "haitao", "rindranasa",
        "aterineto", "tambajotra"
    ],

    "sport": [
        "football", "rugby", "basket",
        "match", "championnat",
        "équipe", "tournoi",
        "ligue", "sélection nationale",
        "qualification", "stade",

        "baolina kitra", "lalao",
        "ekipa", "fifaninanana",
        "stadiona"
    ],

    "culture": [
        "festival", "concert",
        "exposition", "cinéma",
        "théâtre", "album",
        "artiste", "patrimoine",
        "tradition", "danse",
        "musique", "littérature",

        "mozika", "kolontsaina"
    ],

    "environnement": [
        "climat", "écologie",
        "déforestation", "pollution",
        "biodiversité", "catastrophe naturelle",
        "cyclone", "sécheresse",
        "changement climatique",
        "protection de l environnement",

        "tontolo iainana",
        "toetrandro", "fandotoana",
        "rivodoza", "hain tany"
    ],

    "international": [
        "onu", "union européenne",
        "relations internationales",
        "conflit international",
        "diplomatie", "ambassade",
        "coopération bilatérale",
        "sommet international",
        "sanctions internationales",

        "iraisam pirenena",
        "ady iraisam pirenena"
    ],

    "transport": [
        "route", "aéroport",
        "transport public",
        "trafic", "infrastructure",
        "travaux publics",
        "accident de la route",
        "port maritime",
        "compagnie aérienne",

        "fitaterana",
        "seranam piaramanidina",
        "taxibe"
    ]
}


normalized_keywords_map = {
    cat: [normalize_text(kw) for kw in kws]
    for cat, kws in keywords_map.items()
}

_category_cache = {}

def categorize_text(text: str):
    if not text or not text.strip():
        return ["autre"]

    if text in _category_cache:
        return _category_cache[text]

    clean = normalize_text(text)
    words = clean.split()

    if len(words) < 6:
        _category_cache[text] = ["autre"]
        return ["autre"]
        
    def generate_ngrams(words, n):
        return [' '.join(words[i:i+n]) for i in range(len(words)-n+1)]

    words_set = set(words)
    words_set.update(generate_ngrams(words, 2))
    words_set.update(generate_ngrams(words, 3))
    
    # 1️⃣ KEYWORD MATCH
    keyword_scores = {}
    for cat, kws in normalized_keywords_map.items():
        hits = sum(1 for kw in kws if kw in clean)
        if hits > 0:
            keyword_scores[cat] = hits

    if keyword_scores:
        best_score = max(keyword_scores.values())
        categories = [c for c, s in keyword_scores.items() if s >= best_score * 0.7]
        _category_cache[text] = categories[:3]
        return _category_cache[text]

    # 2️⃣ EMBEDDING SIMILARITY
    text_embedding = embedding_model.encode(
        clean[:800],
        convert_to_tensor=True
    )

    similarities = util.cos_sim(text_embedding, category_embeddings)[0]
    best_idx = similarities.argmax().item()
    best_score = similarities[best_idx].item()

    if best_score >= SIMILARITY_THRESHOLD:
        category = category_names[best_idx]
        _category_cache[text] = [category]
        return [category]

    _category_cache[text] = ["autre"]
    return ["autre"]
