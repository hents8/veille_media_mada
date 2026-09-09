# 🇲🇬 Veille Média Madagascar (ETL & NLP Pipeline)

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-Atlas%2FLocal-green.svg)](https://www.mongodb.com/)
[![Google BigQuery](https://img.shields.io/badge/Google%20BigQuery-Data%20Warehouse-orange.svg)](https://cloud.google.com/bigquery)
[![Prefect](https://img.shields.io/badge/Orchestration-Prefect-blueviolet.svg)](https://www.prefect.io/)

Un pipeline ETL complet et automatisé pour la collecte, l'enrichissement par traitement du langage naturel (NLP) et le stockage analytique des actualités issues des médias à Madagascar.

---

## 📌 Sommaire
- [Présentation](#-présentation)
- [Architecture & Flux de Données](#-architecture--flux-de-données)
- [Fonctionnalités Principales](#-fonctionnalités-principales)
- [Stack Technique](#-stack-technique)
- [Structure du Projet](#-structure-du-projet)
- [Installation & Configuration](#-installation--configuration)
- [Utilisation](#-utilisation)
- [CI/CD & Déploiement](#-cicd--déploiement)

---

## 📖 Présentation

**Veille Média Mada** est une solution conçue pour automatiser la surveillance de la presse malgache. Le projet collecte quotidiennement des articles depuis une quinzaine de sources (presse en ligne, dépêches, flux RSS), traite et nettoie les textes, puis applique des modèles NLP adaptés aux spécificités linguistiques locales (**Français** et **Malgache**).

Les données enrichies sont d'abord stockées dans **MongoDB**, puis synchronisées vers **Google BigQuery** pour alimenter des tableaux de bord analytiques et faciliter la prise de décision.

---

## 🏗️ Architecture & Flux de Données

```
 ┌─────────────────────────────────────────────────────────┐
 │                   SOURCES DE DONNÉES                    │
 │  • Flux RSS (Midi, L'Express, 2424.mg, NewsMada, etc.)  │
 │  • Web Scraping HTML (Malagasy News)                    │
 │  • Dynamic Scraping / Selenium (Orange Actu)            │
 └────────────────────────────┬────────────────────────────┘
                              │
                              ▼
 ┌─────────────────────────────────────────────────────────┐
 │                   TRANSFORMATION & NLP                  │
 │  • Nettoyage HTML & Normalisation Texte                 │
 │  • Détection de Langue (FR / MG - Dictionnaire hybride) │
 │  • Analyse de Sentiment (XLM-RoBERTa / Lexique MG)      │
 │  • Catégorisation (Mots-clés + Sentence Transformers)   │
 └────────────────────────────┬────────────────────────────┘
                              │
                              ▼
 ┌─────────────────────────────────────────────────────────┐
 │                  STOCKAGE NO-SQL (Document)             │
 │  • MongoDB Atlas (`veille_media.articles`)              │
 └────────────────────────────┬────────────────────────────┘
                              │
                              ▼
 ┌─────────────────────────────────────────────────────────┐
 │                 DATA WAREHOUSE & ANALYTICS              │
 │  • BigQuery Sync (`veille_mada.articles_clean`)         │
 └─────────────────────────────────────────────────────────┘
```

---

## ✨ Fonctionnalités Principales

### 1. Extraction Multi-sources (ETL - Extract)
- **Collecte RSS Robuste :** Récupération dynamique sur plus de 15 flux avec nettoyage automatique des caractères BOM UTF-8 et correction des balises XML mal formées.
- **Scraping Web Avancé :** Extraction ciblée BeautifulSoup4 avec politiques de *retry* adaptatives et gestion des *User-Agents*.
- **Scraping Dynamique :** Support de Selenium en mode *headless* pour les sites utilisant du rendu JavaScript.

### 2. Enrichissement & NLP Multilingue (ETL - Transform)
- **Détection de Langue Hybride (FR / MG) :** Combinaison d'un dictionnaire étendu de vocabulaire malgache (politique, économie, société, santé) et de `langdetect`.
- **Analyse de Sentiment Adaptée :**
  - **Français :** Utilisation de `cardiffnlp/twitter-xlm-roberta-base-sentiment` (Transformers).
  - **Malgache :** Analyse basée sur des lexiques thématiques de mots à polarité positive/négative.
- **Catégorisation Intelligente :** Classification multi-label (Politique, Économie, Justice, Société, Santé, Éducation, Sport, etc.) par calcul de similarité sémantique (`sentence-transformers/all-MiniLM-L6-v2`) et détection de mots-clés.

### 3. Stockage et Synchronisation BigQuery (ETL - Load)
- **Dédoublonnage Automatique :** Hash SHA-256 / MD5 des URLs et titres pour éviter la duplication des articles.
- **Batch Processing :** Insertions optimisées dans MongoDB via `bulk_write`.
- **Sync BigQuery Automatisée :** Export incremental/total vers GCP BigQuery sous format JSONL, prêt pour la visualisation sur Looker Studio ou Power BI.

---

## 🛠️ Stack Technique

- **Langage :** Python 3.10+
- **Orchestration :** Prefect
- **Scraping & Requesting :** `requests`, `beautifulsoup4`, `feedparser`, `selenium`, `webdriver-manager`
- **NLP & Machine Learning :** `transformers`, `torch`, `sentence-transformers`, `textblob_fr`, `langdetect`
- **Bases de Données :** MongoDB (`pymongo`), Google Cloud BigQuery (`google-cloud-bigquery`)
- **API & Utilities :** `pandas`, `python-dotenv`, `fastapi`

---

## 📁 Structure du Projet

```text
.
├── etl/
│   ├── rss_loader.py         # Module de récupération des flux RSS
│   ├── scraper_loader.py     # Scraper BeautifulSoup pour sites de presse
│   ├── selenium_loader.py    # Scraper Selenium pour contenus dynamiques
│   ├── transform.py          # Logique NLP (Nettoyage, Langue, Sentiment, Catégorisation)
│   └── load.py               # Connexion et insertion MongoDB
├── main.py                   # Script principal d'exécution du pipeline ETL
├── mongo_to_bigquery.py      # Script de synchronisation MongoDB -> BigQuery
├── test_mongo.py             # Diagnostic de connexion MongoDB
├── requirements.txt          # Dépendances Python du projet
└── README.md                 # Documentation du projet
```

---

## 🚀 Installation & Configuration

### 1. PRÉREQUIS
- Python 3.10+
- Une instance **MongoDB** (Atlas ou locale)
- Un projet **Google Cloud Platform (GCP)** avec BigQuery activé

### 2. Cloner le dépôt
```bash
git clone https://github.com/hents8/veille_media_mada.git
cd veille_media_mada
```

### 3. Créer un environnement virtuel et installer les dépendances
```bash
python -m venv venv
# Sur Linux/macOS :
source venv/bin/activate
# Sur Windows :
venv\Scriptsctivate

pip install -r requirements.txt
```

### 4. Configuration des Variables d'Environnement (`.env`)
Créer un fichier `.env` à la racine du projet :

```env
MONGO_URI=mongodb+srv://<user>:<password>@cluster.mongodb.net/
GCP_PROJECT_ID=votre-projet-gcp
BIGQUERY_DATASET=veille_mada
GOOGLE_APPLICATION_CREDENTIALS=chemin/vers/votre/cle-service-account.json
```

---

## 💻 Utilisation

### Exécuter le pipeline d'extraction et de traitement (MongoDB)
```bash
python main.py
```

### Synchroniser les données de MongoDB vers BigQuery
```bash
python mongo_to_bigquery.py
```

### Vérifier la connexion MongoDB
```bash
python test_mongo.py
```

---

## 🔄 CI/CD & Déploiement

Le projet supporte l'authentification **Workload Identity Federation (WIF)** sur GitHub Actions pour la synchronisation sécurisée vers BigQuery sans stockage permanent de clés SA.

---

## 📊 Tableau de Bord Looker Studio

Vous pouvez consulter le tableau de bord interactif des médias malgaches directement via ce lien :

[![Looker Studio](https://img.shields.io/badge/Looker_Studio-Consulter_le_Rapport-blue?style=for-the-badge&logo=google)](https://datastudio.google.com/reporting/0e624834-a151-4eea-8389-fd395ebb5e53)

👉 **[Ouvrir le rapport complet Looker Studio](https://datastudio.google.com/reporting/0e624834-a151-4eea-8389-fd395ebb5e53)**

---

## 📝 Licence

Distribué sous la licence MIT. Voir `LICENSE` pour plus d'informations.
