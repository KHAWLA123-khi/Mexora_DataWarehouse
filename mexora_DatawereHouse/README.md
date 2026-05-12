# 🛒 Mexora ETL — Pipeline Data Warehouse

> Pipeline ETL complet pour la marketplace e-commerce **Mexora** (Tanger, Maroc).  
> Transforme des données brutes (CSV / JSON) en schéma en étoile PostgreSQL,  
> prêt pour l'analyse décisionnelle.

---

## 📁 Structure du projet

```
mexora_etl/
├── data/
│   └── raw/                        # Fichiers sources bruts
│       ├── commandes_mexora.csv    # 51 500 lignes
│       ├── clients_mexora.csv      # 5 188 lignes
│       ├── produits_mexora.json    # 50 produits
│       └── regions_maroc.csv       # 30 villes (référentiel propre)
│
├── config/
│   └── settings.py                 # Chemins + config PostgreSQL
│
├── extract/
│   └── extractor.py                # Lecture des 4 sources
│
├── transform/
│   ├── clean_commandes.py          # Nettoyage commandes
│   ├── clean_clients.py            # Nettoyage clients
│   ├── clean_produits.py           # Nettoyage produits
│   └── build_dimensions.py         # Construction schéma en étoile
│
├── load/
│   └── loader.py                   # Chargement PostgreSQL + vues matérialisées
│
├── utils/
│   └── logger.py                   # Logging console + fichier
│
├── logs/
│   └── etl.log                     # Généré automatiquement à l'exécution
│
├── main.py                         # Point d'entrée — orchestrateur
├── requirements.txt
├── .env                            # Variables d'environnement (ne pas committer)
├── README.md
└── rapport_transformations.md
```

---

## ⚙️ Prérequis

| Outil | Version minimale |
|-------|-----------------|
| Python | 3.10+ |
| PostgreSQL | 14+ |
| pip | 23+ |

---

## 🚀 Installation et lancement

### 1. Cloner le dépôt

```bash
git clone https://github.com/<ton-user>/mexora_etl.git
cd mexora_etl
```

### 2. Créer un environnement virtuel

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. Configurer les variables d'environnement

Crée un fichier `.env` à la racine du projet :

```env
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=ton_mot_de_passe
PG_DB=mexora_dwh
```

### 5. Créer la base de données PostgreSQL

```bash
# Se connecter à PostgreSQL
psql -U postgres

# Dans le shell psql :
CREATE DATABASE mexora_dwh;
\q
```

### 6. Placer les fichiers sources

Assure-toi que les 4 fichiers sont présents dans `data/raw/` :

```
data/raw/commandes_mexora.csv
data/raw/clients_mexora.csv
data/raw/produits_mexora.json
data/raw/regions_maroc.csv
```

### 7. Lancer le pipeline

```bash
python main.py
```

---

## 📊 Ce que le pipeline produit

### Schéma en étoile (PostgreSQL)

```
                    ┌─────────────┐
                    │  dim_temps  │
                    │  4 383 lignes│
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴───────┐    ┌──────────────┐
│ dim_client  │────│ fact_commandes│────│  dim_produit │
│ 5 000 lignes│    │ 48 785 lignes │    │  50 lignes   │
└─────────────┘    └───────┬───────┘    └──────────────┘
                           │
                    ┌──────┴──────┐
                    │ dim_region  │
                    │  30 lignes  │
                    └─────────────┘
```

### Vues matérialisées créées

| Vue | Description |
|-----|-------------|
| `mv_ventes_region` | CA, commandes, taux de retour par région et mois |
| `mv_perf_categorie` | Performance par catégorie de produit |
| `mv_perf_mensuelle` | KPIs globaux mensuels |

---

## 📋 Logs

Les logs sont générés à chaque exécution dans `logs/etl.log` :

```
2024-01-15 10:23:01 | extractor     | INFO  | commandes brutes : 51 500 lignes
2024-01-15 10:23:05 | clean_commandes | INFO | Doublons supprimés : 1 500
2024-01-15 10:23:09 | clean_commandes | INFO | fact_commandes : 48 785 lignes
...
```

---

## 🔄 Stratégies SCD implémentées

| Dimension | Type SCD | Justification |
|-----------|----------|---------------|
| `dim_client` | Type 2 | Historique des changements de ville/segment nécessaire |
| `dim_produit` | Type 1 | Prix et stock : on garde toujours la valeur courante |
| `dim_region` | — | Référentiel stable, pas de SCD |
| `dim_temps` | — | Calendrier immuable |

---

## 📬 Contact

Projet réalisé dans le cadre du miniprojet **Ingénierie de Données — Mexora DWH**.
