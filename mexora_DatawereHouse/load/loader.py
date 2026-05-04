"""Envoi vers PostgreSQL."""
"""
load/loader.py
──────────────
Chargement du schéma en étoile dans PostgreSQL.

Étapes :
  1. Création des tables DWH (si elles n'existent pas)
  2. Chargement des dimensions puis de la table de faits
  3. Création des index analytiques
  4. Création + rafraîchissement des vues matérialisées
"""

import pandas as pd
from sqlalchemy import create_engine, text
from config.settings import POSTGRES_CONFIG
from utils.logger import get_logger

logger = get_logger("loader")


# ── Connexion ──────────────────────────────────────────────────
def get_pg_engine():
    cfg = POSTGRES_CONFIG
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    engine = create_engine(url, echo=False)
    logger.info("Connexion PostgreSQL prête")
    return engine


# ── Création du schéma DWH ─────────────────────────────────────
DDL = """
-- Dimension Temps
CREATE TABLE IF NOT EXISTS dim_temps (
    id_temps        INTEGER PRIMARY KEY,
    date_complete   DATE NOT NULL,
    jour            SMALLINT,
    mois            SMALLINT,
    nom_mois        VARCHAR(20),
    trimestre       SMALLINT,
    annee           SMALLINT,
    semaine         SMALLINT,
    jour_semaine    SMALLINT,
    nom_jour        VARCHAR(20),
    est_weekend     SMALLINT DEFAULT 0,
    est_ramadan     SMALLINT DEFAULT 0
);

-- Dimension Région (depuis regions_maroc.csv — propre)
CREATE TABLE IF NOT EXISTS dim_region (
    id_region           INTEGER PRIMARY KEY,
    code_ville          VARCHAR(10),
    nom_ville_standard  VARCHAR(100),
    province            VARCHAR(150),
    region_admin        VARCHAR(150),
    zone_geo            VARCHAR(50),
    population          INTEGER,
    code_postal         INTEGER
);

-- Dimension Client (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_client (
    sk_client            SERIAL PRIMARY KEY,   -- clé surrogate
    id_client            VARCHAR(20),
    nom_complet          VARCHAR(200),
    nom                  VARCHAR(100),
    prenom               VARCHAR(100),
    email                VARCHAR(200),
    email_valide         BOOLEAN,
    sexe                 VARCHAR(10),
    age                  INTEGER,
    ville                VARCHAR(100),
    region_admin         VARCHAR(150),
    zone_geo             VARCHAR(50),
    segment              VARCHAR(50),
    date_inscription     DATE,
    canal_acquisition    VARCHAR(50),
    -- Champs SCD Type 2
    date_debut_validite  DATE NOT NULL,
    date_fin_validite    DATE,
    est_courant          BOOLEAN DEFAULT TRUE,
    version              SMALLINT DEFAULT 1
);

-- Dimension Produit (SCD Type 1)
CREATE TABLE IF NOT EXISTS dim_produit (
    sk_produit      SERIAL PRIMARY KEY,   -- clé surrogate
    id_produit      VARCHAR(20) UNIQUE,
    nom_produit     VARCHAR(300),
    categorie       VARCHAR(100),
    sous_categorie  VARCHAR(100),
    marque          VARCHAR(100),
    fournisseur     VARCHAR(150),
    origine_pays    VARCHAR(100),
    prix_catalogue  NUMERIC(10,2),
    tranche_prix    VARCHAR(30),
    prix_manquant   SMALLINT DEFAULT 0,
    actif           BOOLEAN,
    date_creation   DATE
);

-- Table de faits
CREATE TABLE IF NOT EXISTS fact_commandes (
    id_fait               SERIAL PRIMARY KEY,
    id_commande           VARCHAR(20),
    id_client             VARCHAR(20),
    id_produit            VARCHAR(20),
    id_temps              INTEGER,
    id_region             INTEGER,
    quantite              INTEGER,
    prix_unitaire         NUMERIC(10,2),
    montant_ligne         NUMERIC(10,2),
    statut_commande       VARCHAR(50),
    mode_paiement         VARCHAR(50),
    ville_livraison       VARCHAR(100),
    id_livreur            VARCHAR(20),
    date_livraison        DATE,
    a_retour              SMALLINT DEFAULT 0
);

-- Index pour performance analytique
CREATE INDEX IF NOT EXISTS idx_fact_temps    ON fact_commandes(id_temps);
CREATE INDEX IF NOT EXISTS idx_fact_client   ON fact_commandes(id_client);
CREATE INDEX IF NOT EXISTS idx_fact_produit  ON fact_commandes(id_produit);
CREATE INDEX IF NOT EXISTS idx_fact_region   ON fact_commandes(id_region);
CREATE INDEX IF NOT EXISTS idx_fact_statut   ON fact_commandes(statut_commande);
CREATE INDEX IF NOT EXISTS idx_fact_retour   ON fact_commandes(a_retour);
CREATE INDEX IF NOT EXISTS idx_fact_paiement ON fact_commandes(mode_paiement);
"""

# ── Vues matérialisées (réponses pré-calculées pour le dashboard) ─
VUES_SQL = """
-- Vue 1 : Ventes par région et par mois
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ventes_region AS
SELECT
    r.region_admin,
    r.nom_ville_standard,
    r.zone_geo,
    t.annee,
    t.mois,
    t.nom_mois,
    t.trimestre,
    t.est_ramadan,
    COUNT(DISTINCT f.id_commande)  AS nb_commandes,
    SUM(f.montant_ligne)           AS chiffre_affaires,
    AVG(f.montant_ligne)           AS panier_moyen,
    SUM(f.quantite)                AS total_articles,
    SUM(f.a_retour)                AS nb_retours,
    ROUND(
        100.0 * SUM(f.a_retour) / NULLIF(COUNT(*), 0), 2
    )                              AS taux_retour_pct
FROM fact_commandes f
JOIN dim_temps  t ON f.id_temps  = t.id_temps
JOIN dim_region r ON f.id_region = r.id_region
GROUP BY r.region_admin, r.nom_ville_standard, r.zone_geo,
         t.annee, t.mois, t.nom_mois, t.trimestre, t.est_ramadan;

-- Vue 2 : Performance par catégorie de produit
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_perf_categorie AS
SELECT
    p.categorie,
    p.sous_categorie,
    p.marque,
    p.tranche_prix,
    t.annee,
    t.mois,
    t.est_ramadan,
    SUM(f.quantite)      AS total_quantite,
    SUM(f.montant_ligne) AS chiffre_affaires,
    SUM(f.a_retour)      AS nb_retours,
    ROUND(
        100.0 * SUM(f.a_retour) / NULLIF(COUNT(*), 0), 2
    )                    AS taux_retour_pct
FROM fact_commandes f
JOIN dim_produit p ON f.id_produit = p.id_produit
JOIN dim_temps   t ON f.id_temps   = t.id_temps
GROUP BY p.categorie, p.sous_categorie, p.marque, p.tranche_prix,
         t.annee, t.mois, t.est_ramadan;

-- Vue 3 : Performance mensuelle globale
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_perf_mensuelle AS
SELECT
    t.annee,
    t.mois,
    t.nom_mois,
    t.trimestre,
    t.est_ramadan,
    COUNT(DISTINCT f.id_commande)  AS nb_commandes,
    COUNT(DISTINCT f.id_client)    AS nb_clients_actifs,
    SUM(f.montant_ligne)           AS chiffre_affaires,
    AVG(f.montant_ligne)           AS panier_moyen,
    SUM(f.a_retour)                AS nb_retours,
    ROUND(
        100.0 * SUM(f.a_retour) / NULLIF(COUNT(*), 0), 2
    )                              AS taux_retour_pct
FROM fact_commandes f
JOIN dim_temps t ON f.id_temps = t.id_temps
GROUP BY t.annee, t.mois, t.nom_mois, t.trimestre, t.est_ramadan;
"""


def create_dwh_schema(engine) -> None:
    logger.info("Création du schéma DWH dans PostgreSQL...")
    with engine.connect() as conn:
        conn.execute(text(DDL))
        conn.commit()
    logger.info("Schéma créé ✓")


def load_table(
    df: pd.DataFrame,
    table: str,
    engine,
    mode: str = "append",
) -> None:
    """
    Charge un DataFrame dans une table PostgreSQL.
    mode : 'replace' (vide la table avant) | 'append' (ajoute)
    """
    try:
        df.to_sql(
    table,
    engine,
    schema="dwh_mexora",   
    if_exists=mode,
    index=False,
    method="multi",
    chunksize=1000,
)
        logger.info(f"[{table}] {len(df)} lignes chargées ✓")
    except Exception as e:
        logger.error(f"Erreur chargement '{table}' : {e}")
        raise


def create_materialized_views(engine) -> None:
    logger.info("Création des vues matérialisées...")
    with engine.connect() as conn:
        conn.execute(text(VUES_SQL))
        conn.commit()
    logger.info("Vues matérialisées créées ✓")


def refresh_materialized_views(engine) -> None:
    vues = ["mv_ventes_region", "mv_perf_categorie", "mv_perf_mensuelle"]
    with engine.connect() as conn:
        for vue in vues:
            try:
                conn.execute(text(f"REFRESH MATERIALIZED VIEW {vue}"))
                logger.info(f"Vue '{vue}' rafraîchie ✓")
            except Exception as e:
                logger.warning(f"Vue '{vue}' non rafraîchie : {e}")
        conn.commit()




def test_connection():
     try:
        engine = get_pg_engine()

        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()

        print("Connexion PostgreSQL réussie ✓")
        print("Version :", version[0])

     except Exception as e:
        print("Erreur connexion PostgreSQL :", e)




if __name__ == "__main__":
        print("Test en cours...")
        test_connection()