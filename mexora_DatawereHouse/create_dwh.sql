-- =====================================================
-- PROJET MEXORA - DATA WAREHOUSE
-- Script de création du schéma en étoile
-- =====================================================

-- ══════════════════════════════════════════════════════
-- 1. CREATION DU SCHEMA
-- ══════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS dwh_mexora;


-- ══════════════════════════════════════════════════════
-- 2. DIMENSION TEMPS
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dwh_mexora.dim_temps (
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
    est_weekend     SMALLINT,
    est_ramadan     SMALLINT
);


-- ══════════════════════════════════════════════════════
-- 3. DIMENSION REGION
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dwh_mexora.dim_region (
    id_region           INTEGER PRIMARY KEY,
    code_ville          VARCHAR(20),
    nom_ville_standard  VARCHAR(100),
    province            VARCHAR(150),
    region_admin        VARCHAR(150),
    zone_geo            VARCHAR(50),
    population          INTEGER,
    code_postal         INTEGER
);


-- ══════════════════════════════════════════════════════
-- 4. DIMENSION CLIENT (SCD TYPE 2)
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dwh_mexora.dim_client (
    sk_client            SERIAL PRIMARY KEY,
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

    date_debut_validite  DATE NOT NULL,
    date_fin_validite    DATE,
    est_courant          BOOLEAN DEFAULT TRUE,
    version              SMALLINT DEFAULT 1
);


-- ══════════════════════════════════════════════════════
-- 5. DIMENSION PRODUIT (SCD TYPE 1)
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dwh_mexora.dim_produit (
    sk_produit      SERIAL PRIMARY KEY,
    id_produit      VARCHAR(20) UNIQUE,
    nom_produit     VARCHAR(300),
    categorie       VARCHAR(100),
    sous_categorie  VARCHAR(100),
    marque          VARCHAR(100),
    fournisseur     VARCHAR(150),
    origine_pays    VARCHAR(100),
    prix_catalogue  NUMERIC(10,2),
    tranche_prix    VARCHAR(30),
    prix_manquant   SMALLINT,
    actif           BOOLEAN,
    date_creation   DATE
);


-- ══════════════════════════════════════════════════════
-- 6. TABLE DE FAITS
-- ══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dwh_mexora.fact_commandes (
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
    a_retour              SMALLINT
);


-- ══════════════════════════════════════════════════════
-- 7. INDEX POUR PERFORMANCE
-- ══════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_fc_temps
ON dwh_mexora.fact_commandes(id_temps);

CREATE INDEX IF NOT EXISTS idx_fc_produit
ON dwh_mexora.fact_commandes(id_produit);

CREATE INDEX IF NOT EXISTS idx_fc_client
ON dwh_mexora.fact_commandes(id_client);

CREATE INDEX IF NOT EXISTS idx_fc_region
ON dwh_mexora.fact_commandes(id_region);

CREATE INDEX IF NOT EXISTS idx_fc_livreur
ON dwh_mexora.fact_commandes(id_livreur);

CREATE INDEX IF NOT EXISTS idx_fc_temps_region
ON dwh_mexora.fact_commandes(id_temps, id_region);

CREATE INDEX IF NOT EXISTS idx_fc_perf
ON dwh_mexora.fact_commandes(id_temps, id_produit)
INCLUDE (montant_ligne, quantite);

CREATE INDEX IF NOT EXISTS idx_fc_statut_livree
ON dwh_mexora.fact_commandes(statut_commande)
WHERE statut_commande = 'livree';


-- =====================================================
-- FIN DU SCRIPT
-- =====================================================