"""Passage au schema en etoile."""
"""
transform/build_dimensions.py
──────────────────────────────
Construction du schéma en étoile à partir des DataFrames nettoyés.

Dimensions construites :
  - dim_temps    : calendrier complet 2019→2030
  - dim_client   : SCD Type 2 (historique des changements)
  - dim_produit  : SCD Type 1 (écrasement) + flag actif pour inactifs
  - dim_region   : depuis regions_maroc.csv (propre)

Table de faits :
  - fact_commandes : granularité = 1 ligne de commande (id_commande + id_produit)
"""

import pandas as pd
from datetime import date
from utils.logger import get_logger

logger = get_logger("build_dimensions")


# ══════════════════════════════════════════════════════════════════
# DIM TEMPS
# ══════════════════════════════════════════════════════════════════

def build_dim_temps(debut: str = "2019-01-01", fin: str = "2030-12-31") -> pd.DataFrame:
    """
    Calendrier complet jour par jour.
    La clé id_temps = YYYYMMDD (entier) pour jointure rapide.
    """
    logger.info("Construction dim_temps...")
    dates = pd.date_range(start=debut, end=fin, freq="D")
    df = pd.DataFrame({"date_complete": dates})

    df["id_temps"]     = df["date_complete"].dt.strftime("%Y%m%d").astype(int)
    df["jour"]         = df["date_complete"].dt.day
    df["mois"]         = df["date_complete"].dt.month
    df["nom_mois"]     = df["date_complete"].dt.strftime("%B")
    df["trimestre"]    = df["date_complete"].dt.quarter
    df["annee"]        = df["date_complete"].dt.year
    df["semaine"]      = df["date_complete"].dt.isocalendar().week.astype(int)
    df["jour_semaine"] = df["date_complete"].dt.dayofweek + 1   # 1=Lundi
    df["nom_jour"]     = df["date_complete"].dt.strftime("%A")
    df["est_weekend"]  = (df["date_complete"].dt.dayofweek >= 5).astype(int)

    # Périodes Ramadan approximatives (à affiner chaque année)
    ramadan_periodes = [
        ("2023-03-22", "2023-04-20"),
        ("2024-03-11", "2024-04-09"),
        ("2025-03-01", "2025-03-29"),
    ]
    df["est_ramadan"] = 0
    for debut_r, fin_r in ramadan_periodes:
        masque = (df["date_complete"] >= debut_r) & (df["date_complete"] <= fin_r)
        df.loc[masque, "est_ramadan"] = 1

    logger.info(f"dim_temps : {len(df)} lignes")
    return df


# ══════════════════════════════════════════════════════════════════
# DIM RÉGION (depuis référentiel propre)
# ══════════════════════════════════════════════════════════════════

def build_dim_region(df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Construit dim_region directement depuis regions_maroc.csv.
    Le fichier est déjà propre — on ajoute juste une clé surrogate.
    """
    logger.info("Construction dim_region...")
    df = df_regions.copy()
    df.insert(0, "id_region", range(1, len(df) + 1))
    df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype(int)
    df["code_postal"] = pd.to_numeric(df["code_postal"], errors="coerce").fillna(0).astype(int)
    logger.info(f"dim_region : {len(df)} lignes")
    return df


# ══════════════════════════════════════════════════════════════════
# DIM CLIENT — SCD TYPE 2
# ══════════════════════════════════════════════════════════════════

def build_dim_client(df_clients: pd.DataFrame) -> pd.DataFrame:
    """
    SCD Type 2 : chaque version d'un client a :
      - date_debut_validite  : date de création de cette version
      - date_fin_validite    : NULL si version courante
      - est_courant          : True pour la version active
      - version              : numéro de version (commence à 1)

    Pour l'initialisation, tous les clients ont version=1, est_courant=True.
    Lors des mises à jour futures (ex: changement de ville), on fermera
    la version courante et on en créera une nouvelle.
    """
    logger.info("Construction dim_client (SCD Type 2)...")

    colonnes = [
        "id_client", "nom_complet", "nom", "prenom",
        "email", "email_valide",
        "sexe", "age",
        "ville", "region_admin", "zone_geo",
        "segment",
        "date_inscription", "canal_acquisition",
    ]
    # Garder uniquement les colonnes disponibles
    colonnes_dispo = [c for c in colonnes if c in df_clients.columns]
    dim = df_clients[colonnes_dispo].copy()

    # Champs SCD Type 2
    dim["date_debut_validite"] = date.today()
    dim["date_fin_validite"]   = None
    dim["est_courant"]         = True
    dim["version"]             = 1

    logger.info(f"dim_client : {len(dim)} lignes (version initiale)")
    return dim.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════
# DIM PRODUIT — SCD TYPE 1
# ══════════════════════════════════════════════════════════════════

def build_dim_produit(df_produits: pd.DataFrame) -> pd.DataFrame:
    """
    SCD Type 1 : on écrase l'ancienne valeur à chaque mise à jour.
    Utilisé pour le prix et le stock — on ne garde pas l'historique.

    Les produits inactifs (actif=False) sont conservés car ils ont
    des commandes associées → nécessaire pour l'intégrité du fait.
    """
    logger.info("Construction dim_produit (SCD Type 1)...")

    colonnes = [
        "id_produit", "nom", "categorie", "sous_categorie",
        "marque", "fournisseur", "origine_pays",
        "prix_catalogue", "tranche_prix", "prix_manquant",
        "actif", "date_creation",
    ]
    colonnes_dispo = [c for c in colonnes if c in df_produits.columns]
    dim = df_produits[colonnes_dispo].copy()
    dim = dim.rename(columns={"nom": "nom_produit"})

    n_inactifs = (~dim["actif"]).sum()
    logger.info(f"dim_produit : {len(dim)} lignes dont {n_inactifs} inactifs (conservés pour SCD)")
    return dim.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════
# FACT COMMANDES
# ══════════════════════════════════════════════════════════════════

def build_fact_commandes(
    df_commandes:  pd.DataFrame,
    dim_client:    pd.DataFrame,
    dim_produit:   pd.DataFrame,
    dim_region:    pd.DataFrame,
) -> pd.DataFrame:
    """
    Granularité : 1 ligne = 1 commande × 1 produit.
    (Dans nos données, chaque ligne de commandes_mexora est déjà à ce niveau.)

    Clés étrangères :
      - id_temps   → dim_temps.id_temps   (YYYYMMDD)
      - id_client  → dim_client.id_client
      - id_produit → dim_produit.id_produit
      - id_region  → dim_region.id_region (via ville_livraison)
    """
    logger.info("Construction fact_commandes...")
    df = df_commandes.copy()

    # ── Clé de temps ────────────────────────────────────────────
    df["id_temps"] = (
        pd.to_datetime(df["date_commande"])
        .dt.strftime("%Y%m%d")
        .astype(int)
    )

    # ── Clé région : ville_livraison → id_region ────────────────
    ref_region = dim_region[["id_region", "nom_ville_standard"]].copy()
    ref_region = ref_region.rename(columns={"nom_ville_standard": "ville_livraison"})
    df = df.merge(ref_region, on="ville_livraison", how="left")
    df["id_region"] = df["id_region"].fillna(-1).astype(int)  # -1 = ville inconnue

    # ── Sélection des colonnes de la table de faits ─────────────
    fact = df[[
        "id_commande",
        "id_client",
        "id_produit",
        "id_temps",
        "id_region",
        "quantite",
        "prix_unitaire",
        "montant_ligne",
        "statut",
        "mode_paiement",
        "ville_livraison",
        "id_livreur",
        "date_livraison",
        "a_retour",
    ]].rename(columns={"statut": "statut_commande"})

    # Vérification intégrité : clients absents de dim_client
    clients_fact   = set(fact["id_client"].unique())
    clients_dim    = set(dim_client["id_client"].astype(str).unique())
    orphelins      = clients_fact - clients_dim
    if orphelins:
        logger.warning(f"id_client sans correspondance dans dim_client : {len(orphelins)}")

    logger.info(f"fact_commandes : {len(fact)} lignes")
    return fact.reset_index(drop=True)