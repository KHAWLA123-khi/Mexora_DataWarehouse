"""
extract/extractor.py
────────────────────
Extraction des données brutes depuis data/raw/.

Sources :
  - commandes_mexora.csv  → pd.DataFrame (50 000+ lignes, données sales)
  - clients_mexora.csv    → pd.DataFrame (5 000+ lignes, données sales)
  - produits_mexora.json  → pd.DataFrame (50 produits, clé "produits")
  - regions_maroc.csv     → pd.DataFrame (30 villes, référentiel propre)

Aucune transformation ici — on lit brut, on logue, on retourne.
"""

import json
import pandas as pd
from config.settings import RAW_FILES
from utils.logger import get_logger

logger = get_logger("extractor")


# ── Fonctions d'extraction individuelles ───────────────────────

def extract_commandes() -> pd.DataFrame:
    """
    Lit commandes_mexora.csv.
    Contient intentionnellement : doublons, formats de dates mixtes,
    statuts non-standards, quantités négatives, prix à 0, villes incohérentes.
    On lit tout en string pour ne rien perdre avant la transformation.
    """
    path = RAW_FILES["commandes"]
    logger.info(f"Lecture commandes : {path}")
    df = pd.read_csv(
        path,
        dtype=str,          # tout en string → la transform gère les types
        keep_default_na=False,
    )
    logger.info(f"commandes brutes : {len(df)} lignes | colonnes : {list(df.columns)}")
    _log_apercu(df, "commandes")
    return df


def extract_clients() -> pd.DataFrame:
    """
    Lit clients_mexora.csv.
    Contient intentionnellement : doublons (même email / id différent),
    sexe encodé différemment, dates de naissance invalides, emails mal formés.
    """
    path = RAW_FILES["clients"]
    logger.info(f"Lecture clients : {path}")
    df = pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
    )
    logger.info(f"clients bruts : {len(df)} lignes | colonnes : {list(df.columns)}")
    _log_apercu(df, "clients")
    return df


def extract_produits() -> pd.DataFrame:
    """
    Lit produits_mexora.json (clé racine = "produits").
    Contient intentionnellement : casse incohérente sur categorie,
    actif=false avec commandes associées (SCD), prix_catalogue null.
    """
    path = RAW_FILES["produits"]
    logger.info(f"Lecture produits : {path}")
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if "produits" not in raw:
        raise KeyError(f"Clé 'produits' introuvable dans {path}")

    df = pd.DataFrame(raw["produits"])
    # Convertir booléen actif en string pour uniformité
    df["actif"] = df["actif"].astype(str)
    logger.info(f"produits bruts : {len(df)} lignes | colonnes : {list(df.columns)}")
    _log_apercu(df, "produits")
    return df


def extract_regions() -> pd.DataFrame:
    """
    Lit regions_maroc.csv — référentiel géographique propre et complet.
    Sera utilisé comme table de jointure pour harmoniser toutes les villes.
    """
    path = RAW_FILES["regions"]
    logger.info(f"Lecture régions : {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    logger.info(f"regions : {len(df)} lignes | colonnes : {list(df.columns)}")
    return df


# ── Extraction complète ─────────────────────────────────────────

def extract_all() -> dict[str, pd.DataFrame]:
    """
    Point d'entrée principal : extrait les 4 sources et retourne
    un dictionnaire de DataFrames bruts prêts pour la transformation.
    """
    logger.info("=" * 55)
    logger.info("  EXTRACTION — DÉMARRAGE")
    logger.info("=" * 55)

    data = {
        "commandes": extract_commandes(),
        "clients":   extract_clients(),
        "produits":  extract_produits(),
        "regions":   extract_regions(),
    }

    logger.info("  EXTRACTION — TERMINÉE ✓")
    logger.info("=" * 55)
    return data


# ── Helper interne ──────────────────────────────────────────────

def _log_apercu(df: pd.DataFrame, nom: str) -> None:
    """Log un aperçu rapide : valeurs manquantes par colonne."""
    vides = (df == "").sum()
    cols_vides = vides[vides > 0]
    if not cols_vides.empty:
        logger.debug(f"[{nom}] Champs vides détectés :\n{cols_vides.to_string()}")