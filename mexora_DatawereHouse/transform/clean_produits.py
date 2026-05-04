"""Nettoyage des donnees produits."""
"""
transform/clean_produits.py
────────────────────────────
Nettoyage du fichier produits_mexora.json.

Problèmes traités :
  1. categorie en casse incohérente (electronique / ELECTRONIQUE / Electronique)
     → normalisée en title case
  2. actif = false avec commandes associées (signalé + conservé pour SCD)
  3. prix_catalogue null sur anciens produits → remplacé par 0.0 + flag
  4. Tranche de prix calculée en MAD
"""

import pandas as pd
from utils.logger import get_logger

logger = get_logger("clean_produits")


def clean_produits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Paramètre : DataFrame brut issu de extract_produits()
    Retourne  : DataFrame nettoyé prêt pour build_dimensions.
    """
    logger.info("── Nettoyage PRODUITS démarré ──")
    initial = len(df)

    # ── 1. Normalisation categorie / sous_categorie / marque ────
    for col in ["categorie", "sous_categorie", "marque", "fournisseur", "origine_pays"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()
    logger.info("[1] Catégories normalisées (title case)")

    # ── 2. Gestion actif ────────────────────────────────────────
    # Convertit "True"/"False" (str) en booléen
    df["actif"] = df["actif"].map({"True": True, "False": False, True: True, False: False})
    n_inactifs = (~df["actif"]).sum()
    logger.info(
        f"[2] Produits inactifs (actif=False) : {n_inactifs} "
        f"— conservés pour gestion SCD"
    )

    # ── 3. prix_catalogue null → 0.0 + flag ────────────────────
    df["prix_catalogue"] = pd.to_numeric(df["prix_catalogue"], errors="coerce")
    df["prix_manquant"]  = df["prix_catalogue"].isna().astype(int)
    n_prix_null = df["prix_manquant"].sum()
    df["prix_catalogue"] = df["prix_catalogue"].fillna(0.0)
    logger.info(f"[3] Prix catalogue nuls remplacés par 0.0 : {n_prix_null}")

    # ── 4. Tranche de prix (en MAD) ─────────────────────────────
    bins   = [0, 100, 500, 1000, 5000, float("inf")]
    labels = ["< 100 MAD", "100-500 MAD", "500-1000 MAD", "1000-5000 MAD", "> 5000 MAD"]
    df["tranche_prix"] = pd.cut(
        df["prix_catalogue"], bins=bins, labels=labels, right=False
    ).astype(str)
    df.loc[df["prix_catalogue"] == 0, "tranche_prix"] = "Non renseigné"

    # ── 5. date_creation → datetime ─────────────────────────────
    df["date_creation"] = pd.to_datetime(df["date_creation"], errors="coerce")

    final = len(df)
    logger.info(f"── PRODUITS : {initial} → {final} lignes après nettoyage ──")
    return df.reset_index(drop=True)