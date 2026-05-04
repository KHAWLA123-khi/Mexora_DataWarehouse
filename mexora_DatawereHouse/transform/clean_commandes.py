"""Nettoyage des donnees commandes."""
"""
transform/clean_commandes.py
─────────────────────────────
Nettoyage du fichier commandes_mexora.csv.

Problèmes traités :
  1. Doublons sur id_commande  (~3%)
  2. Formats de dates mixtes   (YYYY-MM-DD / DD/MM/YYYY / Mon DD YYYY …)
  3. Ville de livraison sale   (Tanger / TNG / TANJA …) → harmonisée via régions
  4. id_livreur manquant       (7%) → remplacé par "INCONNU"
  5. quantite négative         → supprimée
  6. prix_unitaire = 0         → ligne supprimée
  7. statut non-standard       (OK/KO/DONE…) → mappé vers valeurs propres
"""

import re
import pandas as pd
from utils.logger import get_logger

logger = get_logger("clean_commandes")

# ── Mapping statuts non-standards → valeurs normalisées ────────
STATUT_MAP = {
    "livree":         "livree",
    "en_cours":       "en_cours",
    "annulee":        "annulee",
    "retournee":      "retournee",
    "en_preparation": "en_preparation",
    "expedie":        "expedie",
    # valeurs non-standards
    "ok":        "livree",
    "done":      "livree",
    "livree":    "livree",
    "ko":        "annulee",
    "oui":       "livree",
    "1":         "livree",
    "validated": "livree",
}


def _parse_date(val: str) -> pd.Timestamp | None:
    """
    Tente de parser une date dans plusieurs formats courants.
    Retourne NaT si aucun format ne correspond.
    """
    if not val or pd.isna(val):
        return pd.NaT
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%b %d %Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return pd.to_datetime(val, format=fmt)
        except ValueError:
            continue
    return pd.NaT


def _normaliser_ville(ville: str, mapping_villes: dict) -> str:
    """
    Harmonise la ville de livraison en utilisant le mapping
    construit depuis regions_maroc.csv.
    """
    if not ville:
        return "Inconnue"
    return mapping_villes.get(ville.strip().lower(), ville.strip().title())


def clean_commandes(
    df: pd.DataFrame,
    df_regions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Paramètres
    ----------
    df         : DataFrame brut issu de extract_commandes()
    df_regions : DataFrame propre issu de extract_regions()

    Retourne
    --------
    DataFrame nettoyé prêt pour build_dimensions.
    """
    logger.info("── Nettoyage COMMANDES démarré ──")
    initial = len(df)

    # ── 0. Construire le mapping ville brute → ville standard ───
    # On crée un dictionnaire {forme_brute_lower: nom_standard}
    # en utilisant regions_maroc comme référentiel
    mapping_villes = {}
    for _, row in df_regions.iterrows():
        standard = row["nom_ville_standard"]
        # On ajoute le nom standard lui-même + le code ville
        mapping_villes[standard.lower()] = standard
        mapping_villes[row["code_ville"].lower()] = standard
        # Variantes connues pour les villes avec accents
        variantes = {
            "casablanca": ["casa","casbla"],
            "tanger":     ["tng","tanja","tnger"],
            "fès":        ["fes","fez","fès"],
            "meknès":     ["meknes"],
            "kénitra":    ["kenitra"],
            "salé":       ["sale"],
            "tétouan":    ["tetouan"],
        }
        for base, alts in variantes.items():
            if base in standard.lower():
                for alt in alts:
                    mapping_villes[alt] = standard

    # ── 1. Suppression des doublons sur id_commande ─────────────
    avant = len(df)
    df = df.drop_duplicates(subset=["id_commande"], keep="first")
    logger.info(f"[1] Doublons supprimés : {avant - len(df)}")

    # ── 2. Parsing des dates ────────────────────────────────────
    df["date_commande"]  = df["date_commande"].apply(_parse_date)
    df["date_livraison"] = df["date_livraison"].apply(_parse_date)
    n_dates_nulles = df["date_commande"].isna().sum()
    logger.info(f"[2] Dates commande non parsées : {n_dates_nulles}")
    df = df.dropna(subset=["date_commande"])
    logger.info(f"[2] Lignes supprimées (date invalide) : {n_dates_nulles}")

    # ── 3. Harmonisation ville_livraison ────────────────────────
    df["ville_livraison"] = df["ville_livraison"].apply(
        lambda v: _normaliser_ville(v, mapping_villes)
    )
    logger.info("[3] Villes de livraison harmonisées")

    # ── 4. id_livreur manquant → 'INCONNU' ─────────────────────
    df["id_livreur"] = df["id_livreur"].replace("", "INCONNU").fillna("INCONNU")
    n_inconnu = (df["id_livreur"] == "INCONNU").sum()
    logger.info(f"[4] id_livreur manquants remplacés : {n_inconnu}")

    # ── 5. quantite : conversion + suppression négatives ────────
    df["quantite"] = pd.to_numeric(df["quantite"], errors="coerce")
    avant = len(df)
    df = df[df["quantite"] > 0]
    logger.info(f"[5] Lignes supprimées (quantité ≤ 0) : {avant - len(df)}")

    # ── 6. prix_unitaire : conversion + suppression si = 0 ─────
    df["prix_unitaire"] = pd.to_numeric(df["prix_unitaire"], errors="coerce")
    avant = len(df)
    df = df[df["prix_unitaire"] > 0]
    logger.info(f"[6] Lignes supprimées (prix = 0) : {avant - len(df)}")

    # ── 7. Normalisation statut ─────────────────────────────────
    df["statut"] = (
        df["statut"]
        .str.strip()
        .str.lower()
        .map(STATUT_MAP)
        .fillna("inconnu")
    )
    n_inconnu_statut = (df["statut"] == "inconnu").sum()
    logger.info(f"[7] Statuts non mappés (→ 'inconnu') : {n_inconnu_statut}")

    # ── 8. Calcul montant ligne ─────────────────────────────────
    df["montant_ligne"] = (df["quantite"] * df["prix_unitaire"]).round(2)

    # ── 9. Colonne flag retour ──────────────────────────────────
    df["a_retour"] = (df["statut"] == "retournee").astype(int)

    final = len(df)
    logger.info(f"── COMMANDES : {initial} → {final} lignes après nettoyage ──")
    return df.reset_index(drop=True)