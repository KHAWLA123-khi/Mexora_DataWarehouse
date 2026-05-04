"""Nettoyage des donnees clients."""
"""
transform/clean_clients.py
──────────────────────────
Nettoyage du fichier clients_mexora.csv.

Problèmes traités :
  1. Doublons (même email, id_client différent) → on garde le plus récent
  2. Sexe encodé différemment (H/F, m/f, 1/0, Homme/Femme) → M/F/Inconnu
  3. date_naissance invalide (âge négatif ou > 120 ans) → NaT
  4. Emails mal formatés (sans @, sans domaine) → flag email_valide
  5. Ville incohérente → harmonisée via regions_maroc (même logique que commandes)
"""

import re
import pandas as pd
from utils.logger import get_logger

logger = get_logger("clean_clients")

# ── Mapping encodages sexe → M / F ─────────────────────────────
SEXE_MAP = {
    "h": "M", "m": "M", "1": "M", "homme": "M", "male": "M",
    "f": "F", "0": "F", "femme": "F", "female": "F", "fe": "F", "2": "F",
}

EMAIL_REGEX = re.compile(r'^[\w.+-]+@[\w-]+\.[a-z]{2,}$', re.IGNORECASE)


def _valider_email(email: str) -> bool:
    if not email or pd.isna(email):
        return False
    return bool(EMAIL_REGEX.match(email.strip()))


def _normaliser_sexe(val: str) -> str:
    if not val or pd.isna(val):
        return "Inconnu"
    return SEXE_MAP.get(val.strip().lower(), "Inconnu")


def _normaliser_ville(ville: str, mapping: dict) -> str:
    if not ville:
        return "Inconnue"
    return mapping.get(ville.strip().lower(), ville.strip().title())


def clean_clients(
    df: pd.DataFrame,
    df_regions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Paramètres
    ----------
    df         : DataFrame brut issu de extract_clients()
    df_regions : référentiel géographique propre

    Retourne
    --------
    DataFrame nettoyé avec colonnes enrichies.
    """
    logger.info("── Nettoyage CLIENTS démarré ──")
    initial = len(df)

    # ── 0. Mapping ville brute → ville standard ─────────────────
    mapping_villes = {}
    for _, row in df_regions.iterrows():
        standard = row["nom_ville_standard"]
        mapping_villes[standard.lower()] = standard
        mapping_villes[row["code_ville"].lower()] = standard
    variantes = {
        "casablanca": ["casa", "casbla"],
        "tanger":     ["tng", "tanja", "tnger"],
        "fès":        ["fes", "fez"],
        "meknès":     ["meknes"],
        "kénitra":    ["kenitra"],
        "salé":       ["sale"],
        "tétouan":    ["tetouan"],
    }
    for base, alts in variantes.items():
        for row in df_regions.itertuples():
            if base in row.nom_ville_standard.lower():
                for alt in alts:
                    mapping_villes[alt] = row.nom_ville_standard

    # ── 1. Gestion des doublons ─────────────────────────────────
    # Même email → on garde l'id_client le plus récent (date_inscription max)
    avant = len(df)
    df["date_inscription"] = pd.to_datetime(df["date_inscription"], errors="coerce")
    df = df.sort_values("date_inscription", ascending=False)
    df["email_norm"] = df["email"].str.strip().str.lower()
    df = df.drop_duplicates(subset=["email_norm"], keep="first")
    df = df.drop(columns=["email_norm"])
    logger.info(f"[1] Doublons (même email) supprimés : {avant - len(df)}")

    # ── 2. Normalisation sexe ───────────────────────────────────
    df["sexe"] = df["sexe"].apply(_normaliser_sexe)
    logger.info(f"[2] Sexe normalisé → M/F/Inconnu")

    # ── 3. Validation date_naissance ────────────────────────────
    df["date_naissance"] = pd.to_datetime(df["date_naissance"], errors="coerce")
    today = pd.Timestamp.today()
    age_min, age_max = 0, 120

    masque_invalide = (
        df["date_naissance"].notna() &
        (
            ((today - df["date_naissance"]).dt.days / 365.25 < age_min) |
            ((today - df["date_naissance"]).dt.days / 365.25 > age_max)
        )
    )
    n_invalides = masque_invalide.sum()
    df.loc[masque_invalide, "date_naissance"] = pd.NaT
    logger.info(f"[3] Dates naissance invalides mises à NaT : {n_invalides}")

    # Colonne âge calculé
    df["age"] = ((today - df["date_naissance"]).dt.days / 365.25).round(0).astype("Int64")

    # ── 4. Validation email ─────────────────────────────────────
    df["email_valide"] = df["email"].apply(_valider_email)
    n_invalides_mail = (~df["email_valide"]).sum()
    logger.info(f"[4] Emails invalides détectés : {n_invalides_mail}")

    # ── 5. Normalisation ville → standard ───────────────────────
    df["ville"] = df["ville"].apply(lambda v: _normaliser_ville(v, mapping_villes))
    logger.info(f"[5] Villes harmonisées")

    # ── 6. Jointure pour récupérer region_admin ─────────────────
    ref = df_regions[["nom_ville_standard", "region_admin", "zone_geo"]].copy()
    ref.columns = ["ville", "region_admin", "zone_geo"]
    df = df.merge(ref, on="ville", how="left")
    df["region_admin"] = df["region_admin"].fillna("Autre Maroc")
    df["zone_geo"]     = df["zone_geo"].fillna("Inconnue")

    # ── 7. Nettoyage texte nom / prenom ────────────────────────
    df["nom"]    = df["nom"].str.strip().str.title().fillna("Inconnu")
    df["prenom"] = df["prenom"].str.strip().str.title().fillna("Inconnu")
    df["nom_complet"] = df["prenom"] + " " + df["nom"]

    # ── 8. Segment client par défaut ───────────────────────────
    df["segment"] = "Standard"

    final = len(df)
    logger.info(f"── CLIENTS : {initial} → {final} lignes après nettoyage ──")
    return df.reset_index(drop=True)