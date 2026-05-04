"""Fichier principal du pipeline."""
"""
main.py
───────
Orchestrateur du pipeline ETL Mexora.
Lance : python main.py

Flux complet :
  [EXTRACT]  CSV / JSON  →  DataFrames bruts
  [TRANSFORM] Nettoyage  →  DataFrames propres  →  Schéma en étoile
  [LOAD]     PostgreSQL  →  Tables DWH + Index + Vues matérialisées
"""

import sys
from utils.logger import get_logger

from extract.extractor import extract_all

from transform.clean_commandes  import clean_commandes
from transform.clean_clients    import clean_clients
from transform.clean_produits   import clean_produits
from transform.build_dimensions import (
    build_dim_temps,
    build_dim_region,
    build_dim_client,
    build_dim_produit,
    build_fact_commandes,
)
from load.loader import (
    get_pg_engine,
    create_dwh_schema,
    load_table,
    create_materialized_views,
    refresh_materialized_views,
)

logger = get_logger("main")


def run_etl() -> None:
    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║       PIPELINE ETL MEXORA — DÉMARRAGE           ║")
    logger.info("╚══════════════════════════════════════════════════╝")

    try:
        # ════════════════════════════════════════════════════════
        # ÉTAPE 1 — EXTRACTION
        # ════════════════════════════════════════════════════════
        logger.info(">>> [1/3] EXTRACTION depuis data/raw/")
        raw = extract_all()
        # raw contient : commandes, clients, produits, regions

        # ════════════════════════════════════════════════════════
        # ÉTAPE 2 — TRANSFORMATION
        # ════════════════════════════════════════════════════════
        logger.info(">>> [2/3] TRANSFORMATION")

        # 2a. Nettoyage des sources brutes
        df_regions_clean  = raw["regions"]      # déjà propre, pas de nettoyage
        df_clients_clean  = clean_clients(raw["clients"],   df_regions_clean)
        df_produits_clean = clean_produits(raw["produits"])
        df_commandes_clean= clean_commandes(raw["commandes"], df_regions_clean)

        # 2b. Construction du schéma en étoile
        dim_temps   = build_dim_temps()
        dim_region  = build_dim_region(df_regions_clean)
        dim_client  = build_dim_client(df_clients_clean)
        dim_produit = build_dim_produit(df_produits_clean)
        fact        = build_fact_commandes(
            df_commandes_clean,
            dim_client,
            dim_produit,
            dim_region,
        )

        # Résumé de la transformation
        logger.info(f"  dim_temps    : {len(dim_temps):>8,} lignes")
        logger.info(f"  dim_region   : {len(dim_region):>8,} lignes")
        logger.info(f"  dim_client   : {len(dim_client):>8,} lignes")
        logger.info(f"  dim_produit  : {len(dim_produit):>8,} lignes")
        logger.info(f"  fact_commandes : {len(fact):>6,} lignes")

        # ════════════════════════════════════════════════════════
        # ÉTAPE 3 — CHARGEMENT PostgreSQL
        # ════════════════════════════════════════════════════════
        logger.info(">>> [3/3] CHARGEMENT dans PostgreSQL")
        engine = get_pg_engine()

        # Créer les tables et index
        create_dwh_schema(engine)

        # ⚠️ Ordre obligatoire : dimensions avant faits
        load_table(dim_temps,   "dim_temps",    engine, mode="append")
        load_table(dim_region,  "dim_region",   engine, mode="append")
        load_table(dim_client,  "dim_client",   engine, mode="append")
        load_table(dim_produit, "dim_produit",  engine, mode="append")
        load_table(fact,        "fact_commandes", engine, mode="append")

        # Créer et rafraîchir les vues matérialisées
        create_materialized_views(engine)
        refresh_materialized_views(engine)

        logger.info("╔══════════════════════════════════════════════════╗")
        logger.info("║   PIPELINE TERMINÉ AVEC SUCCÈS ✓                ║")
        logger.info("╚══════════════════════════════════════════════════╝")

    except Exception as e:
        logger.error(f"ERREUR CRITIQUE — Pipeline interrompu : {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run_etl()