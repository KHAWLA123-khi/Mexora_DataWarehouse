"""
config/settings.py
──────────────────
Point central de configuration du pipeline ETL Mexora.
Lit les variables d'environnement depuis .env et expose :
  - Les chemins vers les fichiers sources (CSV / JSON)
  - La configuration PostgreSQL (Data Warehouse)
  - Les paramètres de logging
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


BASE_DIR = Path(__file__).resolve().parent.parent


RAW_DIR = BASE_DIR / "data" / "raw"

RAW_FILES = {
    "commandes":      RAW_DIR / "commandes_mexora.csv",
    "clients":        RAW_DIR / "clients_mexora.csv",
    "produits":       RAW_DIR / "produits.json",
    "regions":        RAW_DIR / "regions_maroc.csv",
}


POSTGRES_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
    "database": os.getenv("PG_DB",       "mexora_dwh"),
}


LOG_DIR  = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "etl.log"