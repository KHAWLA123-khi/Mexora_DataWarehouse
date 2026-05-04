"""
utils/logger.py
───────────────
Système de logging centralisé pour tout le pipeline ETL.

Usage dans n'importe quel module :
    from utils.logger import get_logger
    logger = get_logger("nom_module")
    logger.info("message")

Sortie : console (INFO) + fichier logs/etl.log (DEBUG)
"""

import logging
import sys
from config.settings import LOG_DIR, LOG_FILE


def get_logger(name: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Évite les handlers dupliqués si appelé plusieurs fois
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(name)-25s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Console (INFO et +) ─────────────────────────────────────
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    # ── Fichier (DEBUG et +) ────────────────────────────────────
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger