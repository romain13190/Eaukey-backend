# etl_donnees_last24.py
# Python 3.10+ | SQLAlchemy + pg8000 | Connexion directe

import argparse
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import URL
import logging
import json
from datetime import datetime, timezone
import sys

DDL_CREATE = """
CREATE TABLE IF NOT EXISTS donnees_last24 (
  horodatage                       timestamptz NOT NULL,
  nom_automate                     text        NOT NULL,

  -- Champs bruts utiles depuis mesures
  compteur_eau_renvoi_m3           numeric,
  compteur_eau_adoucie_m3          numeric,
  compteur_eau_relevage_m3         numeric,
  compteur_electrique_kwh          numeric,
  hauteur_cuve_traitement_pc       numeric,
  hauteur_cuve_disconnection_pc    numeric,
  ph                               integer,
  chlore_mv                        numeric,

  created_at                       timestamptz DEFAULT now(),
  updated_at                       timestamptz DEFAULT now(),

  PRIMARY KEY (horodatage, nom_automate)
);

CREATE INDEX IF NOT EXISTS donnees_last24_idx_automate_ts
  ON donnees_last24 (nom_automate, horodatage DESC);
"""

# Déduplication par (horodatage, nom_automate) en prenant MAX() de chaque colonne
UPSERT_LAST24 = """
WITH src AS (
  SELECT
    horodatage,
    nom_automate,
    MAX(compteur_eau_renvoi_m3)        AS compteur_eau_renvoi_m3,
    MAX(compteur_eau_adoucie_m3)       AS compteur_eau_adoucie_m3,
    MAX(compteur_eau_relevage_m3)      AS compteur_eau_relevage_m3,
    MAX(compteur_electrique_kwh)       AS compteur_electrique_kwh,
    MAX(hauteur_cuve_traitement_pc)    AS hauteur_cuve_traitement_pc,
    MAX(hauteur_cuve_disconnection_pc) AS hauteur_cuve_disconnection_pc,
    MAX(ph)                            AS ph,
    MAX(chlore_mv)                     AS chlore_mv
  FROM mesures
  WHERE horodatage >= now() - INTERVAL '24 hours'
    AND horodatage <  now()
  GROUP BY horodatage, nom_automate
)
INSERT INTO donnees_last24 AS d (
  horodatage, nom_automate,
  compteur_eau_renvoi_m3, compteur_eau_adoucie_m3, compteur_eau_relevage_m3,
  compteur_electrique_kwh,
  hauteur_cuve_traitement_pc, hauteur_cuve_disconnection_pc,
  ph, chlore_mv,
  updated_at
)
SELECT
  horodatage, nom_automate,
  compteur_eau_renvoi_m3, compteur_eau_adoucie_m3, compteur_eau_relevage_m3,
  compteur_electrique_kwh,
  hauteur_cuve_traitement_pc, hauteur_cuve_disconnection_pc,
  ph, chlore_mv,
  now()
FROM src
ON CONFLICT (horodatage, nom_automate) DO UPDATE SET
  compteur_eau_renvoi_m3        = EXCLUDED.compteur_eau_renvoi_m3,
  compteur_eau_adoucie_m3       = EXCLUDED.compteur_eau_adoucie_m3,
  compteur_eau_relevage_m3      = EXCLUDED.compteur_eau_relevage_m3,
  compteur_electrique_kwh       = EXCLUDED.compteur_electrique_kwh,
  hauteur_cuve_traitement_pc    = EXCLUDED.hauteur_cuve_traitement_pc,
  hauteur_cuve_disconnection_pc = EXCLUDED.hauteur_cuve_disconnection_pc,
  ph                            = EXCLUDED.ph,
  chlore_mv                     = EXCLUDED.chlore_mv,
  updated_at                    = now();
"""

# Ne garder que la fenêtre glissante de 24h
CLEANUP_OLD = """
DELETE FROM donnees_last24
WHERE horodatage < now() - INTERVAL '24 hours';
"""

def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    """Connexion directe à Cloud SQL via IP publique (IP déjà autorisée)."""
    db_user = "romain"
    db_pass = "Lzl?h<P@zxle6xuL"
    db_name = "EaukeyCloudSQLv1"
    db_host = "35.241.253.254"

    url = URL.create(
        drivername="postgresql+pg8000",
        username=db_user,
        password=db_pass,
        host=db_host,
        port=5432,
        database=db_name,
    )
    return sqlalchemy.create_engine(url)

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)

def _setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler("/root/etl_logs.jsonl", encoding="utf-8")
    fh.setFormatter(JsonFormatter())
    logger.addHandler(fh)
    return logger

logger = _setup_logger("etl.last24")

def run(engine: sqlalchemy.engine.base.Engine, rebuild: bool = False):
    with engine.begin() as conn:
        conn.execute(text(DDL_CREATE))
        if rebuild:
            logger.info("REBUILD: truncating donnees_last24")
            conn.execute(text("TRUNCATE donnees_last24"))
        conn.execute(text(UPSERT_LAST24))
        conn.execute(text(CLEANUP_OLD))
    logger.info("OK: donnees_last24 mis à jour (fenêtre glissante 24h, dédupliquée, tous clients).")

def main(rebuild=False):
    """Point d'entrée du script ETL."""
    engine = connect_with_connector()
    run(engine, rebuild=rebuild)
    return "OK", 200

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild", action="store_true",
                        help="Truncate destination table and recalculate from scratch")
    args = parser.parse_args()
    try:
        logger.info("ETL last24: start")
        result = main(rebuild=args.rebuild)
        logger.info("ETL last24: done")
        print(result)
    except Exception:
        logger.exception("ETL last24: failed")
        sys.exit(1)
