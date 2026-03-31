# ETL_volumes_total.py
# Précalcule le dernier compteur renvoi/adoucie par automate
# pour que l'endpoint /volumes/total soit instantané.
# Lance:  python /root/eaukey/eaukey_backend/ETL_all/ETL_volumes_total.py

import argparse
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import URL
import logging
import json
from datetime import datetime, timezone
import sys

DDL_CREATE = """
CREATE TABLE IF NOT EXISTS cache_volumes_total (
  nom_automate              text      NOT NULL PRIMARY KEY,
  compteur_eau_renvoi_m3    numeric,
  compteur_eau_adoucie_m3   numeric,
  updated_at                timestamptz DEFAULT now()
);
"""

# Pour chaque automate, recupere la derniere ligne de mesures
# avec les compteurs non-null. DISTINCT ON est efficace ici.
UPSERT = """
INSERT INTO cache_volumes_total (
  nom_automate, compteur_eau_renvoi_m3, compteur_eau_adoucie_m3, updated_at
)
SELECT
  nom_automate,
  compteur_eau_renvoi_m3,
  compteur_eau_adoucie_m3,
  now()
FROM (
  SELECT DISTINCT ON (nom_automate)
    nom_automate,
    compteur_eau_renvoi_m3,
    compteur_eau_adoucie_m3
  FROM mesures
  WHERE compteur_eau_renvoi_m3 IS NOT NULL
    AND compteur_eau_adoucie_m3 IS NOT NULL
  ORDER BY nom_automate, horodatage DESC
) sub
ON CONFLICT (nom_automate) DO UPDATE SET
  compteur_eau_renvoi_m3  = EXCLUDED.compteur_eau_renvoi_m3,
  compteur_eau_adoucie_m3 = EXCLUDED.compteur_eau_adoucie_m3,
  updated_at              = now();
"""

# Verrou transactionnel anti-collision (cle dediee a cet ETL)
LOCK_SQL = "SELECT pg_try_advisory_xact_lock(1, 2005) AS ok"


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

logger = _setup_logger("etl.volumes_total")

def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    """Connexion directe a Cloud SQL via IP publique."""
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

def run(engine: sqlalchemy.engine.base.Engine, rebuild: bool = False):
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL TIME ZONE 'Europe/Paris'"))

        got = conn.execute(text(LOCK_SQL)).scalar()
        if not got:
            logger.info("Skip: un autre ETL volumes_total est deja en cours.")
            return

        conn.execute(text(DDL_CREATE))
        if rebuild:
            logger.info("REBUILD: truncating cache_volumes_total")
            conn.execute(text("TRUNCATE cache_volumes_total"))
        conn.execute(text(UPSERT))
    logger.info("OK: cache_volumes_total mise a jour.")

def main(rebuild=False):
    engine = connect_with_connector()
    run(engine, rebuild=rebuild)
    return "OK", 200

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild", action="store_true",
                        help="Truncate destination table and recalculate from scratch")
    args = parser.parse_args()
    try:
        logger.info("ETL volumes_total: start")
        result = main(rebuild=args.rebuild)
        logger.info("ETL volumes_total: done")
        print(result)
    except Exception:
        logger.exception("ETL volumes_total: failed")
        sys.exit(1)
