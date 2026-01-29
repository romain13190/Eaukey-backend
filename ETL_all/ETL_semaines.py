# etl_donnees_semaine_safe.py
# Python 3.10+ | SQLAlchemy + pg8000 | Connexion directe
# Lance:  python /root/etl_donnees_semaine_safe.py

import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import URL
import logging
import json
from datetime import datetime, timezone
import sys

DDL_CREATE = """
CREATE TABLE IF NOT EXISTS donnees_semaine (
  jour                  date      NOT NULL,
  nom_automate          text      NOT NULL,

  -- Volumes/jour (deltas positives)
  vol_renvoi_m3         numeric   DEFAULT 0,
  vol_adoucie_m3        numeric   DEFAULT 0,
  vol_relevage_m3       numeric   DEFAULT 0,

  -- Taux recyclage (0..1)
  taux_recyclage        numeric,

  -- Désinfection (moyenne: chlore_mv/2.5)
  taux_desinfection     numeric,

  -- Pressions moyennes/jour
  p1_mbar               integer,
  p2_mbar               integer,
  p3_mbar               integer,
  p4_mbar               integer,
  p5_mbar               integer,

  -- Température moyenne/jour (°C)
  temp_moy_c            numeric,

  -- Chlore moyen/jour (mg/L)
  chlore_moy_mg_l       numeric,

  -- pH moyen/jour
  ph_moyen              numeric,

  -- Électricité: somme des deltas kWh/jour
  conso_kwh             numeric,

  created_at            timestamptz DEFAULT now(),
  updated_at            timestamptz DEFAULT now(),

  PRIMARY KEY (jour, nom_automate)
);

CREATE INDEX IF NOT EXISTS donnees_semaine_idx_automate_jour
  ON donnees_semaine (nom_automate, jour);
"""

UPSERT = """
WITH
today AS (
  SELECT date_trunc('day', now())::date AS d
),
jours AS (
  SELECT generate_series((SELECT d FROM today) - INTERVAL '7 days',
                         (SELECT d FROM today) - INTERVAL '1 day',
                         '1 day')::date AS jour
),
autos AS (
  SELECT DISTINCT nom_automate FROM mesures_aggregate
  UNION
  SELECT DISTINCT nom_automate FROM mesures
),
grid AS (
  SELECT a.nom_automate, j.jour
  FROM autos a CROSS JOIN jours j
),

-- ===== MESURES : volumes & indicateurs (RESTE sur mesures) =====
w_mesures AS (
  SELECT
    horodatage,
    nom_automate,
    compteur_eau_renvoi_m3,
    compteur_eau_adoucie_m3,
    compteur_eau_relevage_m3,
    ph / 100.0      AS ph_val,
    chlore_mv / 2.5 AS taux_desinf
  FROM mesures
  WHERE horodatage >= (SELECT d FROM today) - INTERVAL '7 days'
    AND horodatage <  (SELECT d FROM today)
),

deltas_mesures AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    GREATEST(compteur_eau_renvoi_m3
             - LAG(compteur_eau_renvoi_m3) OVER (PARTITION BY nom_automate ORDER BY horodatage), 0) AS d_renvoi,
    GREATEST(compteur_eau_adoucie_m3
             - LAG(compteur_eau_adoucie_m3) OVER (PARTITION BY nom_automate ORDER BY horodatage), 0) AS d_adoucie,
    GREATEST(compteur_eau_relevage_m3
             - LAG(compteur_eau_relevage_m3) OVER (PARTITION BY nom_automate ORDER BY horodatage), 0) AS d_relevage
  FROM w_mesures
),

vols_jour AS (
  SELECT
    jour, nom_automate,
    ROUND(SUM(d_renvoi)::numeric,   2) AS vol_renvoi_m3,
    ROUND(SUM(d_adoucie)::numeric,  2) AS vol_adoucie_m3,
    ROUND(SUM(d_relevage)::numeric, 2) AS vol_relevage_m3
  FROM deltas_mesures
  GROUP BY 1,2
),

ph_jour AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    ROUND(AVG(ph_val)::numeric, 2) AS ph_moyen
  FROM w_mesures
  GROUP BY 1,2
),

desinf_jour AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    ROUND(AVG(taux_desinf)::numeric, 2) AS taux_desinfection
  FROM w_mesures
  GROUP BY 1,2
),

-- ===== MESURES : pressions / temp / chlore / élec (depuis mesures) =====
w_moy AS (
  SELECT
    horodatage,
    nom_automate,
    pression1_mbar, pression2_mbar, pression3_mbar, pression4_mbar, pression5_mbar,
    temperature_deg,
    chlore_mv,
    compteur_electrique_kwh
  FROM mesures
  WHERE horodatage >= (SELECT d FROM today) - INTERVAL '7 days'
    AND horodatage <  (SELECT d FROM today)
),

press_jour AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    ROUND(AVG(pression1_mbar)) AS p1_mbar,
    ROUND(AVG(pression2_mbar)) AS p2_mbar,
    ROUND(AVG(pression3_mbar)) AS p3_mbar,
    ROUND(AVG(pression4_mbar)) AS p4_mbar,
    ROUND(AVG(pression5_mbar)) AS p5_mbar
  FROM w_moy
  GROUP BY 1,2
),

temp_jour AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    ROUND((AVG(temperature_deg) / 10.0)::numeric, 1) AS temp_moy_c
  FROM w_moy
  GROUP BY 1,2
),

chlore_moy_jour AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    ROUND(AVG(chlore_mv)::numeric, 2) AS chlore_moy_mg_l
  FROM w_moy
  GROUP BY 1,2
),

elec_deltas AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    GREATEST(compteur_electrique_kwh
             - LAG(compteur_electrique_kwh) OVER (PARTITION BY nom_automate ORDER BY horodatage), 0) AS d_kwh
  FROM w_moy
),

elec_jour AS (
  SELECT
    jour, nom_automate,
    ROUND(SUM(d_kwh)::numeric, 2) AS conso_kwh
  FROM elec_deltas
  GROUP BY 1,2
),

final AS (
  SELECT
    g.jour,
    g.nom_automate,

    GREATEST(COALESCE(v.vol_renvoi_m3, 0), 0)   AS vol_renvoi_m3,
    GREATEST(COALESCE(v.vol_adoucie_m3, 0), 0)  AS vol_adoucie_m3,
    GREATEST(COALESCE(v.vol_relevage_m3, 0), 0) AS vol_relevage_m3,

    LEAST(GREATEST(
      COALESCE(
        (v.vol_renvoi_m3::numeric
          / NULLIF(v.vol_renvoi_m3 + v.vol_adoucie_m3, 0)
        ), 0
      ), 0
    ), 1) AS taux_recyclage,

    GREATEST(d.taux_desinfection, 0) AS taux_desinfection,

    p.p1_mbar, p.p2_mbar, p.p3_mbar, p.p4_mbar, p.p5_mbar,

    t.temp_moy_c,

    GREATEST(c.chlore_moy_mg_l, 0) AS chlore_moy_mg_l,

    ph.ph_moyen,

    GREATEST(e.conso_kwh, 0) AS conso_kwh
  FROM grid g
  LEFT JOIN vols_jour       v  ON (v.jour,  v.nom_automate)  = (g.jour, g.nom_automate)
  LEFT JOIN desinf_jour     d  ON (d.jour,  d.nom_automate)  = (g.jour, g.nom_automate)
  LEFT JOIN press_jour      p  ON (p.jour,  p.nom_automate)  = (g.jour, g.nom_automate)
  LEFT JOIN temp_jour       t  ON (t.jour,  t.nom_automate)  = (g.jour, g.nom_automate)
  LEFT JOIN chlore_moy_jour c  ON (c.jour,  c.nom_automate)  = (g.jour, g.nom_automate)
  LEFT JOIN ph_jour         ph ON (ph.jour, ph.nom_automate) = (g.jour, g.nom_automate)
  LEFT JOIN elec_jour       e  ON (e.jour,  e.nom_automate)  = (g.jour, g.nom_automate)
)

INSERT INTO donnees_semaine AS ds (
  jour, nom_automate,
  vol_renvoi_m3, vol_adoucie_m3, vol_relevage_m3,
  taux_recyclage,
  taux_desinfection,
  p1_mbar, p2_mbar, p3_mbar, p4_mbar, p5_mbar,
  temp_moy_c,
  chlore_moy_mg_l,
  ph_moyen,
  conso_kwh,
  updated_at
)
SELECT
  jour, nom_automate,
  vol_renvoi_m3, vol_adoucie_m3, vol_relevage_m3,
  taux_recyclage,
  taux_desinfection,
  p1_mbar, p2_mbar, p3_mbar, p4_mbar, p5_mbar,
  temp_moy_c,
  chlore_moy_mg_l,
  ph_moyen,
  conso_kwh,
  now()
FROM final
ON CONFLICT (jour, nom_automate) DO UPDATE SET
  vol_renvoi_m3   = EXCLUDED.vol_renvoi_m3,
  vol_adoucie_m3  = EXCLUDED.vol_adoucie_m3,
  vol_relevage_m3 = EXCLUDED.vol_relevage_m3,
  taux_recyclage  = EXCLUDED.taux_recyclage,
  taux_desinfection = EXCLUDED.taux_desinfection,
  p1_mbar         = EXCLUDED.p1_mbar,
  p2_mbar         = EXCLUDED.p2_mbar,
  p3_mbar         = EXCLUDED.p3_mbar,
  p4_mbar         = EXCLUDED.p4_mbar,
  p5_mbar         = EXCLUDED.p5_mbar,
  temp_moy_c      = EXCLUDED.temp_moy_c,
  chlore_moy_mg_l = EXCLUDED.chlore_moy_mg_l,
  ph_moyen        = EXCLUDED.ph_moyen,
  conso_kwh       = EXCLUDED.conso_kwh,
  updated_at      = now();
"""

# Supprimer la ligne du jour courant pour ne conserver que jusqu'à J-1
DELETE_TODAY_ROW = """
WITH today AS (SELECT date_trunc('day', now())::date AS d)
DELETE FROM donnees_semaine ds
USING today
WHERE ds.jour = today.d;
"""

# --- Cleanup: ne conserver que [J-7 .. J] ---
CLEANUP_KEEP_LAST_8_DAYS = """
DELETE FROM donnees_semaine ds
USING (SELECT (date_trunc('day', now()) - INTERVAL '7 days')::date AS cutoff) t
WHERE ds.jour < t.cutoff;
"""

# --- Verrou transactionnel anti-collision (clé dédiée à l'ETL "semaine") ---
LOCK_SQL = "SELECT pg_try_advisory_xact_lock(1, 2001) AS ok"

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

logger = _setup_logger("etl.semaines")

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

def run(engine: sqlalchemy.engine.base.Engine):
    with engine.begin() as conn:
        # Petit timeout de verrou pour éviter d'attendre indéfiniment
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        # Troncatures en Europe/Paris pour alignement métier
        conn.execute(text("SET LOCAL TIME ZONE 'Europe/Paris'"))

        # Un seul ETL "semaine" à la fois
        got = conn.execute(text(LOCK_SQL)).scalar()
        if not got:
            logger.info("Skip: un autre ETL donnees_semaine est déjà en cours (advisory lock).")
            return

        conn.execute(text(DDL_CREATE))
        # S'assurer qu'aucune ligne 'jour = aujourd'hui' n'est conservée
        conn.execute(text(DELETE_TODAY_ROW))
        conn.execute(text(UPSERT))
        conn.execute(text(CLEANUP_KEEP_LAST_8_DAYS))
    logger.info("OK: donnees_semaine mise à jour (fenêtre [J-7 .. J] conservée).")

def main(*args):
    """Point d’entrée du script ETL."""
    engine = connect_with_connector()
    run(engine)
    return "OK", 200

if __name__ == "__main__":
    try:
        logger.info("ETL semaines: start")
        result = main()
        logger.info("ETL semaines: done")
        print(result)
    except Exception:
        logger.exception("ETL semaines: failed")
        sys.exit(1)
