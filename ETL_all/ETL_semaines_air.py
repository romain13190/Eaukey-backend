# etl_donnees_semaine_air.py
# Python 3.10+ | SQLAlchemy + pg8000 | Connexion directe
# ETL "semaine" pour les recycleurs d'AIR (grain = jour, fenetre [J-7 .. J-1])
# Lance:  python ETL_semaines_air.py [--rebuild]
#
# Miroir de ETL_semaines.py (recycleurs d'eau) : meme structure / connexion / logging /
# advisory lock / fenetre glissante / UPSERT / cleanup. Seules changent les grandeurs
# (debits/pressions Pa/temperatures/hygrometrie/qualite d'air au lieu des volumes d'eau).
#
# Specificites AIR :
#   - aucune conversion d'unite (temp deja en degC, hygro en %, debit en m3/h, pression en Pa)
#   - volumes d'air = integration trapezoidale du debit (Somme (debit+lag)/2 * dt_heures), dt cape
#   - pressions : sentinelles capteur deconnecte (|x| >= 60000) filtrees avant la moyenne
#   - co2/cov/mes : colonnes creees des maintenant (capteurs pas encore cables -> valeurs 0)
#   - seuls les automates qui remontent des grandeurs AIR sont agreges

import argparse
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import URL
import logging
import json
from datetime import datetime, timezone
import sys

# Cap sur l'intervalle d'integration (en heures) : si un trou de donnees depasse ce cap,
# on n'integre que ce cap pour ne pas gonfler artificiellement le volume sur les coupures.
DT_CAP_HOURS = 0.0833  # 5 minutes

DDL_CREATE = """
CREATE TABLE IF NOT EXISTS donnees_semaine_air (
  jour                  date      NOT NULL,
  nom_automate          text      NOT NULL,

  -- Debits d'air moyens/jour (m3/h)
  debit1_moy_m3h        numeric,
  debit2_moy_m3h        numeric,
  debit3_moy_m3h        numeric,

  -- Debits d'air max/jour (m3/h)
  debit1_max_m3h        numeric,
  debit2_max_m3h        numeric,
  debit3_max_m3h        numeric,

  -- Volumes d'air traites/jour (m3, integration du debit)
  vol_air1_m3           numeric   DEFAULT 0,
  vol_air2_m3           numeric   DEFAULT 0,
  vol_air3_m3           numeric   DEFAULT 0,

  -- Pressions differentielles moyennes/jour (Pa, sentinelles filtrees) -> encrassement filtres
  p1_pa                 numeric,
  p2_pa                 numeric,
  p3_pa                 numeric,
  p4_pa                 numeric,

  -- Temperatures moyennes/jour (degC) -- pas de capteur temp2 sur ce materiel
  temp1_c               numeric,
  temp3_c               numeric,
  temp4_c               numeric,

  -- Hygrometrie moyenne/jour (%)
  hygro1_pc             numeric,
  hygro2_pc             numeric,
  hygro3_pc             numeric,

  -- Qualite d'air moyenne/jour (capteurs futurs)
  co2_ppm               numeric,
  cov_ppm               numeric,
  mes_microg_l          numeric,

  created_at            timestamptz DEFAULT now(),
  updated_at            timestamptz DEFAULT now(),

  PRIMARY KEY (jour, nom_automate)
);

CREATE INDEX IF NOT EXISTS donnees_semaine_air_idx_automate_jour
  ON donnees_semaine_air (nom_automate, jour);
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
-- Seuls les automates AIR (qui remontent debit / hygro / pression_pa) sont agreges
autos AS (
  SELECT DISTINCT nom_automate
  FROM mesures
  WHERE horodatage >= (SELECT d FROM today) - INTERVAL '7 days'
    AND horodatage <  (SELECT d FROM today)
    AND (debit1_m3h IS NOT NULL OR hygro1_pc IS NOT NULL OR pression1_pa IS NOT NULL)
),
grid AS (
  SELECT a.nom_automate, j.jour
  FROM autos a CROSS JOIN jours j
),

-- ===== Fenetre de mesures AIR brutes =====
w_air AS (
  SELECT
    horodatage,
    nom_automate,
    debit1_m3h, debit2_m3h, debit3_m3h,
    pression1_pa, pression2_pa, pression3_pa, pression4_pa,
    temperature1, temperature3, temperature4,
    hygro1_pc, hygro2_pc, hygro3_pc,
    co2_ppm, cov_ppm, mes_microg_l
  FROM mesures
  WHERE horodatage >= (SELECT d FROM today) - INTERVAL '7 days'
    AND horodatage <  (SELECT d FROM today)
    AND nom_automate IN (SELECT nom_automate FROM autos)
),

-- ===== Debits moyens & max /jour =====
debit_jour AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    ROUND(AVG(debit1_m3h)::numeric, 1) AS debit1_moy_m3h,
    ROUND(AVG(debit2_m3h)::numeric, 1) AS debit2_moy_m3h,
    ROUND(AVG(debit3_m3h)::numeric, 1) AS debit3_moy_m3h,
    ROUND(MAX(debit1_m3h)::numeric, 1) AS debit1_max_m3h,
    ROUND(MAX(debit2_m3h)::numeric, 1) AS debit2_max_m3h,
    ROUND(MAX(debit3_m3h)::numeric, 1) AS debit3_max_m3h
  FROM w_air
  GROUP BY 1,2
),

-- ===== Volumes d'air = integration trapezoidale du debit =====
flow AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    debit1_m3h, debit2_m3h, debit3_m3h,
    LAG(debit1_m3h) OVER w AS lag_d1,
    LAG(debit2_m3h) OVER w AS lag_d2,
    LAG(debit3_m3h) OVER w AS lag_d3,
    LEAST(
      GREATEST(
        EXTRACT(EPOCH FROM (horodatage - LAG(horodatage) OVER w)) / 3600.0,
        0
      ),
      {DT_CAP}
    ) AS dt_h
  FROM w_air
  WINDOW w AS (PARTITION BY nom_automate ORDER BY horodatage)
),
vol_jour AS (
  SELECT
    jour, nom_automate,
    ROUND(SUM(COALESCE((debit1_m3h + lag_d1) / 2.0, 0) * dt_h)::numeric, 2) AS vol_air1_m3,
    ROUND(SUM(COALESCE((debit2_m3h + lag_d2) / 2.0, 0) * dt_h)::numeric, 2) AS vol_air2_m3,
    ROUND(SUM(COALESCE((debit3_m3h + lag_d3) / 2.0, 0) * dt_h)::numeric, 2) AS vol_air3_m3
  FROM flow
  GROUP BY 1,2
),

-- ===== Pressions differentielles (sentinelles capteur deconnecte filtrees) =====
press_jour AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    ROUND(AVG(pression1_pa) FILTER (WHERE pression1_pa BETWEEN -60000 AND 60000)) AS p1_pa,
    ROUND(AVG(pression2_pa) FILTER (WHERE pression2_pa BETWEEN -60000 AND 60000)) AS p2_pa,
    ROUND(AVG(pression3_pa) FILTER (WHERE pression3_pa BETWEEN -60000 AND 60000)) AS p3_pa,
    ROUND(AVG(pression4_pa) FILTER (WHERE pression4_pa BETWEEN -60000 AND 60000)) AS p4_pa
  FROM w_air
  GROUP BY 1,2
),

-- ===== Temperatures / hygrometrie / qualite d'air =====
ths_jour AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    ROUND(AVG(temperature1)::numeric, 1) AS temp1_c,
    ROUND(AVG(temperature3)::numeric, 1) AS temp3_c,
    ROUND(AVG(temperature4)::numeric, 1) AS temp4_c,
    ROUND(AVG(hygro1_pc)::numeric, 1)    AS hygro1_pc,
    ROUND(AVG(hygro2_pc)::numeric, 1)    AS hygro2_pc,
    ROUND(AVG(hygro3_pc)::numeric, 1)    AS hygro3_pc,
    ROUND(AVG(co2_ppm)::numeric, 1)      AS co2_ppm,
    ROUND(AVG(cov_ppm)::numeric, 1)      AS cov_ppm,
    ROUND(AVG(mes_microg_l)::numeric, 1) AS mes_microg_l
  FROM w_air
  GROUP BY 1,2
),

final AS (
  SELECT
    g.jour,
    g.nom_automate,
    d.debit1_moy_m3h, d.debit2_moy_m3h, d.debit3_moy_m3h,
    d.debit1_max_m3h, d.debit2_max_m3h, d.debit3_max_m3h,
    GREATEST(COALESCE(v.vol_air1_m3, 0), 0) AS vol_air1_m3,
    GREATEST(COALESCE(v.vol_air2_m3, 0), 0) AS vol_air2_m3,
    GREATEST(COALESCE(v.vol_air3_m3, 0), 0) AS vol_air3_m3,
    p.p1_pa, p.p2_pa, p.p3_pa, p.p4_pa,
    t.temp1_c, t.temp3_c, t.temp4_c,
    t.hygro1_pc, t.hygro2_pc, t.hygro3_pc,
    t.co2_ppm, t.cov_ppm, t.mes_microg_l
  FROM grid g
  LEFT JOIN debit_jour d ON (d.jour, d.nom_automate) = (g.jour, g.nom_automate)
  LEFT JOIN vol_jour   v ON (v.jour, v.nom_automate) = (g.jour, g.nom_automate)
  LEFT JOIN press_jour p ON (p.jour, p.nom_automate) = (g.jour, g.nom_automate)
  LEFT JOIN ths_jour   t ON (t.jour, t.nom_automate) = (g.jour, g.nom_automate)
)

INSERT INTO donnees_semaine_air AS ds (
  jour, nom_automate,
  debit1_moy_m3h, debit2_moy_m3h, debit3_moy_m3h,
  debit1_max_m3h, debit2_max_m3h, debit3_max_m3h,
  vol_air1_m3, vol_air2_m3, vol_air3_m3,
  p1_pa, p2_pa, p3_pa, p4_pa,
  temp1_c, temp3_c, temp4_c,
  hygro1_pc, hygro2_pc, hygro3_pc,
  co2_ppm, cov_ppm, mes_microg_l,
  updated_at
)
SELECT
  jour, nom_automate,
  debit1_moy_m3h, debit2_moy_m3h, debit3_moy_m3h,
  debit1_max_m3h, debit2_max_m3h, debit3_max_m3h,
  vol_air1_m3, vol_air2_m3, vol_air3_m3,
  p1_pa, p2_pa, p3_pa, p4_pa,
  temp1_c, temp3_c, temp4_c,
  hygro1_pc, hygro2_pc, hygro3_pc,
  co2_ppm, cov_ppm, mes_microg_l,
  now()
FROM final
ON CONFLICT (jour, nom_automate) DO UPDATE SET
  debit1_moy_m3h = EXCLUDED.debit1_moy_m3h,
  debit2_moy_m3h = EXCLUDED.debit2_moy_m3h,
  debit3_moy_m3h = EXCLUDED.debit3_moy_m3h,
  debit1_max_m3h = EXCLUDED.debit1_max_m3h,
  debit2_max_m3h = EXCLUDED.debit2_max_m3h,
  debit3_max_m3h = EXCLUDED.debit3_max_m3h,
  vol_air1_m3    = EXCLUDED.vol_air1_m3,
  vol_air2_m3    = EXCLUDED.vol_air2_m3,
  vol_air3_m3    = EXCLUDED.vol_air3_m3,
  p1_pa          = EXCLUDED.p1_pa,
  p2_pa          = EXCLUDED.p2_pa,
  p3_pa          = EXCLUDED.p3_pa,
  p4_pa          = EXCLUDED.p4_pa,
  temp1_c        = EXCLUDED.temp1_c,
  temp3_c        = EXCLUDED.temp3_c,
  temp4_c        = EXCLUDED.temp4_c,
  hygro1_pc      = EXCLUDED.hygro1_pc,
  hygro2_pc      = EXCLUDED.hygro2_pc,
  hygro3_pc      = EXCLUDED.hygro3_pc,
  co2_ppm        = EXCLUDED.co2_ppm,
  cov_ppm        = EXCLUDED.cov_ppm,
  mes_microg_l   = EXCLUDED.mes_microg_l,
  updated_at     = now();
""".replace("{DT_CAP}", str(DT_CAP_HOURS))

# Supprimer la ligne du jour courant pour ne conserver que jusqu'a J-1
DELETE_TODAY_ROW = """
WITH today AS (SELECT date_trunc('day', now())::date AS d)
DELETE FROM donnees_semaine_air ds
USING today
WHERE ds.jour = today.d;
"""

# --- Cleanup: ne conserver que [J-7 .. J] ---
CLEANUP_KEEP_LAST_8_DAYS = """
DELETE FROM donnees_semaine_air ds
USING (SELECT (date_trunc('day', now()) - INTERVAL '7 days')::date AS cutoff) t
WHERE ds.jour < t.cutoff;
"""

# --- Verrou transactionnel anti-collision (cle dediee a l'ETL "semaine AIR") ---
LOCK_SQL = "SELECT pg_try_advisory_xact_lock(1, 2011) AS ok"

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

logger = _setup_logger("etl.semaines_air")

def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    """Connexion directe a Cloud SQL via IP publique (IP deja autorisee)."""
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
        # Petit timeout de verrou pour eviter d'attendre indefiniment
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        # Troncatures en Europe/Paris pour alignement metier
        conn.execute(text("SET LOCAL TIME ZONE 'Europe/Paris'"))

        # Un seul ETL "semaine AIR" a la fois
        got = conn.execute(text(LOCK_SQL)).scalar()
        if not got:
            logger.info("Skip: un autre ETL donnees_semaine_air est deja en cours (advisory lock).")
            return

        conn.execute(text(DDL_CREATE))
        if rebuild:
            logger.info("REBUILD: truncating donnees_semaine_air")
            conn.execute(text("TRUNCATE donnees_semaine_air"))
        # S'assurer qu'aucune ligne 'jour = aujourd'hui' n'est conservee
        conn.execute(text(DELETE_TODAY_ROW))
        conn.execute(text(UPSERT))
        conn.execute(text(CLEANUP_KEEP_LAST_8_DAYS))
    logger.info("OK: donnees_semaine_air mise a jour (fenetre [J-7 .. J] conservee).")

def main(rebuild=False):
    """Point d'entree du script ETL."""
    engine = connect_with_connector()
    run(engine, rebuild=rebuild)
    return "OK", 200

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild", action="store_true",
                        help="Truncate destination table and recalculate from scratch")
    args = parser.parse_args()
    try:
        logger.info("ETL semaines_air: start")
        result = main(rebuild=args.rebuild)
        logger.info("ETL semaines_air: done")
        print(result)
    except Exception:
        logger.exception("ETL semaines_air: failed")
        sys.exit(1)
