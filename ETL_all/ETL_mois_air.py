# etl_donnees_mois_air.py
# Python 3.10+ | SQLAlchemy + pg8000 | Connexion directe
# ETL "mois" pour les recycleurs d'AIR (grain = semaine, fenetre 4 semaines glissantes)
# Lance:  python ETL_mois_air.py [--rebuild]
#
# Miroir de ETL_mois.py (recycleurs d'eau). Specificites AIR :
#   - pas de compteur cumulatif : le volume d'air hebdo = integration trapezoidale du debit
#     sur la semaine (et non MAX/sem - LAG comme pour l'eau)
#   - pressions : sentinelles capteur deconnecte (|x| >= 60000) filtrees (moy + med)
#   - moyennes + medianes pour temperatures / hygrometrie / pressions
#   - co2/cov/mes : colonnes creees des maintenant (capteurs futurs -> 0)
#   - seuls les automates qui remontent des grandeurs AIR sont agreges

import argparse
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import URL
import logging
import json
from datetime import datetime, timezone
import sys

DT_CAP_HOURS = 0.0833  # cap d'integration (5 min) sur les trous de donnees

DDL_CREATE = """
CREATE TABLE IF NOT EXISTS donnees_mois_air (
  semaine_debut          date      NOT NULL,
  nom_automate           text      NOT NULL,

  -- Debits d'air (moyennes + max hebdo, m3/h)
  debit1_moy_m3h         numeric,
  debit2_moy_m3h         numeric,
  debit3_moy_m3h         numeric,
  debit1_max_m3h         numeric,
  debit2_max_m3h         numeric,
  debit3_max_m3h         numeric,

  -- Volumes d'air traites/semaine (m3, integration du debit)
  vol_air1_m3            numeric   DEFAULT 0,
  vol_air2_m3            numeric   DEFAULT 0,
  vol_air3_m3            numeric   DEFAULT 0,

  -- Pressions differentielles (moy + med, Pa) -> encrassement filtres
  p1_pa                  numeric,
  p2_pa                  numeric,
  p3_pa                  numeric,
  p4_pa                  numeric,
  p1_med_pa              numeric,
  p2_med_pa              numeric,
  p3_med_pa              numeric,
  p4_med_pa              numeric,

  -- Temperatures (moy + med, degC)
  temp1_c                numeric,
  temp3_c                numeric,
  temp4_c                numeric,
  temp1_med_c            numeric,
  temp3_med_c            numeric,
  temp4_med_c            numeric,

  -- Hygrometrie (moy + med, %)
  hygro1_pc              numeric,
  hygro2_pc              numeric,
  hygro3_pc              numeric,
  hygro1_med_pc          numeric,
  hygro2_med_pc          numeric,
  hygro3_med_pc          numeric,

  -- Qualite d'air (moy, capteurs futurs)
  co2_ppm                numeric,
  cov_ppm                numeric,
  mes_microg_l           numeric,

  created_at             timestamptz DEFAULT now(),
  updated_at             timestamptz DEFAULT now(),

  PRIMARY KEY (semaine_debut, nom_automate)
);

CREATE INDEX IF NOT EXISTS donnees_mois_air_idx_automate_semaine
  ON donnees_mois_air (nom_automate, semaine_debut);
"""

UPSERT = """
WITH
w0 AS (SELECT date_trunc('week', now())::date AS w),
weeks AS (
  SELECT generate_series(
           (SELECT w FROM w0) - INTERVAL '3 weeks',
           (SELECT w FROM w0),
           '1 week'
         )::date AS semaine_debut
),
autos AS (
  SELECT DISTINCT nom_automate
  FROM mesures, (SELECT (SELECT w FROM w0) - INTERVAL '3 weeks' AS t_min,
                        (SELECT w FROM w0) + INTERVAL '1 week'  AS t_max_plus) b
  WHERE horodatage >= b.t_min AND horodatage < b.t_max_plus
    AND (debit1_m3h IS NOT NULL OR hygro1_pc IS NOT NULL OR pression1_pa IS NOT NULL)
),
grid AS (
  SELECT a.nom_automate, weeks.semaine_debut
  FROM autos a CROSS JOIN weeks
),
bounds AS (
  SELECT
    (SELECT semaine_debut FROM weeks ORDER BY semaine_debut LIMIT 1)                             AS t_min,
    (SELECT (semaine_debut + INTERVAL '1 week') FROM weeks ORDER BY semaine_debut DESC LIMIT 1) AS t_max_plus
),

-- ===== Fenetre de mesures AIR brutes =====
w_air AS (
  SELECT
    horodatage,
    date_trunc('week', horodatage)::date AS semaine_debut,
    nom_automate,
    debit1_m3h, debit2_m3h, debit3_m3h,
    pression1_pa, pression2_pa, pression3_pa, pression4_pa,
    temperature1, temperature3, temperature4,
    hygro1_pc, hygro2_pc, hygro3_pc,
    co2_ppm, cov_ppm, mes_microg_l
  FROM mesures, bounds
  WHERE horodatage >= bounds.t_min
    AND horodatage <  bounds.t_max_plus
    AND nom_automate IN (SELECT nom_automate FROM autos)
),

-- ===== Debits moyens & max /semaine =====
debit_sem AS (
  SELECT
    semaine_debut, nom_automate,
    ROUND(AVG(debit1_m3h)::numeric, 1) AS debit1_moy_m3h,
    ROUND(AVG(debit2_m3h)::numeric, 1) AS debit2_moy_m3h,
    ROUND(AVG(debit3_m3h)::numeric, 1) AS debit3_moy_m3h,
    ROUND(MAX(debit1_m3h)::numeric, 1) AS debit1_max_m3h,
    ROUND(MAX(debit2_m3h)::numeric, 1) AS debit2_max_m3h,
    ROUND(MAX(debit3_m3h)::numeric, 1) AS debit3_max_m3h
  FROM w_air
  GROUP BY 1,2
),

-- ===== Volumes d'air = integration trapezoidale du debit sur la semaine =====
flow AS (
  SELECT
    semaine_debut,
    nom_automate,
    debit1_m3h, debit2_m3h, debit3_m3h,
    LAG(debit1_m3h) OVER w AS lag_d1,
    LAG(debit2_m3h) OVER w AS lag_d2,
    LAG(debit3_m3h) OVER w AS lag_d3,
    LEAST(GREATEST(EXTRACT(EPOCH FROM (horodatage - LAG(horodatage) OVER w)) / 3600.0, 0), {DT_CAP}) AS dt_h
  FROM w_air
  WINDOW w AS (PARTITION BY nom_automate ORDER BY horodatage)
),
vol_sem AS (
  SELECT
    semaine_debut, nom_automate,
    ROUND(SUM(COALESCE((debit1_m3h + lag_d1) / 2.0, 0) * dt_h)::numeric, 2) AS vol_air1_m3,
    ROUND(SUM(COALESCE((debit2_m3h + lag_d2) / 2.0, 0) * dt_h)::numeric, 2) AS vol_air2_m3,
    ROUND(SUM(COALESCE((debit3_m3h + lag_d3) / 2.0, 0) * dt_h)::numeric, 2) AS vol_air3_m3
  FROM flow
  GROUP BY 1,2
),

-- ===== Pressions differentielles (moy + med, sentinelles filtrees) =====
press_sem AS (
  SELECT
    semaine_debut, nom_automate,
    ROUND(AVG(pression1_pa) FILTER (WHERE pression1_pa BETWEEN -60000 AND 60000)) AS p1_pa,
    ROUND(AVG(pression2_pa) FILTER (WHERE pression2_pa BETWEEN -60000 AND 60000)) AS p2_pa,
    ROUND(AVG(pression3_pa) FILTER (WHERE pression3_pa BETWEEN -60000 AND 60000)) AS p3_pa,
    ROUND(AVG(pression4_pa) FILTER (WHERE pression4_pa BETWEEN -60000 AND 60000)) AS p4_pa,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY pression1_pa) FILTER (WHERE pression1_pa BETWEEN -60000 AND 60000))::numeric) AS p1_med_pa,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY pression2_pa) FILTER (WHERE pression2_pa BETWEEN -60000 AND 60000))::numeric) AS p2_med_pa,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY pression3_pa) FILTER (WHERE pression3_pa BETWEEN -60000 AND 60000))::numeric) AS p3_med_pa,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY pression4_pa) FILTER (WHERE pression4_pa BETWEEN -60000 AND 60000))::numeric) AS p4_med_pa
  FROM w_air
  GROUP BY 1,2
),

-- ===== Temperatures / hygrometrie (moy + med) / qualite d'air =====
ths_sem AS (
  SELECT
    semaine_debut, nom_automate,
    ROUND(AVG(temperature1)::numeric, 1) AS temp1_c,
    ROUND(AVG(temperature3)::numeric, 1) AS temp3_c,
    ROUND(AVG(temperature4)::numeric, 1) AS temp4_c,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY temperature1))::numeric, 1) AS temp1_med_c,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY temperature3))::numeric, 1) AS temp3_med_c,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY temperature4))::numeric, 1) AS temp4_med_c,
    ROUND(AVG(hygro1_pc)::numeric, 1) AS hygro1_pc,
    ROUND(AVG(hygro2_pc)::numeric, 1) AS hygro2_pc,
    ROUND(AVG(hygro3_pc)::numeric, 1) AS hygro3_pc,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hygro1_pc))::numeric, 1) AS hygro1_med_pc,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hygro2_pc))::numeric, 1) AS hygro2_med_pc,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hygro3_pc))::numeric, 1) AS hygro3_med_pc,
    ROUND(AVG(co2_ppm)::numeric, 1)      AS co2_ppm,
    ROUND(AVG(cov_ppm)::numeric, 1)      AS cov_ppm,
    ROUND(AVG(mes_microg_l)::numeric, 1) AS mes_microg_l
  FROM w_air
  GROUP BY 1,2
),

final AS (
  SELECT
    g.semaine_debut,
    g.nom_automate,
    d.debit1_moy_m3h, d.debit2_moy_m3h, d.debit3_moy_m3h,
    d.debit1_max_m3h, d.debit2_max_m3h, d.debit3_max_m3h,
    GREATEST(COALESCE(v.vol_air1_m3, 0), 0) AS vol_air1_m3,
    GREATEST(COALESCE(v.vol_air2_m3, 0), 0) AS vol_air2_m3,
    GREATEST(COALESCE(v.vol_air3_m3, 0), 0) AS vol_air3_m3,
    p.p1_pa, p.p2_pa, p.p3_pa, p.p4_pa,
    p.p1_med_pa, p.p2_med_pa, p.p3_med_pa, p.p4_med_pa,
    t.temp1_c, t.temp3_c, t.temp4_c,
    t.temp1_med_c, t.temp3_med_c, t.temp4_med_c,
    t.hygro1_pc, t.hygro2_pc, t.hygro3_pc,
    t.hygro1_med_pc, t.hygro2_med_pc, t.hygro3_med_pc,
    t.co2_ppm, t.cov_ppm, t.mes_microg_l
  FROM grid g
  LEFT JOIN debit_sem d ON (d.semaine_debut, d.nom_automate) = (g.semaine_debut, g.nom_automate)
  LEFT JOIN vol_sem   v ON (v.semaine_debut, v.nom_automate) = (g.semaine_debut, g.nom_automate)
  LEFT JOIN press_sem p ON (p.semaine_debut, p.nom_automate) = (g.semaine_debut, g.nom_automate)
  LEFT JOIN ths_sem   t ON (t.semaine_debut, t.nom_automate) = (g.semaine_debut, g.nom_automate)
)

INSERT INTO donnees_mois_air AS dm (
  semaine_debut, nom_automate,
  debit1_moy_m3h, debit2_moy_m3h, debit3_moy_m3h,
  debit1_max_m3h, debit2_max_m3h, debit3_max_m3h,
  vol_air1_m3, vol_air2_m3, vol_air3_m3,
  p1_pa, p2_pa, p3_pa, p4_pa,
  p1_med_pa, p2_med_pa, p3_med_pa, p4_med_pa,
  temp1_c, temp3_c, temp4_c,
  temp1_med_c, temp3_med_c, temp4_med_c,
  hygro1_pc, hygro2_pc, hygro3_pc,
  hygro1_med_pc, hygro2_med_pc, hygro3_med_pc,
  co2_ppm, cov_ppm, mes_microg_l,
  updated_at
)
SELECT
  semaine_debut, nom_automate,
  debit1_moy_m3h, debit2_moy_m3h, debit3_moy_m3h,
  debit1_max_m3h, debit2_max_m3h, debit3_max_m3h,
  vol_air1_m3, vol_air2_m3, vol_air3_m3,
  p1_pa, p2_pa, p3_pa, p4_pa,
  p1_med_pa, p2_med_pa, p3_med_pa, p4_med_pa,
  temp1_c, temp3_c, temp4_c,
  temp1_med_c, temp3_med_c, temp4_med_c,
  hygro1_pc, hygro2_pc, hygro3_pc,
  hygro1_med_pc, hygro2_med_pc, hygro3_med_pc,
  co2_ppm, cov_ppm, mes_microg_l,
  now()
FROM final
ON CONFLICT (semaine_debut, nom_automate) DO UPDATE SET
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
  p1_med_pa      = EXCLUDED.p1_med_pa,
  p2_med_pa      = EXCLUDED.p2_med_pa,
  p3_med_pa      = EXCLUDED.p3_med_pa,
  p4_med_pa      = EXCLUDED.p4_med_pa,
  temp1_c        = EXCLUDED.temp1_c,
  temp3_c        = EXCLUDED.temp3_c,
  temp4_c        = EXCLUDED.temp4_c,
  temp1_med_c    = EXCLUDED.temp1_med_c,
  temp3_med_c    = EXCLUDED.temp3_med_c,
  temp4_med_c    = EXCLUDED.temp4_med_c,
  hygro1_pc      = EXCLUDED.hygro1_pc,
  hygro2_pc      = EXCLUDED.hygro2_pc,
  hygro3_pc      = EXCLUDED.hygro3_pc,
  hygro1_med_pc  = EXCLUDED.hygro1_med_pc,
  hygro2_med_pc  = EXCLUDED.hygro2_med_pc,
  hygro3_med_pc  = EXCLUDED.hygro3_med_pc,
  co2_ppm        = EXCLUDED.co2_ppm,
  cov_ppm        = EXCLUDED.cov_ppm,
  mes_microg_l   = EXCLUDED.mes_microg_l,
  updated_at     = now();
""".replace("{DT_CAP}", str(DT_CAP_HOURS))

# Nettoyage : conserver uniquement 4 semaines (semaine courante incluse)
CLEANUP_KEEP_4 = """
DELETE FROM donnees_mois_air dm
USING (SELECT (date_trunc('week', now()) - INTERVAL '3 weeks')::date AS cutoff) t
WHERE dm.semaine_debut < t.cutoff;
"""

# Verrou transactionnel dedie a l'ETL donnees_mois_air
LOCK_SQL = "SELECT pg_try_advisory_xact_lock(1, 2012) AS ok"

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

logger = _setup_logger("etl.mois_air")

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
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL TIME ZONE 'Europe/Paris'"))

        got = conn.execute(text(LOCK_SQL)).scalar()
        if not got:
            logger.info("Skip: un autre ETL donnees_mois_air est deja en cours (advisory lock).")
            return

        conn.execute(text(DDL_CREATE))
        if rebuild:
            logger.info("REBUILD: truncating donnees_mois_air")
            conn.execute(text("TRUNCATE donnees_mois_air"))
        conn.execute(text(UPSERT))
        conn.execute(text(CLEANUP_KEEP_4))
    logger.info("OK: donnees_mois_air mis a jour (4 semaines conservees).")

def main(rebuild=False):
    """Point d'entree du script ETL."""
    engine = connect_with_connector()
    run(engine, rebuild=rebuild)
    return 'OK'

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild", action="store_true",
                        help="Truncate destination table and recalculate from scratch")
    args = parser.parse_args()
    try:
        logger.info("ETL mois_air: start")
        result = main(rebuild=args.rebuild)
        logger.info("ETL mois_air: done")
        print(result)
    except Exception:
        logger.exception("ETL mois_air: failed")
        sys.exit(1)
