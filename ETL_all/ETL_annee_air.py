# etl_donnees_annees_air_incremental.py
# Python 3.10+ | SQLAlchemy + pg8000 | Connexion directe
# ETL "annee" pour les recycleurs d'AIR (grain = mois, fenetre incrementale {M-5 .. M})
# Lance:  python ETL_annee_air.py [--rebuild]
#
# Miroir de ETL_annee.py (recycleurs d'eau) : DELETE fenetre {M-5..M} + INSERT (pas d'UPSERT),
# purge au-dela de 12 mois glissants, advisory lock dedie. Specificites AIR :
#   - volume d'air mensuel = integration trapezoidale du debit, partitionnee PAR MOIS
#     (pas de MAX/mois - LAG comme l'eau ; pas besoin de lire M-6)
#   - pressions : sentinelles (|x| >= 60000) filtrees (moy + med)
#   - moyennes + medianes pour temperatures / hygrometrie / pressions
#   - co2/cov/mes : colonnes creees des maintenant (capteurs futurs -> 0)

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
CREATE TABLE IF NOT EXISTS donnees_annees_air (
  mois_debut             date      NOT NULL,
  nom_automate           text      NOT NULL,

  -- Debits d'air (moyennes + max mensuels, m3/h)
  debit1_moy_m3h         numeric,
  debit2_moy_m3h         numeric,
  debit3_moy_m3h         numeric,
  debit1_max_m3h         numeric,
  debit2_max_m3h         numeric,
  debit3_max_m3h         numeric,

  -- Volumes d'air traites/mois (m3, integration du debit)
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

  PRIMARY KEY (mois_debut, nom_automate)
);

CREATE INDEX IF NOT EXISTS donnees_annees_air_idx_automate_mois
  ON donnees_annees_air (nom_automate, mois_debut);
"""

# --- Verrou transactionnel dedie a l'ETL "annee AIR" ---
LOCK_SQL = "SELECT pg_try_advisory_xact_lock(1, 2013) AS ok"

# 1) INSERT pour {M-5 .. M} base uniquement sur "mesures"
INSERT_LAST_SIX_FROM_MESURES = """
WITH
m AS (
  SELECT date_trunc('month', now())::date AS m0
),
bounds AS (
  SELECT
    (SELECT (SELECT m0 FROM m) - INTERVAL '5 months') AS t_min,
    (SELECT (SELECT m0 FROM m) + INTERVAL '1 month')  AS t_max_plus
),
-- Seuls les automates AIR sont agreges
autos AS (
  SELECT DISTINCT nom_automate
  FROM mesures, bounds
  WHERE horodatage >= bounds.t_min AND horodatage < bounds.t_max_plus
    AND (debit1_m3h IS NOT NULL OR hygro1_pc IS NOT NULL OR pression1_pa IS NOT NULL)
),
-- Fenetre de mesures AIR brutes (mois en Europe/Paris)
w_air AS (
  SELECT
    horodatage,
    date_trunc('month', (horodatage AT TIME ZONE 'Europe/Paris'))::date AS mois_debut,
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
-- Debits moyens & max /mois
debit_mois AS (
  SELECT
    mois_debut, nom_automate,
    ROUND(AVG(debit1_m3h)::numeric, 1) AS debit1_moy_m3h,
    ROUND(AVG(debit2_m3h)::numeric, 1) AS debit2_moy_m3h,
    ROUND(AVG(debit3_m3h)::numeric, 1) AS debit3_moy_m3h,
    ROUND(MAX(debit1_m3h)::numeric, 1) AS debit1_max_m3h,
    ROUND(MAX(debit2_m3h)::numeric, 1) AS debit2_max_m3h,
    ROUND(MAX(debit3_m3h)::numeric, 1) AS debit3_max_m3h
  FROM w_air
  GROUP BY 1,2
),
-- Volumes d'air = integration trapezoidale du debit, partitionnee PAR MOIS
flow AS (
  SELECT
    mois_debut,
    nom_automate,
    debit1_m3h, debit2_m3h, debit3_m3h,
    LAG(debit1_m3h) OVER w AS lag_d1,
    LAG(debit2_m3h) OVER w AS lag_d2,
    LAG(debit3_m3h) OVER w AS lag_d3,
    LEAST(GREATEST(EXTRACT(EPOCH FROM (horodatage - LAG(horodatage) OVER w)) / 3600.0, 0), {DT_CAP}) AS dt_h
  FROM w_air
  WINDOW w AS (PARTITION BY nom_automate, mois_debut ORDER BY horodatage)
),
vol_mois AS (
  SELECT
    mois_debut, nom_automate,
    ROUND(SUM(COALESCE((debit1_m3h + lag_d1) / 2.0, 0) * dt_h)::numeric, 2) AS vol_air1_m3,
    ROUND(SUM(COALESCE((debit2_m3h + lag_d2) / 2.0, 0) * dt_h)::numeric, 2) AS vol_air2_m3,
    ROUND(SUM(COALESCE((debit3_m3h + lag_d3) / 2.0, 0) * dt_h)::numeric, 2) AS vol_air3_m3
  FROM flow
  GROUP BY 1,2
),
-- Pressions differentielles (moy + med, sentinelles filtrees)
press_mois AS (
  SELECT
    mois_debut, nom_automate,
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
-- Temperatures / hygrometrie (moy + med) / qualite d'air
ths_mois AS (
  SELECT
    mois_debut, nom_automate,
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
    d.mois_debut,
    d.nom_automate,
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
  FROM debit_mois d
  LEFT JOIN vol_mois   v ON (v.mois_debut, v.nom_automate) = (d.mois_debut, d.nom_automate)
  LEFT JOIN press_mois p ON (p.mois_debut, p.nom_automate) = (d.mois_debut, d.nom_automate)
  LEFT JOIN ths_mois   t ON (t.mois_debut, t.nom_automate) = (d.mois_debut, d.nom_automate)
)
INSERT INTO donnees_annees_air AS da (
  mois_debut, nom_automate,
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
  mois_debut, nom_automate,
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
FROM final;
""".replace("{DT_CAP}", str(DT_CAP_HOURS))

# 2) Purge : conserver 12 mois glissants (M-11 .. M)
DELETE_KEEP_LAST12 = """
DELETE FROM donnees_annees_air da
USING (SELECT (date_trunc('month', now()) - INTERVAL '11 months')::date AS cutoff) t
WHERE da.mois_debut < t.cutoff;
"""

# 3) Effacer la fenetre {M-5 .. M} avant reinsertion
DELETE_WINDOW_LAST_SIX = """
WITH
m AS (SELECT date_trunc('month', now())::date AS m0),
months AS (
  SELECT generate_series((SELECT m0 FROM m) - INTERVAL '5 months',
                         (SELECT m0 FROM m),
                         '1 month')::date AS mois_debut
)
DELETE FROM donnees_annees_air d
USING months
WHERE d.mois_debut = months.mois_debut;
"""

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
    sh = logging.StreamHandler(sys.stdout)
    fmt = JsonFormatter()
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False
    return logger

logger = _setup_logger("etl.annee_air")

def run(engine: sqlalchemy.engine.base.Engine, rebuild: bool = False):
    logger.info("run: start")
    try:
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL TIME ZONE 'Europe/Paris'"))

            got = conn.execute(text(LOCK_SQL)).scalar()
            logger.info(f"advisory_lock_got={bool(got)}")
            if not got:
                logger.info("Skip: un autre ETL donnees_annees_air est deja en cours (advisory lock).")
                return

            conn.execute(text(DDL_CREATE))
            if rebuild:
                logger.info("REBUILD: truncating donnees_annees_air")
                conn.execute(text("TRUNCATE donnees_annees_air"))
            else:
                res = conn.execute(text(DELETE_WINDOW_LAST_SIX))
                logger.info(f"delete_window_last_six.rowcount={res.rowcount}")
            res = conn.execute(text(INSERT_LAST_SIX_FROM_MESURES))
            logger.info(f"insert_last_six_from_mesures.rowcount={res.rowcount}")
            res = conn.execute(text(DELETE_KEEP_LAST12))
            logger.info(f"delete_keep_last12.rowcount={res.rowcount}")
    except Exception:
        logger.exception("run: failed")
        raise
    logger.info("run: success")

def main(request=None, rebuild=False):
    engine = connect_with_connector()
    run(engine, rebuild=rebuild)
    return "done"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild", action="store_true",
                        help="Truncate destination table and recalculate from scratch")
    args = parser.parse_args()
    try:
        logger.info("ETL annee_air: start")
        result = main(rebuild=args.rebuild)
        logger.info("ETL annee_air: done")
        print(result)
    except Exception:
        logger.exception("ETL annee_air: failed")
        sys.exit(1)
