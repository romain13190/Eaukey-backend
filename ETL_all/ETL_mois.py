# etl_donnees_mois.py
# Python 3.10+ | SQLAlchemy + pg8000
# Connexion directe à la DB (IP publique)

import pg8000
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import URL
import logging
import json
from datetime import datetime, timezone
import sys

DDL_CREATE = """
CREATE TABLE IF NOT EXISTS donnees_mois (
  semaine_debut          date      NOT NULL,
  nom_automate           text      NOT NULL,

  -- Volumes hebdo (deltas de compteurs)
  vol_renvoi_m3          numeric   DEFAULT 0,
  vol_adoucie_m3         numeric   DEFAULT 0,
  vol_relevage_m3        numeric   DEFAULT 0,

  -- Taux de recyclage (0..1)
  taux_recyclage         numeric,

  -- Désinfection (depuis mesures.chlore_mv/2.5)
  taux_desinfection_avg  numeric,
  taux_desinfection_med  numeric,

  -- Pressions hebdo (moyennes)
  p1_mbar                integer,
  p2_mbar                integer,
  p3_mbar                integer,
  p4_mbar                integer,
  p5_mbar                integer,

  -- Température & chlore (moyenne + médiane)
  temp_moy_c             numeric,
  temp_med_c             numeric,
  chlore_moy_mg_l        numeric,
  chlore_med_mg_l        numeric,

  -- pH (moyenne + médiane)
  ph_moyen               numeric,
  ph_med                 numeric,

  -- Électricité (kWh) : delta hebdo
  conso_kwh              numeric,

  created_at             timestamptz DEFAULT now(),
  updated_at             timestamptz DEFAULT now(),

  PRIMARY KEY (semaine_debut, nom_automate)
);

CREATE INDEX IF NOT EXISTS donnees_mois_idx_automate_semaine
  ON donnees_mois (nom_automate, semaine_debut);
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
  SELECT DISTINCT nom_automate FROM mesures_aggregate
  UNION
  SELECT DISTINCT nom_automate FROM mesures
),
grid AS (
  SELECT a.nom_automate, weeks.semaine_debut
  FROM autos a CROSS JOIN weeks
),

-- Fenêtre utilisée partout (S-3 début → S fin-exclusive)
bounds AS (
  SELECT
    (SELECT semaine_debut FROM weeks ORDER BY semaine_debut LIMIT 1)                             AS t_min,
    (SELECT (semaine_debut + INTERVAL '1 week') FROM weeks ORDER BY semaine_debut DESC LIMIT 1) AS t_max_plus
),

-- ===== Compteurs : max/sem puis deltas (depuis mesures) =====
w_mesures AS (
  SELECT
    date_trunc('week', horodatage)::date AS semaine_debut,
    nom_automate,
    compteur_eau_renvoi_m3   AS renvoi,
    compteur_eau_adoucie_m3  AS adoucie,
    compteur_eau_relevage_m3 AS relevage,
    compteur_electrique_kwh  AS elec
  FROM mesures, bounds
  WHERE horodatage >= bounds.t_min
    AND horodatage <  bounds.t_max_plus
),
weekly_max AS (
  SELECT
    semaine_debut, nom_automate,
    MAX(renvoi)   AS renvoi_max,
    MAX(adoucie)  AS adoucie_max,
    MAX(relevage) AS relevage_max,
    MAX(elec)     AS elec_max
  FROM w_mesures
  GROUP BY 1,2
),
vols AS (
  SELECT
    semaine_debut, nom_automate,
    GREATEST(renvoi_max   - LAG(renvoi_max)   OVER (PARTITION BY nom_automate ORDER BY semaine_debut), 0) AS vol_renvoi_m3,
    GREATEST(adoucie_max  - LAG(adoucie_max)  OVER (PARTITION BY nom_automate ORDER BY semaine_debut), 0) AS vol_adoucie_m3,
    GREATEST(relevage_max - LAG(relevage_max) OVER (PARTITION BY nom_automate ORDER BY semaine_debut), 0) AS vol_relevage_m3,
    GREATEST(elec_max     - LAG(elec_max)     OVER (PARTITION BY nom_automate ORDER BY semaine_debut), 0) AS conso_kwh
  FROM weekly_max
),

-- ===== Désinfection & pH (RESTE sur mesures) =====
w_mes AS (
  SELECT
    date_trunc('week', horodatage)::date AS semaine_debut,
    nom_automate,
    (chlore_mv / 2.5)::numeric AS desinf,
    (ph / 100.0)::numeric      AS ph_val
  FROM mesures, bounds
  WHERE horodatage >= bounds.t_min
    AND horodatage <  bounds.t_max_plus
),
desinf_ph AS (
  SELECT
    semaine_debut, nom_automate,
    ROUND(AVG(desinf)::numeric, 2)                                                    AS taux_desinfection_avg,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY desinf))::numeric, 2)         AS taux_desinfection_med,
    ROUND(AVG(ph_val)::numeric, 2)                                                   AS ph_moyen,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY ph_val))::numeric, 2)         AS ph_med
  FROM w_mes
  GROUP BY 1,2
),

-- ===== Pressions / Température / Chlore (depuis mesures) =====
w_mesures_ptc AS (
  SELECT
    date_trunc('week', horodatage)::date AS semaine_debut,
    nom_automate,
    pression1_mbar, pression2_mbar, pression3_mbar, pression4_mbar, pression5_mbar,
    temperature_deg,  -- dixièmes °C
    chlore_mv         -- mg/L
  FROM mesures, bounds
  WHERE horodatage >= bounds.t_min
    AND horodatage <  bounds.t_max_plus
),
press AS (
  SELECT
    semaine_debut, nom_automate,
    (ROUND(AVG(pression1_mbar)::numeric))::int AS p1_mbar,
    (ROUND(AVG(pression2_mbar)::numeric))::int AS p2_mbar,
    (ROUND(AVG(pression3_mbar)::numeric))::int AS p3_mbar,
    (ROUND(AVG(pression4_mbar)::numeric))::int AS p4_mbar,
    (ROUND(AVG(pression5_mbar)::numeric))::int AS p5_mbar
  FROM w_mesures_ptc
  GROUP BY 1,2
),
temp_chlore AS (
  SELECT
    semaine_debut, nom_automate,
    ROUND((AVG(temperature_deg)::numeric / 10.0), 1)                                             AS temp_moy_c,
    ROUND(((percentile_cont(0.5) WITHIN GROUP (ORDER BY temperature_deg))::numeric / 10.0), 1)   AS temp_med_c,
    ROUND(AVG(chlore_mv)::numeric, 2)                                                            AS chlore_moy_mg_l,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY chlore_mv))::numeric, 2)                  AS chlore_med_mg_l
  FROM w_mesures_ptc
  GROUP BY 1,2
),

final AS (
  SELECT
    g.semaine_debut,
    g.nom_automate,

    GREATEST(COALESCE(v.vol_renvoi_m3, 0), 0)   AS vol_renvoi_m3,
    GREATEST(COALESCE(v.vol_adoucie_m3, 0), 0)  AS vol_adoucie_m3,
    GREATEST(COALESCE(v.vol_relevage_m3, 0), 0) AS vol_relevage_m3,
    GREATEST(COALESCE(v.conso_kwh, 0), 0)       AS conso_kwh,

    LEAST(GREATEST(COALESCE(
      (v.vol_renvoi_m3::numeric / NULLIF(v.vol_renvoi_m3 + v.vol_adoucie_m3, 0)), 0
    ), 0), 1) AS taux_recyclage,

    d.taux_desinfection_avg,
    d.taux_desinfection_med,

    p.p1_mbar, p.p2_mbar, p.p3_mbar, p.p4_mbar, p.p5_mbar,

    t.temp_moy_c, t.temp_med_c,
    t.chlore_moy_mg_l, t.chlore_med_mg_l,

    d.ph_moyen, d.ph_med
  FROM grid g
  LEFT JOIN vols        v ON (v.semaine_debut, v.nom_automate) = (g.semaine_debut, g.nom_automate)
  LEFT JOIN desinf_ph   d ON (d.semaine_debut, d.nom_automate) = (g.semaine_debut, g.nom_automate)
  LEFT JOIN press       p ON (p.semaine_debut, p.nom_automate) = (g.semaine_debut, g.nom_automate)
  LEFT JOIN temp_chlore t ON (t.semaine_debut, t.nom_automate) = (g.semaine_debut, g.nom_automate)
)

INSERT INTO donnees_mois AS dm (
  semaine_debut, nom_automate,
  vol_renvoi_m3, vol_adoucie_m3, vol_relevage_m3,
  taux_recyclage,
  taux_desinfection_avg, taux_desinfection_med,
  p1_mbar, p2_mbar, p3_mbar, p4_mbar, p5_mbar,
  temp_moy_c, temp_med_c,
  chlore_moy_mg_l, chlore_med_mg_l,
  ph_moyen, ph_med,
  conso_kwh,
  updated_at
)
SELECT
  semaine_debut, nom_automate,
  vol_renvoi_m3, vol_adoucie_m3, vol_relevage_m3,
  taux_recyclage,
  taux_desinfection_avg, taux_desinfection_med,
  p1_mbar, p2_mbar, p3_mbar, p4_mbar, p5_mbar,
  temp_moy_c, temp_med_c,
  chlore_moy_mg_l, chlore_med_mg_l,
  ph_moyen, ph_med,
  conso_kwh,
  now()
FROM final
ON CONFLICT (semaine_debut, nom_automate) DO UPDATE SET
  vol_renvoi_m3         = EXCLUDED.vol_renvoi_m3,
  vol_adoucie_m3        = EXCLUDED.vol_adoucie_m3,
  vol_relevage_m3       = EXCLUDED.vol_relevage_m3,
  taux_recyclage        = EXCLUDED.taux_recyclage,
  taux_desinfection_avg = EXCLUDED.taux_desinfection_avg,
  taux_desinfection_med = EXCLUDED.taux_desinfection_med,
  p1_mbar               = EXCLUDED.p1_mbar,
  p2_mbar               = EXCLUDED.p2_mbar,
  p3_mbar               = EXCLUDED.p3_mbar,
  p4_mbar               = EXCLUDED.p4_mbar,
  p5_mbar               = EXCLUDED.p5_mbar,
  temp_moy_c            = EXCLUDED.temp_moy_c,
  temp_med_c            = EXCLUDED.temp_med_c,
  chlore_moy_mg_l       = EXCLUDED.chlore_moy_mg_l,
  chlore_med_mg_l       = EXCLUDED.chlore_med_mg_l,
  ph_moyen              = EXCLUDED.ph_moyen,
  ph_med                = EXCLUDED.ph_med,
  conso_kwh             = EXCLUDED.conso_kwh,
  updated_at            = now();
"""

# <-- Nettoyage : conserver uniquement 4 semaines (semaine courante incluse) -->
CLEANUP_KEEP_4 = """
DELETE FROM donnees_mois dm
USING (SELECT (date_trunc('week', now()) - INTERVAL '3 weeks')::date AS cutoff) t
WHERE dm.semaine_debut < t.cutoff;
"""

# <-- Verrou transactionnel pour éviter les deadlocks -->
LOCK_SQL = "SELECT pg_try_advisory_xact_lock(1, 2002) AS ok"  # clé dédiée à l’ETL donnees_mois

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

logger = _setup_logger("etl.mois")

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
        # Évite les blocages longs sur d'autres verrous
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        # Alignement métier sur le fuseau Europe/Paris
        conn.execute(text("SET LOCAL TIME ZONE 'Europe/Paris'"))

        # Un seul ETL 'mois' à la fois
        got = conn.execute(text(LOCK_SQL)).scalar()
        if not got:
            logger.info("Skip: un autre ETL donnees_mois est déjà en cours (advisory lock).")
            return

        # Ordre inchangé
        conn.execute(text(DDL_CREATE))
        conn.execute(text(UPSERT))
        conn.execute(text(CLEANUP_KEEP_4))
    logger.info("OK: donnees_mois mis à jour (4 semaines conservées).")

def main(*args):
    """Point d’entrée du script ETL."""
    engine = connect_with_connector()
    run(engine)
    return 'OK'

if __name__ == "__main__":
    try:
        logger.info("ETL mois: start")
        result = main()
        logger.info("ETL mois: done")
        print(result)
    except Exception:
        logger.exception("ETL mois: failed")
        sys.exit(1)
