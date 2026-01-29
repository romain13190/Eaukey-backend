# etl_donnees_annees_incremental.py
# Python 3.10+ | SQLAlchemy + pg8000 | Connexion directe

import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import URL
import logging
import json
from datetime import datetime, timezone
import sys

DDL_CREATE = """
CREATE TABLE IF NOT EXISTS donnees_annees (
  mois_debut             date      NOT NULL,   -- début de mois (date_trunc('month', ...))
  nom_automate           text      NOT NULL,

  -- Volumes mensuels (deltas de compteurs)
  vol_renvoi_m3          numeric   DEFAULT 0,
  vol_adoucie_m3         numeric   DEFAULT 0,
  vol_relevage_m3        numeric   DEFAULT 0,

  -- Taux de recyclage (0..1)
  taux_recyclage         numeric,

  -- Désinfection (depuis mesures.chlore_mv/2.5)
  taux_desinfection_avg  numeric,
  taux_desinfection_med  numeric,

  -- Pressions mensuelles (moyennes)
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

  -- Électricité (kWh) : delta mensuel
  conso_kwh              numeric,

  created_at             timestamptz DEFAULT now(),
  updated_at             timestamptz DEFAULT now(),

  PRIMARY KEY (mois_debut, nom_automate)
);

CREATE INDEX IF NOT EXISTS donnees_annees_idx_automate_mois
  ON donnees_annees (nom_automate, mois_debut);
"""

# --- Verrou transactionnel anti-collision (clé dédiée à l'ETL "années") ---
LOCK_SQL = "SELECT pg_try_advisory_xact_lock(1, 2002) AS ok"

# 1) INSERT pour {M-5 .. M} basé uniquement sur "mesures" (recalcule et insère)
INSERT_LAST_SIX_FROM_MESURES = """
WITH
m AS (
  SELECT date_trunc('month', now())::date AS m0
),
bounds AS (
  SELECT
    -- Inclure M-6 pour que LAG() de M-5 ait un précédent
    (SELECT (SELECT m0 FROM m) - INTERVAL '6 months') AS t_min,
    (SELECT (SELECT m0 FROM m) + INTERVAL '1 month')  AS t_max_plus,
    (SELECT (SELECT m0 FROM m) - INTERVAL '5 months') AS win_start
),
-- Max compteurs par mois/automate (Europe/Paris)
mm AS (
  SELECT
    date_trunc('month', (horodatage AT TIME ZONE 'Europe/Paris'))::date AS mois_debut,
    nom_automate,
    MAX(compteur_eau_renvoi_m3)   AS renvoi_max,
    MAX(compteur_eau_adoucie_m3)  AS adoucie_max,
    MAX(compteur_eau_relevage_m3) AS relevage_max,
    MAX(compteur_electrique_kwh)  AS elec_max
  FROM mesures, bounds
  WHERE horodatage >= bounds.t_min
    AND horodatage <  bounds.t_max_plus
  GROUP BY 1,2
),
-- Deltas via LAG (par automate)
vols AS (
  SELECT
    mois_debut, nom_automate,
    GREATEST(renvoi_max   - LAG(renvoi_max)   OVER (PARTITION BY nom_automate ORDER BY mois_debut), 0) AS vol_renvoi_m3,
    GREATEST(adoucie_max  - LAG(adoucie_max)  OVER (PARTITION BY nom_automate ORDER BY mois_debut), 0) AS vol_adoucie_m3,
    GREATEST(relevage_max - LAG(relevage_max) OVER (PARTITION BY nom_automate ORDER BY mois_debut), 0) AS vol_relevage_m3,
    GREATEST(elec_max     - LAG(elec_max)     OVER (PARTITION BY nom_automate ORDER BY mois_debut), 0) AS conso_kwh
  FROM mm
),
-- Désinfection & pH (Europe/Paris)
desinf_ph AS (
  SELECT
    date_trunc('month', (horodatage AT TIME ZONE 'Europe/Paris'))::date AS mois_debut,
    nom_automate,
    ROUND(AVG((chlore_mv / 2.5)::numeric), 2)                                          AS taux_desinfection_avg,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY (chlore_mv / 2.5)))::numeric, 2) AS taux_desinfection_med,
    ROUND(AVG((ph / 100.0)::numeric), 2)                                               AS ph_moyen,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY (ph / 100.0)))::numeric, 2)     AS ph_med
  FROM mesures, bounds
  WHERE horodatage >= bounds.t_min
    AND horodatage <  bounds.t_max_plus
  GROUP BY 1,2
),
-- Pressions / Température / Chlore (Europe/Paris)
press AS (
  SELECT
    date_trunc('month', (horodatage AT TIME ZONE 'Europe/Paris'))::date AS mois_debut,
    nom_automate,
    (ROUND(AVG(pression1_mbar)::numeric))::int AS p1_mbar,
    (ROUND(AVG(pression2_mbar)::numeric))::int AS p2_mbar,
    (ROUND(AVG(pression3_mbar)::numeric))::int AS p3_mbar,
    (ROUND(AVG(pression4_mbar)::numeric))::int AS p4_mbar,
    (ROUND(AVG(pression5_mbar)::numeric))::int AS p5_mbar
  FROM mesures, bounds
  WHERE horodatage >= bounds.t_min
    AND horodatage <  bounds.t_max_plus
  GROUP BY 1,2
),
temp_chlore AS (
  SELECT
    date_trunc('month', (horodatage AT TIME ZONE 'Europe/Paris'))::date AS mois_debut,
    nom_automate,
    ROUND((AVG(temperature_deg)::numeric / 10.0), 1)                                           AS temp_moy_c,
    ROUND(((percentile_cont(0.5) WITHIN GROUP (ORDER BY temperature_deg))::numeric / 10.0), 1) AS temp_med_c,
    ROUND(AVG(chlore_mv)::numeric, 2)                                                          AS chlore_moy_mg_l,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY chlore_mv))::numeric, 2)                AS chlore_med_mg_l
  FROM mesures, bounds
  WHERE horodatage >= bounds.t_min
    AND horodatage <  bounds.t_max_plus
  GROUP BY 1,2
),
final AS (
  SELECT
    v.mois_debut,
    v.nom_automate,
    v.vol_renvoi_m3,
    v.vol_adoucie_m3,
    v.vol_relevage_m3,
    v.conso_kwh,
    LEAST(GREATEST(COALESCE((v.vol_renvoi_m3::numeric / NULLIF(v.vol_renvoi_m3 + v.vol_adoucie_m3, 0)), 0), 0), 1) AS taux_recyclage,
    d.taux_desinfection_avg,
    d.taux_desinfection_med,
    p.p1_mbar, p.p2_mbar, p.p3_mbar, p.p4_mbar, p.p5_mbar,
    t.temp_moy_c, t.temp_med_c,
    t.chlore_moy_mg_l, t.chlore_med_mg_l,
    d.ph_moyen, d.ph_med
  FROM vols v
  LEFT JOIN desinf_ph   d ON (d.mois_debut, d.nom_automate) = (v.mois_debut, v.nom_automate)
  LEFT JOIN press       p ON (p.mois_debut, p.nom_automate) = (v.mois_debut, v.nom_automate)
  LEFT JOIN temp_chlore t ON (t.mois_debut, t.nom_automate) = (v.mois_debut, v.nom_automate)
  WHERE v.mois_debut >= (SELECT win_start FROM bounds)
)
INSERT INTO donnees_annees AS da (
  mois_debut, nom_automate,
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
  mois_debut, nom_automate,
  vol_renvoi_m3, vol_adoucie_m3, vol_relevage_m3,
  taux_recyclage,
  taux_desinfection_avg, taux_desinfection_med,
  p1_mbar, p2_mbar, p3_mbar, p4_mbar, p5_mbar,
  temp_moy_c, temp_med_c,
  chlore_moy_mg_l, chlore_med_mg_l,
  ph_moyen, ph_med,
  conso_kwh,
  now()
FROM final;
"""

# 2) Purge : conserver 12 mois glissants (M-11 .. M)
DELETE_KEEP_LAST12 = """
DELETE FROM donnees_annees da
USING (SELECT (date_trunc('month', now()) - INTERVAL '11 months')::date AS cutoff) t
WHERE da.mois_debut < t.cutoff;
"""

# 3) Effacer la fenêtre {M-5 .. M} avant réinsertion
DELETE_WINDOW_LAST_SIX = """
WITH
m AS (SELECT date_trunc('month', now())::date AS m0),
months AS (
  SELECT generate_series((SELECT m0 FROM m) - INTERVAL '5 months',
                         (SELECT m0 FROM m),
                         '1 month')::date AS mois_debut
)
DELETE FROM donnees_annees d
USING months
WHERE d.mois_debut = months.mois_debut;
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
    sh = logging.StreamHandler(sys.stdout)
    fmt = JsonFormatter()
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.propagate = False
    return logger

logger = _setup_logger("etl.annee")

def run(engine: sqlalchemy.engine.base.Engine):
    logger.info("run: start")
    try:
        with engine.begin() as conn:
            # Timeout court pour éviter l'attente bloquante + TZ cohérente
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL TIME ZONE 'Europe/Paris'"))

            # Un seul ETL "années" à la fois
            got = conn.execute(text(LOCK_SQL)).scalar()
            logger.info(f"advisory_lock_got={bool(got)}")
            if not got:
                logger.info("Skip: un autre ETL donnees_annees est déjà en cours (advisory lock).")
                return

            conn.execute(text(DDL_CREATE))
            # 1) Efface {M-5 .. M} puis insère les calculs depuis mesures
            res = conn.execute(text(DELETE_WINDOW_LAST_SIX))
            logger.info(f"delete_window_last_six.rowcount={res.rowcount}")
            res = conn.execute(text(INSERT_LAST_SIX_FROM_MESURES))
            logger.info(f"insert_last_six_from_mesures.rowcount={res.rowcount}")
            # 2) Purge au-delà de 12 mois glissants
            res = conn.execute(text(DELETE_KEEP_LAST12))
            logger.info(f"delete_keep_last12.rowcount={res.rowcount}")
    except Exception:
        logger.exception("run: failed")
        raise
    logger.info("run: success")

# --- Entrées "Cloud job" & exécution CLI ---
def main(request=None):
    engine = connect_with_connector()
    run(engine)
    return "done"

if __name__ == "__main__":
    try:
        logger.info("ETL annee: start")
        result = main()
        logger.info("ETL annee: done")
        print(result)
    except Exception:
        logger.exception("ETL annee: failed")
        sys.exit(1)
