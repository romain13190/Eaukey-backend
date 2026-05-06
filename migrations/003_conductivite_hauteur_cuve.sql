-- 003_conductivite_hauteur_cuve.sql
-- Ajoute conductivites (traitement + renvoi) et hauteurs cuves (traitement + disconnection)
-- avec moyenne ET mediane sur donnees_semaine / donnees_mois / donnees_annees,
-- puis backfille toutes les lignes existantes depuis "mesures".
--
-- Apres application : les ETL (modifies en parallele) continueront de remplir ces
-- colonnes a chaque run. Les 6 mois historiques de donnees_annees [M-11..M-6],
-- jamais recalcules par l ETL annee, restent remplis grace au backfill.

BEGIN;

SET LOCAL TIME ZONE 'Europe/Paris';
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '15min';

-- ============================================================
-- 1) ALTER TABLE : ajout des 8 colonnes sur les 3 tables
-- ============================================================
ALTER TABLE donnees_semaine
  ADD COLUMN IF NOT EXISTS cond_traitement_moy                numeric,
  ADD COLUMN IF NOT EXISTS cond_traitement_med                numeric,
  ADD COLUMN IF NOT EXISTS cond_renvoi_moy                    numeric,
  ADD COLUMN IF NOT EXISTS cond_renvoi_med                    numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_traitement_moy_pc     numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_traitement_med_pc     numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_disconnection_moy_pc  numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_disconnection_med_pc  numeric;

ALTER TABLE donnees_mois
  ADD COLUMN IF NOT EXISTS cond_traitement_moy                numeric,
  ADD COLUMN IF NOT EXISTS cond_traitement_med                numeric,
  ADD COLUMN IF NOT EXISTS cond_renvoi_moy                    numeric,
  ADD COLUMN IF NOT EXISTS cond_renvoi_med                    numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_traitement_moy_pc     numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_traitement_med_pc     numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_disconnection_moy_pc  numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_disconnection_med_pc  numeric;

ALTER TABLE donnees_annees
  ADD COLUMN IF NOT EXISTS cond_traitement_moy                numeric,
  ADD COLUMN IF NOT EXISTS cond_traitement_med                numeric,
  ADD COLUMN IF NOT EXISTS cond_renvoi_moy                    numeric,
  ADD COLUMN IF NOT EXISTS cond_renvoi_med                    numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_traitement_moy_pc     numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_traitement_med_pc     numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_disconnection_moy_pc  numeric,
  ADD COLUMN IF NOT EXISTS hauteur_cuve_disconnection_med_pc  numeric;

-- ============================================================
-- 2) BACKFILL donnees_semaine (rétention = 7 jours, granularité = jour)
-- ============================================================
WITH stats AS (
  SELECT
    date_trunc('day', horodatage)::date AS jour,
    nom_automate,
    ROUND(AVG(conductivite_traitement)::numeric, 2)                                        AS cond_t_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY conductivite_traitement))::numeric, 2) AS cond_t_med,
    ROUND(AVG(conductivite_renvoi)::numeric, 2)                                            AS cond_r_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY conductivite_renvoi))::numeric, 2)     AS cond_r_med,
    ROUND(AVG(hauteur_cuve_traitement_pc)::numeric, 2)                                     AS hct_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hauteur_cuve_traitement_pc))::numeric, 2) AS hct_med,
    ROUND(AVG(hauteur_cuve_disconnection_pc)::numeric, 2)                                  AS hcd_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hauteur_cuve_disconnection_pc))::numeric, 2) AS hcd_med
  FROM mesures
  WHERE horodatage >= (date_trunc('day', now()) - INTERVAL '8 days')
    AND horodatage <  date_trunc('day', now())
  GROUP BY 1, 2
)
UPDATE donnees_semaine ds SET
  cond_traitement_moy               = s.cond_t_moy,
  cond_traitement_med               = s.cond_t_med,
  cond_renvoi_moy                   = s.cond_r_moy,
  cond_renvoi_med                   = s.cond_r_med,
  hauteur_cuve_traitement_moy_pc    = s.hct_moy,
  hauteur_cuve_traitement_med_pc    = s.hct_med,
  hauteur_cuve_disconnection_moy_pc = s.hcd_moy,
  hauteur_cuve_disconnection_med_pc = s.hcd_med,
  updated_at                        = now()
FROM stats s
WHERE ds.jour = s.jour AND ds.nom_automate = s.nom_automate;

-- ============================================================
-- 3) BACKFILL donnees_mois (rétention = 4 semaines, granularité = semaine ISO)
-- ============================================================
WITH stats AS (
  SELECT
    date_trunc('week', horodatage)::date AS semaine_debut,
    nom_automate,
    ROUND(AVG(conductivite_traitement)::numeric, 2)                                        AS cond_t_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY conductivite_traitement))::numeric, 2) AS cond_t_med,
    ROUND(AVG(conductivite_renvoi)::numeric, 2)                                            AS cond_r_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY conductivite_renvoi))::numeric, 2)     AS cond_r_med,
    ROUND(AVG(hauteur_cuve_traitement_pc)::numeric, 2)                                     AS hct_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hauteur_cuve_traitement_pc))::numeric, 2) AS hct_med,
    ROUND(AVG(hauteur_cuve_disconnection_pc)::numeric, 2)                                  AS hcd_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hauteur_cuve_disconnection_pc))::numeric, 2) AS hcd_med
  FROM mesures
  WHERE horodatage >= (date_trunc('week', now()) - INTERVAL '4 weeks')
    AND horodatage <  (date_trunc('week', now()) + INTERVAL '1 week')
  GROUP BY 1, 2
)
UPDATE donnees_mois dm SET
  cond_traitement_moy               = s.cond_t_moy,
  cond_traitement_med               = s.cond_t_med,
  cond_renvoi_moy                   = s.cond_r_moy,
  cond_renvoi_med                   = s.cond_r_med,
  hauteur_cuve_traitement_moy_pc    = s.hct_moy,
  hauteur_cuve_traitement_med_pc    = s.hct_med,
  hauteur_cuve_disconnection_moy_pc = s.hcd_moy,
  hauteur_cuve_disconnection_med_pc = s.hcd_med,
  updated_at                        = now()
FROM stats s
WHERE dm.semaine_debut = s.semaine_debut AND dm.nom_automate = s.nom_automate;

-- ============================================================
-- 4) BACKFILL donnees_annees (rétention = 12 mois, granularité = mois Europe/Paris)
--    Couvre [M-11..M] -> remplit aussi les 6 mois historiques jamais recalcules
-- ============================================================
WITH stats AS (
  SELECT
    date_trunc('month', (horodatage AT TIME ZONE 'Europe/Paris'))::date AS mois_debut,
    nom_automate,
    ROUND(AVG(conductivite_traitement)::numeric, 2)                                        AS cond_t_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY conductivite_traitement))::numeric, 2) AS cond_t_med,
    ROUND(AVG(conductivite_renvoi)::numeric, 2)                                            AS cond_r_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY conductivite_renvoi))::numeric, 2)     AS cond_r_med,
    ROUND(AVG(hauteur_cuve_traitement_pc)::numeric, 2)                                     AS hct_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hauteur_cuve_traitement_pc))::numeric, 2) AS hct_med,
    ROUND(AVG(hauteur_cuve_disconnection_pc)::numeric, 2)                                  AS hcd_moy,
    ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hauteur_cuve_disconnection_pc))::numeric, 2) AS hcd_med
  FROM mesures
  WHERE horodatage >= (date_trunc('month', now()) - INTERVAL '12 months')
    AND horodatage <  (date_trunc('month', now()) + INTERVAL '1 month')
  GROUP BY 1, 2
)
UPDATE donnees_annees da SET
  cond_traitement_moy               = s.cond_t_moy,
  cond_traitement_med               = s.cond_t_med,
  cond_renvoi_moy                   = s.cond_r_moy,
  cond_renvoi_med                   = s.cond_r_med,
  hauteur_cuve_traitement_moy_pc    = s.hct_moy,
  hauteur_cuve_traitement_med_pc    = s.hct_med,
  hauteur_cuve_disconnection_moy_pc = s.hcd_moy,
  hauteur_cuve_disconnection_med_pc = s.hcd_med,
  updated_at                        = now()
FROM stats s
WHERE da.mois_debut = s.mois_debut AND da.nom_automate = s.nom_automate;

COMMIT;
