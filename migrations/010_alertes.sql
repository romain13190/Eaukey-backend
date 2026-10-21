-- 010 Alertes des automates (tâches A1 + A2, call Eaukey du 2026-10-19).
-- L'automate envoie ses alertes à l'API d'ingestion (POST /api/alertes, App Engine,
-- compte app_ingestion) ; l'équipe les consulte et les traite dans l'appli
-- (page /alertes, backend Render, compte app_backend). Une alerte peut devenir une panne.
-- Nouvelles tables : aucun impact sur l'existant.
-- A appliquer AVANT de déployer la nouvelle version de l'API d'ingestion et le backend.

-- Une ligne par alerte « regroupée » : les répétitions (même station + même code dans la
-- fenêtre de regroupement de l'API d'ingestion) incrémentent `occurrences`.
CREATE TABLE IF NOT EXISTS alertes (
  id           bigserial PRIMARY KEY,
  nom_automate text NOT NULL,
  code         text NOT NULL,
  libelle      text NOT NULL,
  niveau       text NOT NULL CHECK (niveau IN ('info', 'avertissement', 'critique')),
  classe       text NOT NULL CHECK (classe IN ('panne', 'seuil', 'information', 'maintenance')),
  statut       text NOT NULL DEFAULT 'nouvelle' CHECK (statut IN ('nouvelle', 'vue', 'traitee', 'ignoree')),
  valeur       double precision,
  unite        text,
  occurrences  integer NOT NULL DEFAULT 1 CHECK (occurrences >= 1),
  premiere_at  timestamptz NOT NULL,
  derniere_at  timestamptz NOT NULL,
  recue_at     timestamptz NOT NULL DEFAULT now(),
  panne_id     bigint REFERENCES pannes(id) ON DELETE SET NULL,
  commentaire  text,
  traitee_par  text,
  traitee_at   timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alertes_regroupement_idx ON alertes (nom_automate, code, derniere_at DESC);
CREATE INDEX IF NOT EXISTS alertes_statut_idx       ON alertes (statut, derniere_at DESC);
CREATE INDEX IF NOT EXISTS alertes_panne_idx        ON alertes (panne_id) WHERE panne_id IS NOT NULL;

COMMENT ON TABLE  alertes             IS 'Alertes envoyées par les automates (POST /api/alertes), regroupées par station + code';
COMMENT ON COLUMN alertes.code        IS 'Code d''alerte de l''automate (ex. P01)';
COMMENT ON COLUMN alertes.niveau      IS 'Gravité envoyée par l''automate (la plus haute reçue pour ce regroupement)';
COMMENT ON COLUMN alertes.classe      IS 'Classification : panne, seuil, information, maintenance (modifiable par l''équipe)';
COMMENT ON COLUMN alertes.statut      IS 'Suivi : nouvelle, vue, traitee, ignoree';
COMMENT ON COLUMN alertes.valeur      IS 'Dernière valeur mesurée transmise avec l''alerte (facultative)';
COMMENT ON COLUMN alertes.occurrences IS 'Nombre de messages reçus regroupés dans cette alerte';
COMMENT ON COLUMN alertes.premiere_at IS 'Horodatage automate de la première occurrence';
COMMENT ON COLUMN alertes.derniere_at IS 'Horodatage automate de la dernière occurrence';
COMMENT ON COLUMN alertes.recue_at    IS 'Dernière réception par l''API d''ingestion';
COMMENT ON COLUMN alertes.panne_id    IS 'Panne créée à partir de cette alerte (POST /alertes/{id}/panne)';

-- Une ligne par message reçu : sert à l'idempotence (un renvoi du même message par
-- l'automate n'est pas compté deux fois) et à l'historique des occurrences.
CREATE TABLE IF NOT EXISTS alertes_occurrences (
  id           bigserial PRIMARY KEY,
  alerte_id    bigint NOT NULL REFERENCES alertes(id) ON DELETE CASCADE,
  nom_automate text NOT NULL,
  code         text NOT NULL,
  horodatage   timestamptz NOT NULL,
  niveau       text NOT NULL,
  valeur       double precision,
  recue_at     timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT alertes_occurrences_message_uniq UNIQUE (nom_automate, code, horodatage)
);

CREATE INDEX IF NOT EXISTS alertes_occurrences_alerte_idx ON alertes_occurrences (alerte_id, horodatage DESC);

COMMENT ON TABLE alertes_occurrences IS 'Messages d''alerte reçus (un par envoi de l''automate) ; (nom_automate, code, horodatage) unique = idempotence';

-- Droits du compte applicatif du backend Render (DB_USER = app_backend) :
-- lecture + suivi (statut, classe, lien panne). Pas de suppression.
GRANT SELECT, UPDATE ON alertes TO app_backend;
GRANT SELECT ON alertes_occurrences TO app_backend;

-- Droits du compte de l'API d'ingestion App Engine (DB_USER = app_ingestion) :
-- création / regroupement des alertes, contrôle que la station existe.
GRANT SELECT, INSERT, UPDATE ON alertes TO app_ingestion;
GRANT SELECT, INSERT ON alertes_occurrences TO app_ingestion;
GRANT USAGE, SELECT ON SEQUENCE alertes_id_seq, alertes_occurrences_id_seq TO app_ingestion;
GRANT SELECT (nom_automate) ON automate TO app_ingestion;
-- NB : les privilèges par défaut de romain donnent aussi SELECT à integration_ro et lavance_ro.

-- Rollback (après avoir remis l'ancienne version de l'API d'ingestion et du backend) :
-- REVOKE SELECT (nom_automate) ON automate FROM app_ingestion;
-- DROP TABLE alertes_occurrences, alertes;
