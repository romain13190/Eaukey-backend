-- 008 Base SAV : fiches de dépannage rédigées par l'équipe SAV Eaukey.
-- Jointes automatiquement aux questions posées à l'assistant SAV (POST /sav/message).
-- Nouvelle table : aucun impact sur l'existant.
CREATE TABLE IF NOT EXISTS sav_fiches (
  id          bigserial PRIMARY KEY,
  titre       text NOT NULL,
  symptomes   text,
  solution    text NOT NULL,
  pieces      text,
  type_panne  text,
  mots_cles   text,
  archivee    boolean NOT NULL DEFAULT false,
  created_by  text NOT NULL,
  updated_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  recherche   tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('french', coalesce(titre, '') || ' ' || coalesce(type_panne, '') || ' ' || coalesce(mots_cles, '')), 'A') ||
    setweight(to_tsvector('french', coalesce(symptomes, '')), 'B') ||
    setweight(to_tsvector('french', coalesce(solution, '') || ' ' || coalesce(pieces, '')), 'C')
  ) STORED
);

CREATE INDEX IF NOT EXISTS sav_fiches_recherche_idx ON sav_fiches USING gin (recherche);

COMMENT ON TABLE  sav_fiches           IS 'Base SAV : procédures de dépannage (problème -> solution), utilisées par l''assistant SAV';
COMMENT ON COLUMN sav_fiches.archivee  IS 'Fiche périmée : conservée mais plus utilisée par l''assistant';
COMMENT ON COLUMN sav_fiches.recherche IS 'Index plein texte (français) calculé automatiquement';

-- Droits du compte applicatif du backend Render (DB_USER = app_backend).
-- NB : les privilèges par défaut de romain donnent aussi SELECT à integration_ro et lavance_ro.
GRANT SELECT, INSERT, UPDATE, DELETE ON sav_fiches TO app_backend;
GRANT USAGE, SELECT ON SEQUENCE sav_fiches_id_seq TO app_backend;

-- Rollback :
-- DROP TABLE sav_fiches;
