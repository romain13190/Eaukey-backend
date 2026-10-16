-- 007 Pannes : champs de résolution (fiche d intervention SAV)
-- Colonnes optionnelles : l'ancien code continue de fonctionner sans elles.
ALTER TABLE pannes
  ADD COLUMN IF NOT EXISTS cause            text,
  ADD COLUMN IF NOT EXISTS action_realisee  text,
  ADD COLUMN IF NOT EXISTS pieces_changees  text,
  ADD COLUMN IF NOT EXISTS deplacement      boolean,
  ADD COLUMN IF NOT EXISTS intervenant      text,
  ADD COLUMN IF NOT EXISTS partage_sav      boolean NOT NULL DEFAULT true;

COMMENT ON COLUMN pannes.cause           IS 'Cause identifiée par le technicien';
COMMENT ON COLUMN pannes.action_realisee IS 'Action réalisée / solution appliquée';
COMMENT ON COLUMN pannes.pieces_changees IS 'Pièces remplacées (texte libre)';
COMMENT ON COLUMN pannes.deplacement     IS 'Intervention avec déplacement sur site (NULL = non renseigné)';
COMMENT ON COLUMN pannes.intervenant     IS 'Personne qui a traité la panne';
COMMENT ON COLUMN pannes.partage_sav     IS 'La fiche peut servir de connaissance à l''assistant SAV';

-- Rollback :
-- ALTER TABLE pannes DROP COLUMN cause, DROP COLUMN action_realisee, DROP COLUMN pieces_changees,
--   DROP COLUMN deplacement, DROP COLUMN intervenant, DROP COLUMN partage_sav;
