-- 004_urls_images_corrections.sql
-- Corrections / validation humaine (super_admin) des predictions IA de qualite d'eau.
-- On NE touche PAS aux colonnes pred_* : on ajoute des colonnes de correction.
-- Valeur affichee dans l'app = COALESCE(corr_*, pred_*).
--   * validated_at/by remplis, corr_* NULL  -> le super_admin a valide la prediction du modele.
--   * corr_* remplis                          -> le super_admin a corrige la valeur.
ALTER TABLE urls_images
  ADD COLUMN IF NOT EXISTS corr_qualite_eau real,
  ADD COLUMN IF NOT EXISTS corr_opacite     real,
  ADD COLUMN IF NOT EXISTS validated_by     text,
  ADD COLUMN IF NOT EXISTS validated_at     timestamptz;
