-- Correction humaine (super_admin) du statut "bac vide" d'une photo.
-- NULL  = pas de decision humaine -> on utilise la prediction du modele (pred_bac_vide_prob).
-- TRUE  = un super_admin confirme que le bac est reellement vide -> exclu de la courbe qualite.
-- FALSE = un super_admin indique qu'il y a de l'eau (faux positif du modele) -> compte dans la courbe.
-- Statut effectif = COALESCE(corr_bac_vide, pred_bac_vide_prob >= seuil).
ALTER TABLE urls_images
  ADD COLUMN IF NOT EXISTS corr_bac_vide boolean;
