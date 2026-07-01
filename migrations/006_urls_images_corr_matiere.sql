-- Correction humaine (super_admin) de l'indice "matiere en suspension" (MES) d'une photo.
-- Echelle /10 (comme qualite/opacite). L'affichage = COALESCE(corr_matiere, pred_matiere_prob*10).
-- pred_matiere_prob est une probabilite 0..1 (estimation IA, pas des mg/L).
ALTER TABLE urls_images
  ADD COLUMN IF NOT EXISTS corr_matiere real;
