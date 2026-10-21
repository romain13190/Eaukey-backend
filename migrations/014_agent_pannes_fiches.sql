-- 014 Agent SAV : pannes détectées dans la messagerie (S3) et fiches SAV proposées
-- par l'assistant (S2).
-- Colonnes ajoutées avec une valeur par défaut (ou NULL) : aucun impact sur l'existant.
-- A appliquer AVANT de déployer le backend qui lit messages.panne_id et sav_fiches.brouillon.
-- Nécessite la 008 (sav_fiches), la 009 (agent SAV) et la table pannes. Rejouable.

-- S3. Panne créée (ou liée) à partir d'une réponse de l'assistant SAV.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS panne_id bigint;
DO $$ BEGIN
  ALTER TABLE messages ADD CONSTRAINT messages_panne_id_fkey
    FOREIGN KEY (panne_id) REFERENCES pannes (id) ON DELETE SET NULL;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
-- Panne détectée mais station impossible à déterminer : {type, constat}, pour l'équipe.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS panne_suggeree jsonb;

CREATE INDEX IF NOT EXISTS messages_panne_id_idx ON messages (panne_id) WHERE panne_id IS NOT NULL;

COMMENT ON COLUMN messages.panne_id       IS 'Panne créée ou liée par l''assistant SAV (ou par l''équipe depuis la suggestion)';
COMMENT ON COLUMN messages.panne_suggeree IS 'Panne détectée par l''assistant, station à préciser par l''équipe : {"type", "constat"}';

-- S2. Fiches proposées par l'assistant : brouillons à relire, jamais utilisés par l'assistant.
ALTER TABLE sav_fiches ADD COLUMN IF NOT EXISTS brouillon boolean NOT NULL DEFAULT false;
ALTER TABLE sav_fiches ADD COLUMN IF NOT EXISTS source_client_id text;

COMMENT ON COLUMN sav_fiches.brouillon        IS 'Brouillon proposé par l''assistant : à relire et publier, jamais joint aux questions';
COMMENT ON COLUMN sav_fiches.source_client_id IS 'Fil /chat dont le brouillon a été tiré (interne, jamais montré à l''assistant)';

-- Droits : app_backend a déjà SELECT/INSERT/UPDATE/DELETE sur messages et sav_fiches
-- (008, colonnes ajoutées couvertes par les droits de table). Rien à ajouter.

-- Rollback :
-- ALTER TABLE sav_fiches DROP COLUMN source_client_id, DROP COLUMN brouillon;
-- DROP INDEX messages_panne_id_idx;
-- ALTER TABLE messages DROP COLUMN panne_suggeree, DROP COLUMN panne_id;  -- supprime aussi la contrainte
