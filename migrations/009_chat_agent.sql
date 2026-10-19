-- 009 Agent SAV dans la messagerie /chat : l'assistant Wardian répond dans le fil
-- client <-> équipe (POST /chat/{client_id}/agent) et prévient l'équipe quand un
-- humain est nécessaire (escalade).
-- Colonnes ajoutées avec une valeur par défaut : aucun impact sur l'existant.
-- A appliquer AVANT de déployer le backend qui lit messages.escalade.

ALTER TABLE messages ADD COLUMN IF NOT EXISTS escalade boolean NOT NULL DEFAULT false;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS escalade_motif text;

COMMENT ON COLUMN messages.escalade       IS 'Réponse de l''agent SAV qui demande l''intervention de l''équipe Eaukey';
COMMENT ON COLUMN messages.escalade_motif IS 'Motif de l''escalade donné par l''agent SAV';

-- Conversation Wardian associée au fil /chat d'un client (continuité des échanges avec l'agent).
CREATE TABLE IF NOT EXISTS chat_agent_conv (
  client_id               text PRIMARY KEY,
  wardian_conversation_id text,
  updated_at              timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE chat_agent_conv IS 'Conversation Wardian de l''agent SAV pour chaque fil /chat client';

-- Droits du compte applicatif du backend Render (DB_USER = app_backend).
GRANT SELECT, INSERT, UPDATE, DELETE ON chat_agent_conv TO app_backend;

-- Rollback :
-- DROP TABLE chat_agent_conv;
-- ALTER TABLE messages DROP COLUMN escalade_motif, DROP COLUMN escalade;
