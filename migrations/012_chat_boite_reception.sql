-- 012 Boîte de réception de l'équipe (messagerie /chat) : état « traité » de chaque fil client.
-- Nouvelle table : aucun impact sur l'existant. A appliquer AVANT de déployer le backend
-- (les requêtes d'escalade de la 009 la lisent). Nécessite la 009 (messages.escalade).

CREATE TABLE IF NOT EXISTS chat_fils (
  client_id  text PRIMARY KEY,
  traite_at  timestamptz,
  traite_par text
);

COMMENT ON TABLE  chat_fils           IS 'Etat de chaque fil de la messagerie /chat vu par l''équipe Eaukey';
COMMENT ON COLUMN chat_fils.traite_at IS 'Dernier « marquer traité » ; un message client plus récent rouvre le fil';

-- Droits du compte applicatif du backend Render (DB_USER = app_backend).
GRANT SELECT, INSERT, UPDATE, DELETE ON chat_fils TO app_backend;

-- Rollback :
-- DROP TABLE chat_fils;
