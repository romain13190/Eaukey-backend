-- 013 Canaux de la messagerie (M2) : les clients écrivent à l'assistant SAV par e-mail,
-- dans le même fil que la messagerie /chat de l'application.
-- Colonnes ajoutées avec une valeur par défaut (ou NULL) + nouvelle table : aucun impact
-- sur l'existant. A appliquer AVANT de déployer le backend qui lit messages.canal.
-- Nécessite la 009 (messages.escalade). Rejouable.

-- Canal par lequel le message est arrivé (client) ou reparti (équipe, assistant).
ALTER TABLE messages ADD COLUMN IF NOT EXISTS canal text NOT NULL DEFAULT 'app';
-- Identifiant du message chez le fournisseur (Message-ID de l'e-mail) : un e-mail rejoué
-- par le fournisseur n'est jamais enregistré deux fois.
ALTER TABLE messages ADD COLUMN IF NOT EXISTS external_id text;
-- Adresse de l'expéditeur (message reçu) ou du destinataire (réponse envoyée).
ALTER TABLE messages ADD COLUMN IF NOT EXISTS canal_adresse text;
-- Sujet de l'e-mail reçu (pour répondre « Re: <sujet> » dans le même fil e-mail).
ALTER TABLE messages ADD COLUMN IF NOT EXISTS canal_sujet text;

CREATE UNIQUE INDEX IF NOT EXISTS messages_external_id_uniq
  ON messages (external_id) WHERE external_id IS NOT NULL;

COMMENT ON COLUMN messages.canal         IS 'Canal du message : app (messagerie /chat) ou email';
COMMENT ON COLUMN messages.external_id   IS 'Message-ID reçu du fournisseur (idempotence du webhook entrant)';
COMMENT ON COLUMN messages.canal_adresse IS 'Adresse e-mail de l''expéditeur (reçu) ou du destinataire (envoyé)';
COMMENT ON COLUMN messages.canal_sujet   IS 'Sujet de l''e-mail reçu';

-- Adresses (et plus tard numéros) reconnues pour un client, en plus des e-mails des stations
-- (automate.email / email2 / email3). Saisies par l'équipe Eaukey (admin).
CREATE TABLE IF NOT EXISTS client_contacts (
  id          bigserial PRIMARY KEY,
  client_id   text NOT NULL,
  canal       text NOT NULL,
  valeur      text NOT NULL,
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (canal, valeur)
);

CREATE INDEX IF NOT EXISTS client_contacts_client_idx ON client_contacts (client_id);

COMMENT ON TABLE  client_contacts        IS 'Contacts reconnus d''un client pour la messagerie (e-mail, WhatsApp…)';
COMMENT ON COLUMN client_contacts.canal  IS 'email (whatsapp, sms plus tard)';
COMMENT ON COLUMN client_contacts.valeur IS 'Adresse normalisée (minuscules) ou numéro';

-- Droits du compte applicatif du backend Render (DB_USER = app_backend).
-- NB : les privilèges par défaut de romain donnent aussi SELECT à integration_ro et lavance_ro.
GRANT SELECT, INSERT, UPDATE, DELETE ON client_contacts TO app_backend;
GRANT USAGE, SELECT ON SEQUENCE client_contacts_id_seq TO app_backend;

-- Rollback :
-- DROP TABLE client_contacts;
-- DROP INDEX messages_external_id_uniq;
-- ALTER TABLE messages DROP COLUMN canal_sujet, DROP COLUMN canal_adresse, DROP COLUMN external_id, DROP COLUMN canal;
