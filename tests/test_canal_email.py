"""Tests du canal e-mail de la messagerie (POST /canaux/email/entrant, relais des réponses
de l'équipe, routes /clients/{client_id}/contacts).

A lancer UNIQUEMENT contre une base Postgres jetable locale : le script SUPPRIME puis
recrée les tables messages, automate, chat_agent_conv, chat_fils et client_contacts.

    export CANAL_TEST_PGHOST=/chemin/du/socket   # ou 127.0.0.1
    export CANAL_TEST_PGPORT=55436
    python tests/test_canal_email.py

Données 100 % synthétiques. L'authentification est simulée, l'agent Wardian est un faux
(httpx.MockTransport) et l'envoi SMTP est intercepté (aiosmtplib.send remplacé) :
AUCUN e-mail n'est envoyé.
"""
import json
import os
import sys

import aiosmtplib
import httpx
import psycopg

ICI = os.path.dirname(os.path.abspath(__file__))
DEPOT = os.path.dirname(ICI)

HOST = os.environ.get("CANAL_TEST_PGHOST", "")
PORT = os.environ.get("CANAL_TEST_PGPORT", "")
if not HOST or not PORT or not (HOST.startswith("/") or HOST in ("localhost", "127.0.0.1")):
    sys.exit("CANAL_TEST_PGHOST (socket local ou localhost) et CANAL_TEST_PGPORT sont obligatoires : "
             "base jetable locale uniquement.")

os.environ.update({
    "DB_NAME": "postgres", "DB_USER": "postgres", "DB_HOST": HOST, "DB_PASSWORD": "x",
    "PGPORT": PORT, "AUTH_SECRET": "test",
    "WARDIAN_API_KEY": "wsk_test", "WARDIAN_AGENT_ID": "agent_test",
    # SMTP factice : l'envoi est intercepté plus bas, rien ne part.
    "SMTP_HOST": "smtp.invalid", "SMTP_USER": "u", "SMTP_PASSWORD": "p", "SMTP_FROM": "noreply@eaukey.test",
    "SAV_ESCALADE_EMAIL": "equipe@eaukey.test", "EMAIL_SAV_FROM": "Assistant SAV Eaukey <sav@sav.eaukey.test>",
    "EMAIL_ENTRANT_ADRESSE": "sav@sav.eaukey.test",
})
os.environ.pop("EMAIL_ENTRANT_SECRET", None)
DSN = f"host={HOST} port={PORT} user=postgres dbname=postgres"

# --- Envoi SMTP intercepté (AUCUN e-mail réel) --------------------------------
ENVOIS = []
SMTP_EN_PANNE = []


async def faux_send(msg, **kw):
    if SMTP_EN_PANNE:
        raise OSError("SMTP en panne (simulé)")
    ENVOIS.append(msg)
    return {}, "OK"

aiosmtplib.send = faux_send

with psycopg.connect(DSN, autocommit=True) as c:
    c.execute("""
      DROP TABLE IF EXISTS messages, chat_agent_conv, chat_fils, client_contacts, automate, sav_fiches, pannes;
      CREATE TABLE pannes (id serial PRIMARY KEY, client text, lieu text, nom_automate text, panne text,
        probleme text, date_debut timestamp, date_fin timestamp, created_by text, updated_at timestamp,
        cause text, action_realisee text, pieces_changees text, deplacement boolean, intervenant text,
        partage_sav boolean DEFAULT true);
      DO $$ BEGIN CREATE ROLE app_backend; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
      CREATE TABLE messages (id serial PRIMARY KEY, client_id text, sender_id int, content text,
                             file_url text, created_at text DEFAULT now()::text, is_read boolean DEFAULT false);
      CREATE TABLE automate (nom_automate text PRIMARY KEY, client text, lieu text,
                             email text, email2 text, email3 text);
      INSERT INTO automate VALUES
        ('A1', 'acme', 'Marseille', 'gerant@acme.test', 'Atelier@ACME.test', NULL),
        ('A2', 'acme', 'Aix', NULL, NULL, 'compta@acme.test; resp@acme.test'),
        ('B1', 'beta', 'Lyon', 'contact@beta.test', NULL, NULL),
        ('G1', 'gamma', 'Nice', 'partage@groupe.test', NULL, NULL),
        ('D1', 'delta', 'Nîmes', 'partage@groupe.test', NULL, NULL);
    """)
    c.execute(open(os.path.join(DEPOT, "migrations/008_sav_fiches.sql")).read())
    c.execute(open(os.path.join(DEPOT, "migrations/009_chat_agent.sql")).read())
    p012 = os.path.join(DEPOT, "migrations/012_chat_boite_reception.sql")
    if os.path.exists(p012):
        c.execute(open(p012).read())
    mig = open(os.path.join(DEPOT, "migrations/013_canaux.sql")).read()
    c.execute(mig)
    c.execute(mig)  # rejouable
print("migration 013 : OK (rejouable)")

sys.path.insert(0, DEPOT)
import backend  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

USERS = {
    "client": {"id": 7, "email": "c@x", "roles": ["client"], "client_id": "acme",
               "_automates": set(), "_clients": {"acme"}},
    "admin": {"id": 1, "email": "a@x", "roles": ["admin"], "client_id": None},
}


def fake_auth(request):
    tok = (request.headers.get("authorization") or "").split(" ")[-1]
    if tok not in USERS:
        raise backend.HTTPException(status_code=401, detail="no")
    return dict(USERS[tok])


def fake_admin(request):
    u = fake_auth(request)
    if not backend._is_admin_user(u):
        raise backend.HTTPException(status_code=403, detail="no")
    return u["email"]


backend._require_auth = fake_auth
backend._require_admin = fake_admin

# --- Faux Wardian -------------------------------------------------------------
SCRIPT = []
CALLS = []


def sse(ev, data):
    return f"event: {ev}\ndata: {json.dumps(data)}\n\n"


def handler(req):
    body = json.loads(req.content)
    CALLS.append(body)
    kind, payload = SCRIPT.pop(0)
    if kind == "status":
        return httpx.Response(payload, json={"detail": "x"})
    chunks = [sse("conversation", {"conversation_id": "conv-" + body["external_user_id"]})]
    for i in range(0, len(payload), 4):
        chunks.append(sse("token", {"delta": payload[i:i + 4]}))
    chunks.append(sse("done", {}))
    return httpx.Response(200, content="".join(chunks).encode(), headers={"content-type": "text/event-stream"})


_RealAsync = httpx.AsyncClient
httpx.AsyncClient = lambda **kw: _RealAsync(transport=httpx.MockTransport(handler), **kw)

api = TestClient(backend.app)
SECRET = "s3cret-de-test"
H = {"X-Eaukey-Webhook-Secret": SECRET}
ADMIN = {"Authorization": "Bearer admin"}
CLIENT = {"Authorization": "Bearer client"}


def rows(sql, *a):
    with psycopg.connect(DSN) as c:
        return c.execute(sql, a).fetchall()


def sql(q, *a):
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute(q, a)


def nb_messages():
    return rows("SELECT count(*) FROM messages")[0][0]


def generique(frm, texte="Bonjour, la pompe fait du bruit.", sujet="Pompe", mid=None, **extra):
    d = {"from": frm, "from_name": "Jean", "subject": sujet, "text": texte,
         "message_id": mid, "in_reply_to": None, "headers": {}}
    d.update(extra)
    return d


def recevoir(payload, headers=H, **kw):
    return api.post("/canaux/email/entrant", json=payload, headers=headers, **kw)


ok = 0


def check(cond, label):
    global ok
    assert cond, label
    ok += 1
    print("  ok -", label)


# 1. Fermé par défaut, secret obligatoire
backend._EMAIL_ENTRANT_SECRET = ""
check(recevoir(generique("gerant@acme.test")).status_code == 503, "sans EMAIL_ENTRANT_SECRET : 503 (fermé)")
backend._EMAIL_ENTRANT_SECRET = SECRET
check(recevoir(generique("gerant@acme.test"), headers={}).status_code == 401, "secret absent : 401")
check(recevoir(generique("gerant@acme.test"), headers={"X-Eaukey-Webhook-Secret": "faux"}).status_code == 403,
      "mauvais secret (en-tête) : 403")
check(api.post("/canaux/email/entrant?token=faux", json=generique("gerant@acme.test")).status_code == 403,
      "mauvais secret (?token=) : 403")
check(nb_messages() == 0 and not ENVOIS, "requêtes refusées : rien enregistré, rien envoyé")
check(recevoir(["x"]).status_code == 400 and
      api.post("/canaux/email/entrant", content=b"{pas du json", headers={**H, "content-type": "application/json"}).status_code == 400,
      "format invalide : 400")
check(api.post("/canaux/email/entrant", content=b"{" + b" " * 2_100_000 + b"}",
               headers={**H, "content-type": "application/json"}).status_code == 413, "e-mail trop volumineux : 413")

# 2. Format générique, expéditeur reconnu par automate.email2 (casse différente), via ?token=
SCRIPT.append(("ok", "**Bonjour**, vérifiez le [guide](https://eaukey.test/guide).\n\n* Coupez la pompe\n* Purgez"))
texte = ("Bonjour,\nla pompe fait un bruit étrange.\n\nJean\n\n"
         "Le mar. 21 oct. 2026 à 10:00, Assistant SAV Eaukey <sav@sav.eaukey.test> a écrit :\n"
         "> Ancienne réponse\n> citée")
r = api.post(f"/canaux/email/entrant?token={SECRET}",
             json=generique("Jean Dupont <atelier@Acme.TEST>", texte, "Bruit pompe", "<m1@client.test>"))
check(r.status_code == 200 and r.json()["resultats"] == ["enregistre"], "générique + ?token= : 200, enregistré")
q = rows("SELECT client_id, sender_id, content, canal, canal_adresse, external_id, canal_sujet FROM messages ORDER BY id LIMIT 1")[0]
check(q[:2] == ("acme", 1) and q[3:] == ("email", "atelier@acme.test", "<m1@client.test>", "Bruit pompe"),
      "reconnu via automate.email2 : fil du client acme, canal email, adresse normalisée, Message-ID")
check(q[2] == "Bonjour,\nla pompe fait un bruit étrange.\n\nJean", "citation « Le … a écrit : » et lignes « > » retirées")
check("Canal : e-mail" in CALLS[-1]["message"] and "[CONSIGNE CANAL MESSAGERIE" in CALLS[-1]["message"]
      and CALLS[-1]["external_user_id"] == "eaukey-chat-acme", "agent appelé avec la consigne e-mail, conversation du client")
a = rows("SELECT sender_id, canal, canal_adresse FROM messages ORDER BY id DESC LIMIT 1")[0]
check(a == (-1, "email", "atelier@acme.test"), "réponse de l'agent enregistrée dans le fil (canal email)")
m = ENVOIS[-1]
check(m["To"] == "atelier@acme.test" and m["Subject"] == "Re: Bruit pompe" and m["From"] == "Assistant SAV Eaukey <sav@sav.eaukey.test>",
      "réponse e-mail : To, Subject « Re: », From = EMAIL_SAV_FROM")
check(m["In-Reply-To"] == "<m1@client.test>" and "<m1@client.test>" in m["References"] and m["Reply-To"] == "sav@sav.eaukey.test",
      "réponse e-mail : In-Reply-To, References, Reply-To = EMAIL_ENTRANT_ADRESSE")
corps = m.get_payload(decode=True).decode("utf-8")
check("Vous pouvez répondre à cet e-mail ou continuer sur https://app.eaukey.net/chat" in corps
      and m["Auto-Submitted"] == "auto-replied", "pied de l'e-mail + Auto-Submitted: auto-replied")
check("**" not in corps and "guide (https://eaukey.test/guide)" in corps and "- Coupez la pompe" in corps,
      "markdown simplifié en texte brut")

# 3. Brevo Inbound Parsing, reconnu via client_contacts (prioritaire sur les stations)
sql("INSERT INTO client_contacts (client_id, canal, valeur, created_by) VALUES ('beta', 'email', 'paul@perso.test', 'a@x')")
SCRIPT.append(("ok", "Réponse pour beta."))
item = {"Uuid": ["u1"], "MessageId": "<b1@perso.test>", "InReplyTo": None,
        "From": {"Name": "Paul", "Address": "Paul@Perso.test"}, "Subject": "Fuite",
        "RawTextBody": "Il y a une fuite.\n\nOn Tue, Oct 21, 2026 at 9:00 AM Eaukey wrote:\n> vieux",
        "ExtractedMarkdownMessage": "Il y a une **fuite** sous la cuve.",
        "Headers": {"Message-Id": "<b1@perso.test>", "References": "<ancien@sav.eaukey.test>"}}
n_env = len(ENVOIS)
r = recevoir({"items": [item]})
check(r.status_code == 200 and r.json()["resultats"] == ["enregistre"], "Brevo : 200, enregistré")
check(rows("SELECT client_id, content FROM messages WHERE external_id = '<b1@perso.test>'") == [("beta", "Il y a une **fuite** sous la cuve.")],
      "Brevo : ExtractedMarkdownMessage préféré, reconnu via client_contacts")
check(len(ENVOIS) == n_env + 1 and ENVOIS[-1]["References"] == "<ancien@sav.eaukey.test> <b1@perso.test>",
      "Brevo : réponse envoyée, References = fil existant + Message-ID reçu")

# 4. Idempotence : le même e-mail rejoué ne crée rien
n, n_env, n_calls = nb_messages(), len(ENVOIS), len(CALLS)
r = recevoir({"items": [item]})
check(r.json()["resultats"] == ["doublon"] and nb_messages() == n and len(ENVOIS) == n_env and len(CALLS) == n_calls,
      "rejeu (même MessageId) : rien enregistré, rien envoyé")

# 5. Brevo sans texte extrait : citations retirées par nous
SCRIPT.append(("ok", "OK."))
item2 = dict(item, MessageId="<b2@perso.test>", ExtractedMarkdownMessage=None, Headers={},
             RawTextBody="Toujours la fuite.\r\n\r\nOn Tue, Oct 21, 2026 at 9:00 AM Eaukey <sav@sav.eaukey.test> wrote:\r\n> vieux")
recevoir({"items": [item2]})
check(rows("SELECT content FROM messages WHERE external_id = '<b2@perso.test>'") == [("Toujours la fuite.",)],
      "Brevo sans ExtractedMarkdownMessage : « On … wrote: » retiré")
check(backend._email_nettoyer_texte("Merci\n\nDe : SAV Eaukey\nEnvoyé : mardi\nObjet : x\nancien") == "Merci"
      and backend._email_nettoyer_texte("Merci\n-- \nJean Dupont\n06 00") == "Merci"
      and backend._email_nettoyer_texte("x" * 5000).endswith("…") and len(backend._email_nettoyer_texte("x" * 5000)) == 4000,
      "extraction : en-tête Outlook, signature « -- », texte tronqué à 4000")

# 6. Anti-boucle : chaque en-tête / adresse
n, n_env, n_calls = nb_messages(), len(ENVOIS), len(CALLS)
cas = [
    ("auto-submitted", generique("gerant@acme.test", mid="<l1@x>", headers={"Auto-Submitted": "auto-replied"})),
    ("auto-submitted", generique("gerant@acme.test", mid="<l2@x>", headers=[{"name": "Auto-Submitted", "value": "auto-generated"}])),
    ("precedence", generique("gerant@acme.test", mid="<l3@x>", headers={"Precedence": "bulk"})),
    ("precedence", generique("gerant@acme.test", mid="<l4@x>", headers={"Precedence": "junk"})),
    ("precedence", generique("gerant@acme.test", mid="<l5@x>", headers={"Precedence": "list"})),
    ("precedence", generique("gerant@acme.test", mid="<l6@x>", headers={"Precedence": "auto_reply"})),
    ("x-autoreply", generique("gerant@acme.test", mid="<l7@x>", headers={"X-Autoreply": "yes"})),
    ("notre adresse", generique("sav@sav.eaukey.test", mid="<l8@x>")),
    ("notre adresse", generique("noreply@eaukey.test", mid="<l9@x>")),
    ("adresse automatique", generique("no-reply@banque.test", mid="<l10@x>")),
    ("adresse automatique", generique("MAILER-DAEMON@mx.test", mid="<l11@x>")),
    ("adresse automatique", generique("postmaster@mx.test", mid="<l12@x>")),
]
res = [recevoir(p).json()["resultats"][0] for _, p in cas]
check(res == [f"ignore: {m}" for m, _ in cas], "anti-boucle : Auto-Submitted, Precedence (4), X-Autoreply, nos adresses, noreply/mailer-daemon/postmaster")
check(nb_messages() == n and len(ENVOIS) == n_env and len(CALLS) == n_calls, "anti-boucle : rien enregistré, rien envoyé, agent non appelé")
SCRIPT.append(("ok", "Bien reçu."))
r = recevoir(generique("gerant@acme.test", mid="<l13@x>", headers={"Auto-Submitted": "no"}))
check(r.json()["resultats"] == ["enregistre"], "Auto-Submitted: no : traité normalement")

# 7. Expéditeur inconnu / ambigu : aucune réponse, alerte équipe (adresse + sujet seulement)
n, n_env, n_calls = nb_messages(), len(ENVOIS), len(CALLS)
r = recevoir(generique("inconnu@ailleurs.test", "Mon numéro de carte est 1234", "Demande de devis", "<i1@x>"))
check(r.status_code == 200 and r.json()["resultats"] == ["inconnu"] and nb_messages() == n and len(CALLS) == n_calls,
      "inconnu : 200, rien enregistré, agent non appelé")
alerte = ENVOIS[-1]
corps = alerte.get_payload(decode=True).decode("utf-8")
check(len(ENVOIS) == n_env + 1 and alerte["To"] == "equipe@eaukey.test" and "inconnu@ailleurs.test" in corps
      and "Demande de devis" in corps and "1234" not in corps, "inconnu : alerte à l'équipe avec adresse et sujet, sans le contenu")
check(not any(e["To"] == "inconnu@ailleurs.test" for e in ENVOIS), "inconnu : aucune réponse à l'expéditeur")
recevoir(generique("inconnu@ailleurs.test", "Relance", "Relance", "<i2@x>"))
check(len(ENVOIS) == n_env + 1, "inconnu : pas de 2e alerte pour la même adresse dans la journée")
r = recevoir(generique("partage@groupe.test", "Question", "Question", "<i3@x>"))
check(r.json()["resultats"] == ["inconnu"] and nb_messages() == n, "adresse de stations de 2 clients différents : traitée comme inconnue")
SCRIPT.append(("ok", "Pour acme."))
r = recevoir(generique("resp@acme.test", "Question", "Question", "<i4@x>"))
check(r.json()["resultats"] == ["enregistre"], "champ e-mail de station avec plusieurs adresses (« ; ») : reconnu")

# 8. Escalade par e-mail
n_env = len(ENVOIS)
SCRIPT.append(("ok", "[ESCALADE: pièce à commander]\nL'équipe Eaukey est prévenue."))
recevoir(generique("gerant@acme.test", "La pompe est cassée", "Panne", "<e1@x>"))
dest = [e["To"] for e in ENVOIS[n_env:]]
check(rows("SELECT escalade, escalade_motif FROM messages WHERE sender_id = -1 ORDER BY id DESC LIMIT 1") == [(True, "pièce à commander")],
      "escalade : réponse marquée, motif enregistré")
check(sorted(dest) == ["equipe@eaukey.test", "gerant@acme.test"], "escalade : équipe prévenue + réponse au client")
rep = [e for e in ENVOIS[n_env:] if e["To"] == "gerant@acme.test"][0].get_payload(decode=True).decode("utf-8")
check("ESCALADE" not in rep and rep.startswith("L'équipe Eaukey est prévenue."), "escalade : marqueur jamais envoyé au client")

# 9. Réponse de l'admin relayée par e-mail (dernier message du client par e-mail, < 14 j)
n_env = len(ENVOIS)
r = api.post("/messages/acme", json={"sender_id": 0, "content": "Je passe demain."}, headers=ADMIN)
check(r.status_code == 200 and set(r.json()) >= {"id", "client_id", "sender_id", "content", "created_at"}
      and r.json()["sender_id"] == 0, "POST /messages admin : réponse inchangée")
m = ENVOIS[-1]
check(len(ENVOIS) == n_env + 1 and m["To"] == "gerant@acme.test" and m["Subject"] == "Re: Panne"
      and m["In-Reply-To"] == "<e1@x>" and m["Auto-Submitted"] is None, "réponse admin relayée : même fil e-mail (Re:, In-Reply-To), pas Auto-Submitted")
check("Je passe demain." in m.get_payload(decode=True).decode("utf-8"), "réponse admin relayée : contenu")
check(rows("SELECT canal, canal_adresse FROM messages WHERE id = %s", r.json()["id"]) == [("email", "gerant@acme.test")],
      "réponse admin marquée « par e-mail »")

# 10. Humain actif : l'e-mail entre dans le fil, l'agent se tait
n_env, n_calls = len(ENVOIS), len(CALLS)
r = recevoir(generique("gerant@acme.test", "Merci, à demain", "Panne", "<h1@x>"))
check(r.json()["resultats"] == ["enregistre"] and len(CALLS) == n_calls and len(ENVOIS) == n_env
      and rows("SELECT content FROM messages ORDER BY id DESC LIMIT 1") == [("Merci, à demain",)],
      "humain actif : message enregistré, agent non appelé, pas de réponse automatique")

# 11. Pas de relais au-delà de 14 jours, ni si le client écrit depuis l'appli
sql("UPDATE messages SET created_at = (now() - interval '20 days')::text WHERE client_id = 'acme' AND sender_id = 1")
n_env = len(ENVOIS)
api.post("/messages/acme", json={"sender_id": 0, "content": "Des nouvelles ?"}, headers=ADMIN)
check(len(ENVOIS) == n_env, "dernier e-mail du client > 14 jours : pas de relais")
sql("INSERT INTO messages (client_id, sender_id, content) VALUES ('beta', 1, 'depuis l''appli')")
api.post("/messages/beta", json={"sender_id": 0, "content": "Réponse"}, headers=ADMIN)
check(len(ENVOIS) == n_env, "dernier message du client par l'appli : pas de relais")
n_env = len(ENVOIS)
api.post("/messages/acme", json={"sender_id": 1, "content": "client"}, headers=CLIENT)
check(len(ENVOIS) == n_env, "message d'un client via POST /messages : jamais relayé")

# 12. GET /messages renvoie le canal ; /chat inchangé (canal app)
mc = api.get("/messages/acme", headers=CLIENT).json()
check({m["canal"] for m in mc} == {"app", "email"} and all("canal" in m for m in mc), "GET /messages renvoie canal")
sql("DELETE FROM messages WHERE sender_id = 0")
SCRIPT.append(("ok", "Réponse appli."))
r = api.post("/chat/acme/agent", json={"content": "depuis l'appli"}, headers=CLIENT)
check(r.status_code == 200 and "event: saved" in r.text
      and rows("SELECT canal FROM messages ORDER BY id DESC LIMIT 2") == [("app",), ("app",)]
      and "Canal : e-mail" not in CALLS[-1]["message"], "/chat : canal app, consigne sans la ligne e-mail")

# 13. Agent en panne / SMTP en panne
n_env = len(ENVOIS)
SCRIPT.append(("status", 500))
recevoir(generique("gerant@acme.test", "Encore une question", "Q", "<p1@x>"))
check(len(ENVOIS) == n_env and rows("SELECT content FROM messages ORDER BY id DESC LIMIT 1") == [("Encore une question",)],
      "agent en panne : question conservée pour l'équipe, rien envoyé")
SCRIPT.append(("ok", "Réponse jamais partie."))
SMTP_EN_PANNE.append(1)
recevoir(generique("gerant@acme.test", "Question 2", "Q", "<p2@x>"))
SMTP_EN_PANNE.clear()
check(rows("SELECT sender_id, canal, canal_adresse FROM messages ORDER BY id DESC LIMIT 1") == [(-1, "app", None)],
      "SMTP en panne : réponse visible dans l'appli, pas marquée « par e-mail »")

# 14. Routes contacts (admin)
check(api.get("/clients/acme/contacts").status_code == 401, "contacts sans jeton : 401")
check(api.get("/clients/acme/contacts", headers=CLIENT).status_code == 403
      and api.post("/clients/acme/contacts", json={"valeur": "x@y.test"}, headers=CLIENT).status_code == 403
      and api.delete("/clients/acme/contacts/1", headers=CLIENT).status_code == 403, "contacts (GET/POST/DELETE) refusés au client : 403")
check(api.post("/clients/acme/contacts", json={"valeur": "pas une adresse"}, headers=ADMIN).status_code == 400
      and api.post("/clients/acme/contacts", json={"valeur": "Jean <j@acme.test>"}, headers=ADMIN).status_code == 400,
      "ajout d'une adresse invalide : 400")
r = api.post("/clients/acme/contacts", json={"valeur": "  Nouveau@Acme.test "}, headers=ADMIN)
check(r.status_code == 200 and r.json()["valeur"] == "nouveau@acme.test" and r.json()["created_by"] == "a@x", "ajout : adresse normalisée, auteur")
cid = r.json()["id"]
check(api.post("/clients/acme/contacts", json={"valeur": "nouveau@acme.test"}, headers=ADMIN).json()["id"] == cid,
      "ajout en double pour le même client : renvoie l'existant")
check(api.post("/clients/gamma/contacts", json={"valeur": "nouveau@acme.test"}, headers=ADMIN).status_code == 409,
      "adresse déjà rattachée à un autre client : 409")
g = api.get("/clients/acme/contacts", headers=ADMIN).json()
check([c["valeur"] for c in g["contacts"]] == ["nouveau@acme.test"]
      and g["stations"] == ["atelier@acme.test", "compta@acme.test", "gerant@acme.test", "resp@acme.test"],
      "liste : adresses saisies + e-mails des stations")
SCRIPT.append(("ok", "Bienvenue."))
check(recevoir(generique("nouveau@acme.test", "Bonjour", "Hello", "<c1@x>")).json()["resultats"] == ["enregistre"],
      "adresse ajoutée par l'équipe : reconnue")
check(api.delete(f"/clients/gamma/contacts/{cid}", headers=ADMIN).status_code == 404
      and api.delete(f"/clients/acme/contacts/{cid}", headers=ADMIN).status_code == 200
      and api.delete(f"/clients/acme/contacts/{cid}", headers=ADMIN).status_code == 404, "suppression (et pas depuis un autre client)")
check(recevoir(generique("nouveau@acme.test", "Re", "Hello", "<c2@x>")).json()["resultats"] == ["inconnu"],
      "adresse supprimée : redevient inconnue")

# 15. Aucun e-mail réel : tous les envois sont passés par le faux SMTP
check(all(isinstance(e, backend.MIMEText) for e in ENVOIS) and aiosmtplib.send is faux_send, "SMTP intercepté (aucun envoi réel)")

print(f"\n{ok} vérifications OK")
