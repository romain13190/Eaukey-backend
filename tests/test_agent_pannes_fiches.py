"""Tests de l'agent SAV : pannes détectées dans la messagerie (S3) et fiches SAV proposées
par l'assistant (S2) — POST /chat/{client_id}/agent, POST /messages/{message_id}/panne,
GET /messages/{client_id}, POST /sav/fiches/proposer, /sav/fiches, _sav_contexte.

A lancer UNIQUEMENT contre une base Postgres jetable locale : le script SUPPRIME puis
recrée les tables messages, automate, pannes, sav_fiches, chat_agent_conv, chat_fils et
client_contacts.

    export AGENT_TEST_PGHOST=/chemin/du/socket   # ou 127.0.0.1
    export AGENT_TEST_PGPORT=55438
    python tests/test_agent_pannes_fiches.py

Données 100 % synthétiques. L'authentification est simulée, l'agent Wardian est un faux
(httpx.MockTransport) et l'e-mail d'escalade est intercepté : AUCUN appel réseau,
AUCUN e-mail envoyé.
"""
import json
import os
import sys

import httpx
import psycopg

ICI = os.path.dirname(os.path.abspath(__file__))
DEPOT = os.path.dirname(ICI)

HOST = os.environ.get("AGENT_TEST_PGHOST", "")
PORT = os.environ.get("AGENT_TEST_PGPORT", "")
if not HOST or not PORT or not (HOST.startswith("/") or HOST in ("localhost", "127.0.0.1")):
    sys.exit("AGENT_TEST_PGHOST (socket local ou localhost) et AGENT_TEST_PGPORT sont obligatoires : "
             "base jetable locale uniquement.")

os.environ.update({
    "DB_NAME": "postgres", "DB_USER": "postgres", "DB_HOST": HOST, "DB_PASSWORD": "x",
    "PGPORT": PORT, "AUTH_SECRET": "test",
    "WARDIAN_API_KEY": "wsk_test", "WARDIAN_AGENT_ID": "agent_test",
})
for k in ("SMTP_HOST", "SMTP_USER", "SMTP_PASSWORD", "EMAIL_ENTRANT_SECRET"):
    os.environ.pop(k, None)
DSN = f"host={HOST} port={PORT} user=postgres dbname=postgres"

with psycopg.connect(DSN, autocommit=True) as c:
    c.execute("""
      DROP TABLE IF EXISTS messages, chat_agent_conv, chat_fils, client_contacts, automate, sav_fiches, pannes;
      DO $$ BEGIN CREATE ROLE app_backend; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
      CREATE TABLE pannes (id serial PRIMARY KEY, client text, lieu text, nom_automate text, panne text,
        probleme text, date_debut timestamp, date_fin timestamp, created_by text, updated_at timestamp,
        cause text, action_realisee text, pieces_changees text, deplacement boolean, intervenant text,
        partage_sav boolean DEFAULT true);
      CREATE TABLE messages (id serial PRIMARY KEY, client_id text, sender_id int, content text,
                             file_url text, created_at timestamp DEFAULT now(), is_read boolean DEFAULT false);
      CREATE TABLE automate (nom_automate text PRIMARY KEY, client text, lieu text,
                             email text, email2 text, email3 text);
      INSERT INTO automate (nom_automate, client, lieu) VALUES
        ('S1', 'solo', 'Toulon'),
        ('M1', 'multi', 'Marseille Nord'),
        ('M2', 'multi', 'Marseille'),
        ('M3', 'multi', 'Aix'),
        ('M4', ' Multi ', 'Nîmes');
    """)
    for nom in ("008_sav_fiches.sql", "009_chat_agent.sql", "012_chat_boite_reception.sql", "013_canaux.sql"):
        chemin = os.path.join(DEPOT, "migrations", nom)
        if os.path.exists(chemin):
            c.execute(open(chemin).read())
    mig = open(os.path.join(DEPOT, "migrations/014_agent_pannes_fiches.sql")).read()
    c.execute(mig)
    c.execute(mig)  # rejouable

sys.path.insert(0, DEPOT)
import backend  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

USERS = {
    "solo": {"id": 7, "email": "s@x", "roles": ["client"], "client_id": "solo",
             "_automates": {"S1"}, "_clients": {"solo"}},
    "multi": {"id": 8, "email": "m@x", "roles": ["client"], "client_id": "multi",
              "_automates": {"M1", "M2", "M3", "M4"}, "_clients": {"multi"}},
    "vide": {"id": 9, "email": "v@x", "roles": ["client"], "client_id": "vide",
             "_automates": set(), "_clients": {"vide"}},
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
    chunks = [sse("conversation", {"conversation_id": "conv-" + body["external_user_id"]}), ": keepalive\n\n"]
    # découpe volontairement les marqueurs sur plusieurs tokens
    for i in range(0, len(payload), 3):
        chunks.append(sse("token", {"delta": payload[i:i + 3]}))
    chunks.append(sse("done", {"conversation_id": "conv-" + body["external_user_id"]}))
    return httpx.Response(200, content="".join(chunks).encode(), headers={"content-type": "text/event-stream"})


_RealAsync = httpx.AsyncClient
httpx.AsyncClient = lambda **kw: _RealAsync(transport=httpx.MockTransport(handler), **kw)

ESCALADES = []


async def faux_prevenir(client_id, motif, q, r):
    ESCALADES.append((client_id, motif))

backend._chat_prevenir_equipe = faux_prevenir

api = TestClient(backend.app)
ADMIN = {"Authorization": "Bearer admin"}


def H(who):
    return {"Authorization": f"Bearer {who}"}


def post(who, client, text):
    r = api.post(f"/chat/{client}/agent", json={"content": text}, headers=H(who))
    if r.status_code != 200:
        return r.status_code, [], ""
    evs = []
    for fr in r.text.split("\n\n"):
        ls = fr.split("\n")
        if len(ls) >= 2 and ls[0].startswith("event:"):
            evs.append((ls[0][6:].strip(), json.loads(ls[1][5:])))
    toks = "".join(d["delta"] for e, d in evs if e == "token")
    return 200, evs, toks


def rows(sql, *a):
    with psycopg.connect(DSN) as c:
        return c.execute(sql, a).fetchall()


def sql(q, *a):
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute(q, a)


def dernier_agent(client):
    return rows("SELECT id, content, panne_id, panne_suggeree FROM messages "
                "WHERE client_id = %s AND sender_id = -1 ORDER BY id DESC LIMIT 1", client)[0]


def nb_pannes():
    return rows("SELECT count(*) FROM pannes")[0][0]


def nb_fiches():
    return rows("SELECT count(*) FROM sav_fiches")[0][0]


ok = 0


def check(cond, label):
    global ok
    assert cond, label
    ok += 1
    print("  ok -", label)


print("S3 — pannes détectées par l'agent")

# 0. Migration et consigne
check({r[0] for r in rows("SELECT column_name FROM information_schema.columns WHERE table_name = 'messages'")}
      >= {"panne_id", "panne_suggeree"}
      and {r[0] for r in rows("SELECT column_name FROM information_schema.columns WHERE table_name = 'sav_fiches'")}
      >= {"brouillon", "source_client_id"}, "migration 014 appliquée deux fois (rejouable)")
msg = backend._chat_agent_message("bonjour")
check("[PANNE: type | constat court]" in msg and "Pompe HS" in msg and "<TYPES_PANNES>" not in msg
      and "ne promets jamais" in msg, "consigne : marqueur PANNE + liste des types connus injectée")

# 1. Une seule station : panne créée et liée, marqueur jamais montré
SCRIPT.append(("ok", "[PANNE: Pompe HS | la pompe ne démarre plus]\nCoupez l'alimentation puis relancez."))
st, evs, toks = post("solo", "solo", "Ma pompe ne démarre plus depuis ce matin")
check(st == 200 and "PANNE" not in toks and toks.startswith("Coupez"), "marqueur PANNE découpé en tokens : jamais montré")
saved = evs[-1][1]
check(evs[-1][0] == "saved" and saved["content"] == "Coupez l'alimentation puis relancez."
      and set(saved) == {"id", "created_at", "content", "escalade"}, "saved : texte nettoyé, aucune info de panne pour le client")
p = rows("SELECT id, client, lieu, nom_automate, panne, probleme, created_by, partage_sav, date_fin, intervenant, "
         "date_debut > now() - interval '5 minutes' FROM pannes")
check(len(p) == 1 and p[0][1:] == ("solo", "Toulon", "S1", "Pompe HS",
                                    "la pompe ne démarre plus (signalé par le client via l'assistant SAV)",
                                    "assistant-sav", False, None, None, True),
      "1 station : panne créée (assistant-sav, partage_sav false, en cours, date_debut = maintenant)")
p1 = p[0][0]
m = dernier_agent("solo")
check(m[2] == p1 and m[3] is None, "message de l'agent lié à la panne (panne_id)")

# 2. Anti-doublon 48 h
SCRIPT.append(("ok", "Toujours en panne ? [PANNE: pompe hs | la pompe ne redémarre pas]"))
st, evs, toks = post("solo", "solo", "toujours rien")
check(nb_pannes() == 1 and dernier_agent("solo")[2] == p1 and "PANNE" not in toks and toks.strip() == "Toujours en panne ?",
      "même type encore ouvert (< 48 h, casse différente, marqueur en fin) : pas de doublon, lié à la panne existante")
sql("UPDATE pannes SET date_debut = now() - interval '49 hours' WHERE id = %s", p1)
SCRIPT.append(("ok", "[PANNE: Pompe HS | pompe arrêtée]\nOK."))
post("solo", "solo", "la pompe encore")
p2 = dernier_agent("solo")[2]
check(nb_pannes() == 2 and p2 not in (None, p1), "panne identique ouverte depuis plus de 48 h : nouvelle panne")
sql("UPDATE pannes SET date_fin = now() WHERE id = %s", p2)
SCRIPT.append(("ok", "[PANNE: Pompe HS | pompe arrêtée]\nOK."))
post("solo", "solo", "re la pompe")
p3 = dernier_agent("solo")[2]
check(nb_pannes() == 3 and p3 not in (None, p1, p2), "panne identique déjà résolue : nouvelle panne")
SCRIPT.append(("ok", "[PANNE: Filtre colmaté | pression filtre haute]\nFaites un contre-lavage."))
post("solo", "solo", "pression filtre haute")
check(nb_pannes() == 4 and rows("SELECT panne FROM pannes ORDER BY id DESC LIMIT 1") == [("Filtre colmaté",)],
      "autre type sur la même station : nouvelle panne")

# 3. ESCALADE + PANNE, dans les deux ordres
SCRIPT.append(("ok", "[ESCALADE: pièce à commander]\n[PANNE: Fuite cuve | la cuve fuit]\nL'équipe Eaukey est prévenue."))
st, evs, toks = post("solo", "solo", "la cuve fuit")
check("ESCALADE" not in toks and "PANNE" not in toks and toks.startswith("L'équipe")
      and evs[-1][1]["escalade"] is True and evs[-1][1]["content"] == "L'équipe Eaukey est prévenue.",
      "ESCALADE puis PANNE : les deux marqueurs retirés, escalade signalée")
m = rows("SELECT escalade_motif, p.panne FROM messages m JOIN pannes p ON p.id = m.panne_id "
         "WHERE m.client_id = 'solo' AND m.sender_id = -1 ORDER BY m.id DESC LIMIT 1")
check(m == [("pièce à commander", "Fuite cuve")] and ESCALADES == [("solo", "pièce à commander")],
      "ESCALADE puis PANNE : motif enregistré, e-mail d'escalade, panne créée et liée")
SCRIPT.append(("ok", "[PANNE: Disjonction | le disjoncteur saute] [ESCALADE: électricien]\nL'équipe arrive vers vous."))
st, evs, toks = post("solo", "solo", "ça disjoncte")
check("ESCALADE" not in toks and "PANNE" not in toks and toks.startswith("L'équipe") and evs[-1][1]["escalade"] is True
      and rows("SELECT panne FROM pannes ORDER BY id DESC LIMIT 1") == [("Disjonction",)],
      "PANNE puis ESCALADE : les deux marqueurs retirés, panne créée")

# 4. Type inconnu -> « Autre » (le type proposé reste dans le constat)
SCRIPT.append(("ok", "[PANNE: Écran tactile HS | écran noir]\nRedémarrez l'automate."))
post("solo", "solo", "écran noir")
check(rows("SELECT panne, probleme FROM pannes ORDER BY id DESC LIMIT 1")
      == [("Autre", "Écran tactile HS : écran noir (signalé par le client via l'assistant SAV)")],
      "type inconnu : « Autre », type proposé gardé dans le constat")
SCRIPT.append(("ok", "[PANNE: Capteur niveau HS]\nVérifiez le câble du capteur."))
post("solo", "solo", "le niveau affiche toujours zéro")
check(rows("SELECT panne, probleme FROM pannes ORDER BY id DESC LIMIT 1")
      == [("Capteur niveau HS", "le niveau affiche toujours zéro (signalé par le client via l'assistant SAV)")],
      "marqueur sans constat : constat = question du client")

# 5. Pas de marqueur / faux marqueur markdown : rien
n = nb_pannes()
SCRIPT.append(("ok", "Voir [Pannes fréquentes](https://eaukey.test/faq) et [la notice](https://eaukey.test/n)."))
st, evs, toks = post("solo", "solo", "où est la FAQ ?")
m = dernier_agent("solo")
check(toks == "Voir [Pannes fréquentes](https://eaukey.test/faq) et [la notice](https://eaukey.test/n)."
      and nb_pannes() == n and m[2] is None and m[3] is None, "sans marqueur (liens markdown « [Pannes… ») : texte intact, aucune panne")

# 6. Plusieurs stations
SCRIPT.append(("ok", "[PANNE: Fuite hydraulique | fuite sous la pompe]\nFermez la vanne d'arrivée."))
post("multi", "multi", "Bonjour, à la station de Marseille Nord il y a une fuite sous la pompe")
m = dernier_agent("multi")
check(m[2] is not None and rows("SELECT nom_automate FROM pannes WHERE id = %s", m[2]) == [("M1",)],
      "plusieurs stations + lieu cité (« Marseille Nord », pas « Marseille ») : panne créée sur la bonne station")
SCRIPT.append(("ok", "[PANNE: Pompe HS | pompe bloquée]\nOK."))
post("multi", "multi", "et à nimes la pompe est bloquée")
check(rows("SELECT p.nom_automate FROM messages m JOIN pannes p ON p.id = m.panne_id "
           "WHERE m.client_id = 'multi' ORDER BY m.id DESC LIMIT 1") == [("M4",)],
      "lieu cité sans accent (« nimes » pour « Nîmes », client « Multi » avec espaces) : le message le plus récent l'emporte")
sql("UPDATE messages SET created_at = (now() - interval '30 hours') WHERE client_id = 'multi'")
n = nb_pannes()
SCRIPT.append(("ok", "[PANNE: Pompe désamorcée | la pompe tourne sans débit]\nPurgez la pompe."))
st, evs, toks = post("multi", "multi", "la pompe tourne mais rien ne sort")
m = dernier_agent("multi")
check(nb_pannes() == n and m[2] is None
      and m[3] == {"type": "Pompe désamorcée", "constat": "la pompe tourne sans débit"} and "PANNE" not in toks,
      "plusieurs stations, aucun lieu cité depuis 24 h : pas de panne, suggestion enregistrée")
suggestion_id = m[0]
SCRIPT.append(("ok", "[PANNE: Fuite cuve | fuite]\nOK."))
post("multi", "multi", "fuite à Marseille et à Aix")
check(nb_pannes() == n and dernier_agent("multi")[3] is not None, "deux lieux cités dans le même message : ambigu, suggestion")
SCRIPT.append(("ok", "[PANNE: Fuite cuve | fuite]\nOK."))
post("vide", "vide", "fuite")
check(nb_pannes() == n and dernier_agent("vide")[3] == {"type": "Fuite cuve", "constat": "fuite"},
      "client sans station : suggestion, aucune panne")

# 7. GET /messages : suggestion réservée à l'équipe
mc = api.get("/messages/multi", headers=H("multi")).json()
ma = api.get("/messages/multi", headers=ADMIN).json()
check(all(x["panne_suggeree"] is None for x in mc) and all("panne_id" in x for x in mc),
      "client : panne_suggeree jamais renvoyée")
check(next(x for x in ma if x["id"] == suggestion_id)["panne_suggeree"]["type"] == "Pompe désamorcée"
      and any(x["panne_id"] for x in ma), "admin : panne_id et panne_suggeree renvoyés")

# 8. Liaison par l'équipe : POST /messages/{id}/panne
r = api.post("/pannes", headers=ADMIN, json={"client": "multi", "lieu": "Aix", "nom_automate": "M3",
                                             "panne": "Pompe désamorcée", "probleme": "constaté",
                                             "date_debut": "2026-10-21T08:00:00Z"})
check(r.status_code == 200 and r.json()["created_by"] == "a@x" and r.json()["intervenant"] == "a@x",
      "POST /pannes inchangé (created_by et intervenant = admin)")
pa = r.json()["id"]
check(api.post(f"/messages/{suggestion_id}/panne", json={"panne_id": pa}, headers=H("multi")).status_code == 403
      and api.post(f"/messages/{suggestion_id}/panne", json={"panne_id": pa}).status_code == 401,
      "liaison : 403 pour un client, 401 sans jeton")
check(api.post(f"/messages/{suggestion_id}/panne", json={"panne_id": p1}, headers=ADMIN).status_code == 400
      and api.post("/messages/999999/panne", json={"panne_id": pa}, headers=ADMIN).status_code == 404
      and api.post(f"/messages/{suggestion_id}/panne", json={"panne_id": 999999}, headers=ADMIN).status_code == 404,
      "liaison : panne d'un autre client 400, message ou panne inconnus 404")
r = api.post(f"/messages/{suggestion_id}/panne", json={"panne_id": pa}, headers=ADMIN)
check(r.status_code == 200 and rows("SELECT panne_id, panne_suggeree FROM messages WHERE id = %s", suggestion_id) == [(pa, None)],
      "liaison : panne liée au message, suggestion effacée")
api.delete(f"/pannes/{pa}", headers=ADMIN)
check(rows("SELECT panne_id FROM messages WHERE id = %s", suggestion_id) == [(None,)],
      "panne supprimée : lien remis à NULL (ON DELETE SET NULL)")

# 9. Canal e-mail : même cœur, même traitement de la panne
import asyncio  # noqa: E402


async def _evenements_email(client_id, content):
    return [e async for e in backend._chat_agent_evenements(client_id, content, "email", "gerant@multi.test")]

sql("INSERT INTO messages (client_id, sender_id, content, canal) VALUES ('multi', 1, %s, 'email')",
    "Le filtre d'Aix est colmaté")
SCRIPT.append(("ok", "[PANNE: Filtre colmaté | filtre colmaté]\nLancez un contre-lavage."))
evs = asyncio.run(_evenements_email("multi", "Le filtre d'Aix est colmaté"))
m = rows("SELECT m.content, m.canal, p.nom_automate FROM messages m JOIN pannes p ON p.id = m.panne_id "
         "WHERE m.client_id = 'multi' AND m.sender_id = -1 ORDER BY m.id DESC LIMIT 1")
check(evs[-1][0] == "saved" and m == [("Lancez un contre-lavage.", "email", "M3")],
      "canal e-mail : marqueur retiré de la réponse envoyée, panne créée sur la station citée")

# 10. Filtre de flux (unitaire)
d = backend._chat_texte_diffusable
check(d("Bonjour [PAN") == "Bonjour " and d("Bonjour [PANNE: Pompe HS | x") == "Bonjour "
      and d("Bonjour [PANNE: Pompe HS | x]\nSuite") == "Bonjour Suite" and d("[ESCAL") == ""
      and d("Voir [la notice") == "Voir [la notice" and d("[Pannes") == "[Pannes",
      "flux : marqueur incomplet retenu, texte ordinaire entre crochets transmis")

print("S2 — fiches SAV proposées par l'assistant")
FICHE = {"titre": "Pompe désamorcée à Toulon", "symptomes": "La pompe tourne sans débit",
         "solution": ["1. Fermer la vanne", "2. Purger la pompe"], "pieces": ["joint", "crépine"],
         "type_panne": "Pompe désamorcée", "mots_cles": ["désamorçage", "débit nul"]}
for i in range(15):
    sql("INSERT INTO messages (client_id, sender_id, content) VALUES ('solo', 0, %s)", f"Note de l'équipe {i} : station de Toulon")
n_conv = rows("SELECT count(*) FROM chat_agent_conv")[0][0]
SCRIPT.append(("ok", "Voici la fiche :\n```json\n" + json.dumps(FICHE, ensure_ascii=False) + "\n```\nBonne journée."))
r = api.post("/sav/fiches/proposer", json={"client_id": "solo"}, headers=ADMIN)
f = r.json()
check(r.status_code == 200 and f["brouillon"] is True and f["archivee"] is False and f["created_by"] == "assistant-sav"
      and f["source_client_id"] == "solo", "brouillon créé depuis un bloc ```json avec du texte autour")
check(f["solution"] == "1. Fermer la vanne\n2. Purger la pompe" and f["pieces"] == "joint, crépine"
      and f["mots_cles"] == "désamorçage, débit nul" and f["titre"] == "Pompe désamorcée à [station]",
      "champs normalisés (listes jointes) et noms de station masqués dans la fiche")
appel = CALLS[-1]
check(appel["external_user_id"] == "eaukey-redaction-fiches" and appel["conversation_id"] is None
      and "[CONSIGNE RÉDACTION DE FICHE SAV" in appel["message"] and "Client : " in appel["message"]
      and "Assistant SAV : " in appel["message"] and "Toulon" not in appel["message"]
      and "généralise" in appel["message"], "appel Wardian : utilisateur dédié, nouvelle conversation, consigne, noms masqués")
check(appel["message"].count("\nClient : ") + appel["message"].count("\nAssistant SAV : ")
      + appel["message"].count("\nÉquipe Eaukey : ") == 30, "30 derniers messages du fil transmis")
check(rows("SELECT count(*) FROM chat_agent_conv")[0][0] == n_conv, "conversation du fil client non modifiée")
brouillon_id = f["id"]

SCRIPT.append(("ok", 'Bien sûr ! {"titre": "Filtre colmaté", "solution": "Contre-lavage manuel", "symptomes": null} Voilà.'))
r = api.post("/sav/fiches/proposer", json={"client_id": "solo"}, headers=ADMIN)
check(r.status_code == 200 and r.json()["titre"] == "Filtre colmaté" and r.json()["symptomes"] is None
      and CALLS[-1]["conversation_id"] is None, "JSON sans bloc, texte autour : brouillon créé (nouvelle conversation)")

n = nb_fiches()
SCRIPT.append(("ok", "Je ne peux pas rédiger de fiche {titre: oups"))
r = api.post("/sav/fiches/proposer", json={"client_id": "solo"}, headers=ADMIN)
check(r.status_code == 502 and "aucune fiche" in r.json()["detail"] and nb_fiches() == n, "JSON invalide : 502 propre, aucune fiche")
SCRIPT.append(("ok", '{"titre": "Question générale", "solution": ""}'))
r = api.post("/sav/fiches/proposer", json={"client_id": "solo"}, headers=ADMIN)
check(r.status_code == 422 and nb_fiches() == n, "pas de solution dans l'échange : 422, aucune fiche")
SCRIPT.append(("status", 500))
r = api.post("/sav/fiches/proposer", json={"client_id": "solo"}, headers=ADMIN)
check(r.status_code == 502 and nb_fiches() == n, "Wardian en erreur : 502, aucune fiche")
n_calls = len(CALLS)
check(api.post("/sav/fiches/proposer", json={"client_id": "inconnu"}, headers=ADMIN).status_code == 404
      and len(CALLS) == n_calls, "fil vide : 404, Wardian non appelé")
check(api.post("/sav/fiches/proposer", json={"client_id": "solo"}, headers=H("solo")).status_code == 403
      and api.post("/sav/fiches/proposer", json={"client_id": "solo"}).status_code == 401 and len(CALLS) == n_calls,
      "proposer : 403 pour un client, 401 sans jeton")

# Brouillon exclu du contexte joint aux questions des clients
sql("UPDATE sav_fiches SET solution = solution || ' zorglub' WHERE id = %s", brouillon_id)
check(backend._sav_contexte("zorglub") == "" and "zorglub" not in backend._sav_message_avec_contexte("pompe zorglub")
      .replace("pompe zorglub", ""), "brouillon jamais joint aux questions (_sav_contexte)")
liste = api.get("/sav/fiches", headers=ADMIN).json()
check(liste[0]["brouillon"] is True and all("brouillon" in x for x in liste), "GET /sav/fiches : champ brouillon, brouillons en tête")
corps = {"titre": "Pompe désamorcée", "symptomes": "La pompe tourne sans débit",
         "solution": "1. Fermer la vanne\n2. Purger la pompe zorglub", "archivee": False}
r = api.put(f"/sav/fiches/{brouillon_id}", json=corps, headers=ADMIN)
check(r.status_code == 200 and r.json()["brouillon"] is True and r.json()["titre"] == "Pompe désamorcée"
      and backend._sav_contexte("zorglub") == "", "relecture (PUT sans « brouillon ») : reste un brouillon")
r = api.put(f"/sav/fiches/{brouillon_id}", json={**corps, "brouillon": False}, headers=ADMIN)
check(r.status_code == 200 and r.json()["brouillon"] is False and r.json()["updated_by"] == "a@x"
      and "zorglub" in backend._sav_contexte("zorglub"), "publication (brouillon=false) : fiche utilisée par l'assistant")
check(api.put(f"/sav/fiches/{brouillon_id}", json={**corps, "brouillon": False}, headers=H("solo")).status_code == 403,
      "publication réservée à l'équipe (403 client)")
r = api.post("/sav/fiches", json={"titre": "Manuelle", "solution": "Étape"}, headers=ADMIN)
check(r.status_code == 200 and r.json()["brouillon"] is False and r.json()["source_client_id"] is None,
      "fiche saisie par l'équipe : publiée directement (brouillon false)")
check(api.get("/sav/fiches", headers=H("solo")).status_code == 403, "liste des fiches refusée au client")

print(f"\n{ok} vérifications OK")
