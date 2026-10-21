"""Tests du rapport « Problèmes fréquents » (tâche S4) : GET /rapport/problemes(/texte).

BASE LOCALE JETABLE UNIQUEMENT. Le script vide et recrée les tables pannes, messages,
automate, sav_fiches et alertes : ne JAMAIS le lancer sur la base de prod.
Garde-fou : il refuse de tourner si l'hôte n'est pas un dossier de socket local
(ou localhost / 127.0.0.1) ou si la base contient déjà la table `mesures`.

Lancement (Postgres 16 jetable, socket dans <dossier>) :
    initdb -D <dossier>/data -U postgres -A trust
    pg_ctl -D <dossier>/data -o "-p 55437 -k <dossier> -c listen_addresses=''" -w start
    python tests/test_problemes_frequents.py <dossier> 55437
(dépendances : requirements.txt + httpx pour le TestClient de FastAPI)
"""
import os
import sys

import psycopg

HOTE = os.path.abspath(sys.argv[1]) if len(sys.argv) > 1 else ""
PORT = sys.argv[2] if len(sys.argv) > 2 else "55437"
if not (os.path.isdir(HOTE) or HOTE in ("localhost", "127.0.0.1")):
    sys.exit("Garde-fou : donner un dossier de socket local (base jetable uniquement).")
DSN = f"host={HOTE} port={PORT} user=postgres dbname=postgres"
with psycopg.connect(DSN, autocommit=True) as c:
    if c.execute("SELECT to_regclass('public.mesures')").fetchone()[0]:
        sys.exit("Garde-fou : la table mesures existe, ce n'est pas une base jetable.")

os.environ.update({"DB_NAME": "postgres", "DB_USER": "postgres", "DB_HOST": HOTE, "DB_PASSWORD": "x",
                   "PGPORT": PORT, "AUTH_SECRET": "test"})
B = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def sql(q, params=None):
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute(q, params)


sql("""
  DROP TABLE IF EXISTS pannes, messages, chat_agent_conv, automate, sav_fiches, alertes;
  DO $$ BEGIN CREATE ROLE app_backend; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
  CREATE TABLE automate (nom_automate text, client text, lieu text);
  CREATE TABLE pannes (id serial PRIMARY KEY, client text, lieu text, nom_automate text, panne text,
                       probleme text, date_debut timestamp, date_fin timestamp, created_by text);
  CREATE TABLE messages (id serial PRIMARY KEY, client_id text, sender_id int, content text,
                         file_url text, created_at text DEFAULT now()::text, is_read boolean DEFAULT false);
""")
for m in ("007_pannes_resolution.sql", "008_sav_fiches.sql", "009_chat_agent.sql"):
    sql(open(os.path.join(B, "migrations", m)).read())

# Stations : 'A1 ' a un espace en trop dans automate (jointure par trim)
sql("INSERT INTO automate VALUES ('A1 ', 'Acme', 'Lavage Nord'), ('B2', 'Beta', 'Station Sud'), ('C3', 'Gamma', 'Centre')")


def panne(auto, client, type_, debut_j, fin_j=None, cause=None, pieces=None, depl=None):
    sql("INSERT INTO pannes (client, lieu, nom_automate, panne, probleme, date_debut, date_fin, created_by, "
        "cause, pieces_changees, deplacement) VALUES (%s, 'x', %s, %s, 'constat', "
        "(now() - make_interval(days => %s))::timestamp, "
        "CASE WHEN %s::float IS NULL THEN NULL ELSE (now() - make_interval(secs => %s::float * 86400))::timestamp END, "
        "'t@x', %s, %s, %s)",
        (client, auto, type_, debut_j, fin_j, fin_j, cause, pieces, depl))


# Période courante (30 derniers jours)
panne("A1", "ancien nom", "Pompe HS", 20, 19, "Filtre colmaté", "Filtre, joint", True)      # 24 h
panne("A1", "ancien nom", "pompe hs ", 15, 14.5, "filtre  colmaté.", "filtre", False)       # 12 h
panne("A1", "ancien nom", "Pompe HS", 10, 9, "Filtre Colmaté", "Joint et vanne", True)      # 24 h
panne("A1", "ancien nom", "Fuite", 5)                                                       # en cours
panne("B2", "Beta", "Fuite", 8, 7, "Joint usé", None, None)                                 # 24 h
panne("B2", "Beta", "Capteur niveau HS", 3, 2, "Joint usé", None, True)                     # 24 h
panne("C3", "Gamma", "Fuite", 25, 24, "Électrovanne bloquée", None, False)                  # 24 h
# Période précédente (30 à 60 jours)
panne("C3", "Gamma", "Fuite", 40, 39)
panne("C3", "Gamma", "Pompe HS", 45, 44)
panne("B2", "Beta", "Fuite", 35, 28, "joint usé", "Joint", False)  # signalée avant, résolue dans la période (7 j)
panne("A1", "Acme", "Fuite", 100, 99)                              # hors fenêtre


def esc(client, sender, motif, jours_avant, escalade=True, brut=None):
    sql("INSERT INTO messages (client_id, sender_id, content, created_at, escalade, escalade_motif) "
        "VALUES (%s, %s, 'msg', coalesce(%s, (now() - make_interval(days => %s))::text), %s, %s)",
        (client, sender, brut, jours_avant, escalade, motif))


esc("acme", -1, "Pompe à remplacer", 2)
esc("acme", -1, "pompe à remplacer !", 5)
esc("beta", -1, "Pompe  à remplacer", 10)
esc("beta", -1, "Question facturation", 12)
esc("gamma", -1, None, 3)
esc("delta", -1, "x", 40)                       # période précédente
esc("acme", 0, "équipe", 1)                     # pas l'assistant : ignoré
esc("acme", -1, None, 1, escalade=False)        # réponse sans escalade : ignorée
esc("beta", -1, "date illisible", 0, brut="n/a")  # created_at non daté : ignoré sans erreur

sys.path.insert(0, B)
import backend  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

assert backend._DB_DSN["host"] == HOTE, "Garde-fou : le backend ne pointe pas sur la base jetable"

USERS = {
    "client": {"id": 7, "email": "c@x", "roles": ["client"], "client_id": "acme", "_automates": set(), "_clients": {"acme"}},
    "admin": {"id": 1, "email": "sav@eaukey.test", "roles": ["admin"], "client_id": None},
}


def fake_auth(request):
    tok = (request.headers.get("authorization") or "").split(" ")[-1]
    if tok not in USERS:
        raise backend.HTTPException(status_code=401, detail="no")
    return dict(USERS[tok])


backend._require_auth = fake_auth
api = TestClient(backend.app)
H = {"Authorization": "Bearer admin"}
ok = 0


def check(cond, label):
    global ok
    assert cond, label
    ok += 1
    print("  ok -", label)


def rapport(jours=30):
    r = api.get(f"/rapport/problemes?jours={jours}", headers=H)
    assert r.status_code == 200, r.text
    return r.json()


def regles(r):
    return [(x["regle"], x["priorite"]) for x in r["recommandations"]]


r = rapport()
p = r["pannes"]
check(p["signalees"] == {"actuel": 7, "precedent": 3, "ecart": 4, "pct": 133}, f"pannes signalées + variation : {p['signalees']}")
check(p["resolues"] == {"actuel": 7, "precedent": 2, "ecart": 5, "pct": 250}, f"pannes résolues : {p['resolues']}")
check(p["en_cours"] == 1, "pannes en cours")
check(p["top_types"] == [{"libelle": "Fuite", "nombre": 3}, {"libelle": "Pompe HS", "nombre": 3},
                         {"libelle": "Capteur niveau HS", "nombre": 1}], f"top types (casse/espaces) : {p['top_types']}")
check(p["top_causes"] == [{"libelle": "Filtre colmaté", "nombre": 3}, {"libelle": "Joint usé", "nombre": 3},
                          {"libelle": "Électrovanne bloquée", "nombre": 1}], f"causes regroupées : {p['top_causes']}")
check(p["delai_moyen_heures"] == 42.9 and p["delai_base"] == 7, f"délai moyen : {p['delai_moyen_heures']}")
check(p["deplacement"] == {"avec": 3, "renseignees": 6, "pct": 50}, f"déplacement : {p['deplacement']}")
check(p["top_pieces"] == [{"libelle": "Joint", "nombre": 3}, {"libelle": "Filtre", "nombre": 2},
                          {"libelle": "vanne", "nombre": 1}], f"pièces : {p['top_pieces']}")
st = {s["nom_automate"]: s for s in p["stations"]}
check(set(st) == {"A1", "B2"} and st["A1"]["nombre"] == 4 and st["B2"]["nombre"] == 2, "stations ≥ 2 pannes")
check(st["A1"]["client"] == "Acme" and st["A1"]["lieu"] == "Lavage Nord", "station jointe à automate malgré l'espace en trop")
check("toutes_causes" not in p, "liste interne retirée de la réponse")

e = r["escalades"]
check(e["nombre"] == {"actuel": 5, "precedent": 1, "ecart": 4, "pct": 400}, f"escalades : {e['nombre']}")
check(e["top_motifs"][0] == {"libelle": "Pompe à remplacer", "nombre": 3} and len(e["top_motifs"]) == 2,
      f"motifs regroupés (ponctuation, espaces) : {e['top_motifs']}")
check(e["clients"] == 3 and e["sans_motif"] == 1, "clients concernés (nombre) + sans motif")

check(r["alertes"]["disponible"] is False and r["alertes"]["top_codes"] == [], "table alertes absente : section vide")

check(("fiche_sav", "moyenne") in regles(r)
      and {x["chiffres"]["cause"] for x in r["recommandations"] if x["regle"] == "fiche_sav"} == {"Filtre colmaté", "Joint usé"},
      "reco « créer une fiche » pour les causes répétées sans fiche")
check(not any(x["regle"] == "fiche_sav" and x["chiffres"]["cause"] == "Électrovanne bloquée" for x in r["recommandations"]),
      "pas de reco pour une cause vue une seule fois")
vis = [x for x in r["recommandations"] if x["regle"] == "visite_preventive"]
check(len(vis) == 1 and vis[0]["chiffres"] == {"nom_automate": "A1", "pannes": 4, "jours": 30}
      and "Acme — Lavage Nord" in vis[0]["texte"], "reco visite préventive (A1 seulement)")
ag = [x for x in r["recommandations"] if x["regle"] == "agent_sav"]
check(len(ag) == 1 and ag[0]["chiffres"]["escalades"] == 3, "reco agent SAV pour le motif répété")
check(("hausse_pannes", "moyenne") in regles(r), "reco hausse des pannes (+133 %)")
check(all(x["texte"] and x["pourquoi"] and x["chiffres"] and x["priorite"] in ("haute", "moyenne")
          for x in r["recommandations"]), "chaque reco : texte, pourquoi, chiffres, priorité")

# Une fiche SAV existante couvre « Joint usé » -> plus de reco ; une fiche archivée ne compte pas
sql("INSERT INTO sav_fiches (titre, solution, mots_cles, archivee, created_by) VALUES "
    "('Filtre colmaté', 'Nettoyer', NULL, true, 't'), ('Remplacer un joint usé', 'Changer le joint', 'fuite', false, 't')")
causes = {x["chiffres"]["cause"] for x in rapport()["recommandations"] if x["regle"] == "fiche_sav"}
check(causes == {"Filtre colmaté"}, f"fiche existante -> pas de reco ; fiche archivée ignorée : {causes}")

# Période de 7 jours : variation à 0 %
r7 = rapport(7)
check(r7["pannes"]["signalees"] == {"actuel": 2, "precedent": 2, "ecart": 0, "pct": 0}
      and not any(x["regle"] == "hausse_pannes" for x in r7["recommandations"]), "période 7 jours")

# Table alertes présente (colonnes de la migration 010)
sql("""
  CREATE TABLE alertes (id bigserial PRIMARY KEY, nom_automate text NOT NULL, code text NOT NULL,
    libelle text NOT NULL, niveau text NOT NULL, classe text NOT NULL, statut text NOT NULL DEFAULT 'nouvelle',
    occurrences integer NOT NULL DEFAULT 1, premiere_at timestamptz NOT NULL, derniere_at timestamptz NOT NULL,
    panne_id bigint);
""")


def alerte(auto, code, libelle, occ, jours_avant, panne_id=None):
    sql("INSERT INTO alertes (nom_automate, code, libelle, niveau, classe, occurrences, premiere_at, derniere_at, panne_id) "
        "VALUES (%s, %s, %s, 'avertissement', 'seuil', %s, now() - make_interval(days => %s + 1), "
        "now() - make_interval(days => %s), %s)", (auto, code, libelle, occ, jours_avant, jours_avant, panne_id))


alerte("A1", "E12", "Pression basse", 30, 2)
alerte("B2", "E12", "Pression basse", 5, 3)
alerte("A1", "E40", "Fuite détectée", 3, 4, 4)
alerte("C3", "E50", "Niveau haut", 8, 6)
alerte("C3", "E12", "Pression basse", 100, 45)  # période précédente
r = rapport()
a = r["alertes"]
check(a["disponible"] and a["nombre"] == {"actuel": 4, "precedent": 1, "ecart": 3, "pct": 300} and a["occurrences"] == 46,
      f"alertes : nombre + occurrences {a['nombre']} {a['occurrences']}")
check([(c["code"], c["occurrences"], c["stations"], c["pannes"]) for c in a["top_codes"]]
      == [("E12", 35, 2, 0), ("E50", 8, 1, 0), ("E40", 3, 1, 1)], f"top codes : {a['top_codes']}")
check([(s["nom_automate"], s["occurrences"]) for s in a["stations"]] == [("A1", 33), ("C3", 8), ("B2", 5)]
      and a["stations"][0]["client"] == "Acme", "stations les plus bruyantes")
check(a["devenues_pannes"] == {"nombre": 1, "total": 4, "pct": 25}, "part d'alertes devenues pannes")
seuil = [x for x in r["recommandations"] if x["regle"] == "seuil_alerte"]
check(len(seuil) == 1 and seuil[0]["chiffres"]["code"] == "E12" and seuil[0]["priorite"] == "moyenne",
      "reco seuil : E12 (sans panne) oui, E40 (devenue panne) et E50 (8 occ.) non")
check("tous_codes" not in a, "liste interne des codes retirée")

sql("UPDATE alertes SET occurrences = 60 WHERE nom_automate = 'A1' AND code = 'E12'")
r = rapport()
check(r["recommandations"][0]["regle"] == "seuil_alerte" and r["recommandations"][0]["priorite"] == "haute",
      "priorité haute en tête de liste")

# Texte brut
t = api.get("/rapport/problemes/texte?jours=30", headers=H)
check(t.status_code == 200 and t.headers["content-type"].startswith("text/plain"), "texte : text/plain")
txt = t.text
check(txt.startswith("PROBLÈMES FRÉQUENTS — 30 derniers jours") and "RECOMMANDATIONS\n1. [HAUTE] Revoir le seuil de l'alerte E12" in txt,
      "texte : titre + recommandations en premier")
check("- Signalées : 7 (période précédente : 3, +133 %)" in txt and "- Délai moyen de résolution : 42,9 h (sur 7 pannes)" in txt
      and "  - Filtre colmaté : 3" in txt and "- Clients concernés : 3" in txt and "Acme — Lavage Nord (A1) : 4 pannes" in txt,
      "texte : chiffres lisibles")
sql("DROP TABLE alertes")
check("Pas encore disponible" in api.get("/rapport/problemes/texte", headers=H).text, "texte : alertes pas encore disponibles")

# Paramètre et droits
check(api.get("/rapport/problemes?jours=0", headers=H).status_code == 422, "jours=0 refusé (422)")
for path in ("/rapport/problemes", "/rapport/problemes/texte"):
    check(api.get(path).status_code == 401, f"sans jeton : 401 sur {path}")
    check(api.get(path, headers={"Authorization": "Bearer client"}).status_code == 403, f"client : 403 sur {path}")
check(("GET", "/rapport/problemes") in backend._ADMIN_ONLY_ROUTES, "routes déclarées admin")

print(f"\n{ok} vérifications OK")
