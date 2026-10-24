"""Tests de la file « photos à vérifier » (GET /eau/a_verifier, GET /eau/a_verifier/stats).

A lancer UNIQUEMENT contre une base Postgres jetable locale : le script SUPPRIME
puis recrée les tables automate et urls_images.

    export PHOTOS_TEST_PGHOST=127.0.0.1   # ou un socket local
    export PHOTOS_TEST_PGPORT=55435
    python tests/test_photos_a_verifier.py

Données 100 % synthétiques. L'authentification est simulée (pas de table users).
"""
import os
import sys

import psycopg

ICI = os.path.dirname(os.path.abspath(__file__))
DEPOT = os.path.dirname(ICI)

HOST = os.environ.get("PHOTOS_TEST_PGHOST", "")
PORT = os.environ.get("PHOTOS_TEST_PGPORT", "")
if not HOST or not PORT or not (HOST.startswith("/") or HOST in ("localhost", "127.0.0.1")):
    sys.exit("PHOTOS_TEST_PGHOST (socket local ou localhost) et PHOTOS_TEST_PGPORT sont obligatoires : "
             "base jetable locale uniquement.")

os.environ.update({
    "DB_NAME": "postgres", "DB_USER": "postgres", "DB_HOST": HOST, "DB_PASSWORD": "x",
    "PGPORT": PORT, "AUTH_SECRET": "test",
})
DSN = f"host={HOST} port={PORT} user=postgres dbname=postgres"

SCHEMA = """
DROP TABLE IF EXISTS urls_images, automate;
CREATE TABLE automate (nom_automate text, client text, lieu text);
CREATE TABLE urls_images (
  id serial PRIMARY KEY, url varchar, "timestamp" timestamp NOT NULL, numero_automate integer,
  pred_qualite_eau double precision, pred_opacite double precision, pred_matiere_prob double precision,
  pred_bac_vide_prob double precision, pred_computed_at timestamptz, pred_modele text,
  corr_qualite_eau double precision, corr_opacite double precision, corr_matiere double precision,
  corr_bac_vide boolean, validated_by text, validated_at timestamptz);
INSERT INTO automate VALUES ('2024405.0', 'Lavage Alpha', 'Alpha Marseille'), ('SMH1', 'Beta', 'Beta Lyon');
"""


def heure(h):
    """Début d'une heure passée, h heures avant maintenant (tronqué à l'heure)."""
    return f"date_trunc('hour', now()::timestamp) - interval '{h} hours'"


def photo(c, h, minute=10, auto=2024405, q=9.0, o=1.0, m=0.05, bv=0.02, url="u", validee=False):
    r = c.execute(
        f"""INSERT INTO urls_images (url, "timestamp", numero_automate, pred_qualite_eau, pred_opacite,
                                     pred_matiere_prob, pred_bac_vide_prob, pred_modele, validated_at)
            VALUES (%s, {heure(h)} + interval '{minute} minutes', %s, %s, %s, %s, %s, 'v1', NULL) RETURNING id""",
        (url, auto, q, o, m, bv)).fetchone()
    if validee:
        c.execute("UPDATE urls_images SET validated_at = now() WHERE id = %s", (r[0],))
    return r[0]


def echantillon(i):
    return (i * 2654435761) % 100 < 10


with psycopg.connect(DSN, autocommit=True) as c:
    c.execute(SCHEMA)
    # 200 heures « normales » (bonne eau, modèle sûr) : seules celles de l'échantillon doivent sortir
    normales = [photo(c, 100 + k) for k in range(200)]
    # heure 1 : 3 photos dont la dernière (minute 50) représente l'heure ; modèle : bac vide -> alerte
    photo(c, 1, 5, bv=0.9); photo(c, 1, 30, bv=0.9); p_alerte = photo(c, 1, 50, bv=0.9)
    p_indice = photo(c, 2, q=2.0, o=8.0)          # indice (2 + 2) / 2 = 2 -> alerte
    p_mes = photo(c, 3, m=0.8)                    # MES probable -> alerte
    p_incertain = photo(c, 4, bv=0.4)             # hésite sur bac vide -> incertain
    p_incertain_mes = photo(c, 5, m=0.4)          # hésite sur MES -> incertain
    p_vieille = photo(c, 24 * 40, bv=0.9)         # alerte vieille de 40 jours
    photo(c, 6, 10, bv=0.9, validee=True); p_heure_validee = photo(c, 6, 40, bv=0.9)  # heure déjà relue
    p_sans_url = photo(c, 7, bv=0.9, url=None)
    c.execute(f"""INSERT INTO urls_images (url, "timestamp", numero_automate) VALUES ('u', {heure(8)}, 2024405)""")
    p_autre = photo(c, 1, 20, auto=9999, bv=0.9)  # station absente de la table automate

sys.path.insert(0, DEPOT)
import backend  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

USERS = {
    "admin": {"id": 1, "email": "a@x", "roles": ["admin"], "client_id": None},
    "super": {"id": 2, "email": "s@x", "roles": ["super_admin"], "client_id": None},
}


def fake_auth(request):
    tok = (request.headers.get("authorization") or "").split(" ")[-1]
    if tok not in USERS:
        raise backend.HTTPException(status_code=401, detail="no")
    return dict(USERS[tok])


backend._require_auth = fake_auth
api = TestClient(backend.app)
ok = 0


def check(cond, label):
    global ok
    assert cond, label
    ok += 1
    print("  ok -", label)


def get(path, who="super", **params):
    h = {"Authorization": f"Bearer {who}"} if who else {}
    return api.get(path, params=params, headers=h)


print("Droits")
check(get("/eau/a_verifier", who=None).status_code == 401, "sans jeton -> 401")
check(get("/eau/a_verifier", who="admin").status_code == 403, "admin (pas super admin) -> 403")
check(get("/eau/a_verifier/stats", who="admin").status_code == 403, "stats admin -> 403")

print("File")
r = get("/eau/a_verifier", limit=100)
check(r.status_code == 200, "super admin -> 200")
d = r.json()
ids = [p["id"] for p in d["photos"]]
motif = {p["id"]: p["motif"] for p in d["photos"]}
attendus_ech = [i for i in normales if echantillon(i)]
check(len(attendus_ech) > 5, f"jeu de test : {len(attendus_ech)} heures normales dans l'échantillon")
non_ech = lambda i, m: (motif.get(i) == "echantillon") if echantillon(i) else (motif.get(i) == m)
check(non_ech(p_alerte, "alerte"), "bac vide probable -> alerte (ou échantillon)")
check(non_ech(p_indice, "alerte"), "indice bas -> alerte")
check(non_ech(p_mes, "alerte"), "MES probable -> alerte")
check(non_ech(p_incertain, "incertain"), "bac vide hésitant -> incertain")
check(non_ech(p_incertain_mes, "incertain"), "MES hésitante -> incertain")
check(sum(1 for i in ids if i in normales) == len(attendus_ech)
      and all(motif[i] == "echantillon" for i in ids if i in normales), "heures normales : seulement l'échantillon (~10 %)")
check(set(i for i in ids if i in normales) == set(i for i in attendus_ech if i in ids), "échantillon = règle fixe sur l'id")
check(p_vieille not in ids, "photo de plus de 30 jours exclue")
check(p_heure_validee not in ids, "heure déjà relue exclue")
check(p_sans_url not in ids, "photo sans URL exclue")
check(sum(1 for p in d["photos"] if p["timestamp"] and p["numero_automate"] == 2024405
          and p["timestamp"][:13] == next(x for x in d["photos"] if x["id"] == p_alerte)["timestamp"][:13]) == 1,
      "une seule photo par station et par heure")
check(p_alerte in ids, "la photo retenue pour l'heure est la plus récente")
check(p_autre in ids and next(p for p in d["photos"] if p["id"] == p_autre)["nom_automate"] == "9999.0",
      "station inconnue de la table automate : nom reconstruit")
ordre = [p["motif"] for p in d["photos"]]
rang = {"alerte": 0, "incertain": 1, "echantillon": 2}
check(ordre == sorted(ordre, key=lambda m: rang[m]), "ordre : alertes, puis incertaines, puis échantillon")
p0 = next(p for p in d["photos"] if p["id"] == p_alerte)
check(p0["nom_automate"] == "2024405.0" and p0["client"] == "Lavage Alpha" and p0["lieu"] == "Alpha Marseille",
      "station, client et lieu joints")
check(p0["pred_matiere"] is not None and abs(p0["pred_matiere"] - 0.5) < 1e-9, "MES remise sur /10 (0,05 -> 0,5)")
check(d["restantes"] == len(ids), f"restantes = {d['restantes']}")
check(len(get("/eau/a_verifier", limit=3).json()["photos"]) == 3, "limit respecté")
check(get("/eau/a_verifier", limit=3).json()["restantes"] == d["restantes"], "restantes = total, pas la page")
check(p_vieille in [p["id"] for p in get("/eau/a_verifier", jours=60, limit=100).json()["photos"]], "jours=60 inclut la photo de 40 jours")
check(get("/eau/a_verifier", jours=0).status_code == 422 and get("/eau/a_verifier", limit=500).status_code == 422,
      "paramètres hors bornes -> 422")

print("Validation (route existante) puis file")
h = {"Authorization": "Bearer super"}
r = api.put(f"/eau/photo/{p_indice}/validation", json={}, headers=h)
check(r.status_code == 200, "validation simple (le modèle avait raison)")
ids2 = [p["id"] for p in get("/eau/a_verifier", limit=100).json()["photos"]]
check(p_indice not in ids2 and len(ids2) == len(ids) - 1, "photo validée sortie de la file")
# échantillon noté à l'aveugle : l'admin donne ses propres valeurs
e1, e2 = [i for i in attendus_ech if i in ids][:2]
api.put(f"/eau/photo/{e1}/validation", json={"qualite_eau": 7.0, "opacite": 3.0}, headers=h)   # prédiction 9 / 1
api.put(f"/eau/photo/{e2}/validation", json={"qualite_eau": 9.0, "opacite": 1.0}, headers=h)   # juste
api.put(f"/eau/photo/{p_mes}/validation", json={"qualite_eau": 1.0, "opacite": 9.0}, headers=h)  # hors échantillon (sauf si tiré)
s = get("/eau/a_verifier/stats").json()
check(s["validees_7j"] == 5, f"5 validations sur 7 jours, dont l heure déjà relue ({s['validees_7j']})")
n_ech = 2 + (1 if echantillon(p_mes) else 0)
check(s["echantillon_notees"] == n_ech, f"échantillon noté : {s['echantillon_notees']}")
# écarts attendus : e1 2 et 2, e2 0 et 0, p_mes (prédiction 9 / 1, note 1 / 9) 8 et 8 s'il est dans l'échantillon
attendu = round((2 + 0 + (8 if echantillon(p_mes) else 0)) / n_ech, 2)
check(s["erreur_qualite"] == attendu and s["erreur_opacite"] == attendu,
      f"erreur moyenne calculée sur l'échantillon seulement ({s['erreur_qualite']} = {attendu})")
check(get("/eau/a_verifier/stats", jours=3).status_code == 422, "stats jours < 7 -> 422")

print(f"\n{ok} vérifications OK")
