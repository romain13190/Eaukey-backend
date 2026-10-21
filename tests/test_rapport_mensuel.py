"""Tests du rapport de performance mensuel (routes /rapport/mensuel*).

A lancer UNIQUEMENT contre une base Postgres jetable locale : le script SUPPRIME
puis recree les tables automate, mesures_aggregate et pannes.

    export RAPPORT_TEST_PGHOST=/chemin/du/socket   # ou 127.0.0.1
    export RAPPORT_TEST_PGPORT=55434
    python tests/test_rapport_mensuel.py

Donnees 100 % synthetiques. L'authentification est simulee (pas de table users).
"""
import json
import os
import sys

import psycopg

ICI = os.path.dirname(os.path.abspath(__file__))
DEPOT = os.path.dirname(ICI)

HOST = os.environ.get("RAPPORT_TEST_PGHOST", "")
PORT = os.environ.get("RAPPORT_TEST_PGPORT", "")
if not HOST or not PORT or not (HOST.startswith("/") or HOST in ("localhost", "127.0.0.1")):
    sys.exit("RAPPORT_TEST_PGHOST (socket local ou localhost) et RAPPORT_TEST_PGPORT sont obligatoires : "
             "base jetable locale uniquement.")

os.environ.update({
    "DB_NAME": "postgres", "DB_USER": "postgres", "DB_HOST": HOST, "DB_PASSWORD": "x",
    "PGPORT": PORT, "AUTH_SECRET": "test",
})
os.environ.pop("RAPPORT_PRIX_M3", None)
os.environ.pop("PRIX_EAU_M3", None)
DSN = f"host={HOST} port={PORT} user=postgres dbname=postgres"

SCHEMA = """
DROP TABLE IF EXISTS mesures_aggregate, pannes, automate;
CREATE TABLE automate (nom_automate text, client text, lieu text, numero_automate text,
                       email text, email2 text, email3 text);
CREATE TABLE mesures_aggregate (
  rounded_timestamp timestamp NOT NULL, nom_automate text NOT NULL,
  avg_pression1 int DEFAULT 0, avg_pression2 int DEFAULT 0, avg_pression3 int DEFAULT 0,
  avg_pression4 int DEFAULT 0, avg_pression5 int DEFAULT 0,
  avg_hauteur_cuve_traitement int DEFAULT 0, avg_hauteur_cuve_disconnection int DEFAULT 0,
  avg_chlore int DEFAULT 0, avg_ph int DEFAULT 0, avg_temperature int DEFAULT 0,
  compteur_eau_renvoi int DEFAULT 0, compteur_eau_adoucie int DEFAULT 0, compteur_eau_relevage int DEFAULT 0,
  avg_hauteur_bidon1 int DEFAULT 0, avg_hauteur_bidon2 int DEFAULT 0,
  compteur_electrique int DEFAULT 0, avg_dilution int DEFAULT 0,
  PRIMARY KEY (rounded_timestamp, nom_automate));
CREATE TABLE pannes (id serial PRIMARY KEY, client text, lieu text, nom_automate text, panne text,
  probleme text, date_debut timestamp, date_fin timestamp, created_by text, updated_at timestamp,
  cause text, action_realisee text, pieces_changees text, deplacement boolean, intervenant text,
  partage_sav boolean DEFAULT true);
"""

# Septembre 2026 = 30 jours = 720 heures.
DONNEES = """
INSERT INTO automate (nom_automate, client, lieu) VALUES
  ('A1 ', 'Lavage Alpha', 'Alpha Marseille'),      -- espace en trop dans le nom
  ('A2', 'Lavage Alpha', 'Alpha Aix'),
  ('A3', ' lavage alpha ', 'Alpha Toulon'),         -- casse / espaces differents
  ('B1', 'Beta Wash', 'Beta Lyon');

-- A1, septembre : 720 h ; base elec 3 kWh/h, pH 7,50, chlore 0,5 mg/L
INSERT INTO mesures_aggregate (rounded_timestamp, nom_automate, compteur_electrique, avg_ph, avg_chlore)
SELECT t, 'A1', 3, 750, 500 FROM generate_series('2026-09-01'::timestamp, '2026-09-30 23:00', '1 hour') t;
-- lavage jours 1-20, 8h-17h : 2 m3/h renvoyes (400), 1 m3 d'appoint a 8h (20)
UPDATE mesures_aggregate SET compteur_eau_renvoi = 2
 WHERE nom_automate='A1' AND extract(day FROM rounded_timestamp) <= 20 AND extract(hour FROM rounded_timestamp) BETWEEN 8 AND 17;
UPDATE mesures_aggregate SET compteur_eau_adoucie = 1
 WHERE nom_automate='A1' AND extract(day FROM rounded_timestamp) <= 20 AND extract(hour FROM rounded_timestamp) = 8;
-- remise a zero du compteur le 25 a 3h : increment aberrant (doit etre ecarte)
UPDATE mesures_aggregate SET compteur_eau_renvoi = 99999 WHERE nom_automate='A1' AND rounded_timestamp='2026-09-25 03:00';
-- pH 9,00 pendant 100 h (21-24 + 25 de 0h a 3h)
UPDATE mesures_aggregate SET avg_ph = 900 WHERE nom_automate='A1'
 AND rounded_timestamp >= '2026-09-21' AND rounded_timestamp < '2026-09-25 04:00';
-- chlore absent (0) 72 h, puis 0,1 mg/L 48 h
UPDATE mesures_aggregate SET avg_chlore = 0 WHERE nom_automate='A1'
 AND rounded_timestamp >= '2026-09-26' AND rounded_timestamp < '2026-09-29';
UPDATE mesures_aggregate SET avg_chlore = 100 WHERE nom_automate='A1' AND rounded_timestamp >= '2026-09-29';

-- A2 : trou de donnees, seulement les 10 premiers jours ; 1 m3 a 10h, 1 kWh/h
INSERT INTO mesures_aggregate (rounded_timestamp, nom_automate, compteur_electrique, compteur_eau_renvoi)
SELECT t, 'A2', 1, CASE WHEN extract(hour FROM t) = 10 THEN 1 ELSE 0 END
FROM generate_series('2026-09-01'::timestamp, '2026-09-10 23:00', '1 hour') t;

-- A3 : complet et propre, 1 m3 a midi chaque jour, pas de sonde pH/chlore
INSERT INTO mesures_aggregate (rounded_timestamp, nom_automate, compteur_eau_renvoi)
SELECT t, 'A3', CASE WHEN extract(hour FROM t) = 12 THEN 1 ELSE 0 END
FROM generate_series('2026-09-01'::timestamp, '2026-09-30 23:00', '1 hour') t;

-- A1, aout (M-1) : 10 m3 renvoyes et 1 m3 d'appoint a 9h, jours 1-30 -> 270 recycles
INSERT INTO mesures_aggregate (rounded_timestamp, nom_automate, compteur_eau_renvoi, compteur_eau_adoucie)
SELECT d + interval '9 hour', 'A1', 10, 1 FROM generate_series('2026-08-01'::timestamp, '2026-08-30', '1 day') d;
-- A1, juillet : 5 m3 le 15 ; decembre 2025 : hors annee
INSERT INTO mesures_aggregate (rounded_timestamp, nom_automate, compteur_eau_renvoi) VALUES
  ('2026-07-15 10:00', 'A1', 5), ('2025-12-15 10:00', 'A1', 1000);

-- B1 (autre client) et une station orpheline (absente de automate)
INSERT INTO mesures_aggregate (rounded_timestamp, nom_automate, compteur_eau_renvoi, compteur_electrique)
SELECT t, 'B1', 7, 9 FROM generate_series('2026-08-01'::timestamp, '2026-09-30 23:00', '1 hour') t;
INSERT INTO mesures_aggregate (rounded_timestamp, nom_automate, compteur_eau_renvoi)
VALUES ('2026-09-05 10:00', 'ORPHELIN', 3);

INSERT INTO pannes (client, lieu, nom_automate, panne, probleme, date_debut, date_fin, deplacement) VALUES
  ('Lavage Alpha', 'Alpha Marseille', 'A1', 'Pompe HS', 'x', '2026-09-03 10:00', '2026-09-05 10:00', true),
  ('Lavage Alpha', 'Alpha Marseille', 'A1 ', 'Filtre colmaté', 'x', '2026-08-28 12:00', '2026-09-02 12:00', false),
  ('Lavage Alpha', 'Alpha Aix', 'A2', 'Fuite cuve', 'x', '2026-09-28 08:00', NULL, NULL),
  ('Lavage Alpha', 'Alpha Marseille', 'A1', 'Ancienne', 'x', '2026-07-01 08:00', '2026-07-02 08:00', true),
  ('Beta Wash', 'Beta Lyon', 'B1', 'Panne Beta', 'x', '2026-09-10 08:00', '2026-09-11 08:00', true);
"""

with psycopg.connect(DSN, autocommit=True) as c:
    c.execute(SCHEMA)
    c.execute(DONNEES)

sys.path.insert(0, DEPOT)
import backend  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

USERS = {
    "client": {"id": 7, "email": "c@x", "roles": ["client"], "client_id": "lavage alpha",
               "_automates": {"A1", "A2", "A3"}, "_clients": {"lavage alpha"}},
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


def get(path, who="admin", **params):
    h = {"Authorization": f"Bearer {who}"} if who else {}
    return api.get(path, params=params, headers=h)


# 1. Calculs exacts, septembre
r = get("/rapport/mensuel", client="LAVAGE ALPHA", mois="2026-09")
check(r.status_code == 200, "admin : 200")
R = r.json()
st = {s["nom_automate"]: s for s in R["stations"]}
check(sorted(st) == ["A1", "A2", "A3"] and R["client"] == "Lavage Alpha",
      "client retrouve malgre casse/espaces, 3 stations (noms trimes)")
a1 = st["A1"]
check((a1["eau_renvoyee_m3"], a1["eau_adoucie_m3"], a1["eau_recyclee_m3"], a1["taux_recyclage_pct"])
      == (400, 20, 380, 95.0), "A1 : renvoi 400, appoint 20, recyclee 380, taux 95 %")
check(a1["heures_ecartees"]["eau"] == 1 and a1["donnees_incompletes"]
      and any("remise à zéro" in m for m in a1["motifs_incompletude"]),
      "A1 : remise a zero ecartee (99999 m3 ignores) et signalee")
check((a1["energie_kwh"], a1["kwh_par_m3_recycle"]) == (2160, 5.68), "A1 : 2160 kWh, 5,68 kWh/m3")
check((a1["jours_avec_lavage"], a1["couverture_pct"], a1["heures_attendues"]) == (20, 100.0, 720),
      "A1 : 20 jours de lavage, couverture 100 % sur 720 h")
q = a1["qualite"]
check((q["ph_moyen"], q["ph_pct_dans_plage"], q["heures_mesure_ph"]) == (7.71, 86.1, 720),
      "A1 : pH moyen 7,71, 86,1 % dans la plage")
check((q["chlore_moyen_mg_l"], q["chlore_pct_dans_plage"], q["heures_mesure_chlore"]) == (0.47, 92.6, 648),
      "A1 : chlore 0,47 mg/L, 92,6 % dans la plage, heures a 0 exclues")
a2 = st["A2"]
check((a2["eau_recyclee_m3"], a2["couverture_pct"], a2["jours_sans_donnees"], a2["donnees_incompletes"])
      == (10, 33.3, 20, True), "A2 : trou de donnees -> couverture 33,3 %, 20 jours muets, signale")
a3 = st["A3"]
check((a3["eau_recyclee_m3"], a3["taux_recyclage_pct"], a3["donnees_incompletes"], a3["qualite"]["ph_moyen"])
      == (30, 100.0, False, None), "A3 : complet, non signale, pas de sonde pH -> None")
T = R["total"]
check((T["eau_recyclee_m3"], T["taux_recyclage_pct"], T["economies_eur"], T["equivalent_piscines_olympiques"])
      == (420, 95.5, 1969.8, 0.17), "total : 420 m3, 95,5 %, 1969,80 EUR (4,69 EUR/m3 = PRIX_EAU_M3 de l'appli), 0,17 piscine")
check((T["energie_kwh"], T["jours_avec_lavage"], T["couverture_pct"], T["donnees_incompletes"])
      == (2400, 60, 77.8, True) and sorted(T["stations_donnees_incompletes"]) == ["A1", "A2"],
      "total : 2400 kWh, 60 jours-station, couverture 77,8 %, stations signalees A1 et A2")

# 2. Comparaison M-1 et cumul annuel
C = R["comparaison_mois_precedent"]
check(C["mois"] == "2026-08" and C["eau_recyclee_m3"] == 270 and C["variations_pct"]["eau_recyclee_m3"] == 55.6
      and C["ecart_taux_recyclage_points"] == 5.5, "M-1 : 270 m3 en aout, +55,6 %, +5,5 points de taux")
check(a1["variation_eau_recyclee_pct"] == 40.7 and a2["variation_eau_recyclee_pct"] is None,
      "variation par station (A1 +40,7 %, A2 sans base M-1)")
Y = R["cumul_annee"]
check(Y["eau_recyclee_m3"] == 695 and Y["depuis"] == "2026-07-15" and Y["jusqu_au"] == "2026-09-30"
      and Y["economies_eur"] == 3259.55, "cumul 2026 : 695 m3 depuis le 15/07 (decembre 2025 exclu), 3259,55 EUR")

# 3. Pannes
P = T["pannes"]
check((P["ouvertes_dans_le_mois"], P["resolues_dans_le_mois"], P["delai_moyen_resolution_h"],
       P["deplacements"], P["en_cours_fin_de_mois"]) == (2, 2, 84.0, 1, 1),
      "pannes : 2 ouvertes, 2 resolues, 84 h en moyenne, 1 deplacement, 1 en cours")
check([p["type"] for p in R["pannes_du_mois"]] == ["Filtre colmaté", "Pompe HS", "Fuite cuve"],
      "liste des pannes du mois (juillet et Beta exclues)")
check(st["A2"]["pannes"]["en_cours_fin_de_mois"] == 1 and st["A1"]["pannes"]["resolues_dans_le_mois"] == 2,
      "pannes ventilees par station")

# 4. Isolation
txt = json.dumps(R, ensure_ascii=False)
check("B1" not in txt and "Beta" not in txt and "ORPHELIN" not in txt, "JSON : aucune trace du client B")
h = get("/rapport/mensuel/html", client="Lavage Alpha", mois="2026-09")
check(h.status_code == 200 and h.headers["content-type"].startswith("text/html")
      and "Beta" not in h.text and "B1" not in h.text and "Panne Beta" not in h.text,
      "HTML : 200, aucune trace du client B")
check("septembre 2026" in h.text and "420 m³" in h.text and "1 970 €" in h.text
      and "95,5 %" in h.text and "@media print" in h.text and "size:A4" in h.text,
      "HTML : format francais (1 970 EUR, 95,5 %), CSS d'impression A4")
rb = get("/rapport/mensuel", client="Beta Wash", mois="2026-09").json()
check([s["nom_automate"] for s in rb["stations"]] == ["B1"] and rb["total"]["pannes"]["ouvertes_dans_le_mois"] == 1
      and "A1" not in json.dumps(rb), "rapport B : seulement B1 et sa panne")

# 5. Mois sans donnees, mois en cours, erreurs
v = get("/rapport/mensuel", client="Lavage Alpha", mois="2026-05").json()
check(v["aucune_donnee"] and v["total"]["eau_recyclee_m3"] == 0 and v["total"]["taux_recyclage_pct"] is None
      and all(s["motifs_incompletude"] == ["aucune donnée reçue ce mois"] for s in v["stations"]),
      "mois sans donnees : zero, taux None, stations signalees")
hv = get("/rapport/mensuel/html", client="Lavage Alpha", mois="2026-05")
check(hv.status_code == 200 and "Aucune donnée" in hv.text, "HTML mois vide : message explicite")
cur = backend._rapport_maintenant()
mc = get("/rapport/mensuel", client="Lavage Alpha", mois=f"{cur.year:04d}-{cur.month:02d}").json()
check(mc["periode"]["mois_en_cours"] and mc["total"]["heures_attendues"] <= 3 * 24 * cur.day,
      "mois en cours : marque provisoire, heures attendues jusqu'a maintenant")
check(get("/rapport/mensuel", client="Lavage Alpha", mois="2026-13").status_code == 400
      and get("/rapport/mensuel", client="Lavage Alpha", mois="2099-01").status_code == 400
      and get("/rapport/mensuel", client="Inconnu", mois="2026-09").status_code == 404,
      "mois invalide / futur : 400, client inconnu : 404")
check(get("/rapport/mensuel", client="Lavage Alpha").json()["mois"] == backend._rapport_mois_precedent(),
      "mois par defaut = mois precedent")

# 6. Prix du m3 surchargeable
os.environ["RAPPORT_PRIX_M3"] = "3,00"
check(get("/rapport/mensuel", client="Lavage Alpha", mois="2026-09").json()["total"]["economies_eur"] == 1260.0,
      "RAPPORT_PRIX_M3=3,00 -> 1260 EUR")
os.environ.pop("RAPPORT_PRIX_M3")

# 7. Liste des clients
lc = get("/rapport/mensuel/clients", mois="2026-09").json()
check(lc["clients"] == [{"client": "Beta Wash", "stations_avec_donnees": 1},
                        {"client": "Lavage Alpha", "stations_avec_donnees": 3}],
      "clients de septembre : Beta (1) et Alpha (3), orphelin ignore")
check(get("/rapport/mensuel/clients", mois="2026-05").json()["clients"] == [], "mai : aucun client")

# 8. Droits
routes = [("/rapport/mensuel", {"client": "Lavage Alpha", "mois": "2026-09"}),
          ("/rapport/mensuel/html", {"client": "Lavage Alpha", "mois": "2026-09"}),
          ("/rapport/mensuel/clients", {"mois": "2026-09"})]
check(all(get(p, who=None, **k).status_code == 401 for p, k in routes), "sans jeton : 401 sur les 3 routes")
check(all(get(p, who="client", **k).status_code == 403 for p, k in routes),
      "client (meme sur son propre client) : 403 sur les 3 routes")
check(all(get(p, who="super", **k).status_code == 200 for p, k in routes), "super_admin : 200")

print(f"\n{ok} vérifications OK")
