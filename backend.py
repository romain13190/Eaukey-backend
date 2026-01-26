from fastapi import FastAPI, Query, HTTPException
from typing import List, Tuple, Optional, Union
import psycopg2
from fastapi.middleware.cors import CORSMiddleware
from datetime import date
import decimal
import os 

# Création de l'instance FastAPI
app = FastAPI()


# Origines autorisées
origins = [
    "https://my-app-zeta-blue.vercel.app",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://38.242.210.207:3000",
    "https://38.242.210.207:3000",
    "https://backend-eaukey.duckdns.org",
    "http://backend-eaukey.duckdns.org",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=".*",  # fallback to accept any origin if not listed
    allow_credentials=True,
    allow_methods=["*"],  # Autorisez toutes les méthodes (GET, POST, etc.)
    allow_headers=["*"],  # Autorisez tous les en-têtes
)

# Filet de sécurité CORS (si jamais une réponse passe hors CORSMiddleware)
@app.middleware("http")
async def _force_cors_headers(request, call_next):
    resp = await call_next(request)
    origin = request.headers.get("origin")
    if origin and "access-control-allow-origin" not in resp.headers:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers.setdefault("Vary", "Origin")
    return resp

# Fonction de connexion PostgreSQL
def get_connection():
    return psycopg2.connect(
        dbname="EaukeyCloudSQLv1",
        user="romain",
        password="Lzl?h<P@zxle6xuL",
        host="35.195.185.218",
        connect_timeout=5,  # évite de bloquer le handler si la DB ne répond pas
    )

def executer_requete_sql(requete_sql: str, params: tuple = None) -> List[Tuple]:
    """
    Exécute une requête SQL avec des paramètres optionnels.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 1) Exécute la requête avec (ou sans) paramètres
                if params:
                    cur.execute(requete_sql, params)
                else:
                    cur.execute(requete_sql)

                # 2) S'il s'agit d'une requête *sélective* (SELECT…) → on récupère les lignes
                #    Pour les INSERT/UPDATE/DELETE, ⁠ cursor.description ⁠ vaut None et ⁠ fetchall() ⁠ lèverait
                #    l'erreur  "no result to fetch".
                if cur.description is not None:
                    resultats = cur.fetchall()
                else:
                    resultats = []  # aucune donnée à renvoyer

                # 3) Valide les changements pour les requêtes d'écriture
                conn.commit()

        return resultats
    except Exception as e:
        print(f"Erreur SQL : {e}")
        return []

# Fonction pour calculer la consommation
def calculer_consommation_par_intervalle(resultat_sql: List[Tuple], timeframe: str = "jour") -> dict:
    if not resultat_sql or len(resultat_sql) < 2:
        return {"labels": [], "data": []}

    labels = []
    data = []
    for i in range(1, len(resultat_sql)):
        intervalle_actuel = resultat_sql[i][0]  # First column: could be hour or date
        valeur_actuelle = resultat_sql[i][1]
        valeur_precedente = resultat_sql[i - 1][1]
        consommation = max(valeur_actuelle - valeur_precedente, 0)

        # Format the label based on the timeframe
        if timeframe.lower() == "jour":
            labels.append(intervalle_actuel.strftime("%H:%M"))
        elif timeframe.lower() == "semaine":
            labels.append(str(intervalle_actuel).strip())  # Format as day name (e.g., "Monday")
        elif timeframe.lower() == "mois":
            labels.append(intervalle_actuel.strftime("%Y-%m-%d"))  # Format as "YYYY-MM-DD"
        elif timeframe.lower() == "annee":
            labels.append(intervalle_actuel.strftime("%B"))  # Format as full month name (e.g., "March")
        else:
            labels.append(str(intervalle_actuel))  # Fallback

        data.append(consommation)

    return {"labels": labels, "data": data}

# ---------------------------------------------------------------------------
# Nouvelle fonction : formatte les labels + valeurs pour l'API
# ---------------------------------------------------------------------------
from typing import List, Tuple
import decimal
import datetime as dt

def formater_series(resultat_sql: List[Tuple], timeframe: str = "jour") -> dict:
    """
    Convertit la sortie d'une requête SQL en deux listes :
      - labels  : abscisses formatées (heure, jour, date, mois)
      - data    : valeurs numériques (float)

    Hypothèse : la requête SQL renvoie déjà une ligne par intervalle
                (heure, jour, semaine, mois…), PAS un compteur cumulatif.
    """
    if not resultat_sql:
        return {"labels": [], "data": []}

    labels: List[str] = []
    data:   List[float] = []

    tf = timeframe.lower()

    for row in resultat_sql:
        # 1) Récupère l'abscisse et la valeur
        x = row[0]
        y = row[1] if row[1] is not None else 0

        # 2) Formate le label selon la période demandée
        if tf == "jour":           # x = datetime (heure) -> "HH:MM"
            label = x.strftime("%H:%M") if hasattr(x, "strftime") else str(x)
        elif tf == "semaine":      # x = 'Monday' ou datetime -> nom du jour en anglais
            label = x.strftime("%A") if hasattr(x, "strftime") else str(x).strip()
        elif tf == "mois":         # x = date du lundi -> "YYYY-MM-DD"
            label = x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else str(x)
        elif tf == "annee":        # x = date 1er mois -> "March"
            label = x.strftime("%B") if hasattr(x, "strftime") else str(x)
        else:
            label = str(x)

        # 3) Ajoute aux listes
        labels.append(label)
        data.append(float(y))

    return {"labels": labels, "data": data}

# Helpers basés sur la table pré-agrégée donnees_semaine
def fetch_semaine_simple(nom_automate: str, column: str) -> dict:
    query = f"""
    SELECT
      to_char(jour, 'FMDay') AS day_name,
      COALESCE({column}, 0)  AS value
    FROM donnees_semaine
    WHERE nom_automate = %s
    ORDER BY jour;
    """
    return formater_series(executer_requete_sql(query, (nom_automate,)), timeframe="semaine")

def fetch_mois_simple(nom_automate: str, column: str) -> dict:
    query = f"""
    SELECT
      to_char(semaine_debut, 'YYYY-MM-DD') AS week_label,
      COALESCE({column}, 0)                AS value
    FROM donnees_mois
    WHERE nom_automate = %s
    ORDER BY semaine_debut;
    """
    return formater_series(executer_requete_sql(query, (nom_automate,)), timeframe="mois")

def fetch_annee_simple(nom_automate: str, column: str) -> dict:
    query = f"""
    SELECT
      to_char(mois_debut, 'FMMonth') AS month_label,
      COALESCE({column}, 0)          AS value
    FROM donnees_annees
    WHERE nom_automate = %s
    ORDER BY mois_debut;
    """
    return formater_series(executer_requete_sql(query, (nom_automate,)), timeframe="annee")


# ---------------------------------------------------------------------------
# Endpoint : liste des automates avec email pour filtrage côté front
# ---------------------------------------------------------------------------
@app.get("/recherche/automate_LCA")
def liste_automates():
    query = """
        SELECT client, numero_automate, nom_automate, lieu, email
        FROM automate
        ORDER BY client NULLS LAST, nom_automate
    """
    rows = executer_requete_sql(query)
    return [
        {
            "client": r[0],
            "numero_automate": r[1],
            "nom_automate": r[2],
            "lieu": r[3],
            "email": r[4],
        }
        for r in rows
    ]


# Endpoints pour renvoi
@app.get("/renvoi/jour")
def renvoi_jour_delta_m3(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      date_trunc('hour', horodatage)                    AS heure,
      ROUND(AVG(compteur_eau_inst_renvoi_pc)::numeric, 1) AS inst_pc
    FROM mesures
    WHERE nom_automate = %s
      AND horodatage   >= now() - INTERVAL '24 hours'
      AND horodatage   <  now()
    GROUP  BY heure
    ORDER  BY heure;
    """
    rows = executer_requete_sql(query, (nom_automate,))
    if not rows:
        return {"labels": [], "data": []}

    # labels = ["HH:MM", ...], inst_values = [inst_pc, ...]
    labels      = [r[0].strftime("%H:%M") for r in rows]
    inst_values = [r[1]              for r in rows]

    # calcul des deltas m³ en tenant compte du rollover à 100%
    data = []
    for prev, curr in zip(inst_values, inst_values[1:]):
        delta_pc = curr - prev if curr >= prev else (curr + 100) - prev
        data.append(round(delta_pc / 100, 3))  # conversion % → m³

    return {
        "labels": labels[1:],  # on perd la première heure car pas de delta avant
        "data":   data
    }

@app.get("/renvoi/semaine")
def volume_renvoi_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_semaine_simple(nom_automate, "vol_renvoi_m3")


@app.get("/renvoi/mois")
def volume_renvoi_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_mois_simple(nom_automate, "vol_renvoi_m3")

@app.get("/renvoi/annee")
def volume_renvoi_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_annee_simple(nom_automate, "vol_renvoi_m3")

@app.get("/adoucie/jour")
def adoucie_jour_delta_m3(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      date_trunc('hour', horodatage)                       AS heure,
      ROUND(AVG(compteur_eau_inst_adoucie_pc)::numeric, 1) AS inst_pc
    FROM mesures
    WHERE nom_automate = %s
      AND horodatage   >= now() - INTERVAL '24 hours'
      AND horodatage   <  now()
    GROUP  BY heure
    ORDER  BY heure;
    """
    rows = executer_requete_sql(query, (nom_automate,))
    if not rows:
        return {"labels": [], "data": []}

    # Extraire labels et valeurs brutes (%)
    labels      = [r[0].strftime("%H:%M") for r in rows]
    inst_values = [r[1]               for r in rows]

    # Calculer le delta m³ en gérant le rollover à 100%
    data = []
    for prev, curr in zip(inst_values, inst_values[1:]):
        delta_pc = curr - prev if curr >= prev else (curr + 100) - prev
        data.append(round(delta_pc / 100, 3))  # conversion % → m³

    return {
        "labels": labels[1:],  # on perd la première heure (pas de delta avant)
        "data":   data
    }


@app.get("/adoucie/semaine")
def volume_adoucie_semaine(nom_automate: str = Query(...)):
    return fetch_semaine_simple(nom_automate, "vol_adoucie_m3")

@app.get("/adoucie/mois")
def volume_adoucie_mois(nom_automate: str = Query(...)):
    return fetch_mois_simple(nom_automate, "vol_adoucie_m3")

@app.get("/adoucie/annee")
def volume_adoucie_annee(nom_automate: str = Query(...)):
    return fetch_annee_simple(nom_automate, "vol_adoucie_m3")

@app.get("/relevage/jour")
def relevage_jour_delta_m3(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      date_trunc('hour', horodatage)                         AS heure,
      ROUND(AVG(compteur_eau_inst_relevage_pc)::numeric, 1)  AS inst_pc
    FROM mesures
    WHERE nom_automate = %s
      AND horodatage   >= now() - INTERVAL '24 hours'
      AND horodatage   <  now()
    GROUP  BY heure
    ORDER  BY heure;
    """
    rows = executer_requete_sql(query, (nom_automate,))
    if not rows:
        return {"labels": [], "data": []}

    # Extraire labels et valeurs brutes (%)
    labels      = [r[0].strftime("%H:%M") for r in rows]
    inst_values = [r[1]               for r in rows]

    # Calculer le delta m³ en gérant le rollover à 100%
    data = []
    for prev, curr in zip(inst_values, inst_values[1:]):
        delta_pc = curr - prev if curr >= prev else (curr + 100) - prev
        data.append(round(delta_pc / 100, 3))  # conversion % → m³

    return {
        "labels": labels[1:],  # on perd la première heure (pas de delta avant)
        "data":   data
    }


@app.get("/relevage/semaine")
def volume_relevage_semaine(nom_automate: str = Query(...)):
    return fetch_semaine_simple(nom_automate, "vol_relevage_m3")

@app.get("/relevage/mois")
def volume_relevage_mois(nom_automate: str = Query(...)):
    return fetch_mois_simple(nom_automate, "vol_relevage_m3")

@app.get("/relevage/annee")
def volume_relevage_annee(nom_automate: str = Query(...)):
    return fetch_annee_simple(nom_automate, "vol_relevage_m3")

@app.get("/avg_pression5/jour")
def avg_pression5_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    """
    Retourne la moyenne horaire de avg_pression5 pour les dernières 24 heures glissantes.
    Les labels affichent l'heure au format "HH:00".
    """
    query = """
        SELECT 
            EXTRACT(HOUR FROM rounded_timestamp) AS heure,
            AVG(avg_pression5) AS moyenne_pression
        FROM moyenne
        WHERE nom_automate = %s
          AND rounded_timestamp >= NOW() - INTERVAL '24 hours'
        GROUP BY EXTRACT(HOUR FROM rounded_timestamp)
        ORDER BY heure;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [f"{int(row[0]):02d}:00" for row in result],  # Format as "HH:00"
        "data": [float(row[1]) for row in result]
    }

@app.get("/avg_pression5/semaine")
def avg_pression5_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_semaine_simple(nom_automate, "p5_mbar")

@app.get("/avg_pression5/mois")
def avg_pression5_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_mois_simple(nom_automate, "p5_mbar")

@app.get("/avg_pression5/annee")
def avg_pression5_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_annee_simple(nom_automate, "p5_mbar")

@app.get("/taux_recyclage/jour")
def taux_recyclage_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    WITH w AS (
    SELECT
        horodatage,
        nom_automate,
        compteur_eau_adoucie_m3,
        compteur_eau_renvoi_m3
    FROM   mesures
    WHERE  nom_automate = %s
      AND  horodatage  >= NOW() - INTERVAL '24 hours'
      AND  horodatage  <  NOW()
),
deltas AS (
    SELECT
        date_trunc('hour', horodatage) AS heure_ts,
        GREATEST(
            compteur_eau_adoucie_m3
          - LAG(compteur_eau_adoucie_m3)
              OVER (PARTITION BY nom_automate ORDER BY horodatage),
            0
        ) AS vol_adoucie,
        GREATEST(
            compteur_eau_renvoi_m3
          - LAG(compteur_eau_renvoi_m3)
              OVER (PARTITION BY nom_automate ORDER BY horodatage),
            0
        ) AS vol_renvoi
    FROM w
),
par_heure AS (
    SELECT
        heure_ts,
        SUM(vol_adoucie) AS vol_adoucie,
        SUM(vol_renvoi)  AS vol_renvoi
    FROM   deltas
    GROUP  BY heure_ts
)
SELECT
    TO_CHAR(heure_ts, 'HH24:MI')                AS heure_label,
    CASE
        WHEN vol_adoucie + vol_renvoi = 0 THEN 0
        ELSE ROUND( (vol_renvoi / (vol_adoucie + vol_renvoi))::numeric, 2 )
    END                                         AS taux_recyclage
FROM   par_heure
ORDER  BY heure_ts;
    """

    result = executer_requete_sql(query, (nom_automate,))
    return formater_series(result, timeframe="jour")

@app.get("/taux_recyclage/semaine")
def taux_recyclage_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_semaine_simple(nom_automate, "taux_recyclage")

@app.get("/taux_recyclage/mois")
def taux_recyclage_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_mois_simple(nom_automate, "taux_recyclage")

@app.get("/taux_recyclage/annee")
def taux_recyclage_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_annee_simple(nom_automate, "taux_recyclage")

@app.get("/taux_desinfection/jour")
def taux_desinfection_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
WITH heures AS (
    SELECT generate_series(
               date_trunc('hour', NOW() - INTERVAL '23 hours'),
               date_trunc('hour', NOW()),
               '1 hour'
           ) AS heure_ts
),
mediane AS (
    SELECT
        date_trunc('hour', rounded_timestamp) AS heure_ts,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY avg_chlore / 2.5)
            AS taux_med
    FROM   moyenne
    WHERE  nom_automate      = %s
      AND  rounded_timestamp >= NOW() - INTERVAL '24 hours'
      AND  rounded_timestamp <  NOW()
    GROUP  BY heure_ts
)
SELECT
    TO_CHAR(h.heure_ts, 'HH24:MI')            AS heure_label,
    COALESCE(m.taux_med, 0)                  AS taux_desinfection
FROM   heures h
LEFT   JOIN mediane m USING (heure_ts)
ORDER  BY h.heure_ts;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return formater_series(result, timeframe="jour")

@app.get("/taux_desinfection/semaine")
def taux_desinfection_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_semaine_simple(nom_automate, "taux_desinfection")

@app.get("/taux_desinfection/mois")
def taux_desinfection_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_mois_simple(nom_automate, "taux_desinfection_avg")

@app.get("/taux_desinfection/annee")
def taux_desinfection_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_annee_simple(nom_automate, "taux_desinfection_avg")

@app.get("/pression_medianes/jour")
def pression_medianes_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
        SELECT
            date_trunc('hour', horodatage) AS heure,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY pression1_mbar) AS p1_med_mbar,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY pression2_mbar) AS p2_med_mbar,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY pression3_mbar) AS p3_med_mbar,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY pression4_mbar) AS p4_med_mbar,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY pression5_mbar) AS p5_med_mbar
        FROM   mesures
        WHERE  nom_automate = %s
          AND  horodatage  >= now() - INTERVAL '24 hours'
          AND  horodatage  <  now()
        GROUP  BY heure
        ORDER  BY heure;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strftime("%H:%M") for row in result],
        "p1_med_mbar": [row[1] for row in result],
        "p2_med_mbar": [row[2] for row in result],
        "p3_med_mbar": [row[3] for row in result],
        "p4_med_mbar": [row[4] for row in result],
        "p5_med_mbar": [row[5] for row in result],
    }

# -------------------
# ENDPOINTS PRESSION ALL
# -------------------

@app.get("/pression_all/jour")
def pression_all_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
        date_trunc('hour', horodatage) AS heure,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY pression1_mbar) AS p1_med_mbar,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY pression2_mbar) AS p2_med_mbar,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY pression3_mbar) AS p3_med_mbar,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY pression4_mbar) AS p4_med_mbar,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY pression5_mbar) AS p5_med_mbar
    FROM   mesures
    WHERE  nom_automate = %s
      AND  horodatage  >= now() - INTERVAL '24 hours'
      AND  horodatage  <  now()
    GROUP  BY heure
    ORDER  BY heure;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strftime("%H:%M") for row in result],
        "p1_med_mbar": [row[1] for row in result],
        "p2_med_mbar": [row[2] for row in result],
        "p3_med_mbar": [row[3] for row in result],
        "p4_med_mbar": [row[4] for row in result],
        "p5_med_mbar": [row[5] for row in result],
    }

@app.get("/pression_all/semaine")
def pression_all_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(jour, 'FMDay') AS day_name,
      COALESCE(p1_mbar, 0)   AS p1_mbar,
      COALESCE(p2_mbar, 0)   AS p2_mbar,
      COALESCE(p3_mbar, 0)   AS p3_mbar,
      COALESCE(p4_mbar, 0)   AS p4_mbar,
      COALESCE(p5_mbar, 0)   AS p5_mbar
    FROM donnees_semaine
    WHERE nom_automate = %s
    ORDER BY jour;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strip() for row in result],
        "p1_mbar": [float(row[1]) for row in result],
        "p2_mbar": [float(row[2]) for row in result],
        "p3_mbar": [float(row[3]) for row in result],
        "p4_mbar": [float(row[4]) for row in result],
        "p5_mbar": [float(row[5]) for row in result],
    }

@app.get("/pression_all/mois")
def pression_all_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(semaine_debut, 'YYYY-MM-DD') AS week_label,
      COALESCE(p1_mbar, 0) AS p1_mbar,
      COALESCE(p2_mbar, 0) AS p2_mbar,
      COALESCE(p3_mbar, 0) AS p3_mbar,
      COALESCE(p4_mbar, 0) AS p4_mbar,
      COALESCE(p5_mbar, 0) AS p5_mbar
    FROM donnees_mois
    WHERE nom_automate = %s
    ORDER BY semaine_debut;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0] for row in result],
        "pression1_mbar": [float(row[1]) for row in result],
        "pression2_mbar": [float(row[2]) for row in result],
        "pression3_mbar": [float(row[3]) for row in result],
        "pression4_mbar": [float(row[4]) for row in result],
        "pression5_mbar": [float(row[5]) for row in result],
    }

@app.get("/pression_all/annee")
def pression_all_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(mois_debut, 'FMMonth') AS month_label,
      COALESCE(p1_mbar, 0) AS p1_mbar,
      COALESCE(p2_mbar, 0) AS p2_mbar,
      COALESCE(p3_mbar, 0) AS p3_mbar,
      COALESCE(p4_mbar, 0) AS p4_mbar,
      COALESCE(p5_mbar, 0) AS p5_mbar
    FROM donnees_annees
    WHERE nom_automate = %s
    ORDER BY mois_debut;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strip() for row in result],
        "pression1_mbar": [float(row[1]) for row in result],
        "pression2_mbar": [float(row[2]) for row in result],
        "pression3_mbar": [float(row[3]) for row in result],
        "pression4_mbar": [float(row[4]) for row in result],
        "pression5_mbar": [float(row[5]) for row in result],
    }

# -------------------
# ENDPOINTS VOLUMES RENVOI/ADOUCIE/RELEVAGE ALL
# -------------------

@app.get("/volumes_all/jour")
def volumes_all_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      date_trunc('hour', horodatage)                       AS heure,
      ROUND(AVG(compteur_eau_inst_renvoi_pc)::numeric, 1)   AS renvoi_pc,
      ROUND(AVG(compteur_eau_inst_adoucie_pc)::numeric, 1)  AS adoucie_pc,
      ROUND(AVG(compteur_eau_inst_relevage_pc)::numeric, 1) AS relevage_pc
    FROM mesures
    WHERE nom_automate = %s
      AND horodatage   >= now() - INTERVAL '24 hours'
      AND horodatage   <  now()
    GROUP  BY heure
    ORDER  BY heure;
    """
    rows = executer_requete_sql(query, (nom_automate,))

    if not rows:
        return {
            "labels": [],
            "vol_renvoi_m3": [],
            "vol_adoucie_m3": [],
            "vol_relevage_m3": [],
        }

    labels = [r[0].strftime("%H:%M") for r in rows]
    renvoi_pc   = [r[1] for r in rows]
    adoucie_pc  = [r[2] for r in rows]
    relevage_pc = [r[3] for r in rows]

    def _deltas(values: list[float]):
        deltas = []
        for prev, curr in zip(values, values[1:]):
            delta_pc = curr - prev if curr >= prev else (curr + 100) - prev
            deltas.append(round(delta_pc / 100, 3))  # 1 % = 0,01 m³
        return deltas

    return {
        "labels": labels[1:],
        "vol_renvoi_m3":   _deltas(renvoi_pc),
        "vol_adoucie_m3":  _deltas(adoucie_pc),
        "vol_relevage_m3": _deltas(relevage_pc),
    }

@app.get("/volumes_all/semaine")
def volumes_all_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(jour, 'FMDay') AS day_name,
      COALESCE(vol_renvoi_m3, 0)   AS vol_renvoi_m3,
      COALESCE(vol_adoucie_m3, 0)  AS vol_adoucie_m3,
      COALESCE(vol_relevage_m3, 0) AS vol_relevage_m3
    FROM donnees_semaine
    WHERE nom_automate = %s
    ORDER BY jour;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strip() for row in result],
        "vol_renvoi_m3": [float(row[1]) for row in result],
        "vol_adoucie_m3": [float(row[2]) for row in result],
        "vol_relevage_m3": [float(row[3]) for row in result],
    }

@app.get("/volumes_all/mois")
def volumes_all_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(semaine_debut, 'YYYY-MM-DD') AS week_label,
      COALESCE(vol_renvoi_m3, 0)   AS vol_renvoi_m3,
      COALESCE(vol_adoucie_m3, 0)  AS vol_adoucie_m3,
      COALESCE(vol_relevage_m3, 0) AS vol_relevage_m3
    FROM donnees_mois
    WHERE nom_automate = %s
    ORDER BY semaine_debut;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0] for row in result],
        "vol_renvoi_m3": [float(row[1]) for row in result],
        "vol_adoucie_m3": [float(row[2]) for row in result],
        "vol_relevage_m3": [float(row[3]) for row in result],
    }

@app.get("/volumes_all/annee")
def volumes_all_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(mois_debut, 'FMMonth') AS month_label,
      COALESCE(vol_renvoi_m3, 0)   AS vol_renvoi_m3,
      COALESCE(vol_adoucie_m3, 0)  AS vol_adoucie_m3,
      COALESCE(vol_relevage_m3, 0) AS vol_relevage_m3
    FROM donnees_annees
    WHERE nom_automate = %s
    ORDER BY mois_debut;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strip() for row in result],
        "vol_renvoi_m3": [float(row[1]) for row in result],
        "vol_adoucie_m3": [float(row[2]) for row in result],
        "vol_relevage_m3": [float(row[3]) for row in result],
    }

# -------------------
# ENDPOINTS TEMPÉRATURE
# -------------------

@app.get("/temperature/jour")
def temperature_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
        date_trunc('hour', horodatage) AS heure,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY temperature_deg) AS temp_med_C
    FROM   mesures
    WHERE  nom_automate = %s
      AND  horodatage  >= now() - INTERVAL '24 hours'
      AND  horodatage  <  now()
    GROUP  BY heure
    ORDER  BY heure;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strftime("%H:%M") for row in result],
        "data": [row[1] for row in result],
    }

@app.get("/temperature/semaine")
def temperature_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_semaine_simple(nom_automate, "temp_moy_c")

@app.get("/temperature/mois")
def temperature_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_mois_simple(nom_automate, "temp_moy_c")

@app.get("/temperature/annee")
def temperature_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_annee_simple(nom_automate, "temp_moy_c")

# -------------------
# ENDPOINTS CHLORE
# -------------------

@app.get("/chlore/jour")
def chlore_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
        date_trunc('hour', horodatage) AS heure,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY chlore_mv) AS chlore_med_mv
    FROM   mesures
    WHERE  nom_automate = %s
      AND  horodatage  >= now() - INTERVAL '24 hours'
      AND  horodatage  <  now()
    GROUP  BY heure
    ORDER  BY heure;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strftime("%H:%M") for row in result],
        "data": [row[1] for row in result],
    }

@app.get("/chlore/semaine")
def chlore_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_semaine_simple(nom_automate, "chlore_moy_mg_l")

@app.get("/chlore/mois")
def chlore_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_mois_simple(nom_automate, "chlore_moy_mg_l")

@app.get("/chlore/annee")
def chlore_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_annee_simple(nom_automate, "chlore_moy_mg_l")

# -------------------
# ENDPOINTS PH
# -------------------

@app.get("/ph/jour")
def ph_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
        date_trunc('hour', horodatage) AS heure,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY ph) AS ph_mediane
    FROM   mesures
    WHERE  nom_automate = %s
      AND  horodatage   >= now() - INTERVAL '24 hours'
      AND  horodatage   <  now()
    GROUP  BY heure
    ORDER  BY heure;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strftime("%H:%M") for row in result],
        "data": [row[1] for row in result],
    }

@app.get("/ph/semaine")
def ph_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_semaine_simple(nom_automate, "ph_moyen")

@app.get("/ph/mois")
def ph_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_mois_simple(nom_automate, "ph_moyen")

@app.get("/ph/annee")
def ph_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_annee_simple(nom_automate, "ph_moyen")

# -------------------
# ENDPOINTS COMPTEUR ÉLECTRIQUE
# -------------------

@app.get("/compteur_elec/jour")
def compteur_elec_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    WITH w AS (
        SELECT
            horodatage,
            nom_automate,
            compteur_electrique_kwh
        FROM   mesures
        WHERE  nom_automate = %s
          AND  horodatage  >= now() - INTERVAL '24 hours'
          AND  horodatage  <  now()
    ),
    deltas AS (
        SELECT
            date_trunc('hour', horodatage) AS heure,
            GREATEST(
                compteur_electrique_kwh
              - LAG(compteur_electrique_kwh)
                  OVER (PARTITION BY nom_automate ORDER BY horodatage),
                0
            ) AS d_kwh
        FROM w
    )
    SELECT
        heure,
        ROUND(SUM(d_kwh)::numeric, 2) AS conso_kwh
    FROM   deltas
    GROUP  BY heure
    ORDER  BY heure;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strftime("%H:%M") for row in result],
        "data": [row[1] for row in result],
    }

@app.get("/compteur_elec/semaine")
def compteur_elec_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_semaine_simple(nom_automate, "conso_kwh")

@app.get("/compteur_elec/mois")
def compteur_elec_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_mois_simple(nom_automate, "conso_kwh")

@app.get("/compteur_elec/annee")
def compteur_elec_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return fetch_annee_simple(nom_automate, "conso_kwh")

# -------------------
# ENDPOINTS DONNÉES TEMPS RÉEL
# -------------------

@app.get("/temps_reel/cuve_relevage")
def temps_reel_cuve_relevage(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      horodatage,
      compteur_eau_inst_relevage_pc AS cuve_relevage_pct
    FROM mesures
    WHERE nom_automate = %s
    ORDER BY horodatage DESC
    LIMIT 1;
    """
    result = executer_requete_sql(query, (nom_automate,))
    if not result:
        return {"horodatage": None, "valeur": 0}
    
    return {
        "horodatage": result[0][0].strftime("%Y-%m-%d %H:%M:%S"),
        "valeur": float(result[0][1]) if result[0][1] is not None else 0
    }

@app.get("/temps_reel/cuve_renvoi")
def temps_reel_cuve_renvoi(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      horodatage,
      compteur_eau_inst_renvoi_pc AS cuve_renvoi_pct
    FROM mesures
    WHERE nom_automate = %s
    ORDER BY horodatage DESC
    LIMIT 1;
    """
    result = executer_requete_sql(query, (nom_automate,))
    if not result:
        return {"horodatage": None, "valeur": 0}
    
    return {
        "horodatage": result[0][0].strftime("%Y-%m-%d %H:%M:%S"),
        "valeur": float(result[0][1]) if result[0][1] is not None else 0
    }

@app.get("/temps_reel/cuve_adoucie")
def temps_reel_cuve_adoucie(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      horodatage,
      compteur_eau_inst_adoucie_pc AS cuve_adoucie_pct
    FROM mesures
    WHERE nom_automate = %s
    ORDER BY horodatage DESC
    LIMIT 1;
    """
    result = executer_requete_sql(query, (nom_automate,))
    if not result:
        return {"horodatage": None, "valeur": 0}
    
    return {
        "horodatage": result[0][0].strftime("%Y-%m-%d %H:%M:%S"),
        "valeur": float(result[0][1]) if result[0][1] is not None else 0
    }

@app.get("/temps_reel/hauteur_cuve_traitement")
def temps_reel_hauteur_cuve_traitement(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      m.horodatage,
      m.nom_automate,
      LEAST(100, GREATEST(0, m.hauteur_cuve_traitement_pc))::numeric AS hauteur_cuve_traitement_pct
    FROM mesures m
    WHERE m.nom_automate = %s
      AND m.hauteur_cuve_traitement_pc IS NOT NULL
    ORDER BY m.horodatage DESC
    LIMIT 1;
    """
    result = executer_requete_sql(query, (nom_automate,))
    if not result:
        return {"horodatage": None, "valeur": 0}

    return {
        "horodatage": result[0][0].strftime("%Y-%m-%d %H:%M:%S"),
        "valeur": float(result[0][2]) if result[0][2] is not None else 0
    }

@app.get("/temps_reel/hauteur_cuve_disconnection")
def temps_reel_hauteur_cuve_disconnection(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      m.horodatage,
      m.nom_automate,
      LEAST(100, GREATEST(0, m.hauteur_cuve_disconnection_pc))::numeric AS hauteur_cuve_disconnection_pct
    FROM mesures m
    WHERE m.nom_automate = %s
      AND m.hauteur_cuve_disconnection_pc IS NOT NULL
    ORDER BY m.horodatage DESC
    LIMIT 1;
    """
    result = executer_requete_sql(query, (nom_automate,))
    if not result:
        return {"horodatage": None, "valeur": 0}

    return {
        "horodatage": result[0][0].strftime("%Y-%m-%d %H:%M:%S"),
        "valeur": float(result[0][2]) if result[0][2] is not None else 0
    }

@app.get("/temps_reel/volume_renvoi")
def temps_reel_volume_renvoi(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      horodatage,
      compteur_eau_renvoi_m3 AS vol_renvoi_m3
    FROM mesures
    WHERE nom_automate = %s
    ORDER BY horodatage DESC
    LIMIT 1;
    """
    result = executer_requete_sql(query, (nom_automate,))
    if not result:
        return {"horodatage": None, "valeur": 0}
    
    return {
        "horodatage": result[0][0].strftime("%Y-%m-%d %H:%M:%S"),
        "valeur": float(result[0][1]) if result[0][1] is not None else 0
    }

@app.get("/temps_reel/compteur_electrique")
def temps_reel_compteur_electrique(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      horodatage,
      compteur_electrique_kwh AS conso_kwh
    FROM mesures
    WHERE nom_automate = %s
     AND compteur_electrique_kwh > 0
    ORDER BY horodatage DESC
    LIMIT 1;
    """
    result = executer_requete_sql(query, (nom_automate,))
    if not result:
        return {"horodatage": None, "valeur": 0}
    
    return {
        "horodatage": result[0][0].strftime("%Y-%m-%d %H:%M:%S"),
        "valeur": float(result[0][1]) if result[0][1] is not None else 0
    }

@app.get("/recherche/automate_LCA")
def recherche_automate_lca(
    nom_automate: Optional[str] = Query(None, description="Nom de l'automate (optionnel)"),
):
    """Récupère les métadonnées d'un automate.

    - Si nom_automate est fourni ➜ renvoie un seul enregistrement (dict).
    - Sinon ➜ renvoie *tous* les automates (liste de dicts).
    """

    # Cas 1️⃣ : on veut un automate précis
    if nom_automate:
        query = (
            "SELECT nom_automate, client, lieu "
            "FROM automate "
            "WHERE nom_automate = %s;"
        )
        rows = executer_requete_sql(query, (nom_automate,))
        if not rows:
            return {"nom_automate": None, "client": None, "lieu": None}

        row = rows[0]
        return {"nom_automate": row[0], "client": row[1], "lieu": row[2]}

    # Cas 2️⃣ : pas de filtre ➜ on renvoie tout
    query = "SELECT nom_automate, client, lieu FROM automate;"
    rows = executer_requete_sql(query)
    return [
        {"nom_automate": r[0], "client": r[1], "lieu": r[2]} for r in rows
    ]

# ---------------------------------------------------------------------------
# ENDPOINTS CRUD SUR LA TABLE AUTOMATE (réservés aux admins côté front)
# ---------------------------------------------------------------------------

from pydantic import BaseModel


class AutomateIn(BaseModel):
    nom_automate: str
    client: str
    lieu: str


@app.post("/automate")
def add_automate(auto: AutomateIn):
    """Ajoute un automate dans la table automate."""
    query = """
        INSERT INTO automate (nom_automate, client, lieu)
        VALUES (%s, %s, %s)
        ON CONFLICT (nom_automate) DO NOTHING;
    """
    try:
        executer_requete_sql(query, (auto.nom_automate, auto.client, auto.lieu))
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


class AutomateUpdate(BaseModel):
    client: str | None = None
    lieu: str | None = None


@app.put("/automate/{nom_automate}")
def update_automate(nom_automate: str, upd: AutomateUpdate):
    """Met à jour client et/ou lieu pour un automate."""
    fields = []
    values = []
    if upd.client is not None:
        fields.append("client = %s")
        values.append(upd.client)
    if upd.lieu is not None:
        fields.append("lieu   = %s")
        values.append(upd.lieu)

    if not fields:
        return {"status": "error", "message": "Aucune donnée à mettre à jour"}

    values.append(nom_automate)
    query = f"UPDATE automate SET {', '.join(fields)} WHERE nom_automate = %s;"
    try:
        executer_requete_sql(query, tuple(values))
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.delete("/automate/{nom_automate}")
def delete_automate(nom_automate: str):
    """Supprime un automate par son id."""
    query = "DELETE FROM automate WHERE nom_automate = %s;"
    try:
        executer_requete_sql(query, (nom_automate,))
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# -------------------
# ENDPOINTS MESSAGERIE
# -------------------

# Modèles Pydantic pour la messagerie
class ConversationOut(BaseModel):
    id: int
    client_id: int
    type: str
    created_at: str


class MessageIn(BaseModel):
    sender_id: int
    content: Optional[str] = None
    file_url: Optional[str] = None


class MessageOut(BaseModel):
    id: int
    conversation_id: int
    sender_id: int
    content: Optional[str] = None
    file_url: Optional[str] = None
    created_at: str


# Helper pour récupérer une seule ligne
def executer_requete_sql_one(query: str, params: tuple = ()):
    rows = executer_requete_sql(query, params)
    return rows[0] if rows else None


@app.get("/conversations", response_model=List[ConversationOut])
def get_conversations(user_id: Optional[str] = None, is_admin: bool = False):
    """Renvoie toutes les conversations.

    • admin ➜ assure qu'il existe au moins une conversation "support" pour *chaque* client
      (clients récupérés via la table automate).
    • client ➜ crée une conversation "support" pour le client courant si absente.
    """

    if is_admin:
        # Récupère la liste des clients (colonne automate.client)
        client_rows = executer_requete_sql("SELECT DISTINCT client FROM automate WHERE client IS NOT NULL")
        for (cid,) in client_rows:
            _ensure_conversation(cid)

        q = "SELECT id, client_id, type, created_at FROM conversations ORDER BY created_at DESC"
        params: tuple = ()
    else:
        if user_id is None:
            raise HTTPException(status_code=400, detail="user_id query param required")

        # On garantit la conversation du client
        _ensure_conversation(user_id)

        q = "SELECT id, client_id, type, created_at FROM conversations WHERE client_id = %s ORDER BY created_at DESC"
        params = (user_id,)

    rows = executer_requete_sql(q, params)
    return [{"id": r[0], "client_id": r[1], "type": r[2], "created_at": r[3]} for r in rows]


class ConversationIn(BaseModel):
    client_id: Union[str, int]
    type: str

@app.post("/conversations", response_model=ConversationOut)
def create_conversation(conv: ConversationIn):
    q = "INSERT INTO conversations (client_id, type) VALUES (%s, %s) RETURNING id, created_at"
    row = executer_requete_sql_one(q, (conv.client_id, conv.type))
    if not row:
        raise HTTPException(status_code=500, detail="Failed to create conversation")
    return {"id": row[0], "client_id": conv.client_id, "type": conv.type, "created_at": row[1]}


@app.get("/conversations/{conversation_id}/messages", response_model=List[MessageOut])
def get_messages(conversation_id: int):
    q = "SELECT id, conversation_id, sender_id, content, file_url, created_at FROM messages WHERE conversation_id = %s ORDER BY created_at"
    rows = executer_requete_sql(q, (conversation_id,))
    return [
        {"id": r[0], "conversation_id": r[1], "sender_id": r[2], "content": r[3], "file_url": r[4], "created_at": r[5]} for r in rows
    ]


@app.post("/conversations/{conversation_id}/messages", response_model=MessageOut)
def post_message(conversation_id: int, message: MessageIn):
    q = "INSERT INTO messages (conversation_id, sender_id, content, file_url) VALUES (%s, %s, %s, %s) RETURNING id, created_at"
    row = executer_requete_sql_one(q, (conversation_id, message.sender_id, message.content, message.file_url))
    if not row:
        raise HTTPException(status_code=500, detail="Failed to insert message")
    return {
        "id": row[0],
        "conversation_id": conversation_id,
        "sender_id": message.sender_id,
        "content": message.content,
        "file_url": message.file_url,
        "created_at": row[1],
    }

# -------------------
# FIN ENDPOINTS MESSAGERIE
# -------------------

# --------------------------------------------------
# Helper: assure qu'une conversation "support" existe pour un client
# --------------------------------------------------

def _ensure_conversation(client_id: str | int, conv_type: str = "support") -> None:
    """Crée une conversation (client_id, conv_type) si elle n'existe pas."""
    existing = executer_requete_sql_one(
        "SELECT id FROM conversations WHERE client_id = %s AND type = %s",
        (client_id, conv_type),
    )
    if existing is None:
        executer_requete_sql_one(
            "INSERT INTO conversations (client_id, type) VALUES (%s, %s) RETURNING id",
            (client_id, conv_type),
        )

# -------------------
# ENDPOINTS MESSAGERIE SIMPLIFIÉS
# -------------------

class MessageIn(BaseModel):
    sender_id: int
    content: Optional[str] = None
    file_url: Optional[str] = None

class MessageOut(BaseModel):
    id: int
    client_id: str
    sender_id: int
    content: Optional[str] = None
    file_url: Optional[str] = None
    created_at: str
    is_read: Optional[bool] = None

class ClientConversation(BaseModel):
    client_id: str
    client_name: str

@app.get("/clients", response_model=List[ClientConversation])
def get_clients(is_admin: bool = False):
    """
    Renvoie la liste des clients (= conversations disponibles).
    - Admin: tous les clients
    - Client: juste le sien
    """
    if is_admin:
        query = "SELECT DISTINCT client FROM automate WHERE client IS NOT NULL ORDER BY client"
        rows = executer_requete_sql(query)
        return [{"client_id": r[0], "client_name": r[0]} for r in rows]
    else:
        # Pour un client, on pourrait retourner juste le sien, mais pas vraiment utile
        return []

@app.get("/messages/{client_id}", response_model=List[MessageOut])
def get_messages_for_client(client_id: str):
    """Récupère tous les messages pour un client donné."""
    query = """
        SELECT id, client_id, sender_id, content, file_url, created_at, is_read
        FROM messages 
        WHERE client_id = %s 
        ORDER BY created_at
    """
    rows = executer_requete_sql(query, (client_id,))
    to_str = lambda dt: dt.isoformat() if hasattr(dt, "isoformat") else (dt or "")
    return [
        {
            "id": r[0],
            "client_id": r[1],
            "sender_id": r[2],
            "content": r[3],
            "file_url": r[4],
            "created_at": to_str(r[5]),
            "is_read": r[6],
        }
        for r in rows
    ]

@app.post("/messages/{client_id}", response_model=MessageOut)
def post_message_for_client(client_id: str, message: MessageIn):
    """Ajoute un message pour un client."""
    query = """
        INSERT INTO messages (client_id, sender_id, content, file_url, is_read) 
        VALUES (%s, %s, %s, %s, false) 
        RETURNING id, created_at
    """
    row = executer_requete_sql_one(query, (client_id, message.sender_id, message.content, message.file_url))
    if not row:
        raise HTTPException(status_code=500, detail="Failed to insert message")
    
    return {
        "id": row[0],
        "client_id": client_id,
        "sender_id": message.sender_id,
        "content": message.content,
        "file_url": message.file_url,
        "created_at": row[1],
        "is_read": False,
    }

# -------------------
# NOTIFICATIONS & MARQUAGE LU
# -------------------

ADMIN_SENDER_IDS = (0, 677)
CLIENT_SENDER_IDS = (1, 6863)


@app.get("/notifications/client/{client_id}")
def notifications_client(client_id: str):
    """Nombre de messages non lus envoyés par l'admin vers le client."""
    query = f"""
        SELECT COUNT(*) 
        FROM messages
        WHERE client_id = %s
          AND is_read = false
          AND sender_id = ANY(%s)
    """
    row = executer_requete_sql_one(query, (client_id, list(ADMIN_SENDER_IDS)))
    return {"count": int(row[0]) if row else 0}


@app.get("/notifications/admin/{client_id}")
def notifications_admin(client_id: str):
    """Nombre de messages non lus envoyés par le client (pour un admin)."""
    query = f"""
        SELECT COUNT(*) 
        FROM messages
        WHERE client_id = %s
          AND is_read = false
          AND sender_id = ANY(%s)
    """
    row = executer_requete_sql_one(query, (client_id, list(CLIENT_SENDER_IDS)))
    return {"count": int(row[0]) if row else 0}


@app.post("/messages_client/{client_id}/marquer_non_lu")
def marquer_messages_admin_lus(client_id: str):
    """Marque comme lus les messages envoyés par l'admin au client."""
    query = """
        UPDATE messages
        SET is_read = true
        WHERE client_id = %s
          AND is_read = false
          AND sender_id = ANY(%s)
    """
    rows = executer_requete_sql(query, (client_id, list(ADMIN_SENDER_IDS)))
    return {"updated": rows if isinstance(rows, int) else 0}


@app.post("/messages_admin/{client_id}/marquer_non_lu")
def marquer_messages_client_lus(client_id: str):
    """Marque comme lus les messages envoyés par le client (vu par un admin)."""
    query = """
        UPDATE messages
        SET is_read = true
        WHERE client_id = %s
          AND is_read = false
          AND sender_id = ANY(%s)
    """
    rows = executer_requete_sql(query, (client_id, list(CLIENT_SENDER_IDS)))
    return {"updated": rows if isinstance(rows, int) else 0}

# -------------------
# FIN ENDPOINTS MESSAGERIE SIMPLIFIÉS
# -------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8011)