from fastapi import FastAPI, Query, HTTPException, Request
import logging
from typing import List, Tuple, Optional, Union
from pydantic import BaseModel, Field
import base64
import hashlib
import hmac
import json
import psycopg2
from psycopg2.pool import SimpleConnectionPool, ThreadedConnectionPool
import threading
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, datetime
import decimal
import os
import secrets
import time
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Création de l'instance FastAPI
app = FastAPI()


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_env_file(Path(__file__).with_name(".env"))


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

_DB_DSN = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "5")),
}

_pool: ThreadedConnectionPool | None = None
_pool_sem: threading.BoundedSemaphore | None = None
_conn_checked_out: set[int] = set()
_conn_sem_acquired: set[int] = set()
_conn_lock = threading.Lock()
_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "10000"))
_POOL_WAIT_TIMEOUT = float(os.getenv("DB_POOL_WAIT_TIMEOUT", "2"))


def _get_pool() -> ThreadedConnectionPool:
    global _pool, _pool_sem
    if _pool is None:
        max_conn = int(os.getenv("DB_MAX_CONN", "50"))
        # Pool thread-safe pour supporter le parallélisme FastAPI
        _pool = ThreadedConnectionPool(1, max_conn, **_DB_DSN)
        _pool_sem = threading.BoundedSemaphore(max_conn)
    return _pool


def get_connection():
    pool = _get_pool()
    if _pool_sem and not _pool_sem.acquire(timeout=_POOL_WAIT_TIMEOUT):
        raise HTTPException(status_code=503, detail="DB pool wait timeout")
    try:
        conn = pool.getconn()
        with _conn_lock:
            _conn_checked_out.add(id(conn))
            _conn_sem_acquired.add(id(conn))
        _apply_statement_timeout(conn)
        return conn
    except Exception:
        if _pool_sem:
            _pool_sem.release()
        raise


def _release_connection(conn):
    try:
        _get_pool().putconn(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
    finally:
        released = False
        sem_release = False
        with _conn_lock:
            if id(conn) in _conn_checked_out:
                _conn_checked_out.discard(id(conn))
                released = True
            if id(conn) in _conn_sem_acquired:
                _conn_sem_acquired.discard(id(conn))
                sem_release = True
        if released and sem_release and _pool_sem:
            _pool_sem.release()


def _apply_statement_timeout(conn):
    if _STATEMENT_TIMEOUT_MS <= 0:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {_STATEMENT_TIMEOUT_MS}")
    except Exception:
        pass

def executer_requete_sql(requete_sql: str, params: tuple = None) -> List[Tuple]:
    """
    Exécute une requête SQL avec des paramètres optionnels.
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            if params:
                cur.execute(requete_sql, params)
            else:
                cur.execute(requete_sql)

            if cur.description is not None:
                resultats = cur.fetchall()
            else:
                resultats = []

        conn.commit()
        return resultats
    except Exception as e:
        print(f"Erreur SQL : {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return []
    finally:
        if conn:
            _release_connection(conn)


def _normalize_emails(raw: Optional[Union[str, List[str]]]) -> tuple[list[str], Optional[str]]:
    """
    Normalise une liste d'emails provenant d'une chaîne "a,b" ou d'un tableau.
    - trim + lower
    - dédoublonne
    - enlève les entrées vides
    Retourne (liste_normée, chaîne_csv_ou_None)
    """
    if raw is None:
        return [], None
    if isinstance(raw, str):
        # Autoriser séparateur ";" en plus de ","
        items = raw.replace(";", ",").split(",")
    else:
        items = list(raw)
    cleaned: list[str] = []
    for item in items:
        email = (item or "").strip().lower()
        if email and email not in cleaned:
            cleaned.append(email)
    return cleaned, ",".join(cleaned) if cleaned else None


def _split_emails(raw: Optional[Union[str, List[str]]]) -> list[str]:
    """Alias pratique pour ne récupérer que la liste."""
    return _normalize_emails(raw)[0]

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
        SELECT client, numero_automate, nom_automate, lieu, email, email2, email3
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
            "email2": r[5],
            "email3": r[6],
            "emails": [e for e in [r[4], r[5], r[6]] if e],
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
        "p1_med_mbar": [float(row[1]) for row in result],
        "p2_med_mbar": [float(row[2]) for row in result],
        "p3_med_mbar": [float(row[3]) for row in result],
        "p4_med_mbar": [float(row[4]) for row in result],
        "p5_med_mbar": [float(row[5]) for row in result],
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
        "p1_med_mbar": [float(row[1]) for row in result],
        "p2_med_mbar": [float(row[2]) for row in result],
        "p3_med_mbar": [float(row[3]) for row in result],
        "p4_med_mbar": [float(row[4]) for row in result],
        "p5_med_mbar": [float(row[5]) for row in result],
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
        "p1_med_mbar": [float(row[1]) for row in result],
        "p2_med_mbar": [float(row[2]) for row in result],
        "p3_med_mbar": [float(row[3]) for row in result],
        "p4_med_mbar": [float(row[4]) for row in result],
        "p5_med_mbar": [float(row[5]) for row in result],
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
        "data": [float(row[1] or 0) / 10 for row in result],  # valeur en °C
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
        "data": [float(row[1] or 0) / 100 for row in result],  # valeur en pH
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
            "SELECT nom_automate, client, lieu, email, email2, email3 "
            "FROM automate "
            "WHERE nom_automate = %s;"
        )
        rows = executer_requete_sql(query, (nom_automate,))
        if not rows:
            return {"nom_automate": None, "client": None, "lieu": None, "email": None, "email2": None, "email3": None, "emails": []}

        row = rows[0]
        return {
            "nom_automate": row[0],
            "client": row[1],
            "lieu": row[2],
            "email": row[3],
            "email2": row[4],
            "email3": row[5],
            "emails": [e for e in [row[3], row[4], row[5]] if e],
        }

    # Cas 2️⃣ : pas de filtre ➜ on renvoie tout
    query = "SELECT nom_automate, client, lieu, email, email2, email3 FROM automate;"
    rows = executer_requete_sql(query)
    return [
        {
            "nom_automate": r[0],
            "client": r[1],
            "lieu": r[2],
            "email": r[3],
            "email2": r[4],
            "email3": r[5],
            "emails": [e for e in [r[3], r[4], r[5]] if e],
        }
        for r in rows
    ]

# ---------------------------------------------------------------------------
# ENDPOINTS CRUD SUR LA TABLE AUTOMATE (réservés aux admins côté front)
# ---------------------------------------------------------------------------

class AutomateIn(BaseModel):
    nom_automate: str
    client: str
    lieu: str
    email: str | None = None
    email2: str | None = None
    email3: str | None = None


@app.post("/automate")
def add_automate(auto: AutomateIn):
    """Ajoute un automate dans la table automate."""
    def norm(v: str | None):
        v = (v or "").strip().lower()
        return v if v else None
    emails = [norm(auto.email), norm(auto.email2), norm(auto.email3)]
    query = """
        INSERT INTO automate (nom_automate, client, lieu, email, email2, email3)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (nom_automate) DO NOTHING;
    """
    try:
        executer_requete_sql(
            query,
            (
                auto.nom_automate,
                auto.client,
                auto.lieu,
                emails[0],
                emails[1],
                emails[2],
            ),
        )
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


class AutomateUpdate(BaseModel):
    client: str | None = None
    lieu: str | None = None
    email: str | None = None
    email2: str | None = None
    email3: str | None = None


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
    def norm(v: str | None):
        v = (v or "").strip().lower()
        return v if v else None
    if upd.email is not None:
        fields.append("email  = %s")
        values.append(norm(upd.email))
    if upd.email2 is not None:
        fields.append("email2 = %s")
        values.append(norm(upd.email2))
    if upd.email3 is not None:
        fields.append("email3 = %s")
        values.append(norm(upd.email3))

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


# ---------------------------------------------------------------------------
# Auth locale (passwords + token signé HMAC)
# ---------------------------------------------------------------------------
_AUTH_SECRET = os.getenv("AUTH_SECRET", "")
_AUTH_TTL_SECONDS = int(os.getenv("AUTH_TTL_SECONDS", "86400"))
_PBKDF2_ITER = int(os.getenv("AUTH_PBKDF2_ITER", "260000"))
_AUTH_LOGGER = logging.getLogger("auth")


def _get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()
    return request.client.host if request.client else ""


def _auth_log(event: str, **fields: str) -> None:
    payload = " ".join(f"{k}={v}" for k, v in fields.items() if v)
    _AUTH_LOGGER.info("%s%s", event, f" {payload}" if payload else "")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ITER)
    return f"pbkdf2_sha256${_PBKDF2_ITER}${_b64url(salt)}${_b64url(dk)}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        algo, iterations, salt_b64, hash_b64 = stored.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iters = int(iterations)
        salt = _b64url_decode(salt_b64)
        expected = _b64url_decode(hash_b64)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters)
        return hmac.compare_digest(dk, expected)
    except Exception:
        return False


def _sign_token(payload: dict) -> str:
    if not _AUTH_SECRET:
        raise HTTPException(status_code=500, detail="AUTH_SECRET manquant")
    body = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(_AUTH_SECRET.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).digest()
    return f"{body}.{_b64url(sig)}"


def _verify_token(token: str) -> dict:
    if not _AUTH_SECRET:
        raise HTTPException(status_code=500, detail="AUTH_SECRET manquant")
    try:
        body_b64, sig_b64 = token.split(".", 1)
        expected = hmac.new(
            _AUTH_SECRET.encode("utf-8"),
            body_b64.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(_b64url(expected), sig_b64):
            raise HTTPException(status_code=401, detail="Token invalide")
        payload = json.loads(_b64url_decode(body_b64).decode("utf-8"))
        exp = int(payload.get("exp", 0))
        if exp and time.time() > exp:
            raise HTTPException(status_code=401, detail="Token expiré")
        return payload
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Token invalide")


def _get_roles_for_user(user_id: int) -> list[str]:
    rows = executer_requete_sql(
        """
        SELECT r.name
        FROM user_roles ur
        JOIN roles r ON r.id = ur.role_id
        WHERE ur.user_id = %s
        ORDER BY r.name;
        """,
        (user_id,),
    )
    return [r[0] for r in rows if r and r[0]]


def _require_auth(request: Request) -> dict:
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Bearer token manquant")
    token = auth.split(" ", 1)[1].strip()
    payload = _verify_token(token)
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token invalide")
    row = executer_requete_sql_one(
        "SELECT id, email, is_active, client_id FROM users WHERE id = %s",
        (user_id,),
    )
    if not row or not row[2]:
        raise HTTPException(status_code=401, detail="Utilisateur inactif")
    roles = _get_roles_for_user(row[0])
    return {"id": row[0], "email": row[1], "roles": roles, "client_id": row[3]}


def _get_org_for_user(user_id: int) -> Optional[tuple[int, str]]:
    row = executer_requete_sql_one(
        """
        SELECT o.id, o.name
        FROM organization_users ou
        JOIN organizations o ON o.id = ou.organization_id
        WHERE ou.user_id = %s
        ORDER BY o.id
        LIMIT 1;
        """,
        (user_id,),
    )
    return (row[0], row[1]) if row else None


class AuthRegisterIn(BaseModel):
    email: str
    password: str


class AuthLoginIn(BaseModel):
    email: str
    password: str


@app.post("/auth/register")
def register(payload: AuthRegisterIn):
    email = (payload.email or "").strip().lower()
    if "@" not in email:
        raise HTTPException(status_code=400, detail="Email invalide")
    if len(payload.password or "") < 8:
        raise HTTPException(status_code=400, detail="Mot de passe trop court")
    exists = executer_requete_sql_one("SELECT id FROM users WHERE email = %s", (email,))
    if exists:
        raise HTTPException(status_code=409, detail="Email déjà utilisé")
    pw_hash = _hash_password(payload.password)
    row = executer_requete_sql_one(
        "INSERT INTO users (email, password_hash, is_active) VALUES (%s, %s, TRUE) RETURNING id, client_id",
        (email, pw_hash),
    )
    if not row:
        raise HTTPException(status_code=500, detail="Création utilisateur échouée")
    role = executer_requete_sql_one("SELECT id FROM roles WHERE name = 'client'")
    if role:
        executer_requete_sql_one(
            "INSERT INTO user_roles (user_id, role_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (row[0], role[0]),
        )
    roles = _get_roles_for_user(row[0])
    token = _sign_token({"sub": row[0], "exp": int(time.time()) + _AUTH_TTL_SECONDS})
    return {"token": token, "user": {"id": row[0], "email": email, "roles": roles, "client_id": row[1]}}


@app.post("/auth/login")
def login(payload: AuthLoginIn, request: Request):
    email = (payload.email or "").strip().lower()
    ip = _get_client_ip(request)
    if not email:
        _auth_log("login_failed", reason="empty_email", ip=ip)
        raise HTTPException(status_code=401, detail="Identifiants invalides")
    row = executer_requete_sql_one(
        "SELECT id, password_hash, is_active, client_id FROM users WHERE email = %s",
        (email,),
    )
    if not row or not row[2]:
        reason = "user_missing_or_inactive"
        if row and not row[2]:
            reason = "user_inactive"
        _auth_log("login_failed", reason=reason, email=email, ip=ip)
        raise HTTPException(status_code=401, detail="Identifiants invalides")
    if not row[1]:
        _auth_log("login_failed", reason="missing_password_hash", email=email, ip=ip)
        raise HTTPException(status_code=401, detail="Identifiants invalides")
    if not _verify_password(payload.password or "", row[1]):
        _auth_log("login_failed", reason="bad_password", email=email, ip=ip)
        raise HTTPException(status_code=401, detail="Identifiants invalides")
    roles = _get_roles_for_user(row[0])
    token = _sign_token({"sub": row[0], "exp": int(time.time()) + _AUTH_TTL_SECONDS})
    _auth_log("login_success", email=email, ip=ip)
    return {"token": token, "user": {"id": row[0], "email": email, "roles": roles, "client_id": row[3]}}


@app.get("/auth/me")
def me(request: Request):
    return _require_auth(request)


# ---------------------------------------------------------------------------
# Helpers accès admin (basé sur l'en-tête transmis par le front Auth0)
# ---------------------------------------------------------------------------
def _require_admin(request: Request) -> str:
    """
    Vérifie que l'appelant est admin.
    Attend un bearer token valide.
    Retourne l'email pour l'audit.
    """
    user = _require_auth(request)
    if "admin" not in [r.lower() for r in user.get("roles", [])]:
        raise HTTPException(status_code=403, detail="Accès réservé à l'admin")
    return user.get("email") or ""


def _require_super_admin(request: Request) -> dict:
    user = _require_auth(request)
    roles = [r.lower() for r in user.get("roles", [])]
    if "super_admin" not in roles:
        raise HTTPException(status_code=403, detail="Accès réservé au super admin")
    return user


def _require_admin_or_super(request: Request) -> dict:
    user = _require_auth(request)
    roles = [r.lower() for r in user.get("roles", [])]
    if "admin" not in roles and "super_admin" not in roles:
        raise HTTPException(status_code=403, detail="Accès réservé à l'admin")
    return user


@app.get("/my/automates")
def list_my_automates(request: Request):
    user = _require_auth(request)
    roles = [r.lower() for r in user.get("roles", [])]
    if "admin" in roles or "super_admin" in roles:
        rows = executer_requete_sql("SELECT nom_automate, client, lieu, email, email2, email3 FROM automate;")
    else:
        org = _get_org_for_user(user["id"])
        if not org:
            return []
        rows = executer_requete_sql(
            """
            SELECT nom_automate, client, lieu, email, email2, email3
            FROM automate
            WHERE lower(client) = lower(%s);
            """,
            (org[1],),
        )
    return [
        {
            "nom_automate": r[0],
            "client": r[1],
            "lieu": r[2],
            "email": r[3],
            "email2": r[4],
            "email3": r[5],
            "emails": [e for e in [r[3], r[4], r[5]] if e],
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# ENDPOINTS PANNES (ADMIN-ONLY)
# ---------------------------------------------------------------------------


class PanneIn(BaseModel):
    client: str = Field(..., min_length=1)
    lieu: str = Field(..., min_length=1)
    nom_automate: str = Field(..., min_length=1)
    panne: str = Field(..., min_length=1)
    probleme: str = Field(..., min_length=1)
    date_debut: datetime
    date_fin: Optional[datetime] = None


def _validate_automate(client: str, lieu: str, nom_automate: str):
    row = executer_requete_sql_one(
        "SELECT nom_automate FROM automate WHERE client = %s AND lieu = %s LIMIT 1",
        (client, lieu),
    )
    if row is None:
        raise HTTPException(status_code=400, detail="Couple client/lieu introuvable")
    if str(row[0]) != str(nom_automate):
        raise HTTPException(status_code=400, detail="nom_automate ne correspond pas")


@app.get("/pannes/types")
def pannes_types(request: Request):
    """
    ADMIN ONLY
    Renvoie la liste persistante des types de pannes (défauts + types déjà saisis),
    triée alphabétiquement (case-insensitive) et dédupliquée (case-insensitive).
    """
    _require_admin(request)

    types_defaut = [
        "Pompe HS",
        "Pompe désamorcée",
        "Filtre colmaté",
        "Filtre saturé",
        "Électrovanne HS",
        "Vanne bloquée",
        "Pressostat défectueux",
        "Capteur niveau HS",
        "Sonde conductivité HS",
        "Fuite hydraulique",
        "Fuite cuve",
        "Cuve pleine",
        "Cuve vide",
        "Débordement",
        "Manque produit chimique",
        "Surdosage produit",
        "Qualité eau insuffisante",
        "Odeur anormale",
        "Carte électronique HS",
        "Erreur automate",
        "Problème électrique",
        "Disjonction",
        "Air dans le circuit",
        "Bobine eV HS",
        "VAR 3 en défaut",
    ]

    rows = executer_requete_sql(
        """
        SELECT DISTINCT panne
        FROM public.pannes
        WHERE panne IS NOT NULL AND trim(panne) <> '';
        """
    )
    types_db = [str(r[0]).strip() for r in rows if r and r[0] is not None and str(r[0]).strip()]

    merged: dict[str, str] = {}
    for s in types_defaut:
        t = (s or "").strip()
        if not t:
            continue
        merged.setdefault(t.casefold(), t)
    for s in types_db:
        t = (s or "").strip()
        if not t:
            continue
        merged.setdefault(t.casefold(), t)

    return sorted(merged.values(), key=lambda x: x.casefold())


@app.options("/pannes/types")
def pannes_types_options():
    return {}


@app.get("/pannes/clients")
def pannes_clients(request: Request):
    _require_admin(request)
    rows = executer_requete_sql(
        "SELECT DISTINCT client FROM automate WHERE client IS NOT NULL ORDER BY client;"
    )
    return [r[0] for r in rows if r and r[0]]


@app.get("/pannes/stations")
def pannes_stations(request: Request, client: str = Query(..., description="Client")):
    _require_admin(request)
    rows = executer_requete_sql(
        "SELECT DISTINCT lieu FROM automate WHERE client = %s AND lieu IS NOT NULL ORDER BY lieu;",
        (client,),
    )
    return [r[0] for r in rows if r and r[0]]


@app.get("/pannes/automate")
def pannes_automate(
    request: Request,
    client: str = Query(..., description="Client"),
    lieu: str = Query(..., description="Station"),
):
    _require_admin(request)
    row = executer_requete_sql_one(
        "SELECT nom_automate FROM automate WHERE client = %s AND lieu = %s LIMIT 1;",
        (client, lieu),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Automate introuvable")
    return {"nom_automate": row[0]}


@app.get("/pannes")
def lister_pannes(request: Request):
    _require_admin(request)
    rows = executer_requete_sql(
        """
        SELECT id, client, lieu, nom_automate, panne, probleme, date_debut, date_fin, created_by
        FROM pannes
        ORDER BY id DESC;
        """
    )
    def _fmt(dt_val):
        return dt_val.isoformat() if hasattr(dt_val, "isoformat") else dt_val

    return [
        {
            "id": r[0],
            "client": r[1],
            "lieu": r[2],
            "nom_automate": r[3],
            "panne": r[4],
            "probleme": r[5],
            "date_debut": _fmt(r[6]),
            "date_fin": _fmt(r[7]),
            "created_by": r[8],
        }
        for r in rows
    ]


@app.post("/pannes")
def creer_panne(p: PanneIn, request: Request):
    email = _require_admin(request)

    if p.date_fin and p.date_fin < p.date_debut:
        raise HTTPException(status_code=400, detail="date_fin doit être >= date_debut")

    _validate_automate(p.client, p.lieu, p.nom_automate)

    row = executer_requete_sql_one(
        """
        INSERT INTO pannes (client, lieu, nom_automate, panne, probleme, date_debut, date_fin, created_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            p.client,
            p.lieu,
            p.nom_automate,
            p.panne,
            p.probleme,
            p.date_debut,
            p.date_fin,
            email or "",
        ),
    )
    if row is None:
        raise HTTPException(status_code=500, detail="Insertion panne échouée")

    return {
        "id": row[0],
        "client": p.client,
        "lieu": p.lieu,
        "nom_automate": p.nom_automate,
        "panne": p.panne,
        "probleme": p.probleme,
        "date_debut": p.date_debut.isoformat(),
        "date_fin": p.date_fin.isoformat() if p.date_fin else None,
        "created_by": email or "",
    }


@app.put("/pannes/{panne_id}")
def maj_panne(panne_id: int, p: PanneIn, request: Request):
    _require_admin(request)

    if p.date_fin and p.date_fin < p.date_debut:
        raise HTTPException(status_code=400, detail="date_fin doit être >= date_debut")

    _validate_automate(p.client, p.lieu, p.nom_automate)

    row = executer_requete_sql_one(
        """
        UPDATE pannes
        SET client = %s,
            lieu = %s,
            nom_automate = %s,
            panne = %s,
            probleme = %s,
            date_debut = %s,
            date_fin = %s
        WHERE id = %s
        RETURNING id, client, lieu, nom_automate, panne, probleme, date_debut, date_fin, created_by;
        """,
        (
            p.client,
            p.lieu,
            p.nom_automate,
            p.panne,
            p.probleme,
            p.date_debut,
            p.date_fin,
            panne_id,
        ),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Panne introuvable")

    def _fmt(dt_val):
        return dt_val.isoformat() if hasattr(dt_val, "isoformat") else dt_val

    return {
        "id": row[0],
        "client": row[1],
        "lieu": row[2],
        "nom_automate": row[3],
        "panne": row[4],
        "probleme": row[5],
        "date_debut": _fmt(row[6]),
        "date_fin": _fmt(row[7]),
        "created_by": row[8],
    }


@app.options("/pannes/{panne_id}")
def pannes_id_options():
    return {}


@app.delete("/pannes/{panne_id}")
def supprimer_panne(panne_id: int, request: Request):
    _require_admin(request)
    row = executer_requete_sql_one(
        "DELETE FROM pannes WHERE id = %s RETURNING id;", (panne_id,)
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Panne introuvable")
    return {"status": "deleted", "id": panne_id}


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


# ---------------------------------------------------------------------------
# SUPER ADMIN OVERVIEW
# ---------------------------------------------------------------------------
@app.get("/super_admin/overview")
def super_admin_overview(request: Request):
    _require_super_admin(request)

    def _users_by_role(role_name: str):
        rows = executer_requete_sql(
            """
            SELECT u.id, u.email, u.client_id
            FROM users u
            JOIN user_roles ur ON ur.user_id = u.id
            JOIN roles r ON r.id = ur.role_id
            WHERE r.name = %s
            ORDER BY u.email;
            """,
            (role_name,),
        )
        return [{"id": r[0], "email": r[1], "client_id": r[2]} for r in rows]

    super_admins = _users_by_role("super_admin")
    admins = _users_by_role("admin")
    accounts = executer_requete_sql(
        "SELECT id, email, client_id FROM users WHERE client_id IS NOT NULL ORDER BY email;"
    )
    organizations = executer_requete_sql(
        "SELECT DISTINCT client_id FROM users WHERE client_id IS NOT NULL ORDER BY client_id;"
    )
    clients = executer_requete_sql(
        "SELECT DISTINCT client FROM automate WHERE client IS NOT NULL ORDER BY client;"
    )

    return {
        "super_admins": super_admins,
        "admins": admins,
        "organizations": [r[0] for r in organizations if r and r[0]],
        "accounts": [{"id": r[0], "email": r[1], "client_id": r[2]} for r in accounts],
        "clients": [r[0] for r in clients if r and r[0]],
    }


class AccessAssignIn(BaseModel):
    user_id: int
    roles: list[str]
    client_id: Optional[str] = None


@app.get("/access/roles")
def list_roles(request: Request):
    _require_admin_or_super(request)
    rows = executer_requete_sql("SELECT name FROM roles ORDER BY name;")
    return [r[0] for r in rows if r and r[0]]


@app.get("/access/users")
def list_users(request: Request):
    _require_admin_or_super(request)
    rows = executer_requete_sql(
        """
        SELECT u.id, u.email, u.client_id, COALESCE(array_agg(r.name ORDER BY r.name) FILTER (WHERE r.name IS NOT NULL), '{}') AS roles
        FROM users u
        LEFT JOIN user_roles ur ON ur.user_id = u.id
        LEFT JOIN roles r ON r.id = ur.role_id
        GROUP BY u.id, u.email, u.client_id
        ORDER BY u.email;
        """
    )
    return [
        {"id": r[0], "email": r[1], "client_id": r[2], "roles": list(r[3])}
        for r in rows
    ]


@app.post("/access/assign")
def assign_access(payload: AccessAssignIn, request: Request):
    _require_admin_or_super(request)
    roles = [r.strip().lower() for r in payload.roles if r and r.strip()]
    client_id = (payload.client_id or "").strip() or None
    # Update legacy organization link (used by chat + some UI)
    executer_requete_sql(
        "UPDATE users SET client_id = %s WHERE id = %s;",
        (client_id, payload.user_id),
    )
    # Keep organizations table in sync when possible (email -> 1 org)
    executer_requete_sql("DELETE FROM organization_users WHERE user_id = %s;", (payload.user_id,))
    if client_id:
        org_row = executer_requete_sql_one("SELECT id FROM organizations WHERE name = %s", (client_id,))
        if org_row:
            executer_requete_sql_one(
                """
                INSERT INTO organization_users (user_id, organization_id, org_role)
                VALUES (%s, %s, 'employee')
                ON CONFLICT (user_id, organization_id) DO UPDATE SET org_role = EXCLUDED.org_role;
                """,
                (payload.user_id, org_row[0]),
            )
    # Replace role set
    executer_requete_sql("DELETE FROM user_roles WHERE user_id = %s;", (payload.user_id,))
    for role_name in roles:
        row = executer_requete_sql_one("SELECT id FROM roles WHERE name = %s", (role_name,))
        if row:
            executer_requete_sql_one(
                "INSERT INTO user_roles (user_id, role_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                (payload.user_id, row[0]),
            )
    return {"status": "success"}


# ---------------------------------------------------------------------------
# ORGANIZATIONS + AUTOMATES ACCESS (admin/super)
# ---------------------------------------------------------------------------
class OrganizationCreateIn(BaseModel):
    name: str
    admin_email: str


class OrganizationUserIn(BaseModel):
    email: str
    org_role: str


class AutomateAccessIn(BaseModel):
    emails: list[str]


@app.get("/orgs")
def list_orgs(request: Request):
    _require_admin_or_super(request)
    rows = executer_requete_sql("SELECT id, name FROM organizations ORDER BY name;")
    return [{"id": r[0], "name": r[1]} for r in rows]


@app.post("/orgs")
def create_org(payload: OrganizationCreateIn, request: Request):
    _require_admin_or_super(request)
    name = (payload.name or "").strip()
    admin_email = (payload.admin_email or "").strip().lower()
    if not name or "@" not in admin_email:
        raise HTTPException(status_code=400, detail="Données invalides")

    user_row = executer_requete_sql_one("SELECT id FROM users WHERE email = %s", (admin_email,))
    if not user_row:
        raise HTTPException(status_code=400, detail="Utilisateur introuvable (créer un compte d'abord)")

    org_row = executer_requete_sql_one(
        "INSERT INTO organizations (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
        (name,),
    )
    if not org_row:
        raise HTTPException(status_code=500, detail="Création organisation échouée")

    executer_requete_sql_one(
        """
        INSERT INTO organization_users (user_id, organization_id, org_role)
        VALUES (%s, %s, 'org_admin')
        ON CONFLICT (user_id, organization_id) DO UPDATE SET org_role = 'org_admin';
        """,
        (user_row[0], org_row[0]),
    )
    # Compat front historique
    executer_requete_sql_one("UPDATE users SET client_id = %s WHERE id = %s", (name, user_row[0]))
    return {"id": org_row[0], "name": name}


@app.get("/orgs/{org_id}/users")
def list_org_users(org_id: int, request: Request):
    _require_admin_or_super(request)
    rows = executer_requete_sql(
        """
        SELECT u.id, u.email, ou.org_role
        FROM organization_users ou
        JOIN users u ON u.id = ou.user_id
        WHERE ou.organization_id = %s
        ORDER BY u.email;
        """,
        (org_id,),
    )
    return [{"id": r[0], "email": r[1], "org_role": r[2]} for r in rows]


@app.post("/orgs/{org_id}/users")
def add_org_user(org_id: int, payload: OrganizationUserIn, request: Request):
    _require_admin_or_super(request)
    email = (payload.email or "").strip().lower()
    org_role = (payload.org_role or "").strip().lower()
    if org_role not in ("org_admin", "employee"):
        raise HTTPException(status_code=400, detail="Rôle org invalide")
    user_row = executer_requete_sql_one("SELECT id FROM users WHERE email = %s", (email,))
    if not user_row:
        raise HTTPException(status_code=400, detail="Utilisateur introuvable (créer un compte d'abord)")
    # email = 1 organisation
    executer_requete_sql_one("DELETE FROM organization_users WHERE user_id = %s", (user_row[0],))
    if org_role == "org_admin":
        executer_requete_sql_one(
            "UPDATE organization_users SET org_role = 'employee' WHERE organization_id = %s AND user_id <> %s;",
            (org_id, user_row[0]),
        )
    executer_requete_sql_one(
        """
        INSERT INTO organization_users (user_id, organization_id, org_role)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, organization_id) DO UPDATE SET org_role = EXCLUDED.org_role;
        """,
        (user_row[0], org_id, org_role),
    )
    # Compat front historique (stations/chat)
    org_name_row = executer_requete_sql_one("SELECT name FROM organizations WHERE id = %s", (org_id,))
    if org_name_row and org_name_row[0]:
        executer_requete_sql_one("UPDATE users SET client_id = %s WHERE id = %s", (org_name_row[0], user_row[0]))
    return {"status": "success"}


@app.delete("/orgs/{org_id}/users/{user_id}")
def remove_org_user(org_id: int, user_id: int, request: Request):
    _require_admin_or_super(request)
    executer_requete_sql_one(
        "DELETE FROM organization_users WHERE organization_id = %s AND user_id = %s",
        (org_id, user_id),
    )
    # email = 1 organisation (donc suppression => plus d'org)
    executer_requete_sql_one("UPDATE users SET client_id = NULL WHERE id = %s", (user_id,))
    return {"status": "deleted"}


@app.get("/orgs/{org_id}/automates")
def list_org_automates(org_id: int, request: Request):
    _require_admin_or_super(request)
    org_row = executer_requete_sql_one("SELECT name FROM organizations WHERE id = %s", (org_id,))
    if not org_row:
        raise HTTPException(status_code=404, detail="Organisation introuvable")
    org_name = org_row[0]
    rows = executer_requete_sql(
        """
        SELECT nom_automate, numero_automate, lieu, email, email2, email3
        FROM automate
        WHERE organization_id = %s OR lower(client) = lower(%s)
        ORDER BY nom_automate;
        """,
        (org_id, org_name),
    )
    return [
        {
            "nom_automate": r[0],
            "numero_automate": r[1],
            "lieu": r[2],
            "emails": [e for e in [r[3], r[4], r[5]] if e],
        }
        for r in rows
    ]


@app.get("/automates/{nom_automate}/access")
def list_automate_access(nom_automate: str, request: Request):
    _require_admin_or_super(request)
    rows = executer_requete_sql(
        """
        SELECT u.id, u.email
        FROM automate_access aa
        JOIN users u ON u.id = aa.user_id
        WHERE aa.automate_nom = %s
        ORDER BY u.email;
        """,
        (nom_automate,),
    )
    return [{"id": r[0], "email": r[1]} for r in rows]


@app.post("/automates/{nom_automate}/access")
def set_automate_access(nom_automate: str, payload: AutomateAccessIn, request: Request):
    _require_admin_or_super(request)
    emails = [e.strip().lower() for e in payload.emails if e and e.strip()]
    if not emails:
        raise HTTPException(status_code=400, detail="Aucun email")

    missing = []
    user_ids = []
    for email in emails:
        row = executer_requete_sql_one("SELECT id FROM users WHERE email = %s", (email,))
        if not row:
            missing.append(email)
        else:
            user_ids.append(row[0])
    if missing:
        raise HTTPException(status_code=400, detail=f"Comptes introuvables: {', '.join(missing)}")

    executer_requete_sql_one(
        "DELETE FROM automate_access WHERE automate_nom = %s",
        (nom_automate,),
    )
    for uid in user_ids:
        executer_requete_sql_one(
            "INSERT INTO automate_access (automate_nom, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (nom_automate, uid),
        )
    return {"status": "success"}

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


@app.get("/notifications/admin_all")
def notifications_admin_all():
    """Counts non lus envoyés par les clients (pour un admin), groupés par client_id."""
    rows = executer_requete_sql(
        """
        SELECT client_id, COUNT(*)::int
        FROM messages
        WHERE is_read = false
          AND sender_id = ANY(%s)
        GROUP BY client_id;
        """,
        (list(CLIENT_SENDER_IDS),),
    )
    return {str(r[0]): int(r[1]) for r in rows if r and r[0] is not None}


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
    port = int(os.getenv("PORT", "8011"))
    uvicorn.run(app, host="0.0.0.0", port=port)