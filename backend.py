from fastapi import FastAPI, Query, HTTPException, Request, BackgroundTasks, Depends
import logging  # v2 - force redeploy with taux_recyclage endpoint
from typing import List, Tuple, Optional, Union
from pydantic import BaseModel, Field
import base64
import hashlib
import hmac
import json
import psycopg
from psycopg_pool import ConnectionPool
import threading
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, datetime
import decimal
import os
import re
import secrets
import time
from pathlib import Path
from dotenv import load_dotenv
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
load_dotenv()

# =============================================================================
# SECURITE (2026-10-14) : authentification obligatoire par defaut
# =============================================================================
# Avant ce correctif, la plupart des routes (mesures, liste des stations avec les
# e-mails clients, creation/suppression d'automates, messagerie) etaient
# accessibles sans jeton. Desormais TOUTE route exige un jeton valide, sauf la
# liste blanche _PUBLIC_ROUTES. En plus, pour un utilisateur non admin :
#   - un parametre `nom_automate` (query ou chemin) doit designer une station de
#     ses organisations (meme regle que /my/automates) ;
#   - un parametre `client_id` / `client` doit etre son client ou une de ses
#     organisations ;
#   - les routes de _ADMIN_ONLY_ROUTES sont refusees.
# Une nouvelle route est donc protegee par defaut. Pour en ouvrir une, l'ajouter
# explicitement a _PUBLIC_ROUTES.
_PUBLIC_ROUTES = {
    ("POST", "/auth/login"),
    ("POST", "/auth/register"),          # cree un compte "client" sans station : il ne voit rien tant qu'un admin ne l'a pas rattache
    ("POST", "/auth/forgot-password"),
    ("POST", "/auth/reset-password"),
    ("GET", "/auth/verify-reset-token"),
}
_ADMIN_ONLY_ROUTES = {
    ("POST", "/automate"),
    ("PUT", "/automate/{nom_automate}"),
    ("DELETE", "/automate/{nom_automate}"),
    ("GET", "/conversations"),
    ("POST", "/conversations"),
    ("GET", "/conversations/{conversation_id}/messages"),
    ("POST", "/conversations/{conversation_id}/messages"),
    ("GET", "/notifications/admin/{client_id}"),
    ("GET", "/notifications/admin_all"),
    ("POST", "/messages_admin/{client_id}/marquer_non_lu"),
    ("GET", "/chat/escalades"),
    ("GET", "/chat/boite"),
    ("GET", "/chat/boite/compteur"),
    ("POST", "/chat/{client_id}/traite"),
    ("DELETE", "/chat/{client_id}/traite"),
}


def _security_gate(request: Request):
    """Dependance globale executee avant chaque route (voir bloc ci-dessus)."""
    if request.method == "OPTIONS":
        return
    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path)
    key = (request.method, path)
    if key in _PUBLIC_ROUTES:
        return
    user = _require_auth(request)
    request.state.user = user
    if _is_admin_user(user):
        return
    if key in _ADMIN_ONLY_ROUTES:
        raise HTTPException(status_code=403, detail="Accès réservé à l'admin")
    params = dict(request.query_params)
    params.update(request.path_params)
    nom = params.get("nom_automate")
    if nom and nom not in _user_automates(user):
        raise HTTPException(status_code=403, detail="Accès refusé à cette station")
    client = params.get("client_id") or params.get("client")
    if client and str(client).strip().lower() not in _user_clients(user):
        raise HTTPException(status_code=403, detail="Accès refusé à ce client")


# Création de l'instance FastAPI (documentation /docs et /openapi.json desactivees en production)
app = FastAPI(
    dependencies=[Depends(_security_gate)],
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


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


# Origines autorisées (navigateurs). Les appels serveur a serveur n'envoient pas
# d'en-tete Origin et ne sont pas concernes par le CORS.
origins = [
    "https://app.eaukey.net",
    "https://my-app-zeta-blue.vercel.app",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]
# Origines supplementaires sans redeploiement du code : CORS_EXTRA_ORIGINS="https://a,https://b"
origins += [o.strip() for o in os.getenv("CORS_EXTRA_ORIGINS", "").split(",") if o.strip()]
# Previews et alias Vercel de l'equipe eaukeys-projects (projet my-app)
_ORIGIN_REGEX = r"^https://my-app(-[a-z0-9-]+)?-eaukeys-projects\.vercel\.app$"


def _origin_allowed(origin: str) -> bool:
    return origin in origins or re.fullmatch(_ORIGIN_REGEX, origin) is not None


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],  # Autorisez toutes les méthodes (GET, POST, etc.)
    allow_headers=["*"],  # Autorisez tous les en-têtes
)

# Filet de sécurité CORS (si jamais une réponse passe hors CORSMiddleware, ex. erreur 500)
@app.middleware("http")
async def _force_cors_headers(request, call_next):
    resp = await call_next(request)
    origin = request.headers.get("origin")
    if origin and _origin_allowed(origin) and "access-control-allow-origin" not in resp.headers:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers.setdefault("Vary", "Origin")
    return resp

# =============================================================================
# FUSEAU HORAIRE : a lire avant d'ecrire une requete sur les mesures
# =============================================================================
# La base tourne en UTC, mais `mesures.horodatage` et `moyenne.rounded_timestamp`
# sont des timestamps SANS fuseau qui contiennent de l'heure locale de Paris.
# Comparer ces colonnes a `now()` decale donc la fenetre de 1h (hiver) a 2h (ete).
#
# Symptome constate le 27/07/2026 : a 14h37, les graphiques "Jour" s'arretaient a
# 12h37. Les donnees etaient bien en base, la condition `horodatage < now()` les
# excluait.
#
# Regle : face a une colonne timestamp SANS fuseau contenant de l'heure de Paris,
# toujours comparer a `(now() AT TIME ZONE 'Europe/Paris')`, qui gere aussi le
# passage heure d'ete / heure d'hiver.
#
# EXCEPTION : `password_reset_tokens.expires_at` est aussi un timestamp sans
# fuseau, mais il est ecrit en UTC depuis Python (datetime.utcnow). Il doit rester
# compare a `NOW()` brut, sinon tous les liens de reinitialisation de mot de passe
# expirent 2h trop tot. Ne pas "harmoniser" ces lignes.
# =============================================================================

_DB_DSN = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "5")),
}

_pool: ConnectionPool | None = None
_pool_sem: threading.BoundedSemaphore | None = None
_conn_checked_out: set[int] = set()
_conn_sem_acquired: set[int] = set()
_conn_lock = threading.Lock()
_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "10000"))
_POOL_WAIT_TIMEOUT = float(os.getenv("DB_POOL_WAIT_TIMEOUT", "2"))


def _dsn_to_conninfo() -> str:
    return " ".join(f"{k}={v}" for k, v in _DB_DSN.items() if v is not None)


_pool_init_lock = threading.Lock()


def _get_pool() -> ConnectionPool:
    # Création sous verrou : au réveil du service (Render free), plusieurs requêtes
    # arrivent en même temps ; sans verrou, chacune créait son pool et son sémaphore,
    # et une connexion prise sur l'un était rendue sur l'autre
    # (« ValueError: Semaphore released too many times » → erreur 500).
    global _pool, _pool_sem
    if _pool is None:
        with _pool_init_lock:
            if _pool is None:
                max_conn = int(os.getenv("DB_MAX_CONN", "50"))
                # Pool thread-safe pour supporter le parallélisme FastAPI
                _pool_sem = threading.BoundedSemaphore(max_conn)
                _pool = ConnectionPool(conninfo=_dsn_to_conninfo(), min_size=1, max_size=max_conn)
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
def liste_automates(request: Request):
    query = """
        SELECT client, numero_automate, nom_automate, lieu, email, email2, email3
        FROM automate
        ORDER BY client NULLS LAST, nom_automate
    """
    rows = executer_requete_sql(query)
    user = _require_auth(request)
    if not _is_admin_user(user):
        visibles = _user_automates(user)
        rows = [r for r in rows if r[2] in visibles]
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


# ---------------------------------------------------------------------------
# Endpoint : dernière ligne des données modifiables (consignes / paramètres)
# pour un automate donné. Renvoie {"data": null} si aucune ligne.
# ---------------------------------------------------------------------------
DONNEES_MODIFIABLES_COLS = [
    "horodatage",
    "numero_automate",
    "nom_automate",
    "relevage_on",
    "filtration_on",
    "renvoi_on",
    "consigne_pompe_relevage",
    "consigne_debit_max_pompe_relevage_m3h",
    "choix_pompe_relevage",
    "temps_ouverture_decanteur_min",
    "volume_relevage_entre_pause_decal",
    "temps_pause_ms",
    "hauteur_cuve_traitement_demarrage_relevage_pc",
    "consigne_vitesse_pompe_filtration",
    "consigne_pression_max_filtre_mbar",
    "hauteur_stop_filtration_pc",
    "hauteur_relance_filtration_pc",
    "choix_pompe_filtration",
    "hauteur_cuve_traitement_demarrage_filtration_pc",
    "hauteur_min_remplissage_eau_adoucie_pc",
    "hauteur_max_pc",
    "valeur_min_conductivite_us_cm2",
    "valeur_max_conductivite_us_cm2",
    "volume_actualisation_renvoi_dilution_m3",
    "choix_pompe_renvoi",
    "consigne_vitesse_pompe_renvoi",
    "consigne_pression_station_mbar",
    "hysteresis_renvoi_mbar",
    "ouverture_electrovanne_station_mbar",
    "fermeture_electrovanne_station_mbar",
    "temps_cl_filtre_media",
    "temps_cl_ca_filtre_transparent",
    "frequence_vidange_cuve",
    "frequence_vidange_filtration",
    "temps_dosage",
]


@app.get("/donnees_modifiables/{nom_automate}")
def get_donnees_modifiables(nom_automate: str):
    cols_sql = ", ".join(DONNEES_MODIFIABLES_COLS)
    query = f"""
        SELECT {cols_sql}
        FROM donnees_modifiables
        WHERE nom_automate = %s
        ORDER BY horodatage DESC
        LIMIT 1
    """
    rows = executer_requete_sql(query, (nom_automate,))
    if not rows:
        return {"data": None}
    r = rows[0]
    row = dict(zip(DONNEES_MODIFIABLES_COLS, r))
    if row.get("horodatage") is not None:
        row["horodatage"] = row["horodatage"].isoformat()
    return {"data": row}


# Endpoints pour renvoi
@app.get("/renvoi/jour")
def renvoi_jour_delta_m3(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      date_trunc('hour', horodatage)                    AS heure,
      ROUND(AVG(compteur_eau_inst_renvoi_pc)::numeric, 1) AS inst_pc
    FROM mesures
    WHERE nom_automate = %s
      AND horodatage   >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND horodatage   <  (now() AT TIME ZONE 'Europe/Paris')
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
      AND horodatage   >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND horodatage   <  (now() AT TIME ZONE 'Europe/Paris')
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
      AND horodatage   >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND horodatage   <  (now() AT TIME ZONE 'Europe/Paris')
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
          AND rounded_timestamp >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
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

@app.get("/temps_reel/taux_recyclage")
def temps_reel_taux_recyclage(nom_automate: str = Query(..., description="Nom de l'automate")):
    """Taux de recyclage sur les dernieres 24h, fenetre alignee sur l'heure tronquee :
    (vol_renvoi - vol_adoucie) / vol_renvoi

    La carte du front affiche "MAJ par heure" et refresh toutes les heures. On aligne donc
    la fenetre sur [date_trunc('hour', NOW()) - 24h, date_trunc('hour', NOW())], de sorte
    que la valeur soit identique a la derniere barre du graph /taux_recyclage/jour.
    """
    query = """
    WITH bornes AS (
        SELECT
            MAX(compteur_eau_renvoi_m3)  - MIN(compteur_eau_renvoi_m3)  AS vol_renvoi,
            MAX(compteur_eau_adoucie_m3) - MIN(compteur_eau_adoucie_m3) AS vol_adoucie,
            MAX(horodatage) AS dernier_ts
        FROM mesures
        WHERE nom_automate = %s
          AND horodatage >  date_trunc('hour', (now() AT TIME ZONE 'Europe/Paris')) - INTERVAL '24 hours'
          AND horodatage <= date_trunc('hour', (now() AT TIME ZONE 'Europe/Paris'))
          AND compteur_eau_renvoi_m3 IS NOT NULL
          AND compteur_eau_adoucie_m3 IS NOT NULL
    )
    SELECT
        dernier_ts,
        CASE
            WHEN vol_renvoi = 0 OR vol_renvoi IS NULL THEN 0
            ELSE ROUND(((vol_renvoi - vol_adoucie) / vol_renvoi)::numeric, 4)
        END AS taux_recyclage
    FROM bornes;
    """
    result = executer_requete_sql(query, (nom_automate,))
    if not result or not result[0][0]:
        return {"horodatage": None, "valeur": None}
    return {
        "horodatage": result[0][0].strftime("%Y-%m-%d %H:%M:%S"),
        "valeur": float(result[0][1]) if result[0][1] is not None else None
    }

@app.get("/taux_recyclage/jour")
def taux_recyclage_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    """Rendement recycleur sur les dernieres 24h, en fenetre glissante 24h pour chaque heure.

    Les compteurs eau_renvoi / eau_adoucie incrementent par paliers de 1 m3 (quelques ticks
    par jour seulement). Un calcul MAX-MIN par heure donne des ratios 0% ou 100% noisy.
    On calcule donc pour chaque heure H le taux sur la fenetre [H-24h, H], ce qui converge
    a la derniere heure vers le KPI temps reel et lisse les valeurs sur la journee.
    """
    query = """
    WITH hours AS (
        SELECT generate_series(
            date_trunc('hour', (now() AT TIME ZONE 'Europe/Paris')) - INTERVAL '23 hours',
            date_trunc('hour', (now() AT TIME ZONE 'Europe/Paris')),
            INTERVAL '1 hour'
        ) AS heure
    ),
    mesures_window AS (
        SELECT horodatage, compteur_eau_renvoi_m3, compteur_eau_adoucie_m3
        FROM mesures
        WHERE nom_automate = %s
          AND horodatage > (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '48 hours'
          AND compteur_eau_renvoi_m3 IS NOT NULL
          AND compteur_eau_adoucie_m3 IS NOT NULL
    )
    SELECT
        h.heure,
        CASE
            WHEN (MAX(m.compteur_eau_renvoi_m3) - MIN(m.compteur_eau_renvoi_m3)) IS NULL
              OR (MAX(m.compteur_eau_renvoi_m3) - MIN(m.compteur_eau_renvoi_m3)) <= 0
            THEN 0
            ELSE ROUND(((
                  (MAX(m.compteur_eau_renvoi_m3) - MIN(m.compteur_eau_renvoi_m3))
                - (MAX(m.compteur_eau_adoucie_m3) - MIN(m.compteur_eau_adoucie_m3))
              ) / NULLIF(MAX(m.compteur_eau_renvoi_m3) - MIN(m.compteur_eau_renvoi_m3), 0))::numeric, 4)
        END AS taux_recyclage
    FROM hours h
    LEFT JOIN mesures_window m
      ON m.horodatage > h.heure - INTERVAL '24 hours'
     AND m.horodatage <= h.heure
    GROUP BY h.heure
    ORDER BY h.heure;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return formater_series(result, timeframe="jour")

@app.get("/taux_recyclage/semaine")
def taux_recyclage_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    """Rendement recycleur par jour depuis donnees_semaine (ETL).
    Recalcule (vol_renvoi - vol_adoucie) / vol_renvoi a partir des volumes."""
    query = """
    SELECT
        to_char(jour, 'FMDay') AS day_name,
        GREATEST(0, LEAST(1,
            CASE
                WHEN vol_renvoi_m3 IS NULL OR vol_renvoi_m3 <= 0 THEN 0
                ELSE ((vol_renvoi_m3 - COALESCE(vol_adoucie_m3, 0)) / vol_renvoi_m3)
            END
        ))::numeric AS taux_recyclage
    FROM donnees_semaine
    WHERE nom_automate = %s
    ORDER BY jour;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return formater_series(result, timeframe="semaine")

@app.get("/taux_recyclage/mois")
def taux_recyclage_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    """Rendement recycleur par semaine depuis donnees_mois (ETL).
    Recalcule (vol_renvoi - vol_adoucie) / vol_renvoi a partir des volumes."""
    query = """
    SELECT
        to_char(semaine_debut, 'YYYY-MM-DD') AS week_label,
        GREATEST(0, LEAST(1,
            CASE
                WHEN vol_renvoi_m3 IS NULL OR vol_renvoi_m3 <= 0 THEN 0
                ELSE ((vol_renvoi_m3 - COALESCE(vol_adoucie_m3, 0)) / vol_renvoi_m3)
            END
        ))::numeric AS taux_recyclage
    FROM donnees_mois
    WHERE nom_automate = %s
    ORDER BY semaine_debut;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return formater_series(result, timeframe="mois")

@app.get("/taux_recyclage/annee")
def taux_recyclage_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    """Rendement recycleur par mois depuis donnees_annees (ETL).
    Recalcule (vol_renvoi - vol_adoucie) / vol_renvoi a partir des volumes."""
    query = """
    SELECT
        to_char(mois_debut, 'FMMonth') AS month_label,
        GREATEST(0, LEAST(1,
            CASE
                WHEN vol_renvoi_m3 IS NULL OR vol_renvoi_m3 <= 0 THEN 0
                ELSE ((vol_renvoi_m3 - COALESCE(vol_adoucie_m3, 0)) / vol_renvoi_m3)
            END
        ))::numeric AS taux_recyclage
    FROM donnees_annees
    WHERE nom_automate = %s
    ORDER BY mois_debut;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return formater_series(result, timeframe="annee")

@app.get("/taux_desinfection/jour")
def taux_desinfection_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
        date_trunc('hour', horodatage) AS heure,
        ROUND(AVG(chlore_mv / 3.0)::numeric, 2) AS taux_desinfection
    FROM   mesures
    WHERE  nom_automate = %s
      AND  horodatage  >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND  horodatage  <  (now() AT TIME ZONE 'Europe/Paris')
    GROUP  BY heure
    ORDER  BY heure;
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
          AND  horodatage  >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
          AND  horodatage  <  (now() AT TIME ZONE 'Europe/Paris')
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
      AND  horodatage  >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND  horodatage  <  (now() AT TIME ZONE 'Europe/Paris')
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
      AND horodatage   >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND horodatage   <  (now() AT TIME ZONE 'Europe/Paris')
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
      AND  horodatage  >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND  horodatage  <  (now() AT TIME ZONE 'Europe/Paris')
      AND  temperature_deg / 10.0 BETWEEN 0 AND 60
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
    return _filtrer_extremes(fetch_semaine_simple(nom_automate, "temp_moy_c"), 0, 60)

@app.get("/temperature/mois")
def temperature_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_mois_simple(nom_automate, "temp_moy_c"), 0, 60)

@app.get("/temperature/annee")
def temperature_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_annee_simple(nom_automate, "temp_moy_c"), 0, 60)

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
      AND  horodatage  >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND  horodatage  <  (now() AT TIME ZONE 'Europe/Paris')
      AND  chlore_mv BETWEEN 0 AND 2000
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
    return _filtrer_extremes(fetch_semaine_simple(nom_automate, "chlore_moy_mg_l"), 0, 2000)

@app.get("/chlore/mois")
def chlore_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_mois_simple(nom_automate, "chlore_moy_mg_l"), 0, 2000)

@app.get("/chlore/annee")
def chlore_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_annee_simple(nom_automate, "chlore_moy_mg_l"), 0, 2000)

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
      AND  horodatage   >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND  horodatage   <  (now() AT TIME ZONE 'Europe/Paris')
      AND  ph / 100.0 BETWEEN 2 AND 12
    GROUP  BY heure
    ORDER  BY heure;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strftime("%H:%M") for row in result],
        "data": [float(row[1] or 0) / 100 for row in result],  # valeur en pH
    }

def _filtrer_extremes(series: dict, val_min: float, val_max: float) -> dict:
    """Remplace les valeurs hors [val_min, val_max] par la mediane des valeurs valides."""
    data = series.get("data", [])
    if not data:
        return series

    # Calcul de la mediane des valeurs valides
    valides = [v for v in data if val_min <= v <= val_max]
    if not valides:
        return series
    valides_sorted = sorted(valides)
    n = len(valides_sorted)
    mediane = (valides_sorted[n // 2] + valides_sorted[(n - 1) // 2]) / 2

    # Remplacement des outliers par la mediane
    series["data"] = [v if val_min <= v <= val_max else mediane for v in data]
    return series

@app.get("/ph/semaine")
def ph_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_semaine_simple(nom_automate, "ph_moyen"), 2, 12)

@app.get("/ph/mois")
def ph_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_mois_simple(nom_automate, "ph_moyen"), 2, 12)

@app.get("/ph/annee")
def ph_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_annee_simple(nom_automate, "ph_moyen"), 2, 12)

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
          AND  horodatage  >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
          AND  horodatage  <  (now() AT TIME ZONE 'Europe/Paris')
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
# ENDPOINTS RATIO KWH / M3 RELEVÉ (Évolution de la consommation)
# -------------------

@app.get("/ratio_kwh_m3/jour")
def ratio_kwh_m3_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    """
    Ratio kWh / m³ relevé par heure sur les dernières 24h, lissé par
    moyenne glissante sur 3 heures. Les heures sans relevage renvoient null.
    """
    query = """
    WITH w AS (
        SELECT
            horodatage,
            nom_automate,
            compteur_electrique_kwh,
            compteur_eau_relevage_m3
        FROM   mesures
        WHERE  nom_automate = %s
          AND  horodatage  >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
          AND  horodatage  <  (now() AT TIME ZONE 'Europe/Paris')
    ),
    deltas AS (
        SELECT
            date_trunc('hour', horodatage) AS heure,
            GREATEST(
                compteur_electrique_kwh
              - LAG(compteur_electrique_kwh)
                  OVER (PARTITION BY nom_automate ORDER BY horodatage),
                0
            ) AS d_kwh,
            GREATEST(
                compteur_eau_relevage_m3
              - LAG(compteur_eau_relevage_m3)
                  OVER (PARTITION BY nom_automate ORDER BY horodatage),
                0
            ) AS d_m3
        FROM w
    ),
    hourly AS (
        SELECT
            heure,
            CASE
                WHEN SUM(d_m3) > 0
                THEN SUM(d_kwh) / SUM(d_m3)
                ELSE NULL
            END AS ratio_brut
        FROM   deltas
        GROUP  BY heure
    )
    SELECT
        heure,
        ROUND(
            AVG(ratio_brut) OVER (
                ORDER BY heure
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            )::numeric,
            3
        ) AS ratio
    FROM   hourly
    ORDER  BY heure;
    """
    result = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [row[0].strftime("%H:%M") for row in result],
        "data":   [float(row[1]) if row[1] is not None else None for row in result],
    }

@app.get("/ratio_kwh_m3/semaine")
def ratio_kwh_m3_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(jour, 'FMDay') AS day_name,
      CASE
        WHEN COALESCE(vol_relevage_m3, 0) > 0
        THEN ROUND((conso_kwh / vol_relevage_m3)::numeric, 3)
        ELSE 0
      END AS ratio
    FROM donnees_semaine
    WHERE nom_automate = %s
    ORDER BY jour;
    """
    return formater_series(executer_requete_sql(query, (nom_automate,)), timeframe="semaine")

@app.get("/ratio_kwh_m3/mois")
def ratio_kwh_m3_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(semaine_debut, 'YYYY-MM-DD') AS week_label,
      CASE
        WHEN COALESCE(vol_relevage_m3, 0) > 0
        THEN ROUND((conso_kwh / vol_relevage_m3)::numeric, 3)
        ELSE 0
      END AS ratio
    FROM donnees_mois
    WHERE nom_automate = %s
    ORDER BY semaine_debut;
    """
    return formater_series(executer_requete_sql(query, (nom_automate,)), timeframe="mois")

@app.get("/ratio_kwh_m3/annee")
def ratio_kwh_m3_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(mois_debut, 'FMMonth') AS month_label,
      CASE
        WHEN COALESCE(vol_relevage_m3, 0) > 0
        THEN ROUND((conso_kwh / vol_relevage_m3)::numeric, 3)
        ELSE 0
      END AS ratio
    FROM donnees_annees
    WHERE nom_automate = %s
    ORDER BY mois_debut;
    """
    return formater_series(executer_requete_sql(query, (nom_automate,)), timeframe="annee")

# -------------------
# ENDPOINTS CONDUCTIVITE (traitement + renvoi, multi-series)
# -------------------

@app.get("/conductivite/jour")
def conductivite_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
        date_trunc('hour', horodatage) AS heure,
        ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY conductivite_traitement))::numeric, 1) AS cond_t,
        ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY conductivite_renvoi))::numeric, 1)     AS cond_r
    FROM   mesures
    WHERE  nom_automate = %s
      AND  horodatage  >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND  horodatage  <  (now() AT TIME ZONE 'Europe/Paris')
    GROUP  BY heure
    ORDER  BY heure;
    """
    rows = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [r[0].strftime("%H:%M") for r in rows],
        "cond_traitement": [float(r[1]) if r[1] is not None else 0 for r in rows],
        "cond_renvoi":     [float(r[2]) if r[2] is not None else 0 for r in rows],
    }

@app.get("/conductivite/semaine")
def conductivite_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(jour, 'FMDay') AS day_name,
      COALESCE(cond_traitement_moy, 0) AS cond_t,
      COALESCE(cond_renvoi_moy, 0)     AS cond_r
    FROM donnees_semaine
    WHERE nom_automate = %s
    ORDER BY jour;
    """
    rows = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [r[0].strip() for r in rows],
        "cond_traitement": [float(r[1]) for r in rows],
        "cond_renvoi":     [float(r[2]) for r in rows],
    }

@app.get("/conductivite/mois")
def conductivite_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(semaine_debut, 'YYYY-MM-DD') AS week_label,
      COALESCE(cond_traitement_moy, 0) AS cond_t,
      COALESCE(cond_renvoi_moy, 0)     AS cond_r
    FROM donnees_mois
    WHERE nom_automate = %s
    ORDER BY semaine_debut;
    """
    rows = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [r[0] for r in rows],
        "cond_traitement": [float(r[1]) for r in rows],
        "cond_renvoi":     [float(r[2]) for r in rows],
    }

@app.get("/conductivite/annee")
def conductivite_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      to_char(mois_debut, 'FMMonth') AS month_label,
      COALESCE(cond_traitement_moy, 0) AS cond_t,
      COALESCE(cond_renvoi_moy, 0)     AS cond_r
    FROM donnees_annees
    WHERE nom_automate = %s
    ORDER BY mois_debut;
    """
    rows = executer_requete_sql(query, (nom_automate,))
    return {
        "labels": [r[0].strip() for r in rows],
        "cond_traitement": [float(r[1]) for r in rows],
        "cond_renvoi":     [float(r[2]) for r in rows],
    }

# -------------------
# ENDPOINTS HAUTEUR CUVE TRAITEMENT (mono-serie, %)
# -------------------

@app.get("/hauteur_cuve_traitement/jour")
def hauteur_cuve_traitement_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
        date_trunc('hour', horodatage) AS heure,
        ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hauteur_cuve_traitement_pc))::numeric, 1) AS h_pc
    FROM   mesures
    WHERE  nom_automate = %s
      AND  horodatage  >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND  horodatage  <  (now() AT TIME ZONE 'Europe/Paris')
      AND  hauteur_cuve_traitement_pc BETWEEN 0 AND 150
    GROUP  BY heure
    ORDER  BY heure;
    """
    return formater_series(executer_requete_sql(query, (nom_automate,)), timeframe="jour")

@app.get("/hauteur_cuve_traitement/semaine")
def hauteur_cuve_traitement_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_semaine_simple(nom_automate, "hauteur_cuve_traitement_moy_pc"), 0, 150)

@app.get("/hauteur_cuve_traitement/mois")
def hauteur_cuve_traitement_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_mois_simple(nom_automate, "hauteur_cuve_traitement_moy_pc"), 0, 150)

@app.get("/hauteur_cuve_traitement/annee")
def hauteur_cuve_traitement_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_annee_simple(nom_automate, "hauteur_cuve_traitement_moy_pc"), 0, 150)

# -------------------
# ENDPOINTS HAUTEUR CUVE DISCONNEXION (mono-serie, %)
# -------------------

@app.get("/hauteur_cuve_disconnection/jour")
def hauteur_cuve_disconnection_jour(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
        date_trunc('hour', horodatage) AS heure,
        ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY hauteur_cuve_disconnection_pc))::numeric, 1) AS h_pc
    FROM   mesures
    WHERE  nom_automate = %s
      AND  horodatage  >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
      AND  horodatage  <  (now() AT TIME ZONE 'Europe/Paris')
      AND  hauteur_cuve_disconnection_pc BETWEEN 0 AND 150
    GROUP  BY heure
    ORDER  BY heure;
    """
    return formater_series(executer_requete_sql(query, (nom_automate,)), timeframe="jour")

@app.get("/hauteur_cuve_disconnection/semaine")
def hauteur_cuve_disconnection_semaine(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_semaine_simple(nom_automate, "hauteur_cuve_disconnection_moy_pc"), 0, 150)

@app.get("/hauteur_cuve_disconnection/mois")
def hauteur_cuve_disconnection_mois(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_mois_simple(nom_automate, "hauteur_cuve_disconnection_moy_pc"), 0, 150)

@app.get("/hauteur_cuve_disconnection/annee")
def hauteur_cuve_disconnection_annee(nom_automate: str = Query(..., description="Nom de l'automate")):
    return _filtrer_extremes(fetch_annee_simple(nom_automate, "hauteur_cuve_disconnection_moy_pc"), 0, 150)

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

@app.get("/temps_reel/volume_relevage")
def temps_reel_volume_relevage(nom_automate: str = Query(..., description="Nom de l'automate")):
    query = """
    SELECT
      horodatage,
      compteur_eau_relevage_m3 AS vol_relevage_m3
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

# SMTP Configuration (for password reset emails)
_SMTP_HOST = os.getenv("SMTP_HOST", "")
_SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
_SMTP_USER = os.getenv("SMTP_USER", "")
_SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
_SMTP_FROM = os.getenv("SMTP_FROM", "noreply@eaukey.com")
_FRONTEND_URL = os.getenv("FRONTEND_URL", "https://my-app-zeta-blue.vercel.app")
_RESET_TOKEN_TTL_SECONDS = int(os.getenv("RESET_TOKEN_TTL_SECONDS", "3600"))  # 1 hour


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


# Cache des utilisateurs authentifies (jeton -> utilisateur) : evite 2 a 4 requetes SQL
# transatlantiques par appel. Un changement de role ou une desactivation est pris en
# compte au plus tard apres _AUTH_CACHE_TTL secondes.
_AUTH_CACHE_TTL = 60
_AUTH_CACHE: dict[str, tuple[float, dict]] = {}
_AUTH_CACHE_LOCK = threading.Lock()


def _require_auth(request: Request) -> dict:
    cached = getattr(request.state, "user", None)
    if cached:
        return cached
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Bearer token manquant")
    token = auth.split(" ", 1)[1].strip()
    payload = _verify_token(token)
    now = time.time()
    with _AUTH_CACHE_LOCK:
        hit = _AUTH_CACHE.get(token)
    if hit and hit[0] > now:
        return dict(hit[1])
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
    user = {"id": row[0], "email": row[1], "roles": roles, "client_id": row[3]}
    if not _is_admin_user(user):
        orgs = _get_orgs_for_user(row[0])
        org_names = [o[1] for o in orgs]
        automates = set()
        if org_names:
            placeholders = ",".join(["%s"] * len(org_names))
            automates = {
                r[0]
                for r in executer_requete_sql(
                    f"SELECT nom_automate FROM automate WHERE lower(client) IN ({placeholders})",
                    tuple(n.lower() for n in org_names),
                )
            }
        clients = {n.strip().lower() for n in org_names if n}
        if row[3] is not None:
            clients.add(str(row[3]).strip().lower())
        user["_automates"] = automates
        user["_clients"] = clients
    with _AUTH_CACHE_LOCK:
        if len(_AUTH_CACHE) > 5000:
            _AUTH_CACHE.clear()
        _AUTH_CACHE[token] = (now + _AUTH_CACHE_TTL, user)
    return dict(user)


def _is_admin_user(user: dict) -> bool:
    roles = [r.lower() for r in user.get("roles", [])]
    return "admin" in roles or "super_admin" in roles


def _user_automates(user: dict) -> set:
    """Stations visibles par un utilisateur non admin (meme regle que /my/automates)."""
    return user.get("_automates") or set()


def _user_clients(user: dict) -> set:
    """Clients (en minuscules) qu'un utilisateur non admin peut consulter : ses organisations + son client_id."""
    return user.get("_clients") or set()


def _get_orgs_for_user(user_id: int) -> list[tuple[int, str]]:
    """Retourne TOUTES les organisations d'un utilisateur (pas seulement la première)."""
    rows = executer_requete_sql(
        """
        SELECT o.id, o.name
        FROM organization_users ou
        JOIN organizations o ON o.id = ou.organization_id
        WHERE ou.user_id = %s
        ORDER BY o.name;
        """,
        (user_id,),
    )
    return [(r[0], r[1]) for r in rows]


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
    user = _require_auth(request)
    # Ajouter info organisations (un utilisateur peut appartenir à plusieurs)
    org_rows = executer_requete_sql(
        """
        SELECT ou.organization_id, ou.org_role, o.name
        FROM organization_users ou
        JOIN organizations o ON o.id = ou.organization_id
        WHERE ou.user_id = %s
        ORDER BY o.name;
        """,
        (user["id"],),
    )
    user["organizations"] = [
        {"id": r[0], "org_role": r[1], "name": r[2]} for r in org_rows
    ]
    # Rétro-compatibilité : garder les champs simples (première org)
    if org_rows:
        user["organization_id"] = org_rows[0][0]
        user["org_role"] = org_rows[0][1]
        user["organization_name"] = org_rows[0][2]
    return user


# ---------------------------------------------------------------------------
# Password Reset (Forgot Password)
# ---------------------------------------------------------------------------

class ForgotPasswordIn(BaseModel):
    email: str


class ResetPasswordIn(BaseModel):
    token: str
    password: str


async def _send_reset_email(email: str, reset_token: str) -> bool:
    """Send password reset email via SMTP."""
    if not _SMTP_HOST or not _SMTP_USER or not _SMTP_PASSWORD:
        _AUTH_LOGGER.warning("SMTP not configured, cannot send reset email")
        return False

    reset_link = f"{_FRONTEND_URL}/reset-password?token={reset_token}"

    # Create HTML email
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .button {{
                display: inline-block;
                padding: 12px 24px;
                background-color: #3b82f6;
                color: white;
                text-decoration: none;
                border-radius: 6px;
                margin: 20px 0;
            }}
            .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>Réinitialisation de votre mot de passe</h2>
            <p>Bonjour,</p>
            <p>Vous avez demandé la réinitialisation de votre mot de passe sur Eaukey.</p>
            <p>Cliquez sur le bouton ci-dessous pour créer un nouveau mot de passe :</p>
            <a href="{reset_link}" class="button">Réinitialiser mon mot de passe</a>
            <p>Ou copiez ce lien dans votre navigateur :</p>
            <p style="word-break: break-all; color: #3b82f6;">{reset_link}</p>
            <p><strong>Ce lien expire dans 1 heure.</strong></p>
            <p>Si vous n'avez pas demandé cette réinitialisation, ignorez simplement cet email.</p>
            <div class="footer">
                <p>— L'équipe Eaukey</p>
            </div>
        </div>
    </body>
    </html>
    """

    text_content = f"""
Réinitialisation de votre mot de passe

Bonjour,

Vous avez demandé la réinitialisation de votre mot de passe sur Eaukey.

Cliquez sur ce lien pour créer un nouveau mot de passe :
{reset_link}

Ce lien expire dans 1 heure.

Si vous n'avez pas demandé cette réinitialisation, ignorez simplement cet email.

— L'équipe Eaukey
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Réinitialisation de votre mot de passe - Eaukey"
    msg["From"] = _SMTP_FROM
    msg["To"] = email

    msg.attach(MIMEText(text_content, "plain", "utf-8"))
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    try:
        await aiosmtplib.send(
            msg,
            hostname=_SMTP_HOST,
            port=_SMTP_PORT,
            username=_SMTP_USER,
            password=_SMTP_PASSWORD,
            start_tls=True,
        )
        _AUTH_LOGGER.info(f"Reset email sent to {email}")
        return True
    except Exception as e:
        _AUTH_LOGGER.error(f"Failed to send reset email to {email}: {e}")
        return False


def _generate_reset_token() -> str:
    """Generate a secure random token for password reset."""
    return secrets.token_hex(32)  # 64 characters


def _cleanup_expired_tokens() -> None:
    """Remove expired password reset tokens."""
    try:
        executer_requete_sql_one(
            "DELETE FROM password_reset_tokens WHERE expires_at < NOW() OR used = TRUE"
        )
    except Exception:
        pass  # Table might not exist yet


@app.post("/auth/forgot-password")
async def forgot_password(payload: ForgotPasswordIn, request: Request, background_tasks: BackgroundTasks):
    """
    Request a password reset email.
    Always returns success to prevent email enumeration.
    """
    email = (payload.email or "").strip().lower()
    ip = _get_client_ip(request)

    if "@" not in email:
        # Still return success to prevent enumeration
        return {"message": "Si cet email existe, un lien de réinitialisation a été envoyé."}

    # Check if user exists
    row = executer_requete_sql_one(
        "SELECT id, is_active FROM users WHERE email = %s",
        (email,),
    )

    if not row or not row[1]:
        _auth_log("forgot_password_user_not_found", email=email, ip=ip)
        # Return same message to prevent email enumeration
        return {"message": "Si cet email existe, un lien de réinitialisation a été envoyé."}

    user_id = row[0]

    # Rate limiting: check recent requests (max 3 per hour)
    recent = executer_requete_sql_one(
        """
        SELECT COUNT(*) FROM password_reset_tokens
        WHERE user_id = %s AND created_at > NOW() - INTERVAL '1 hour'
        """,
        (user_id,),
    )
    if recent and recent[0] >= 3:
        _auth_log("forgot_password_rate_limited", email=email, ip=ip)
        return {"message": "Si cet email existe, un lien de réinitialisation a été envoyé."}

    # Invalidate previous unused tokens for this user
    executer_requete_sql_one(
        "UPDATE password_reset_tokens SET used = TRUE WHERE user_id = %s AND used = FALSE",
        (user_id,),
    )

    # Generate new token
    reset_token = _generate_reset_token()
    expires_at = datetime.utcnow().timestamp() + _RESET_TOKEN_TTL_SECONDS

    # Store token in database
    executer_requete_sql_one(
        """
        INSERT INTO password_reset_tokens (user_id, token, expires_at, created_at)
        VALUES (%s, %s, TO_TIMESTAMP(%s), NOW())
        """,
        (user_id, reset_token, expires_at),
    )

    _auth_log("forgot_password_requested", email=email, ip=ip)

    # Send email in background
    background_tasks.add_task(_send_reset_email, email, reset_token)

    return {"message": "Si cet email existe, un lien de réinitialisation a été envoyé."}


@app.post("/auth/reset-password")
def reset_password(payload: ResetPasswordIn, request: Request):
    """
    Reset password using a valid token.
    """
    token = (payload.token or "").strip()
    password = payload.password or ""
    ip = _get_client_ip(request)

    if not token:
        raise HTTPException(status_code=400, detail="Token manquant")

    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Le mot de passe doit contenir au moins 8 caractères")

    # Find valid token
    row = executer_requete_sql_one(
        """
        SELECT prt.id, prt.user_id, u.email
        FROM password_reset_tokens prt
        JOIN users u ON u.id = prt.user_id
        WHERE prt.token = %s
          AND prt.used = FALSE
          AND prt.expires_at > NOW()
          AND u.is_active = TRUE
        """,
        (token,),
    )

    if not row:
        _auth_log("reset_password_invalid_token", ip=ip)
        raise HTTPException(status_code=400, detail="Lien invalide ou expiré")

    token_id, user_id, email = row[0], row[1], row[2]

    # Hash new password
    pw_hash = _hash_password(password)

    # Update password
    executer_requete_sql_one(
        "UPDATE users SET password_hash = %s WHERE id = %s",
        (pw_hash, user_id),
    )

    # Mark token as used
    executer_requete_sql_one(
        "UPDATE password_reset_tokens SET used = TRUE WHERE id = %s",
        (token_id,),
    )

    _auth_log("reset_password_success", email=email, ip=ip)

    # Cleanup expired tokens in background
    _cleanup_expired_tokens()

    return {"message": "Mot de passe réinitialisé avec succès"}


@app.get("/auth/verify-reset-token")
def verify_reset_token(token: str = Query(...)):
    """
    Verify if a reset token is valid (for frontend validation before showing form).
    """
    if not token:
        raise HTTPException(status_code=400, detail="Token manquant")

    row = executer_requete_sql_one(
        """
        SELECT prt.id, u.email
        FROM password_reset_tokens prt
        JOIN users u ON u.id = prt.user_id
        WHERE prt.token = %s
          AND prt.used = FALSE
          AND prt.expires_at > NOW()
          AND u.is_active = TRUE
        """,
        (token,),
    )

    if not row:
        raise HTTPException(status_code=400, detail="Lien invalide ou expiré")

    return {"valid": True, "email": row[1]}


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


# Prix du m3 d'eau, utilise pour convertir les m3 recycles en euros economises.
#
# Valeur par defaut : 4,69 EUR TTC/m3 = prix moyen national francais (2,32 EUR
# d'eau potable + 2,37 EUR d'assainissement collectif), rapport SISPEA 2025 sur
# donnees 2023 : https://www.eaufrance.fr/
#
# Les deux composantes comptent : un m3 recycle est un m3 qui n'est ni achete au
# reseau, ni rejete a l'egout.
#
# C'est une MOYENNE NATIONALE : le prix reel va de ~2 EUR/m3 a plus de 8 EUR/m3
# selon la commune. Pour un chiffre juste sur un site donne, surcharger la
# variable d'environnement PRIX_EAU_M3 avec le prix lu sur la facture d'eau du
# client. Le chiffre est presente comme une estimation cote frontend.
_PRIX_EAU_M3 = float(os.getenv("PRIX_EAU_M3", "4.69"))


@app.get("/volumes/total")
def volumes_total(request: Request):
    """Total m3 recyclés (renvoi - adoucie) pour toutes les stations accessibles par l'utilisateur.
    Lit depuis la table cache_volumes_total (précalculée par ETL_volumes_total).
    Les stations dont la difference est negative sont exclues du total.

    Renvoie aussi l'economie estimee en euros (m3 recycles x prix du m3).
    Champs ajoutes, la reponse reste compatible avec l'existant."""
    user = _require_auth(request)
    roles = [r.lower() for r in user.get("roles", [])]

    if "admin" in roles or "super_admin" in roles:
        rows = executer_requete_sql(
            """SELECT c.compteur_eau_renvoi_m3, c.compteur_eau_adoucie_m3
               FROM cache_volumes_total c
               JOIN automate a ON a.nom_automate = c.nom_automate"""
        )
    else:
        orgs = _get_orgs_for_user(user["id"])
        if not orgs:
            return {
                "total_recycle_m3": 0,
                "nb_stations": 0,
                "prix_eau_m3": _PRIX_EAU_M3,
                "economie_eur": 0,
            }
        org_names = [o[1] for o in orgs]
        placeholders = ",".join(["%s"] * len(org_names))
        rows = executer_requete_sql(
            f"""SELECT c.compteur_eau_renvoi_m3, c.compteur_eau_adoucie_m3
                FROM cache_volumes_total c
                JOIN automate a ON a.nom_automate = c.nom_automate
                WHERE lower(a.client) IN ({placeholders})""",
            tuple(n.lower() for n in org_names),
        )

    total = 0.0
    nb = 0
    for renvoi, adoucie in rows:
        diff = renvoi - adoucie
        if diff >= 0:
            total += float(diff)
            nb += 1

    return {
        "total_recycle_m3": round(total, 1),
        "nb_stations": nb,
        "prix_eau_m3": _PRIX_EAU_M3,
        "economie_eur": round(total * _PRIX_EAU_M3),
    }


@app.get("/export/csv")
def export_csv(
    request: Request,
    stations: str = Query(..., description="Noms automates séparés par virgule"),
    colonnes: str = Query(..., description="Colonnes séparées par virgule"),
    source: str = Query("semaine", description="Table source: semaine, mois, annee"),
    date_debut: Optional[str] = Query(None, description="Date début YYYY-MM-DD"),
    date_fin: Optional[str] = Query(None, description="Date fin YYYY-MM-DD"),
):
    """Export CSV des données agrégées. Admin/Super-admin uniquement."""
    _require_admin_or_super(request)

    # Validation de la source
    source_config = {
        "semaine": {"table": "donnees_semaine", "date_col": "jour"},
        "mois": {"table": "donnees_mois", "date_col": "semaine_debut"},
        "annee": {"table": "donnees_annees", "date_col": "mois_debut"},
    }
    if source not in source_config:
        raise HTTPException(status_code=400, detail="Source invalide")
    cfg = source_config[source]

    # Colonnes autorisées
    allowed_columns = {
        "vol_renvoi_m3", "vol_adoucie_m3", "vol_relevage_m3",
        "taux_recyclage", "taux_desinfection", "taux_desinfection_avg", "taux_desinfection_med",
        "p1_mbar", "p2_mbar", "p3_mbar", "p4_mbar", "p5_mbar",
        "temp_moy_c", "temp_med_c", "chlore_moy_mg_l", "chlore_med_mg_l",
        "ph_moyen", "ph_med", "conso_kwh",
    }
    requested_cols = [c.strip() for c in colonnes.split(",") if c.strip()]
    safe_cols = [c for c in requested_cols if c in allowed_columns]
    if not safe_cols:
        raise HTTPException(status_code=400, detail="Aucune colonne valide")

    # Stations
    station_list = [s.strip() for s in stations.split(",") if s.strip()]
    if not station_list:
        raise HTTPException(status_code=400, detail="Aucune station sélectionnée")

    placeholders = ",".join(["%s"] * len(station_list))
    col_sql = ", ".join(safe_cols)
    query = f"""
        SELECT {cfg['date_col']}, nom_automate, {col_sql}
        FROM {cfg['table']}
        WHERE nom_automate IN ({placeholders})
    """
    params = list(station_list)

    if date_debut:
        query += f" AND {cfg['date_col']} >= %s"
        params.append(date_debut)
    if date_fin:
        query += f" AND {cfg['date_col']} <= %s"
        params.append(date_fin)

    query += f" ORDER BY {cfg['date_col']}, nom_automate"

    rows = executer_requete_sql(query, tuple(params))

    # Lissage taux_recyclage : forward-fill + backward-fill pour les 0 en debut (par station)
    taux_idx = safe_cols.index("taux_recyclage") if "taux_recyclage" in safe_cols else None
    if taux_idx is not None:
        col = 2 + taux_idx
        rows = [list(r) for r in rows]
        # Grouper par station
        stations = {}
        for i, row in enumerate(rows):
            stations.setdefault(row[1], []).append(i)
        for station, indices in stations.items():
            # Forward-fill
            last_positive = None
            for i in indices:
                val = rows[i][col]
                if val is not None and float(val) > 0:
                    last_positive = val
                elif val is not None and float(val) == 0 and last_positive is not None:
                    rows[i][col] = last_positive
            # Backward-fill pour les 0 en debut de serie
            first_positive = None
            for i in indices:
                if rows[i][col] is not None and float(rows[i][col]) > 0:
                    first_positive = rows[i][col]
                    break
            if first_positive is not None:
                for i in indices:
                    if rows[i][col] is not None and float(rows[i][col]) == 0:
                        rows[i][col] = first_positive
                    else:
                        break

    # Construction CSV
    import io, csv
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["date", "nom_automate"] + safe_cols)
    for row in rows:
        date_val = row[0].strftime("%Y-%m-%d") if hasattr(row[0], "strftime") else str(row[0])
        writer.writerow([date_val, row[1]] + [str(v) if v is not None else "" for v in row[2:]])

    from fastapi.responses import StreamingResponse
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=export_eaukey.csv"},
    )


@app.get("/export/premiere-date")
def export_premiere_date(
    request: Request,
    station: str = Query(..., description="Nom automate"),
    source: str = Query("semaine", description="Table source: semaine, mois, annee"),
):
    """Retourne la date de la premiere donnee disponible (mise en route) pour une
    station, selon la granularite demandee. Admin/Super-admin uniquement."""
    _require_admin_or_super(request)

    source_config = {
        "semaine": {"table": "donnees_semaine", "date_col": "jour"},
        "mois": {"table": "donnees_mois", "date_col": "semaine_debut"},
        "annee": {"table": "donnees_annees", "date_col": "mois_debut"},
    }
    if source not in source_config:
        raise HTTPException(status_code=400, detail="Source invalide")
    cfg = source_config[source]

    query = f"SELECT MIN({cfg['date_col']}) FROM {cfg['table']} WHERE nom_automate = %s"
    rows = executer_requete_sql(query, (station,))
    premiere = rows[0][0] if rows and rows[0] else None
    if premiere is None:
        return {"premiere_date": None}
    date_str = premiere.strftime("%Y-%m-%d") if hasattr(premiere, "strftime") else str(premiere)
    return {"premiere_date": date_str}


@app.get("/my/automates")
def list_my_automates(request: Request):
    user = _require_auth(request)
    roles = [r.lower() for r in user.get("roles", [])]
    if "admin" in roles or "super_admin" in roles:
        rows = executer_requete_sql("SELECT nom_automate, client, lieu, email, email2, email3 FROM automate;")
    else:
        orgs = _get_orgs_for_user(user["id"])
        if not orgs:
            return []
        org_names = [o[1] for o in orgs]
        placeholders = ",".join(["%s"] * len(org_names))
        rows = executer_requete_sql(
            f"""
            SELECT nom_automate, client, lieu, email, email2, email3
            FROM automate
            WHERE lower(client) IN ({placeholders});
            """,
            tuple(n.lower() for n in org_names),
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
# ADMIN: VISUALISER L'INTERFACE D'UN UTILISATEUR
# ---------------------------------------------------------------------------

@app.get("/admin/view-as/{user_id}")
def admin_view_as_user(user_id: int, request: Request):
    """Retourne les automates visibles par un utilisateur donné (admin/super_admin only)."""
    _require_admin_or_super(request)
    # Vérifier que l'utilisateur cible existe
    target = executer_requete_sql_one(
        "SELECT id, email, client_id FROM users WHERE id = %s", (user_id,)
    )
    if not target:
        raise HTTPException(status_code=404, detail="Utilisateur non trouvé")
    # Récupérer ses organisations
    orgs = _get_orgs_for_user(user_id)
    if not orgs:
        return {"user": {"id": target[0], "email": target[1], "client_id": target[2]}, "automates": []}
    org_names = [o[1] for o in orgs]
    placeholders = ",".join(["%s"] * len(org_names))
    rows = executer_requete_sql(
        f"""
        SELECT nom_automate, client, lieu, email, email2, email3
        FROM automate
        WHERE lower(client) IN ({placeholders});
        """,
        tuple(n.lower() for n in org_names),
    )
    automates = [
        {
            "nom_automate": r[0], "client": r[1], "lieu": r[2],
            "email": r[3], "email2": r[4], "email3": r[5],
            "emails": [e for e in [r[3], r[4], r[5]] if e],
        }
        for r in rows
    ]
    return {
        "user": {"id": target[0], "email": target[1], "client_id": target[2]},
        "organizations": [{"id": o[0], "name": o[1]} for o in orgs],
        "automates": automates,
    }


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
    # Fiche de résolution (optionnelle, cf. migrations/007_pannes_resolution.sql)
    cause: Optional[str] = Field(None, max_length=2000)
    action_realisee: Optional[str] = Field(None, max_length=4000)
    pieces_changees: Optional[str] = Field(None, max_length=1000)
    deplacement: Optional[bool] = None
    intervenant: Optional[str] = Field(None, max_length=200)
    partage_sav: bool = True


_PANNE_COLS = (
    "id, client, lieu, nom_automate, panne, probleme, date_debut, date_fin, created_by, "
    "cause, action_realisee, pieces_changees, deplacement, intervenant, partage_sav"
)


def _clean_opt(v: Optional[str]) -> Optional[str]:
    v = (v or "").strip()
    return v or None


def _panne_to_dict(r) -> dict:
    def _fmt(dt_val):
        return dt_val.isoformat() if hasattr(dt_val, "isoformat") else dt_val

    return {
        "id": r[0],
        "client": r[1],
        "lieu": r[2],
        "nom_automate": r[3],
        "panne": r[4],
        "probleme": r[5],
        "date_debut": _fmt(r[6]),
        "date_fin": _fmt(r[7]),
        "created_by": r[8],
        "cause": r[9],
        "action_realisee": r[10],
        "pieces_changees": r[11],
        "deplacement": r[12],
        "intervenant": r[13],
        "partage_sav": r[14],
    }


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
    rows = executer_requete_sql(f"SELECT {_PANNE_COLS} FROM pannes ORDER BY id DESC;")
    return [_panne_to_dict(r) for r in rows]


@app.post("/pannes")
def creer_panne(p: PanneIn, request: Request):
    email = _require_admin(request)

    if p.date_fin and p.date_fin < p.date_debut:
        raise HTTPException(status_code=400, detail="date_fin doit être >= date_debut")

    _validate_automate(p.client, p.lieu, p.nom_automate)

    row = executer_requete_sql_one(
        f"""
        INSERT INTO pannes (client, lieu, nom_automate, panne, probleme, date_debut, date_fin, created_by,
                            cause, action_realisee, pieces_changees, deplacement, intervenant, partage_sav)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING {_PANNE_COLS};
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
            _clean_opt(p.cause),
            _clean_opt(p.action_realisee),
            _clean_opt(p.pieces_changees),
            p.deplacement,
            _clean_opt(p.intervenant) or email or None,
            p.partage_sav,
        ),
    )
    if row is None:
        raise HTTPException(status_code=500, detail="Insertion panne échouée")
    return _panne_to_dict(row)


@app.put("/pannes/{panne_id}")
def maj_panne(panne_id: int, p: PanneIn, request: Request):
    _require_admin(request)

    if p.date_fin and p.date_fin < p.date_debut:
        raise HTTPException(status_code=400, detail="date_fin doit être >= date_debut")

    _validate_automate(p.client, p.lieu, p.nom_automate)

    row = executer_requete_sql_one(
        f"""
        UPDATE pannes
        SET client = %s,
            lieu = %s,
            nom_automate = %s,
            panne = %s,
            probleme = %s,
            date_debut = %s,
            date_fin = %s,
            cause = %s,
            action_realisee = %s,
            pieces_changees = %s,
            deplacement = %s,
            intervenant = %s,
            partage_sav = %s,
            updated_at = now()
        WHERE id = %s
        RETURNING {_PANNE_COLS};
        """,
        (
            p.client,
            p.lieu,
            p.nom_automate,
            p.panne,
            p.probleme,
            p.date_debut,
            p.date_fin,
            _clean_opt(p.cause),
            _clean_opt(p.action_realisee),
            _clean_opt(p.pieces_changees),
            p.deplacement,
            _clean_opt(p.intervenant),
            p.partage_sav,
            panne_id,
        ),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Panne introuvable")
    return _panne_to_dict(row)


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
    created_at = row[1].isoformat() if hasattr(row[1], "isoformat") else row[1]
    return {"id": row[0], "client_id": conv.client_id, "type": conv.type, "created_at": created_at}


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
        "created_at": row[1].isoformat() if hasattr(row[1], "isoformat") else row[1],
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
    escalade: Optional[bool] = None
    escalade_motif: Optional[str] = None

class ClientConversation(BaseModel):
    client_id: str
    client_name: str

@app.get("/clients", response_model=List[ClientConversation])
def get_clients(request: Request, is_admin: bool = False):
    """
    Renvoie la liste des clients (= conversations disponibles).
    - Admin: tous les clients
    - Client: juste le sien
    """
    if is_admin:
        if not _is_admin_user(_require_auth(request)):
            raise HTTPException(status_code=403, detail="Accès réservé à l'admin")
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
    organization_names: Optional[list[str]] = None


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
        SELECT u.id, u.email, u.client_id,
               COALESCE(array_agg(DISTINCT r.name ORDER BY r.name) FILTER (WHERE r.name IS NOT NULL), '{}') AS roles
        FROM users u
        LEFT JOIN user_roles ur ON ur.user_id = u.id
        LEFT JOIN roles r ON r.id = ur.role_id
        GROUP BY u.id, u.email, u.client_id
        ORDER BY u.email;
        """
    )
    # Récupérer toutes les associations organisation-utilisateur en une requête
    org_rows = executer_requete_sql(
        """
        SELECT ou.user_id, o.name, ou.org_role
        FROM organization_users ou
        JOIN organizations o ON o.id = ou.organization_id
        ORDER BY o.name;
        """
    )
    # Indexer par user_id
    user_orgs: dict[int, list[dict]] = {}
    for r in org_rows:
        user_orgs.setdefault(r[0], []).append({"org_name": r[1], "org_role": r[2]})
    result = []
    for r in rows:
        orgs = user_orgs.get(r[0], [])
        entry = {
            "id": r[0], "email": r[1], "client_id": r[2], "roles": list(r[3]),
            "organizations": orgs,
            # Rétro-compatibilité
            "org_role": orgs[0]["org_role"] if orgs else None,
            "org_name": orgs[0]["org_name"] if orgs else None,
        }
        result.append(entry)
    return result


@app.post("/access/assign")
def assign_access(payload: AccessAssignIn, request: Request):
    _require_admin_or_super(request)
    roles = [r.strip().lower() for r in payload.roles if r and r.strip()]

    # Gestion des organisations (multi-org)
    if payload.organization_names is not None:
        org_names = [n.strip() for n in payload.organization_names if n and n.strip()]
        # Récupérer les org_role existants pour les préserver
        existing_roles = {}
        existing_rows = executer_requete_sql(
            """
            SELECT o.name, ou.org_role
            FROM organization_users ou
            JOIN organizations o ON o.id = ou.organization_id
            WHERE ou.user_id = %s;
            """,
            (payload.user_id,),
        )
        for er in existing_rows:
            existing_roles[er[0]] = er[1]
        # Supprimer les anciennes associations
        executer_requete_sql(
            "DELETE FROM organization_users WHERE user_id = %s;",
            (payload.user_id,),
        )
        # Ajouter les nouvelles en préservant le org_role existant
        for org_name in org_names:
            org_row = executer_requete_sql_one("SELECT id FROM organizations WHERE name = %s", (org_name,))
            if org_row:
                preserved_role = existing_roles.get(org_name, "employee")
                executer_requete_sql_one(
                    """
                    INSERT INTO organization_users (user_id, organization_id, org_role)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, organization_id) DO NOTHING;
                    """,
                    (payload.user_id, org_row[0], preserved_role),
                )
        # Mettre à jour client_id legacy (première org ou NULL)
        first_org = org_names[0] if org_names else None
        executer_requete_sql(
            "UPDATE users SET client_id = %s WHERE id = %s;",
            (first_org, payload.user_id),
        )
    elif payload.client_id is not None:
        # Rétro-compatibilité : ancien comportement avec client_id unique
        client_id = (payload.client_id or "").strip() or None
        executer_requete_sql(
            "UPDATE users SET client_id = %s WHERE id = %s;",
            (client_id, payload.user_id),
        )
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


@app.get("/automates/orphan-clients")
def list_orphan_clients(request: Request):
    _require_admin_or_super(request)
    rows = executer_requete_sql(
        """
        SELECT DISTINCT a.client
        FROM automate a
        WHERE a.client IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM organizations o
              WHERE lower(o.name) = lower(a.client)
          )
        ORDER BY a.client;
        """
    )
    return [r[0] for r in rows]


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
    # Lier les automates existants à la nouvelle org
    executer_requete_sql_one(
        "UPDATE automate SET organization_id = %s WHERE lower(client) = lower(%s) AND organization_id IS NULL",
        (org_row[0], name),
    )
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
    # Un utilisateur peut appartenir à plusieurs organisations
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
    # Mettre à jour client_id : prendre une autre org si elle existe, sinon NULL
    remaining = executer_requete_sql_one(
        """
        SELECT o.name FROM organization_users ou
        JOIN organizations o ON o.id = ou.organization_id
        WHERE ou.user_id = %s ORDER BY o.name LIMIT 1
        """,
        (user_id,),
    )
    executer_requete_sql_one(
        "UPDATE users SET client_id = %s WHERE id = %s",
        (remaining[0] if remaining else None, user_id),
    )
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


# ---------------------------------------------------------------------------
# Gestion des accès par l'admin d'organisation (org_admin)
# ---------------------------------------------------------------------------

def _require_org_admin(request: Request, org_id: int | None = None) -> dict:
    """Vérifie que l'appelant est org_admin et retourne user + org info.
    Si org_id est fourni, vérifie que l'utilisateur est org_admin de cette org spécifique.
    Sinon, prend la première org où il est org_admin.
    """
    user = _require_auth(request)
    if org_id is not None:
        row = executer_requete_sql_one(
            """
            SELECT ou.organization_id, ou.org_role, o.name
            FROM organization_users ou
            JOIN organizations o ON o.id = ou.organization_id
            WHERE ou.user_id = %s AND ou.organization_id = %s AND ou.org_role = 'org_admin';
            """,
            (user["id"], org_id),
        )
    else:
        row = executer_requete_sql_one(
            """
            SELECT ou.organization_id, ou.org_role, o.name
            FROM organization_users ou
            JOIN organizations o ON o.id = ou.organization_id
            WHERE ou.user_id = %s AND ou.org_role = 'org_admin'
            ORDER BY o.name LIMIT 1;
            """,
            (user["id"],),
        )
    if not row:
        raise HTTPException(status_code=403, detail="Accès réservé aux administrateurs d'organisation")
    user["organization_id"] = row[0]
    user["org_role"] = row[1]
    user["organization_name"] = row[2]
    # Ajouter toutes les orgs admin
    all_admin_orgs = executer_requete_sql(
        """
        SELECT ou.organization_id, o.name
        FROM organization_users ou
        JOIN organizations o ON o.id = ou.organization_id
        WHERE ou.user_id = %s AND ou.org_role = 'org_admin'
        ORDER BY o.name;
        """,
        (user["id"],),
    )
    user["admin_organizations"] = [{"id": r[0], "name": r[1]} for r in all_admin_orgs]
    return user


@app.get("/org-admin/organizations")
def org_admin_list_organizations(request: Request):
    """Retourne les organisations où l'utilisateur est org_admin."""
    admin = _require_org_admin(request)
    return admin["admin_organizations"]


@app.get("/org-admin/members")
def org_admin_list_members(request: Request, org_id: int | None = None):
    """Liste les membres de l'organisation de l'org_admin connecté."""
    admin = _require_org_admin(request, org_id=org_id)
    org_id = admin["organization_id"]
    rows = executer_requete_sql(
        """
        SELECT u.id, u.email, ou.org_role
        FROM organization_users ou
        JOIN users u ON u.id = ou.user_id
        WHERE ou.organization_id = %s
        ORDER BY ou.org_role DESC, u.email;
        """,
        (org_id,),
    )
    return [{"id": r[0], "email": r[1], "org_role": r[2]} for r in rows]


@app.get("/org-admin/automates")
def org_admin_list_automates(request: Request, org_id: int | None = None):
    """Liste les automates (stations) de l'organisation de l'org_admin."""
    admin = _require_org_admin(request, org_id=org_id)
    org_id = admin["organization_id"]
    org_name = admin["organization_name"]
    rows = executer_requete_sql(
        """
        SELECT nom_automate, numero_automate, lieu
        FROM automate
        WHERE organization_id = %s OR lower(client) = lower(%s)
        ORDER BY nom_automate;
        """,
        (org_id, org_name),
    )
    return [{"nom_automate": r[0], "numero_automate": r[1], "lieu": r[2]} for r in rows]


@app.get("/org-admin/member/{user_id}/automates")
def org_admin_get_member_access(user_id: int, request: Request, org_id: int | None = None):
    """Retourne la liste des noms d'automates accessibles par un membre."""
    admin = _require_org_admin(request, org_id=org_id)
    org_id = admin["organization_id"]
    # Vérifier que l'utilisateur fait partie de l'organisation
    member = executer_requete_sql_one(
        "SELECT user_id FROM organization_users WHERE user_id = %s AND organization_id = %s",
        (user_id, org_id),
    )
    if not member:
        raise HTTPException(status_code=404, detail="Membre introuvable dans l'organisation")
    rows = executer_requete_sql(
        "SELECT automate_nom FROM automate_access WHERE user_id = %s ORDER BY automate_nom",
        (user_id,),
    )
    return [r[0] for r in rows]


class OrgAdminSetAccessIn(BaseModel):
    automate_noms: list[str]


@app.put("/org-admin/member/{user_id}/automates")
def org_admin_set_member_access(user_id: int, payload: OrgAdminSetAccessIn, request: Request, org_id: int | None = None):
    """Définit les accès automates d'un membre (limité à l'org de l'admin)."""
    admin = _require_org_admin(request, org_id=org_id)
    org_id = admin["organization_id"]
    org_name = admin["organization_name"]
    # Vérifier que l'utilisateur fait partie de l'organisation
    member = executer_requete_sql_one(
        "SELECT user_id FROM organization_users WHERE user_id = %s AND organization_id = %s",
        (user_id, org_id),
    )
    if not member:
        raise HTTPException(status_code=404, detail="Membre introuvable dans l'organisation")
    # Récupérer les automates de l'organisation
    org_automates = executer_requete_sql(
        """
        SELECT nom_automate FROM automate
        WHERE organization_id = %s OR lower(client) = lower(%s)
        """,
        (org_id, org_name),
    )
    valid_automates = {r[0] for r in org_automates}
    invalid = [n for n in payload.automate_noms if n not in valid_automates]
    if invalid:
        raise HTTPException(status_code=400, detail=f"Automates hors organisation: {', '.join(invalid)}")
    # Supprimer les accès existants UNIQUEMENT pour les automates de cette org
    for nom in valid_automates:
        executer_requete_sql_one(
            "DELETE FROM automate_access WHERE automate_nom = %s AND user_id = %s",
            (nom, user_id),
        )
    # Insérer les nouveaux accès
    for nom in payload.automate_noms:
        executer_requete_sql_one(
            "INSERT INTO automate_access (automate_nom, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (nom, user_id),
        )
    return {"status": "success"}


@app.get("/messages/{client_id}", response_model=List[MessageOut])
def get_messages_for_client(client_id: str, request: Request):
    """Récupère tous les messages pour un client donné."""
    is_admin = _is_admin_user(_require_auth(request))
    query = """
        SELECT id, client_id, sender_id, content, file_url, created_at, is_read, escalade, escalade_motif
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
            "escalade": bool(r[7]),
            # Le motif est une note pour l'équipe : jamais renvoyé au client.
            "escalade_motif": r[8] if is_admin else None,
        }
        for r in rows
    ]

@app.post("/messages/{client_id}", response_model=MessageOut)
def post_message_for_client(client_id: str, message: MessageIn, request: Request):
    """Ajoute un message pour un client."""
    if not _is_admin_user(_require_auth(request)) and message.sender_id in ADMIN_SENDER_IDS:
        message.sender_id = CLIENT_SENDER_IDS[0]
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
        # messages.created_at est un timestamp : le modèle de réponse attend une chaîne
        # (sans conversion, la route répondait 500 alors que le message était enregistré).
        "created_at": row[1].isoformat() if hasattr(row[1], "isoformat") else row[1],
        "is_read": False,
    }

# -------------------
# NOTIFICATIONS & MARQUAGE LU
# -------------------

ADMIN_SENDER_IDS = (0, 677)
CLIENT_SENDER_IDS = (1, 6863)
# Réponses de l'agent SAV dans la messagerie (POST /chat/{client_id}/agent).
# Négatif pour ne jamais entrer en collision avec un id utilisateur.
AGENT_SENDER_ID = -1


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


# ===========================================================================
# ENDPOINTS RECYCLEURS D'AIR (additif - ne touche AUCUNE logique recycleur eau)
# ---------------------------------------------------------------------------
# Grandeurs air dans `mesures` : debit{1,2,3}_m3h, pression{1..4}_pa,
# temperature{1,3,4}, hygro{1,2,3}_pc, co2_ppm, cov_ppm, mes_microg_l.
# Tables pre-agregees ETL : donnees_semaine_air / donnees_mois_air / donnees_annees_air.
# Sentinelle capteur pression deconnecte = |x| >= 60000 (filtree).
# ===========================================================================

# Cap d'integration (heures) pour les volumes d'air a la volee (cf. ETL air)
_AIR_DT_CAP_H = 0.0833  # 5 min
# Borne de validite des pressions (Pa) : hors de cet intervalle = capteur deconnecte
_AIR_PA_MIN, _AIR_PA_MAX = -60000, 60000

# --- Detection eau/air par les valeurs remontees (avec petit cache TTL) ---
_AIR_TYPES_CACHE = {"ts": 0.0, "data": None}
_AIR_TYPES_TTL = 300  # secondes

@app.get("/automates/types")
def automates_types(request: Request):
    """Retourne {nom_automate: 'air'|'eau'} d'apres les grandeurs remontees sur 7 jours.
    Un automate est 'air' s'il remonte debit/hygro/pression_pa ; sinon 'eau'.
    Un utilisateur non admin ne recoit que ses stations."""
    user = _require_auth(request)
    if not _is_admin_user(user):
        visibles = _user_automates(user)
        return {k: v for k, v in _automates_types_all().items() if k in visibles}
    return _automates_types_all()


def _automates_types_all() -> dict:
    now_ts = time.time()
    if _AIR_TYPES_CACHE["data"] is not None and (now_ts - _AIR_TYPES_CACHE["ts"]) < _AIR_TYPES_TTL:
        return _AIR_TYPES_CACHE["data"]

    query = """
      SELECT nom_automate,
        CASE WHEN bool_or(
               debit1_m3h IS NOT NULL OR hygro1_pc IS NOT NULL OR pression1_pa IS NOT NULL
             ) THEN 'air' ELSE 'eau' END AS type
      FROM mesures
      WHERE horodatage > (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '7 days'
      GROUP BY nom_automate;
    """
    rows = executer_requete_sql(query)
    data = {r[0]: r[1] for r in rows}
    _AIR_TYPES_CACHE["ts"] = now_ts
    _AIR_TYPES_CACHE["data"] = data
    return data


# --- Helpers generiques de series air ---
def _air_strip_label(x) -> str:
    return x.strip() if isinstance(x, str) else str(x)

def _air_series_pre_aggrege(table: str, date_col: str, label_kind: str,
                            nom_automate: str, series: list) -> dict:
    """Lit une table ETL air pre-agregee et renvoie {labels, <key>:[...], ...}.
    series = liste de tuples (out_key, db_column). Colonnes hardcodees cote serveur."""
    cols_sql = ",\n      ".join(f"COALESCE({col}, 0) AS {key}" for key, col in series)
    if label_kind == "semaine":
        label_sql = f"to_char({date_col}, 'FMDay')"
    elif label_kind == "mois":
        label_sql = f"to_char({date_col}, 'YYYY-MM-DD')"
    else:  # annee
        label_sql = f"to_char({date_col}, 'FMMonth')"
    query = f"""
      SELECT {label_sql} AS label,
      {cols_sql}
      FROM {table}
      WHERE nom_automate = %s
      ORDER BY {date_col};
    """
    rows = executer_requete_sql(query, (nom_automate,))
    out = {"labels": [_air_strip_label(r[0]) for r in rows]}
    for i, (key, _) in enumerate(series, start=1):
        out[key] = [float(r[i]) if r[i] is not None else 0 for r in rows]
    return out

def _air_series_jour(nom_automate: str, aggrs: list) -> dict:
    """Series 'jour' (24h glissantes) calculees a la volee depuis `mesures`.
    aggrs = liste de tuples (out_key, sql_aggregate_expr)."""
    agg_sql = ",\n      ".join(f"{expr} AS {key}" for key, expr in aggrs)
    query = f"""
      SELECT date_trunc('hour', horodatage) AS heure,
      {agg_sql}
      FROM mesures
      WHERE nom_automate = %s
        AND horodatage >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
        AND horodatage <  (now() AT TIME ZONE 'Europe/Paris')
      GROUP BY heure
      ORDER BY heure;
    """
    rows = executer_requete_sql(query, (nom_automate,))
    out = {"labels": [r[0].strftime("%H:%M") for r in rows]}
    for i, (key, _) in enumerate(aggrs, start=1):
        out[key] = [float(r[i]) if r[i] is not None else 0 for r in rows]
    return out

def _air_empty(keys: list) -> dict:
    return {"labels": [], **{k: [] for k in keys}}


# --- DEBITS D'AIR (m3/h) : debit1/2/3 ---
@app.get("/air/debit_all/{period}")
def air_debit_all(period: str, nom_automate: str = Query(..., description="Nom de l'automate")):
    keys = ["debit1_m3h", "debit2_m3h", "debit3_m3h"]
    if period == "jour":
        return _air_series_jour(nom_automate, [
            ("debit1_m3h", "ROUND(AVG(debit1_m3h)::numeric, 1)"),
            ("debit2_m3h", "ROUND(AVG(debit2_m3h)::numeric, 1)"),
            ("debit3_m3h", "ROUND(AVG(debit3_m3h)::numeric, 1)"),
        ])
    table, date_col, kind = _AIR_PERIOD_MAP.get(period, (None, None, None))
    if not table:
        return _air_empty(keys)
    return _air_series_pre_aggrege(table, date_col, kind, nom_automate, [
        ("debit1_m3h", "debit1_moy_m3h"),
        ("debit2_m3h", "debit2_moy_m3h"),
        ("debit3_m3h", "debit3_moy_m3h"),
    ])


# --- VOLUMES D'AIR TRAITES (m3) : vol_air1/2/3 ---
@app.get("/air/volume_air/{period}")
def air_volume_air(period: str, nom_automate: str = Query(..., description="Nom de l'automate")):
    keys = ["vol_air1_m3", "vol_air2_m3", "vol_air3_m3"]
    if period == "jour":
        # Integration trapezoidale du debit, par heure (cf. ETL air)
        query = f"""
          WITH flow AS (
            SELECT date_trunc('hour', horodatage) AS heure,
              debit1_m3h, debit2_m3h, debit3_m3h,
              LAG(debit1_m3h) OVER w AS l1,
              LAG(debit2_m3h) OVER w AS l2,
              LAG(debit3_m3h) OVER w AS l3,
              LEAST(GREATEST(EXTRACT(EPOCH FROM (horodatage - LAG(horodatage) OVER w)) / 3600.0, 0), {_AIR_DT_CAP_H}) AS dt_h
            FROM mesures
            WHERE nom_automate = %s
              AND horodatage >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours'
              AND horodatage <  (now() AT TIME ZONE 'Europe/Paris')
            WINDOW w AS (ORDER BY horodatage)
          )
          SELECT heure,
            ROUND(SUM(COALESCE((debit1_m3h + l1) / 2.0, 0) * dt_h)::numeric, 2),
            ROUND(SUM(COALESCE((debit2_m3h + l2) / 2.0, 0) * dt_h)::numeric, 2),
            ROUND(SUM(COALESCE((debit3_m3h + l3) / 2.0, 0) * dt_h)::numeric, 2)
          FROM flow
          GROUP BY heure
          ORDER BY heure;
        """
        rows = executer_requete_sql(query, (nom_automate,))
        return {
            "labels": [r[0].strftime("%H:%M") for r in rows],
            "vol_air1_m3": [float(r[1]) if r[1] is not None else 0 for r in rows],
            "vol_air2_m3": [float(r[2]) if r[2] is not None else 0 for r in rows],
            "vol_air3_m3": [float(r[3]) if r[3] is not None else 0 for r in rows],
        }
    table, date_col, kind = _AIR_PERIOD_MAP.get(period, (None, None, None))
    if not table:
        return _air_empty(keys)
    return _air_series_pre_aggrege(table, date_col, kind, nom_automate, [
        ("vol_air1_m3", "vol_air1_m3"),
        ("vol_air2_m3", "vol_air2_m3"),
        ("vol_air3_m3", "vol_air3_m3"),
    ])


# --- PRESSIONS DIFFERENTIELLES (Pa) : encrassement filtres, sentinelles filtrees ---
@app.get("/air/pression_all/{period}")
def air_pression_all(period: str, nom_automate: str = Query(..., description="Nom de l'automate")):
    keys = ["p1_pa", "p2_pa", "p3_pa", "p4_pa"]
    if period == "jour":
        flt = f"FILTER (WHERE {{c}} BETWEEN {_AIR_PA_MIN} AND {_AIR_PA_MAX})"
        return _air_series_jour(nom_automate, [
            ("p1_pa", f"ROUND(AVG(pression1_pa) {flt.format(c='pression1_pa')})"),
            ("p2_pa", f"ROUND(AVG(pression2_pa) {flt.format(c='pression2_pa')})"),
            ("p3_pa", f"ROUND(AVG(pression3_pa) {flt.format(c='pression3_pa')})"),
            ("p4_pa", f"ROUND(AVG(pression4_pa) {flt.format(c='pression4_pa')})"),
        ])
    table, date_col, kind = _AIR_PERIOD_MAP.get(period, (None, None, None))
    if not table:
        return _air_empty(keys)
    return _air_series_pre_aggrege(table, date_col, kind, nom_automate, [
        ("p1_pa", "p1_pa"), ("p2_pa", "p2_pa"), ("p3_pa", "p3_pa"), ("p4_pa", "p4_pa"),
    ])


# --- TEMPERATURES (degC) : temperature1/3/4 ---
@app.get("/air/temperature_all/{period}")
def air_temperature_all(period: str, nom_automate: str = Query(..., description="Nom de l'automate")):
    keys = ["temp1_c", "temp3_c", "temp4_c"]
    if period == "jour":
        return _air_series_jour(nom_automate, [
            ("temp1_c", "ROUND(AVG(temperature1)::numeric, 1)"),
            ("temp3_c", "ROUND(AVG(temperature3)::numeric, 1)"),
            ("temp4_c", "ROUND(AVG(temperature4)::numeric, 1)"),
        ])
    table, date_col, kind = _AIR_PERIOD_MAP.get(period, (None, None, None))
    if not table:
        return _air_empty(keys)
    return _air_series_pre_aggrege(table, date_col, kind, nom_automate, [
        ("temp1_c", "temp1_c"), ("temp3_c", "temp3_c"), ("temp4_c", "temp4_c"),
    ])


# --- HYGROMETRIE (%) : hygro1/2/3 ---
@app.get("/air/hygro_all/{period}")
def air_hygro_all(period: str, nom_automate: str = Query(..., description="Nom de l'automate")):
    keys = ["hygro1_pc", "hygro2_pc", "hygro3_pc"]
    if period == "jour":
        return _air_series_jour(nom_automate, [
            ("hygro1_pc", "ROUND(AVG(hygro1_pc)::numeric, 1)"),
            ("hygro2_pc", "ROUND(AVG(hygro2_pc)::numeric, 1)"),
            ("hygro3_pc", "ROUND(AVG(hygro3_pc)::numeric, 1)"),
        ])
    table, date_col, kind = _AIR_PERIOD_MAP.get(period, (None, None, None))
    if not table:
        return _air_empty(keys)
    return _air_series_pre_aggrege(table, date_col, kind, nom_automate, [
        ("hygro1_pc", "hygro1_pc"), ("hygro2_pc", "hygro2_pc"), ("hygro3_pc", "hygro3_pc"),
    ])


# --- QUALITE D'AIR : co2_ppm, cov_ppm, mes_microg_l (capteurs futurs, peut etre 0) ---
@app.get("/air/qualite_air/{period}")
def air_qualite_air(period: str, nom_automate: str = Query(..., description="Nom de l'automate")):
    keys = ["co2_ppm", "cov_ppm", "mes_microg_l"]
    if period == "jour":
        return _air_series_jour(nom_automate, [
            ("co2_ppm", "ROUND(AVG(co2_ppm)::numeric, 1)"),
            ("cov_ppm", "ROUND(AVG(cov_ppm)::numeric, 1)"),
            ("mes_microg_l", "ROUND(AVG(mes_microg_l)::numeric, 1)"),
        ])
    table, date_col, kind = _AIR_PERIOD_MAP.get(period, (None, None, None))
    if not table:
        return _air_empty(keys)
    return _air_series_pre_aggrege(table, date_col, kind, nom_automate, [
        ("co2_ppm", "co2_ppm"), ("cov_ppm", "cov_ppm"), ("mes_microg_l", "mes_microg_l"),
    ])


# Mapping period -> (table ETL air, colonne date, type de label)
_AIR_PERIOD_MAP = {
    "semaine": ("donnees_semaine_air", "jour", "semaine"),
    "mois":    ("donnees_mois_air", "semaine_debut", "mois"),
    "annee":   ("donnees_annees_air", "mois_debut", "annee"),
}


# --- TEMPS REEL (cartes KPI) : derniere mesure air ---
def _air_clean_pa(v):
    """Neutralise les sentinelles capteur pression."""
    if v is None:
        return None
    return v if _AIR_PA_MIN <= v <= _AIR_PA_MAX else None

@app.get("/air/temps_reel/{metric}")
def air_temps_reel(metric: str, nom_automate: str = Query(..., description="Nom de l'automate")):
    """Derniere valeur pour les cartes KPI air. JSON : {horodatage, valeur, valeurs?}.
    `valeur` = valeur principale (compat KpiCard) ; `valeurs` = detail par capteur."""
    # Cas special : volume d'air traite sur les 24 dernieres heures (integration)
    if metric == "volume_air":
        query = f"""
          WITH flow AS (
            SELECT
              debit1_m3h, debit2_m3h, debit3_m3h,
              LAG(debit1_m3h) OVER w AS l1, LAG(debit2_m3h) OVER w AS l2, LAG(debit3_m3h) OVER w AS l3,
              LEAST(GREATEST(EXTRACT(EPOCH FROM (horodatage - LAG(horodatage) OVER w)) / 3600.0, 0), {_AIR_DT_CAP_H}) AS dt_h
            FROM mesures
            WHERE nom_automate = %s AND horodatage >= (now() AT TIME ZONE 'Europe/Paris') - INTERVAL '24 hours' AND horodatage < (now() AT TIME ZONE 'Europe/Paris')
            WINDOW w AS (ORDER BY horodatage)
          )
          SELECT
            ROUND(SUM(COALESCE((debit1_m3h + l1) / 2.0, 0) * dt_h)::numeric, 1),
            ROUND(SUM(COALESCE((debit2_m3h + l2) / 2.0, 0) * dt_h)::numeric, 1),
            ROUND(SUM(COALESCE((debit3_m3h + l3) / 2.0, 0) * dt_h)::numeric, 1)
          FROM flow;
        """
        r = executer_requete_sql(query, (nom_automate,))
        if not r or r[0][0] is None:
            return {"horodatage": None, "valeur": 0}
        v1, v2, v3 = [float(x) if x is not None else 0 for x in r[0]]
        return {"horodatage": None, "valeur": round(v1 + v2 + v3, 1),
                "valeurs": {"vol_air1_m3": v1, "vol_air2_m3": v2, "vol_air3_m3": v3}}

    # Metriques instantanees : derniere ligne de mesures
    query = """
      SELECT horodatage,
        debit1_m3h, debit2_m3h, debit3_m3h,
        pression1_pa, pression2_pa, pression3_pa, pression4_pa,
        temperature1, temperature3, temperature4,
        hygro1_pc, hygro2_pc, hygro3_pc,
        co2_ppm, cov_ppm, mes_microg_l
      FROM mesures
      WHERE nom_automate = %s
      ORDER BY horodatage DESC
      LIMIT 1;
    """
    r = executer_requete_sql(query, (nom_automate,))
    if not r:
        return {"horodatage": None, "valeur": 0}
    row = r[0]
    ho = row[0].strftime("%Y-%m-%d %H:%M:%S") if row[0] is not None else None

    def f(v):
        return float(v) if v is not None else None

    mapping = {
        "debit":       (f(row[1]), {"debit1_m3h": f(row[1]), "debit2_m3h": f(row[2]), "debit3_m3h": f(row[3])}),
        "pression":    (f(_air_clean_pa(row[4])), {"p1_pa": f(_air_clean_pa(row[4])), "p2_pa": f(_air_clean_pa(row[5])),
                                                    "p3_pa": f(_air_clean_pa(row[6])), "p4_pa": f(_air_clean_pa(row[7]))}),
        "temperature": (f(row[8]), {"temp1_c": f(row[8]), "temp3_c": f(row[9]), "temp4_c": f(row[10])}),
        "hygro":       (f(row[11]), {"hygro1_pc": f(row[11]), "hygro2_pc": f(row[12]), "hygro3_pc": f(row[13])}),
        "qualite":     (f(row[14]), {"co2_ppm": f(row[14]), "cov_ppm": f(row[15]), "mes_microg_l": f(row[16])}),
    }
    if metric not in mapping:
        return {"horodatage": ho, "valeur": 0}
    principale, detail = mapping[metric]
    return {"horodatage": ho, "valeur": principale if principale is not None else 0, "valeurs": detail}

# ===========================================================================
# FIN ENDPOINTS RECYCLEURS D'AIR
# ===========================================================================


# ===========================================================================
# QUALITE D'EAU : predictions IA sur les images recycleur (table urls_images)
# ===========================================================================
# Le modele ecrit pred_qualite_eau / pred_opacite / pred_bac_vide_prob sur
# urls_images (cf. app_labelisation/backend/predict_production.py).
# On exclut les images "bac sans eau" (qualite non significative).
_EAU_BAC_VIDE_SEUIL = 0.5
# period -> (intervalle SQL, granularite date_trunc, format du label)
_EAU_PERIOD_MAP = {
    "jour":    ("24 hours",  "hour",  "HH24:MI"),
    "semaine": ("7 days",    "day",   "FMDay"),
    "mois":    ("30 days",   "day",   "YYYY-MM-DD"),
    "annee":   ("12 months", "month", "FMMonth"),
}


@app.get("/eau/qualite_eau/{period}")
def eau_qualite_eau(period: str, nom_automate: str = Query(..., description="Nom de l'automate"),
                    include_bac_vide: bool = Query(False, description="Inclure les images detectees comme 'bac vide'")):
    """Serie de qualite d'eau predite par le modele IA depuis urls_images.
    Filtre les images 'bac sans eau' par defaut. Avec include_bac_vide=true on
    garde tout et on renvoie aussi la prob 'bac vide' moyenne par bucket (pour
    diagnostiquer un automate dont toutes les photos sont classees 'bac vide').
    JSON : {labels, qualite_eau, opacite, bac_vide}.
    Jointure : urls_images.numero_automate (INT) <-> automate.nom_automate (ex '2022121.0')."""
    keys = ["qualite_eau", "opacite", "bac_vide", "matiere"]
    cfg = _EAU_PERIOD_MAP.get(period)
    if not cfg:
        return {"labels": [], **{k: [] for k in keys}}
    interval, trunc, label_fmt = cfg
    # Statut "bac vide" effectif = correction humaine (corr_bac_vide) sinon prediction modele.
    # include_bac_vide=true (front)  : on montre tout SAUF ce qu'un super_admin a confirme vide.
    # include_bac_vide=false (defaut): comportement historique, on exclut les 'bac vide' effectifs.
    if include_bac_vide:
        filtre_bac_vide = "AND corr_bac_vide IS NOT TRUE"
    else:
        filtre_bac_vide = "AND COALESCE(corr_bac_vide, pred_bac_vide_prob >= %s) IS NOT TRUE"
    query = f"""
      SELECT to_char(bucket, %s) AS label, qualite_eau, opacite, bac_vide, matiere
      FROM (
        SELECT date_trunc(%s, timestamp) AS bucket,
               -- valeur affichee = correction humaine (super_admin) sinon prediction modele
               ROUND(AVG(COALESCE(corr_qualite_eau, pred_qualite_eau))::numeric, 1) AS qualite_eau,
               ROUND(AVG(COALESCE(corr_opacite, pred_opacite))::numeric, 1)         AS opacite,
               ROUND(AVG(pred_bac_vide_prob)::numeric, 2)                           AS bac_vide,
               -- MES : pred_matiere_prob (0..1) remis sur echelle /10, corrige si besoin
               ROUND(AVG(COALESCE(corr_matiere, pred_matiere_prob * 10))::numeric, 1) AS matiere
        FROM urls_images
        WHERE numero_automate = CAST(SPLIT_PART(%s, '.', 1) AS INTEGER)
          AND timestamp >= now() - INTERVAL '{interval}'
          AND pred_qualite_eau IS NOT NULL
          {filtre_bac_vide}
        GROUP BY bucket
      ) t
      ORDER BY bucket;
    """
    params = [label_fmt, trunc, nom_automate]
    if not include_bac_vide:
        params.append(_EAU_BAC_VIDE_SEUIL)
    rows = executer_requete_sql(query, tuple(params))
    out = {"labels": [r[0].strip() if isinstance(r[0], str) else r[0] for r in rows]}
    out["qualite_eau"] = [float(r[1]) if r[1] is not None else 0 for r in rows]
    out["opacite"]     = [float(r[2]) if r[2] is not None else 0 for r in rows]
    out["bac_vide"]    = [float(r[3]) if r[3] is not None else None for r in rows]
    out["matiere"]     = [float(r[4]) if r[4] is not None else 0 for r in rows]
    return out


@app.get("/eau/derniere_photo")
def eau_derniere_photo(nom_automate: str = Query(..., description="Nom de l'automate"),
                       include_bac_vide: bool = Query(False, description="Inclure les images detectees comme 'bac vide'")):
    """Derniere photo prise par le recycleur (table urls_images), avec sa qualite
    et sa prob 'bac vide' predites. Par defaut on exclut les images 'bac sans eau' ;
    avec include_bac_vide=true on renvoie la toute derniere photo quelle que soit la
    prediction (le front affiche alors un badge si bac_vide_prob est eleve, ce qui
    permet de verifier visuellement si le bac est reellement vide).
    JSON : {url, timestamp, qualite, bac_vide_prob}. url=None si aucune photo.
    Jointure : urls_images.numero_automate (INT) <-> nom_automate (ex '2022121.0')."""
    if include_bac_vide:
        filtre_bac_vide = "AND corr_bac_vide IS NOT TRUE"
    else:
        filtre_bac_vide = "AND COALESCE(corr_bac_vide, pred_bac_vide_prob >= %s) IS NOT TRUE"
    query = f"""
      SELECT id, url, timestamp,
             COALESCE(corr_qualite_eau, pred_qualite_eau) AS qualite,
             pred_bac_vide_prob, validated_by, validated_at,
             (corr_qualite_eau IS NOT NULL OR corr_opacite IS NOT NULL OR corr_matiere IS NOT NULL) AS corrigee,
             corr_bac_vide,
             COALESCE(corr_matiere, pred_matiere_prob * 10) AS matiere,
             COALESCE(corr_opacite, pred_opacite) AS opacite
      FROM urls_images
      WHERE numero_automate = CAST(SPLIT_PART(%s, '.', 1) AS INTEGER)
        AND url IS NOT NULL
        AND pred_qualite_eau IS NOT NULL
        {filtre_bac_vide}
      ORDER BY timestamp DESC
      LIMIT 1;
    """
    params = [nom_automate]
    if not include_bac_vide:
        params.append(_EAU_BAC_VIDE_SEUIL)
    rows = executer_requete_sql(query, tuple(params))
    if not rows:
        return {"url": None, "timestamp": None, "qualite": None, "bac_vide_prob": None}
    pid, url, ts, qualite, bac_vide_prob, validated_by, validated_at, corrigee, corr_bac_vide, matiere, opacite = rows[0]
    return {
        "id": pid,
        "url": url,
        "timestamp": ts.isoformat() if ts is not None else None,
        "qualite": float(qualite) if qualite is not None else None,
        "opacite": float(opacite) if opacite is not None else None,
        "matiere": float(matiere) if matiere is not None else None,
        "bac_vide_prob": float(bac_vide_prob) if bac_vide_prob is not None else None,
        "corr_bac_vide": corr_bac_vide,
        "validated_by": validated_by,
        "validated_at": validated_at.isoformat() if validated_at is not None else None,
        "corrigee": bool(corrigee),
    }


@app.get("/eau/photos")
def eau_photos(request: Request,
               nom_automate: str = Query(..., description="Nom de l'automate"),
               period: str = Query("mois", description="Periode (jour/semaine/mois/annee)"),
               limit: int = Query(60, ge=1, le=300)):
    """Liste des photos individuelles d'un automate pour validation/correction
    par le super_admin (qualite & opacite). Pour chaque photo : prediction modele,
    correction humaine eventuelle, valeur affichee (COALESCE) et statut de validation.
    Reserve au super_admin."""
    _require_super_admin(request)
    cfg = _EAU_PERIOD_MAP.get(period)
    interval = cfg[0] if cfg else "30 days"
    query = f"""
      SELECT id, url, timestamp,
             pred_qualite_eau, pred_opacite, pred_bac_vide_prob,
             corr_qualite_eau, corr_opacite, validated_by, validated_at,
             corr_bac_vide, pred_matiere_prob, corr_matiere
      FROM urls_images
      WHERE numero_automate = CAST(SPLIT_PART(%s, '.', 1) AS INTEGER)
        AND url IS NOT NULL
        AND timestamp >= now() - INTERVAL '{interval}'
      ORDER BY timestamp DESC
      LIMIT %s;
    """
    rows = executer_requete_sql(query, (nom_automate, limit))
    photos = []
    for (pid, url, ts, pq, po, bv, cq, co, vby, vat, cbv, pm, cm) in rows:
        # statut "bac vide" effectif = correction humaine sinon prediction modele
        if cbv is not None:
            eff_bac_vide = bool(cbv)
        else:
            eff_bac_vide = (bv is not None and bv >= _EAU_BAC_VIDE_SEUIL)
        # MES : prediction (prob 0..1) remise sur echelle /10 ; correction humaine prioritaire
        pred_matiere = float(pm) * 10 if pm is not None else None
        photos.append({
            "id": pid,
            "url": url,
            "timestamp": ts.isoformat() if ts is not None else None,
            "pred_qualite": float(pq) if pq is not None else None,
            "pred_opacite": float(po) if po is not None else None,
            "pred_matiere": pred_matiere,
            "bac_vide_prob": float(bv) if bv is not None else None,
            "corr_qualite": float(cq) if cq is not None else None,
            "corr_opacite": float(co) if co is not None else None,
            "corr_matiere": float(cm) if cm is not None else None,
            "corr_bac_vide": cbv,
            # valeur affichee = correction humaine sinon prediction modele
            "qualite": float(cq) if cq is not None else (float(pq) if pq is not None else None),
            "opacite": float(co) if co is not None else (float(po) if po is not None else None),
            "matiere": float(cm) if cm is not None else pred_matiere,
            "bac_vide": eff_bac_vide,
            "validated_by": vby,
            "validated_at": vat.isoformat() if vat is not None else None,
        })
    return {"photos": photos}


class PhotoValidationIn(BaseModel):
    # correction d'une valeur (None = on ne corrige pas cette valeur, on garde l'existant)
    qualite_eau: float | None = None
    opacite: float | None = None
    # correction MES (matiere en suspension), echelle /10
    matiere: float | None = None
    # correction du statut "bac vide" : True = reellement vide, False = contient de l'eau,
    # None = on ne touche pas a la decision existante.
    bac_vide: bool | None = None
    # reset=true -> efface la correction et la validation (retour 100% modele)
    reset: bool = False
    # apply_to_hour=true -> applique la meme correction a TOUTES les photos de la meme
    # heure (meme automate) que la photo de reference (photos toutes les 10 min, quasi
    # identiques). Sinon on ne touche que la photo ciblee.
    apply_to_hour: bool = False


@app.put("/eau/photo/{photo_id}/validation")
def eau_photo_validation(photo_id: int, payload: PhotoValidationIn, request: Request):
    """Valide / corrige la qualite, l'opacite, la MES et le statut 'bac vide' d'une photo
    (super_admin uniquement).
    - qualite_eau / opacite / matiere / bac_vide fournis -> ecrit la correction (corr_*).
    - non fournis -> validation simple : le modele etait bon, corr_* inchanges.
    - bac_vide=false -> le super_admin indique qu'il y a de l'eau (faux positif modele).
    - bac_vide=true  -> il confirme que le bac est reellement vide (exclu de la courbe).
    - reset=true -> efface corr_* et la validation (retour a la prediction modele).
    - apply_to_hour=true -> applique a toutes les photos de la meme heure/automate.
    Trace validated_by (email du super_admin) et validated_at (now())."""
    user = _require_super_admin(request)
    email = user.get("email") or ""
    # Cible : la photo seule, ou toutes les photos de la meme heure + meme automate.
    if payload.apply_to_hour:
        where = """numero_automate = (SELECT numero_automate FROM urls_images WHERE id = %s)
                   AND date_trunc('hour', timestamp) =
                       (SELECT date_trunc('hour', timestamp) FROM urls_images WHERE id = %s)"""
        where_params = [photo_id, photo_id]
    else:
        where = "id = %s"
        where_params = [photo_id]
    returning = """RETURNING id, pred_qualite_eau, pred_opacite, pred_bac_vide_prob, pred_matiere_prob,
                             corr_qualite_eau, corr_opacite, corr_matiere, corr_bac_vide,
                             validated_by, validated_at"""
    if payload.reset:
        query = f"""
          UPDATE urls_images
          SET corr_qualite_eau = NULL, corr_opacite = NULL, corr_matiere = NULL, corr_bac_vide = NULL,
              validated_by = NULL, validated_at = NULL
          WHERE {where}
          {returning};
        """
        rows = executer_requete_sql(query, tuple(where_params))
    else:
        # Un bac marque "reellement vide" n'a pas de qualite/opacite/MES a mesurer :
        # on efface alors les eventuelles corrections associees.
        clear_quality = payload.bac_vide is True
        query = f"""
          UPDATE urls_images
          SET corr_qualite_eau = CASE WHEN %s THEN NULL WHEN %s THEN %s ELSE corr_qualite_eau END,
              corr_opacite     = CASE WHEN %s THEN NULL WHEN %s THEN %s ELSE corr_opacite END,
              corr_matiere     = CASE WHEN %s THEN NULL WHEN %s THEN %s ELSE corr_matiere END,
              corr_bac_vide    = CASE WHEN %s THEN %s ELSE corr_bac_vide END,
              validated_by = %s,
              validated_at = now()
          WHERE {where}
          {returning};
        """
        rows = executer_requete_sql(query, tuple([
            clear_quality, payload.qualite_eau is not None, payload.qualite_eau,
            clear_quality, payload.opacite is not None, payload.opacite,
            clear_quality, payload.matiere is not None, payload.matiere,
            payload.bac_vide is not None, payload.bac_vide,
            email,
        ] + where_params))
    if not rows:
        raise HTTPException(status_code=404, detail="Photo introuvable")
    # ligne de reference = la photo ciblee (meme corr partout, mais pred propre a chaque photo)
    target = next((r for r in rows if r[0] == photo_id), rows[0])
    pid, pq, po, bv, pm, cq, co, cm, cbv, vby, vat = target
    eff_bac_vide = bool(cbv) if cbv is not None else (bv is not None and bv >= _EAU_BAC_VIDE_SEUIL)
    pred_matiere = float(pm) * 10 if pm is not None else None
    return {
        "status": "success",
        "id": pid,
        "affected": len(rows),
        "qualite": float(cq) if cq is not None else (float(pq) if pq is not None else None),
        "opacite": float(co) if co is not None else (float(po) if po is not None else None),
        "matiere": float(cm) if cm is not None else pred_matiere,
        "corr_qualite": float(cq) if cq is not None else None,
        "corr_opacite": float(co) if co is not None else None,
        "corr_matiere": float(cm) if cm is not None else None,
        "corr_bac_vide": cbv,
        "bac_vide": eff_bac_vide,
        "validated_by": vby,
        "validated_at": vat.isoformat() if vat is not None else None,
    }


# ---------------------------------------------------------------------------
# Assistant SAV (Wardian) - proxy backend SSE
# ---------------------------------------------------------------------------
# La cle Wardian (wsk_...) est un SECRET serveur : elle ne doit JAMAIS etre
# exposee au navigateur. Le front appelle /sav/message avec son token Eaukey ;
# on injecte ici l'Authorization Wardian et on re-streame le flux SSE tel quel.
# external_user_id est derive cote serveur => isolation par utilisateur garantie
# et non spoofable par le client (cf. spec Wardian sections 4 et 6).

_WARDIAN_API_BASE = os.getenv("WARDIAN_API_BASE", "https://app.wardian-ai.com").rstrip("/")
_WARDIAN_AGENT_ID = os.getenv("WARDIAN_AGENT_ID", "")
_WARDIAN_API_KEY = os.getenv("WARDIAN_API_KEY", "")


class SavMessageIn(BaseModel):
    message: str
    conversation_id: Optional[str] = None


@app.post("/sav/message")
async def sav_message(payload: SavMessageIn, request: Request):
    """Proxy SSE vers l'agent SAV Wardian (read-only).

    - Auth : utilisateur Eaukey connecte (_require_auth).
    - external_user_id derive cote serveur (eaukey-user-<id>) : conversations
      isolees par utilisateur, impossible a usurper depuis le front.
    - Re-streame le Content-Type text/event-stream de Wardian sans le modifier.
    """
    user = _require_auth(request)

    if not _WARDIAN_API_KEY or not _WARDIAN_AGENT_ID:
        raise HTTPException(status_code=503, detail="Assistant SAV non configure")

    message = (payload.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message vide")

    body = {
        "message": _sav_message_avec_contexte(message),
        "external_user_id": f"eaukey-user-{user['id']}",
        "conversation_id": payload.conversation_id or None,
    }
    url = f"{_WARDIAN_API_BASE}/api/v1/agents/{_WARDIAN_AGENT_ID}/messages"
    headers = {
        "Authorization": f"Bearer {_WARDIAN_API_KEY}",
        "Content-Type": "application/json",
    }

    import httpx
    from fastapi.responses import StreamingResponse

    client = httpx.AsyncClient(timeout=httpx.Timeout(180.0, connect=10.0))
    try:
        req = client.build_request("POST", url, headers=headers, json=body)
        upstream = await client.send(req, stream=True)
    except httpx.HTTPError:
        await client.aclose()
        raise HTTPException(status_code=502, detail="Assistant SAV injoignable")

    # Non-200 => pas de flux, corps JSON {"detail": "..."}. On lit et on propage.
    if upstream.status_code != 200:
        raw = await upstream.aread()
        await upstream.aclose()
        await client.aclose()
        detail = "Assistant SAV indisponible"
        try:
            detail = json.loads(raw.decode("utf-8", "replace")).get("detail", detail)
        except Exception:
            pass
        # 409 (conversation a redemarrer) et 429 (quota) sont utiles au front ;
        # 401/403/404 = mauvaise config cote serveur => on masque en 502.
        status = upstream.status_code if upstream.status_code in (409, 429) else 502
        raise HTTPException(status_code=status, detail=detail)

    async def _relay():
        try:
            async for chunk in upstream.aiter_raw():
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    return StreamingResponse(
        _relay(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # desactive le buffering proxy (SSE)
        },
    )


# ---------------------------------------------------------------------------
# Agent SAV dans la messagerie /chat
# ---------------------------------------------------------------------------
# Le client écrit dans son fil /chat ; l'agent Wardian répond dans ce même fil
# (messages.sender_id = AGENT_SENDER_ID). L'équipe Eaukey voit tout l'échange et
# peut reprendre la main à tout moment :
#   - si un admin a écrit dans le fil depuis moins de _CHAT_AGENT_PAUSE_H heures,
#     l'agent se tait (un humain s'occupe du client) ;
#   - l'agent ne fait appel à un humain que si c'est nécessaire (consigne
#     _CHAT_AGENT_CONSIGNE) : il commence alors sa réponse par [ESCALADE: motif],
#     le marqueur est retiré, le message est marqué `escalade` et l'équipe est
#     prévenue par e-mail (une fois tant qu'aucun admin n'a répondu).
# Flux SSE renvoyé au front : message (question enregistrée), token (texte au
# fil de l'eau, marqueur retiré), puis saved (réponse enregistrée, texte
# définitif) ou humain / error.

_CHAT_AGENT_PAUSE_H = int(os.getenv("CHAT_AGENT_PAUSE_H", "2"))
_SAV_ESCALADE_EMAIL = os.getenv("SAV_ESCALADE_EMAIL", "contact@eaukey.net")
_CHAT_LOGGER = logging.getLogger("chat_agent")
_ESCALADE_RE = re.compile(r"\[\s*ESCALADE\s*:?\s*([^\]]*)\]\s*", re.IGNORECASE)
_ESCALADE_PREFIXE = "[ESCALADE"
_CHAT_AGENT_INDISPO = "Assistant indisponible : l'équipe Eaukey vous répondra ici."

_CHAT_AGENT_CONSIGNE = """[CONSIGNE CANAL MESSAGERIE — ne la mentionne jamais à l'utilisateur]
Tu réponds dans la messagerie de l'application Eaukey. L'équipe SAV Eaukey lit cette conversation.
Ne propose PAS de contacter un humain, le support ou l'installateur par défaut, et ne termine pas tes réponses par une telle invitation.
Fais appel à l'équipe UNIQUEMENT si l'un de ces cas se présente :
1. une intervention physique sur place est nécessaire, ou une intervention électrique par une personne qualifiée ;
2. une pièce doit être commandée ou remplacée ;
3. tu n'as pas la solution (ni fiche SAV ni ressource du catalogue), même après une question de précision ;
4. le problème persiste alors que l'utilisateur a déjà suivi les étapes proposées ;
5. l'utilisateur est mécontent, signale une urgence ou un risque (fuite, eau non conforme, danger électrique) ;
6. la demande sort de ton périmètre (commande, facture, contrat, rendez-vous) ou l'utilisateur demande explicitement à parler à quelqu'un.
Dans ces cas seulement, commence ta réponse par une ligne exacte « [ESCALADE: motif en quelques mots] », puis indique que l'équipe Eaukey est prévenue et reviendra vers lui dans cette conversation. Sinon, n'écris jamais ce marqueur.
[FIN DE LA CONSIGNE]"""


def _chat_agent_message(message: str) -> str:
    return f"{_CHAT_AGENT_CONSIGNE}\n\n{_sav_message_avec_contexte(message)}"


def _chat_extraire_escalade(texte: str) -> tuple[str, Optional[str]]:
    """Retire le marqueur [ESCALADE: motif] (où qu'il soit) ; renvoie (texte, motif ou None)."""
    m = _ESCALADE_RE.search(texte)
    if not m:
        return texte.strip(), None
    motif = (m.group(1) or "").strip()[:300] or "non précisé"
    return _ESCALADE_RE.sub("", texte).strip(), motif


def _chat_debut_marqueur_possible(texte: str) -> bool:
    """Vrai tant que le début de la réponse peut encore être un marqueur [ESCALADE: ...] incomplet."""
    debut = texte.lstrip()
    if len(debut) >= 400:
        return False
    if len(debut) < len(_ESCALADE_PREFIXE):
        return _ESCALADE_PREFIXE.startswith(debut.upper())
    return debut.upper().startswith(_ESCALADE_PREFIXE) and "]" not in debut


def _chat_humain_actif(client_id: str) -> bool:
    row = executer_requete_sql_one(
        """
        SELECT 1 FROM messages
        WHERE client_id = %s AND sender_id = ANY(%s)
          AND created_at::timestamptz > now() - make_interval(hours => %s)
        LIMIT 1;
        """,
        (client_id, list(ADMIN_SENDER_IDS), _CHAT_AGENT_PAUSE_H),
    )
    return row is not None


def _chat_escalade_en_attente(client_id: str) -> bool:
    """Une escalade déjà signalée, sans réponse d'un admin ni « traité » depuis ?"""
    row = executer_requete_sql_one(
        """
        SELECT 1 FROM messages e
        WHERE e.client_id = %s AND e.escalade
          AND NOT EXISTS (
            SELECT 1 FROM messages a
            WHERE a.client_id = e.client_id AND a.sender_id = ANY(%s)
              AND a.created_at::timestamptz > e.created_at::timestamptz
          )
          AND NOT EXISTS (
            SELECT 1 FROM chat_fils f
            WHERE f.client_id = e.client_id AND f.traite_at >= e.created_at::timestamptz
          )
        LIMIT 1;
        """,
        (client_id, list(ADMIN_SENDER_IDS)),
    )
    return row is not None


def _chat_conv_get(client_id: str) -> Optional[str]:
    row = executer_requete_sql_one(
        "SELECT wardian_conversation_id FROM chat_agent_conv WHERE client_id = %s;", (client_id,)
    )
    return row[0] if row else None


def _chat_conv_set(client_id: str, conv_id: Optional[str]) -> None:
    executer_requete_sql_one(
        """
        INSERT INTO chat_agent_conv (client_id, wardian_conversation_id, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (client_id) DO UPDATE
          SET wardian_conversation_id = EXCLUDED.wardian_conversation_id, updated_at = now()
        RETURNING client_id;
        """,
        (client_id, conv_id),
    )


def _chat_inserer_message(client_id: str, sender_id: int, content: str,
                          escalade: bool = False, motif: Optional[str] = None):
    return executer_requete_sql_one(
        """
        INSERT INTO messages (client_id, sender_id, content, is_read, escalade, escalade_motif)
        VALUES (%s, %s, %s, false, %s, %s)
        RETURNING id, created_at;
        """,
        (client_id, sender_id, content, escalade, motif),
    )


async def _chat_prevenir_equipe(client_id: str, motif: str, question: str, reponse: str) -> None:
    """E-mail à l'équipe SAV. Ne lève jamais : l'alerte dans l'appli reste visible."""
    if not _SMTP_HOST or not _SMTP_USER or not _SMTP_PASSWORD or not _SAV_ESCALADE_EMAIL:
        _CHAT_LOGGER.warning("SMTP non configure : escalade %s non envoyee par e-mail", client_id)
        return
    lien = f"{_FRONTEND_URL}/chat/{client_id}"
    texte = (
        f"L'assistant SAV demande l'intervention de l'équipe pour le client {client_id}.\n\n"
        f"Motif : {motif}\n\n"
        f"Dernier message du client :\n{question}\n\n"
        f"Réponse de l'assistant :\n{reponse}\n\n"
        f"Ouvrir la conversation : {lien}\n"
    )
    msg = MIMEText(texte, "plain", "utf-8")
    msg["Subject"] = f"[SAV Eaukey] Intervention demandée — client {client_id}"
    msg["From"] = _SMTP_FROM
    msg["To"] = _SAV_ESCALADE_EMAIL
    try:
        await aiosmtplib.send(
            msg, hostname=_SMTP_HOST, port=_SMTP_PORT,
            username=_SMTP_USER, password=_SMTP_PASSWORD, start_tls=True,
        )
    except Exception as e:
        _CHAT_LOGGER.error("Echec e-mail d'escalade (client %s) : %s", client_id, e)


def _sse(event: str, data: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


class ChatAgentIn(BaseModel):
    content: str = Field(..., min_length=1, max_length=4000)


@app.post("/chat/{client_id}/agent")
async def chat_agent(client_id: str, payload: ChatAgentIn, request: Request):
    """Message d'un client dans son fil /chat + réponse de l'agent SAV (flux SSE).

    L'accès au fil est contrôlé par _security_gate (paramètre client_id).
    """
    user = _require_auth(request)
    if _is_admin_user(user):
        raise HTTPException(status_code=403, detail="Réservé aux clients : l'équipe répond via POST /messages")

    content = payload.content.strip()
    if not content:
        raise HTTPException(status_code=400, detail="Message vide")

    row = _chat_inserer_message(client_id, CLIENT_SENDER_IDS[0], content)
    if not row:
        raise HTTPException(status_code=500, detail="Failed to insert message")

    import httpx
    from fastapi.responses import StreamingResponse

    async def _flux():
        yield _sse("message", {"id": row[0], "created_at": _iso(row[1])})

        # Un humain de l'équipe suit le fil : l'agent se tait.
        if _chat_humain_actif(client_id):
            yield _sse("humain", {"detail": "Un membre de l'équipe Eaukey suit votre conversation."})
            return
        if not _WARDIAN_API_KEY or not _WARDIAN_AGENT_ID:
            yield _sse("error", {"message": _CHAT_AGENT_INDISPO})
            return

        url = f"{_WARDIAN_API_BASE}/api/v1/agents/{_WARDIAN_AGENT_ID}/messages"
        headers = {"Authorization": f"Bearer {_WARDIAN_API_KEY}", "Content-Type": "application/json"}
        message = _chat_agent_message(content)
        conv_id = _chat_conv_get(client_id)
        texte = ""        # réponse complète de l'agent
        envoye = 0        # caractères de `texte` déjà transmis au front
        retenue = True    # on retient le début tant qu'il peut s'agir du marqueur

        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0, connect=10.0)) as client:
            for essai in range(2):
                body = {
                    "message": message,
                    # Un seul fil par client (partagé par les utilisateurs de ce client).
                    "external_user_id": f"eaukey-chat-{client_id}",
                    "conversation_id": conv_id,
                }
                try:
                    async with client.stream("POST", url, headers=headers, json=body) as up:
                        if up.status_code == 409 and essai == 0:
                            # Conversation Wardian périmée : on repart sur un fil neuf.
                            conv_id = None
                            _chat_conv_set(client_id, None)
                            continue
                        if up.status_code != 200:
                            _CHAT_LOGGER.warning("Wardian HTTP %s (client %s)", up.status_code, client_id)
                            yield _sse("error", {"message": _CHAT_AGENT_INDISPO})
                            return
                        buf = ""
                        async for chunk in up.aiter_text():
                            buf += chunk
                            *frames, buf = buf.split("\n\n")
                            for frame in frames:
                                ev = data = None
                                for line in frame.split("\n"):
                                    if line.startswith("event:"):
                                        ev = line[6:].strip()
                                    elif line.startswith("data:"):
                                        try:
                                            data = json.loads(line[5:].strip())
                                        except ValueError:
                                            data = None
                                if not ev or not isinstance(data, dict):
                                    continue  # ": keepalive" et commentaires
                                if ev in ("conversation", "done") and data.get("conversation_id"):
                                    if data["conversation_id"] != conv_id:
                                        conv_id = data["conversation_id"]
                                        _chat_conv_set(client_id, conv_id)
                                elif ev == "token":
                                    texte += data.get("delta") or ""
                                    if retenue:
                                        if _chat_debut_marqueur_possible(texte):
                                            continue
                                        retenue = False
                                        visible, _ = _chat_extraire_escalade(texte)
                                        if visible:
                                            yield _sse("token", {"delta": visible})
                                    elif len(texte) > envoye:
                                        yield _sse("token", {"delta": texte[envoye:]})
                                    envoye = len(texte)
                                elif ev == "error":
                                    _CHAT_LOGGER.warning("Wardian error (client %s) : %s", client_id, data)
                                    yield _sse("error", {"message": _CHAT_AGENT_INDISPO})
                                    return
                    break
                except httpx.HTTPError as e:
                    _CHAT_LOGGER.warning("Wardian injoignable (client %s) : %s", client_id, e)
                    yield _sse("error", {"message": _CHAT_AGENT_INDISPO})
                    return

        reponse, motif = _chat_extraire_escalade(texte)
        if not reponse:
            yield _sse("error", {"message": _CHAT_AGENT_INDISPO})
            return
        escalade = motif is not None
        deja_signale = escalade and _chat_escalade_en_attente(client_id)
        saved = _chat_inserer_message(client_id, AGENT_SENDER_ID, reponse, escalade, motif)
        yield _sse("saved", {
            "id": saved[0] if saved else None,
            "created_at": _iso(saved[1]) if saved else None,
            "content": reponse,
            "escalade": escalade,
        })
        if escalade and not deja_signale:
            await _chat_prevenir_equipe(client_id, motif, content, reponse)

    return StreamingResponse(
        _flux(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@app.get("/chat/escalades")
def chat_escalades(request: Request):
    """Fils où l'agent a demandé l'équipe, sans réponse d'un admin ni « traité » depuis (admin)."""
    _require_admin(request)
    rows = executer_requete_sql(
        """
        SELECT e.client_id, max(e.created_at::timestamptz)
        FROM messages e
        WHERE e.escalade
          AND NOT EXISTS (
            SELECT 1 FROM messages a
            WHERE a.client_id = e.client_id AND a.sender_id = ANY(%s)
              AND a.created_at::timestamptz > e.created_at::timestamptz
          )
          AND NOT EXISTS (
            SELECT 1 FROM chat_fils f
            WHERE f.client_id = e.client_id AND f.traite_at >= e.created_at::timestamptz
          )
        GROUP BY e.client_id;
        """,
        (list(ADMIN_SENDER_IDS),),
    )
    return {str(r[0]): _iso(r[1]) for r in rows if r and r[0] is not None}


# ---------------------------------------------------------------------------
# Boîte de réception de l'équipe (M3)
# ---------------------------------------------------------------------------
# Un fil par client (table messages). Statut d'un fil, du plus urgent au moins urgent :
#   equipe_demandee : l'assistant a demandé l'équipe (escalade en attente) ;
#   non_lu          : message(s) du client non lus par l'équipe ;
#   assistant       : le dernier message du client a reçu une réponse de l'assistant ;
#   a_jour          : rien à faire (traité, ou dernier message de l'équipe).
# « Marquer traité » (chat_fils.traite_at) lève l'escalade et marque lus les messages
# du client ; un nouveau message du client rouvre le fil de lui-même.

_CHAT_STATUTS_A_TRAITER = ("equipe_demandee", "non_lu")


def _chat_boite_fils() -> list[dict]:
    rows = executer_requete_sql(
        """
        WITH dernier AS (
          SELECT DISTINCT ON (client_id) client_id, sender_id, content, created_at::timestamptz AS at
          FROM messages
          WHERE client_id IS NOT NULL
          ORDER BY client_id, created_at::timestamptz DESC, id DESC
        ),
        stats AS (
          SELECT client_id,
                 count(*) FILTER (WHERE NOT coalesce(is_read, false) AND sender_id = ANY(%(clients)s)) AS non_lus,
                 max(created_at::timestamptz) FILTER (WHERE sender_id = ANY(%(clients)s)) AS dernier_client,
                 count(*) AS total
          FROM messages
          WHERE client_id IS NOT NULL
          GROUP BY client_id
        ),
        esc AS (
          SELECT e.client_id, max(e.created_at::timestamptz) AS at,
                 (array_agg(e.escalade_motif ORDER BY e.created_at::timestamptz DESC))[1] AS motif
          FROM messages e
          WHERE e.escalade
            AND NOT EXISTS (
              SELECT 1 FROM messages a
              WHERE a.client_id = e.client_id AND a.sender_id = ANY(%(admins)s)
                AND a.created_at::timestamptz > e.created_at::timestamptz
            )
            AND NOT EXISTS (
              SELECT 1 FROM chat_fils f
              WHERE f.client_id = e.client_id AND f.traite_at >= e.created_at::timestamptz
            )
          GROUP BY e.client_id
        )
        SELECT d.client_id, d.sender_id, d.content, d.at, s.non_lus, s.dernier_client, s.total,
               esc.at, esc.motif, f.traite_at, f.traite_par
        FROM dernier d
        JOIN stats s ON s.client_id = d.client_id
        LEFT JOIN esc ON esc.client_id = d.client_id
        LEFT JOIN chat_fils f ON f.client_id = d.client_id
        ORDER BY d.at DESC;
        """,
        {"clients": list(CLIENT_SENDER_IDS), "admins": list(ADMIN_SENDER_IDS)},
    )
    fils = []
    for (client_id, sender_id, content, at, non_lus, dernier_client, total,
         esc_at, motif, traite_at, traite_par) in rows:
        traite = traite_at is not None and (dernier_client is None or traite_at >= dernier_client)
        if esc_at is not None:
            statut = "equipe_demandee"
        elif non_lus and not traite:
            statut = "non_lu"
        elif sender_id == AGENT_SENDER_ID and not traite:
            statut = "assistant"
        else:
            statut = "a_jour"
        if sender_id == AGENT_SENDER_ID:
            auteur = "assistant"
        elif sender_id in ADMIN_SENDER_IDS:
            auteur = "equipe"
        else:
            auteur = "client"
        apercu = " ".join((content or "").split())
        fils.append({
            "client_id": str(client_id),
            "statut": statut,
            "dernier_message": {
                "auteur": auteur,
                "apercu": apercu[:140] + ("…" if len(apercu) > 140 else ""),
                "created_at": _iso(at),
            },
            "non_lus": int(non_lus or 0),
            "messages": int(total or 0),
            "escalade_motif": motif if esc_at is not None else None,
            "traite_at": _iso(traite_at),
            "traite_par": traite_par,
        })
    return fils


@app.get("/chat/boite")
def chat_boite(request: Request):
    """Fils de la messagerie avec leur statut, les plus urgents d'abord (admin)."""
    _require_admin(request)
    ordre = {"equipe_demandee": 0, "non_lu": 1, "assistant": 2, "a_jour": 3}
    fils = _chat_boite_fils()
    fils.sort(key=lambda f: ordre[f["statut"]])  # tri stable : le plus récent d'abord dans chaque statut
    return fils


@app.get("/chat/boite/compteur")
def chat_boite_compteur(request: Request):
    """Nombre de fils à traiter (équipe demandée ou non lus) pour le badge du menu (admin)."""
    _require_admin(request)
    fils = _chat_boite_fils()
    return {
        "a_traiter": sum(1 for f in fils if f["statut"] in _CHAT_STATUTS_A_TRAITER),
        "equipe_demandee": sum(1 for f in fils if f["statut"] == "equipe_demandee"),
    }


@app.post("/chat/{client_id}/traite")
def chat_marquer_traite(client_id: str, request: Request):
    """Marque le fil comme traité : lève l'escalade et marque lus les messages du client (admin)."""
    email = _require_admin(request)
    row = executer_requete_sql_one(
        """
        INSERT INTO chat_fils (client_id, traite_at, traite_par)
        VALUES (%s, now(), %s)
        ON CONFLICT (client_id) DO UPDATE SET traite_at = now(), traite_par = EXCLUDED.traite_par
        RETURNING traite_at;
        """,
        (client_id, email),
    )
    executer_requete_sql(
        "UPDATE messages SET is_read = true WHERE client_id = %s AND is_read = false AND sender_id = ANY(%s)",
        (client_id, list(CLIENT_SENDER_IDS)),
    )
    return {"client_id": client_id, "traite_at": _iso(row[0]) if row else None, "traite_par": email}


@app.delete("/chat/{client_id}/traite")
def chat_rouvrir(client_id: str, request: Request):
    """Annule « traité » : le fil retrouve son statut (admin)."""
    _require_admin(request)
    executer_requete_sql_one(
        "UPDATE chat_fils SET traite_at = NULL, traite_par = NULL WHERE client_id = %s RETURNING client_id;",
        (client_id,),
    )
    return {"client_id": client_id, "traite_at": None}


# ---------------------------------------------------------------------------
# Base SAV : fiches de dépannage (admin) + contexte joint aux questions SAV
# ---------------------------------------------------------------------------
# Les fiches sont écrites par l'équipe SAV (comptes admin) dans la plateforme,
# stockées dans la base Eaukey (table sav_fiches, migrations/008_sav_fiches.sql).
# A chaque question posée à l'assistant, on joint les fiches et les pannes
# résolues (partage_sav) les plus proches, sans nom de client/lieu/station.
# Aucune copie n'est stockée chez Wardian en dehors de la conversation.

_SAV_FICHES_COLS = (
    "id, titre, symptomes, solution, pieces, type_panne, mots_cles, archivee, "
    "created_by, updated_by, created_at, updated_at"
)
_SAV_CONTEXTE_MAX_CHARS = 6000
_SAV_NOMS_TTL = 600
_sav_noms_cache = {"t": 0.0, "re": None}


class SavFicheIn(BaseModel):
    titre: str = Field(..., min_length=1, max_length=200)
    symptomes: Optional[str] = Field(None, max_length=4000)
    solution: str = Field(..., min_length=1, max_length=8000)
    pieces: Optional[str] = Field(None, max_length=1000)
    type_panne: Optional[str] = Field(None, max_length=200)
    mots_cles: Optional[str] = Field(None, max_length=500)
    archivee: bool = False


def _sav_fiche_to_dict(r) -> dict:
    def _fmt(dt_val):
        return dt_val.isoformat() if hasattr(dt_val, "isoformat") else dt_val

    return {
        "id": r[0],
        "titre": r[1],
        "symptomes": r[2],
        "solution": r[3],
        "pieces": r[4],
        "type_panne": r[5],
        "mots_cles": r[6],
        "archivee": r[7],
        "created_by": r[8],
        "updated_by": r[9],
        "created_at": _fmt(r[10]),
        "updated_at": _fmt(r[11]),
    }


def _sav_fiche_params(f: SavFicheIn) -> tuple:
    titre = f.titre.strip()
    solution = f.solution.strip()
    if not titre or not solution:
        raise HTTPException(status_code=400, detail="Titre et solution obligatoires")
    return (
        titre,
        _clean_opt(f.symptomes),
        solution,
        _clean_opt(f.pieces),
        _clean_opt(f.type_panne),
        _clean_opt(f.mots_cles),
        f.archivee,
    )


@app.get("/sav/fiches")
def lister_sav_fiches(request: Request):
    _require_admin(request)
    rows = executer_requete_sql(
        f"SELECT {_SAV_FICHES_COLS} FROM sav_fiches ORDER BY archivee, updated_at DESC, id DESC;"
    )
    return [_sav_fiche_to_dict(r) for r in rows]


@app.post("/sav/fiches")
def creer_sav_fiche(f: SavFicheIn, request: Request):
    email = _require_admin(request) or ""
    row = executer_requete_sql_one(
        f"""
        INSERT INTO sav_fiches (titre, symptomes, solution, pieces, type_panne, mots_cles, archivee, created_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING {_SAV_FICHES_COLS};
        """,
        _sav_fiche_params(f) + (email,),
    )
    if row is None:
        raise HTTPException(status_code=500, detail="Création de la fiche échouée")
    return _sav_fiche_to_dict(row)


@app.put("/sav/fiches/{fiche_id}")
def modifier_sav_fiche(fiche_id: int, f: SavFicheIn, request: Request):
    email = _require_admin(request) or ""
    row = executer_requete_sql_one(
        f"""
        UPDATE sav_fiches
           SET titre = %s, symptomes = %s, solution = %s, pieces = %s, type_panne = %s,
               mots_cles = %s, archivee = %s, updated_by = %s, updated_at = now()
         WHERE id = %s
        RETURNING {_SAV_FICHES_COLS};
        """,
        _sav_fiche_params(f) + (email, fiche_id),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Fiche introuvable")
    return _sav_fiche_to_dict(row)


@app.delete("/sav/fiches/{fiche_id}")
def supprimer_sav_fiche(fiche_id: int, request: Request):
    _require_admin(request)
    row = executer_requete_sql_one("DELETE FROM sav_fiches WHERE id = %s RETURNING id;", (fiche_id,))
    if row is None:
        raise HTTPException(status_code=404, detail="Fiche introuvable")
    return {"status": "deleted", "id": fiche_id}


def _sav_noms_regex():
    """Regex des noms de clients / lieux / automates à masquer (cache 10 min)."""
    now = time.time()
    if _sav_noms_cache["re"] is not None and now - _sav_noms_cache["t"] < _SAV_NOMS_TTL:
        return _sav_noms_cache["re"]
    rows = executer_requete_sql("SELECT DISTINCT client, lieu, nom_automate FROM automate;")
    noms = set()
    for r in rows:
        for v in r:
            v = str(v or "").strip()
            if len(v) >= 4:
                noms.add(v)
    pattern = None
    if noms:
        alts = "|".join(re.escape(n) for n in sorted(noms, key=len, reverse=True))
        pattern = re.compile(rf"(?<!\w)(?:{alts})(?!\w)", re.IGNORECASE)
    _sav_noms_cache.update(t=now, re=pattern)
    return pattern


def _sav_masquer(texte: Optional[str], pattern) -> str:
    texte = (texte or "").strip()
    return pattern.sub("[station]", texte) if pattern and texte else texte


# Requête plein texte "souple" : n'importe quel mot significatif de la question.
_SAV_TSQUERY = "(SELECT replace(plainto_tsquery('french', %s)::text, ' & ', ' | ')::tsquery AS q) tsq"


def _sav_contexte(message: str) -> str:
    """Bloc de contexte (fiches SAV + pannes résolues) pour une question, ou ''."""
    fiches = executer_requete_sql(
        f"""
        SELECT titre, type_panne, symptomes, solution, pieces
          FROM sav_fiches, {_SAV_TSQUERY}
         WHERE NOT archivee AND recherche @@ q
         ORDER BY ts_rank(recherche, q) DESC, updated_at DESC
         LIMIT 4;
        """,
        (message,),
    )
    cas = executer_requete_sql(
        f"""
        SELECT panne, probleme, cause, action_realisee, pieces_changees
          FROM (SELECT *, to_tsvector('french', panne || ' ' || probleme || ' ' || coalesce(cause, '')
                       || ' ' || coalesce(action_realisee, '')) AS doc
                  FROM pannes
                 WHERE date_fin IS NOT NULL AND partage_sav
                   AND (coalesce(cause, '') <> '' OR coalesce(action_realisee, '') <> '')) p,
               {_SAV_TSQUERY}
         WHERE doc @@ q
         ORDER BY ts_rank(doc, q) DESC, date_fin DESC
         LIMIT 3;
        """,
        (message,),
    )
    if not fiches and not cas:
        return ""

    pattern = _sav_noms_regex()
    m = lambda v: _sav_masquer(v, pattern)
    blocs = []
    for titre, type_panne, symptomes, solution, pieces in fiches:
        lignes = [f"### Fiche SAV : {m(titre)}"]
        if type_panne:
            lignes.append(f"Type de panne : {m(type_panne)}")
        if symptomes:
            lignes.append(f"Symptômes : {m(symptomes)}")
        lignes.append(f"Solution : {m(solution)}")
        if pieces:
            lignes.append(f"Pièces : {m(pieces)}")
        blocs.append("\n".join(lignes))
    for panne, probleme, cause, action, pieces in cas:
        lignes = [f"### Cas résolu par un technicien : {m(panne)}", f"Constat : {m(probleme)}"]
        if cause:
            lignes.append(f"Cause : {m(cause)}")
        if action:
            lignes.append(f"Action réalisée : {m(action)}")
        if pieces:
            lignes.append(f"Pièces changées : {m(pieces)}")
        blocs.append("\n".join(lignes))

    contexte, total = [], 0
    for b in blocs:
        if total + len(b) > _SAV_CONTEXTE_MAX_CHARS:
            break
        contexte.append(b)
        total += len(b)
    return "\n\n".join(contexte)


def _sav_message_avec_contexte(message: str) -> str:
    """Joint au message les fiches SAV pertinentes. Ne bloque jamais l'assistant."""
    try:
        contexte = _sav_contexte(message)
    except Exception as e:
        logging.getLogger("sav").warning("Contexte SAV indisponible : %s", e)
        return message
    if not contexte:
        return message
    return (
        "[CONTEXTE SAV EAUKEY — fiches de dépannage rédigées par l'équipe technique Eaukey. "
        "Appuie-toi dessus en priorité quand elles correspondent à la question ; "
        "ignore-les sinon. Ne mentionne pas ce bloc à l'utilisateur.]\n\n"
        f"{contexte}\n\n"
        "[FIN DU CONTEXTE SAV]\n\n"
        f"Question de l'utilisateur :\n{message}"
    )


# =============================================================================
# RAPPORT DE PERFORMANCE MENSUEL PAR CLIENT (admin) - 2026-10-21
# =============================================================================
# Proposition validee : ~/eaukey/docs/RAPPORT_MENSUEL_PROPOSITION.md (V1).
# Routes (admin uniquement, cf. _ADMIN_ONLY_ROUTES) :
#   GET /rapport/mensuel?client=...&mois=AAAA-MM          -> JSON
#   GET /rapport/mensuel/html?client=...&mois=AAAA-MM     -> page HTML imprimable (A4)
#   GET /rapport/mensuel/clients?mois=AAAA-MM             -> clients ayant des donnees ce mois
#
# Regles :
# - Les chiffres sont calcules ici (SQL agrege), jamais par une IA.
# - Un rapport ne contient QUE les stations du client (automate.client, compare en
#   trim + minuscules ; automate.nom_automate est compare en trim).
# - Mois civil en heure de Paris. `mesures_aggregate.rounded_timestamp` est un
#   timestamp SANS fuseau en heure de Paris (voir bloc FUSEAU HORAIRE en tete de
#   fichier) : les bornes du mois sont donc des timestamps naifs 'AAAA-MM-01 00:00'.
#
# SOURCE : `mesures_aggregate` (1 ligne par station et par heure, depuis le
# 23/06/2026). ATTENTION, d'apres ETL_aggregate_horaire.py, les colonnes
# compteur_eau_renvoi / compteur_eau_adoucie / compteur_electrique y contiennent
# l'INCREMENT de l'heure (max - min des valeurs non nulles du compteur cumule
# dans l'heure, arrondi a l'entier, negatif ramene a 0), pas le compteur cumule.
# Le volume du mois est donc la SOMME des increments horaires. Consequences :
# - remise a zero d'un compteur : elle apparait comme une heure avec un increment
#   enorme (ex. 5000 -> 3 dans l'heure = 4997). Toute heure au-dela de
#   _RAPPORT_MAX_M3_HEURE (ou _RAPPORT_MAX_KWH_HEURE) est ecartee et comptee dans
#   heures_ecartees ; la station est alors signalee « donnees incompletes » ;
# - trou de donnees (station muette) : aucune ligne, donc aucun volume compte
#   pendant le trou. La couverture (heures avec donnees / heures du mois) le
#   signale, rien n'est extrapole.
# - pH : avg_ph = mediane horaire de mesures.ph, qui est en pH x 100.
# - Chlore : avg_chlore = mediane horaire de mesures.chlore_mv (0..2000 dans
#   l'appli). HYPOTHESE A CONFIRMER : valeur brute / 1000 = mg/L
#   (_RAPPORT_CHLORE_ECHELLE). 0 = sonde absente / pas de mesure (l'ETL remplace
#   les manques par 0) : ces heures sont exclues des moyennes de chlore.
# - Passage heure d'ete / d'hiver : en heure locale naive, un mois compte
#   toujours jours x 24 creneaux (en mars, le creneau 02:00 n'existe pas : la
#   couverture plafonne a 99,9 %).
# =============================================================================
import calendar as _rapport_calendar
import html as _rapport_html
from zoneinfo import ZoneInfo as _RapportZoneInfo

from fastapi.responses import HTMLResponse

# --- Valeurs par defaut (decisions Loane du 2026-10-21, faciles a changer) ---
# Routes du rapport réservées aux admins (déclarées ici, à côté du code, plutôt que dans
# la liste centrale : évite les conflits avec les autres PR qui y ajoutent leurs routes).
_ADMIN_ONLY_ROUTES.update({
    ("GET", "/rapport/mensuel"),
    ("GET", "/rapport/mensuel/html"),
    ("GET", "/rapport/mensuel/clients"),
})
_RAPPORT_PRIX_M3_DEFAUT = _PRIX_EAU_M3     # EUR / m3 : même prix que les économies de l'appli (PRIX_EAU_M3), surchargeable par RAPPORT_PRIX_M3
_RAPPORT_PISCINE_OLYMPIQUE_M3 = 2500.0    # equivalence « piscine olympique »
_RAPPORT_PH_MIN, _RAPPORT_PH_MAX = 6.5, 8.5
_RAPPORT_CHLORE_MIN_MG_L, _RAPPORT_CHLORE_MAX_MG_L = 0.2, 2.0
# --- Unites et filtres ---
_RAPPORT_PH_ECHELLE = 100.0               # avg_ph brut = pH x 100
_RAPPORT_PH_VALIDE = (2.0, 12.0)          # meme filtre que /ph/jour
_RAPPORT_CHLORE_ECHELLE = 1000.0          # HYPOTHESE : avg_chlore brut / 1000 = mg/L
_RAPPORT_CHLORE_BRUT_MAX = 2000           # meme filtre que /chlore/jour
_RAPPORT_MAX_M3_HEURE = 50                # au-dela : heure aberrante (remise a zero)
_RAPPORT_MAX_KWH_HEURE = 200
# --- Signalement « donnees incompletes » ---
_RAPPORT_SEUIL_COUVERTURE_PCT = 90.0
_RAPPORT_JOURS_MUETS_MAX = 1              # >= 2 jours sans aucune donnee : signale

_RAPPORT_MOIS_RE = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")
_RAPPORT_MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet",
                    "août", "septembre", "octobre", "novembre", "décembre"]


def _rapport_prix_m3(client: str) -> float:
    """Prix du m3 utilise pour les economies. Point d'extension : un prix par
    client pourra etre lu ici plus tard (table ou colonne dediee)."""
    brut = (os.getenv("RAPPORT_PRIX_M3") or "").strip().replace(",", ".")
    try:
        prix = float(brut) if brut else _RAPPORT_PRIX_M3_DEFAUT
    except ValueError:
        prix = _RAPPORT_PRIX_M3_DEFAUT
    return prix if prix > 0 else _RAPPORT_PRIX_M3_DEFAUT


def _rapport_maintenant() -> dt.datetime:
    """Heure de Paris, sans fuseau (meme convention que mesures_aggregate)."""
    return dt.datetime.now(_RapportZoneInfo("Europe/Paris")).replace(tzinfo=None)


def _rapport_mois_precedent(ref: Optional[dt.datetime] = None) -> str:
    ref = ref or _rapport_maintenant()
    a, m = (ref.year, ref.month - 1) if ref.month > 1 else (ref.year - 1, 12)
    return f"{a:04d}-{m:02d}"


def _rapport_bornes(mois: str) -> tuple:
    """'AAAA-MM' -> (debut inclus, fin exclue), timestamps naifs heure de Paris."""
    m = _RAPPORT_MOIS_RE.match((mois or "").strip())
    if not m:
        raise HTTPException(status_code=400, detail="Paramètre mois invalide (attendu AAAA-MM)")
    a, mo = int(m.group(1)), int(m.group(2))
    debut = dt.datetime(a, mo, 1)
    fin = dt.datetime(a + 1, 1, 1) if mo == 12 else dt.datetime(a, mo + 1, 1)
    return debut, fin


def _rapport_sql(query: str, params) -> list:
    """Comme executer_requete_sql, mais une erreur SQL remonte (un rapport ne doit
    jamais afficher 0 m3 parce que la requete a echoue)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall() if cur.description is not None else []
        conn.commit()
        return rows
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logging.getLogger("rapport").error("Erreur SQL rapport mensuel : %s", e)
        raise HTTPException(status_code=500, detail="Erreur de calcul du rapport")
    finally:
        _release_connection(conn)


def _rapport_stations(client: str) -> list:
    """Stations du client : [{nom, bruts, lieu, client}], noms trimes."""
    rows = _rapport_sql(
        """
        SELECT trim(nom_automate)                 AS nom,
               array_agg(DISTINCT nom_automate)   AS bruts,
               min(NULLIF(trim(lieu), ''))        AS lieu,
               array_agg(trim(client))            AS graphies
        FROM   automate
        WHERE  lower(trim(client)) = lower(trim(%s))
          AND  NULLIF(trim(nom_automate), '') IS NOT NULL
        GROUP  BY trim(nom_automate)
        ORDER  BY 3 NULLS LAST, 1
        """,
        (client,),
    )
    # Nom affiche du client : la graphie la plus frequente dans automate.
    graphies = [g for r in rows for g in r[3]]
    nom_client = max(sorted(set(graphies)), key=graphies.count) if graphies else client
    return [{"nom": r[0], "bruts": list(r[1]), "lieu": r[2], "client": nom_client} for r in rows]


def _rapport_noms_sql(stations: list) -> list:
    """Noms a passer a `nom_automate = ANY(...)` : formes brutes ET trimees, pour
    garder l'index de mesures_aggregate (pas de trim() sur la colonne)."""
    noms = set()
    for s in stations:
        noms.add(s["nom"])
        noms.update(s["bruts"])
    return sorted(noms)


_RAPPORT_SQL_AGREGATS = """
WITH h AS (
    SELECT trim(nom_automate)                         AS nom,
           rounded_timestamp                          AS ts,
           compteur_eau_renvoi::numeric               AS renvoi,
           compteur_eau_adoucie::numeric              AS adoucie,
           compteur_electrique::numeric               AS elec,
           avg_ph::numeric / %(ph_ech)s               AS ph,
           avg_chlore::numeric                        AS cl_brut
    FROM   mesures_aggregate
    WHERE  nom_automate = ANY(%(noms)s)
      AND  rounded_timestamp >= %(debut)s
      AND  rounded_timestamp <  %(fin)s
), q AS (
    SELECT h.*,
           COALESCE(renvoi  BETWEEN 0 AND %(max_m3)s
                AND adoucie BETWEEN 0 AND %(max_m3)s, false)   AS eau_ok,
           COALESCE(elec    BETWEEN 0 AND %(max_kwh)s, false)  AS elec_ok,
           COALESCE(ph BETWEEN %(ph_vmin)s AND %(ph_vmax)s, false) AS ph_ok,
           COALESCE(cl_brut > 0 AND cl_brut <= %(cl_max)s, false)  AS cl_ok
    FROM   h
)
SELECT nom,
       count(DISTINCT ts)                                             AS heures,
       count(DISTINCT ts::date)                                       AS jours_donnees,
       COALESCE(sum(renvoi)  FILTER (WHERE eau_ok), 0)                AS renvoi,
       COALESCE(sum(adoucie) FILTER (WHERE eau_ok), 0)                AS adoucie,
       count(*) FILTER (WHERE NOT eau_ok)                             AS heures_eau_ecartees,
       COALESCE(sum(elec) FILTER (WHERE elec_ok), 0)                  AS kwh,
       count(*) FILTER (WHERE NOT elec_ok)                            AS heures_elec_ecartees,
       count(DISTINCT ts::date) FILTER (WHERE eau_ok AND renvoi > 0)  AS jours_lavage,
       sum(ph) FILTER (WHERE ph_ok)                                   AS ph_somme,
       count(*) FILTER (WHERE ph_ok)                                  AS ph_n,
       count(*) FILTER (WHERE ph_ok AND ph BETWEEN %(ph_min)s AND %(ph_max)s) AS ph_n_plage,
       sum(cl_brut / %(cl_ech)s) FILTER (WHERE cl_ok)                 AS cl_somme,
       count(*) FILTER (WHERE cl_ok)                                  AS cl_n,
       count(*) FILTER (WHERE cl_ok AND cl_brut / %(cl_ech)s
                        BETWEEN %(cl_min)s AND %(cl_max_mg)s)         AS cl_n_plage,
       min(ts)                                                        AS premiere_donnee
FROM   q
GROUP  BY nom
"""

_RAPPORT_CLES_AGREGATS = (
    "heures", "jours_donnees", "renvoi", "adoucie", "heures_eau_ecartees", "kwh",
    "heures_elec_ecartees", "jours_lavage", "ph_somme", "ph_n", "ph_n_plage",
    "cl_somme", "cl_n", "cl_n_plage", "premiere_donnee",
)


def _rapport_agregats(stations: list, debut: dt.datetime, fin: dt.datetime) -> dict:
    """Agregats bruts par station (nom trime) sur [debut, fin). Station sans donnees : absente."""
    rows = _rapport_sql(_RAPPORT_SQL_AGREGATS, {
        "noms": _rapport_noms_sql(stations), "debut": debut, "fin": fin,
        "ph_ech": _RAPPORT_PH_ECHELLE, "ph_vmin": _RAPPORT_PH_VALIDE[0], "ph_vmax": _RAPPORT_PH_VALIDE[1],
        "ph_min": _RAPPORT_PH_MIN, "ph_max": _RAPPORT_PH_MAX,
        "cl_ech": _RAPPORT_CHLORE_ECHELLE, "cl_max": _RAPPORT_CHLORE_BRUT_MAX,
        "cl_min": _RAPPORT_CHLORE_MIN_MG_L, "cl_max_mg": _RAPPORT_CHLORE_MAX_MG_L,
        "max_m3": _RAPPORT_MAX_M3_HEURE, "max_kwh": _RAPPORT_MAX_KWH_HEURE,
    })
    out = {}
    for r in rows:
        d = dict(zip(_RAPPORT_CLES_AGREGATS, r[1:]))
        for k, v in d.items():
            if isinstance(v, decimal.Decimal):
                d[k] = float(v)
        out[r[0]] = d
    return out


def _rapport_pannes(stations: list, debut: dt.datetime, fin: dt.datetime) -> tuple:
    """(stats par station, liste des pannes du mois). Jointure par nom de station
    trime : aucune panne d'une autre station ne peut remonter."""
    noms = [s["nom"] for s in stations]
    p = {"noms": noms, "debut": debut, "fin": fin}
    stats = _rapport_sql(
        """
        SELECT trim(nom_automate),
               count(*) FILTER (WHERE date_debut >= %(debut)s AND date_debut < %(fin)s),
               count(*) FILTER (WHERE date_fin   >= %(debut)s AND date_fin   < %(fin)s),
               COALESCE(sum(EXTRACT(EPOCH FROM (date_fin - date_debut)) / 3600.0)
                        FILTER (WHERE date_fin >= %(debut)s AND date_fin < %(fin)s
                                  AND date_fin >= date_debut), 0),
               count(*) FILTER (WHERE date_fin >= %(debut)s AND date_fin < %(fin)s
                                  AND date_fin >= date_debut),
               count(*) FILTER (WHERE deplacement IS TRUE
                                  AND ((date_debut >= %(debut)s AND date_debut < %(fin)s)
                                    OR (date_fin   >= %(debut)s AND date_fin   < %(fin)s))),
               count(*) FILTER (WHERE date_debut < %(fin)s
                                  AND (date_fin IS NULL OR date_fin >= %(fin)s))
        FROM   pannes
        WHERE  trim(nom_automate) = ANY(%(noms)s)
        GROUP  BY trim(nom_automate)
        """,
        p,
    )
    par_station = {}
    for r in stats:
        par_station[r[0]] = {
            "ouvertes": int(r[1]), "resolues": int(r[2]), "delai_somme_h": float(r[3] or 0),
            "delai_n": int(r[4]), "deplacements": int(r[5]), "en_cours_fin_mois": int(r[6]),
        }
    liste = _rapport_sql(
        """
        SELECT trim(nom_automate), panne, date_debut, date_fin
        FROM   pannes
        WHERE  trim(nom_automate) = ANY(%(noms)s)
          AND  ((date_debut >= %(debut)s AND date_debut < %(fin)s)
             OR (date_fin   >= %(debut)s AND date_fin   < %(fin)s))
        ORDER  BY date_debut, id
        """,
        p,
    )
    return par_station, [
        {
            "nom_automate": r[0], "type": r[1],
            "date_debut": r[2].isoformat() if hasattr(r[2], "isoformat") else r[2],
            "date_fin": r[3].isoformat() if hasattr(r[3], "isoformat") else r[3],
            "resolue_dans_le_mois": bool(r[3] is not None and debut <= _rapport_naif(r[3]) < fin),
            "duree_h": (round((_rapport_naif(r[3]) - _rapport_naif(r[2])).total_seconds() / 3600, 1)
                        if r[3] is not None and r[2] is not None else None),
        }
        for r in liste
    ]


def _rapport_naif(v):
    return v.replace(tzinfo=None) if isinstance(v, dt.datetime) else v


def _rapport_pct(num, den, nd=1):
    return round(100.0 * num / den, nd) if den else None


def _rapport_variation(cur, prev):
    if cur is None or prev is None or not prev:
        return None
    return round(100.0 * (cur - prev) / prev, 1)


_RAPPORT_VIDE = {k: 0 for k in _RAPPORT_CLES_AGREGATS}
_RAPPORT_VIDE.update({"ph_somme": None, "cl_somme": None, "premiere_donnee": None})


def _rapport_somme(agregats: list) -> dict:
    tot = dict(_RAPPORT_VIDE)
    for a in agregats:
        for k in _RAPPORT_CLES_AGREGATS:
            v = a.get(k)
            if k == "premiere_donnee":
                if v is not None and (tot[k] is None or v < tot[k]):
                    tot[k] = v
            elif k in ("ph_somme", "cl_somme"):
                if v is not None:
                    tot[k] = (tot[k] or 0) + v
            else:
                tot[k] += v or 0
    return tot


def _rapport_indicateurs(a: dict, prix: float, heures_attendues: int, jours_periode: int,
                         nb_stations: int = 1) -> dict:
    """Indicateurs lisibles a partir des agregats bruts (station ou total)."""
    renvoi, adoucie = float(a["renvoi"]), float(a["adoucie"])
    recyclee = max(renvoi - adoucie, 0.0)
    kwh = float(a["kwh"])
    taux = None
    if renvoi > 0:
        taux = round(min(max(100.0 * recyclee / renvoi, 0.0), 100.0), 1)
    attendues = heures_attendues * nb_stations
    couverture = min(_rapport_pct(a["heures"], attendues) or 0.0, 100.0) if attendues else None
    # jours_donnees d'un total = somme sur les stations : jours sans donnees en moyenne
    jours_sans = max(jours_periode * nb_stations - int(a["jours_donnees"]), 0)
    motifs = []
    if couverture is not None and couverture < _RAPPORT_SEUIL_COUVERTURE_PCT:
        motifs.append(f"couverture des données {couverture:.1f} % (< {_RAPPORT_SEUIL_COUVERTURE_PCT:.0f} %)"
                      .replace(".", ",").replace(" %", "\u00a0%"))
    if nb_stations == 1 and jours_sans > _RAPPORT_JOURS_MUETS_MAX:
        motifs.append(f"{jours_sans} jours sans aucune donnée")
    if a["heures_eau_ecartees"]:
        motifs.append(f"{a['heures_eau_ecartees']} heure(s) de compteur d'eau écartée(s) (remise à zéro ou valeur aberrante)")
    if a["heures_elec_ecartees"]:
        motifs.append(f"{a['heures_elec_ecartees']} heure(s) de compteur électrique écartée(s)")
    return {
        "eau_renvoyee_m3": round(renvoi, 1),
        "eau_adoucie_m3": round(adoucie, 1),
        "eau_recyclee_m3": round(recyclee, 1),
        "taux_recyclage_pct": taux,
        "economies_eur": round(recyclee * prix, 2),
        "equivalent_piscines_olympiques": round(recyclee / _RAPPORT_PISCINE_OLYMPIQUE_M3, 2),
        "energie_kwh": round(kwh, 1),
        "kwh_par_m3_recycle": round(kwh / recyclee, 2) if recyclee > 0 else None,
        "jours_avec_lavage": int(a["jours_lavage"]),
        "jours_avec_donnees": int(a["jours_donnees"]),
        "jours_sans_donnees": jours_sans,
        "heures_avec_donnees": int(a["heures"]),
        "heures_attendues": attendues,
        "couverture_pct": couverture,
        "heures_ecartees": {"eau": int(a["heures_eau_ecartees"]), "electricite": int(a["heures_elec_ecartees"])},
        "qualite": {
            "ph_moyen": round(a["ph_somme"] / a["ph_n"], 2) if a["ph_n"] else None,
            "ph_pct_dans_plage": _rapport_pct(a["ph_n_plage"], a["ph_n"]),
            "heures_mesure_ph": int(a["ph_n"]),
            "chlore_moyen_mg_l": round(a["cl_somme"] / a["cl_n"], 2) if a["cl_n"] else None,
            "chlore_pct_dans_plage": _rapport_pct(a["cl_n_plage"], a["cl_n"]),
            "heures_mesure_chlore": int(a["cl_n"]),
        },
        "premiere_donnee": a["premiere_donnee"].isoformat() if a.get("premiere_donnee") else None,
        "donnees_incompletes": bool(motifs),
        "motifs_incompletude": motifs,
    }


def _rapport_pannes_indic(stats: list) -> dict:
    t = {"ouvertes": 0, "resolues": 0, "delai_somme_h": 0.0, "delai_n": 0, "deplacements": 0,
         "en_cours_fin_mois": 0}
    for s in stats:
        for k in t:
            t[k] += s.get(k, 0)
    return {
        "ouvertes_dans_le_mois": t["ouvertes"],
        "resolues_dans_le_mois": t["resolues"],
        "delai_moyen_resolution_h": round(t["delai_somme_h"] / t["delai_n"], 1) if t["delai_n"] else None,
        "deplacements": t["deplacements"],
        "en_cours_fin_de_mois": t["en_cours_fin_mois"],
    }


def rapport_mensuel(client: str, mois: str) -> dict:
    """Indicateurs V1 du mois civil `mois` ('AAAA-MM', heure de Paris) pour les
    stations du client `client` : detail par station + total client."""
    client = (client or "").strip()
    if not client:
        raise HTTPException(status_code=400, detail="Paramètre client obligatoire")
    debut, fin = _rapport_bornes(mois)
    maintenant = _rapport_maintenant()
    if debut > maintenant:
        raise HTTPException(status_code=400, detail="Mois futur : aucun rapport possible")
    stations = _rapport_stations(client)
    if not stations:
        raise HTTPException(status_code=404, detail="Client inconnu ou sans station")

    mois_en_cours = maintenant < fin
    fin_effective = min(fin, maintenant.replace(minute=0, second=0, microsecond=0)) if mois_en_cours else fin
    heures_attendues = max(int((fin_effective - debut).total_seconds() // 3600), 0)
    jours_periode = (fin_effective - debut).days + (1 if mois_en_cours else 0)
    jours_periode = min(jours_periode, _rapport_calendar.monthrange(debut.year, debut.month)[1])

    mois_prec = f"{debut.year - 1:04d}-12" if debut.month == 1 else f"{debut.year:04d}-{debut.month - 1:02d}"
    debut_prec, fin_prec = _rapport_bornes(mois_prec)
    heures_prec = int((fin_prec - debut_prec).total_seconds() // 3600)
    jours_prec = (fin_prec - debut_prec).days
    debut_annee = dt.datetime(debut.year, 1, 1)

    prix = _rapport_prix_m3(client)
    ag = _rapport_agregats(stations, debut, fin)
    ag_prec = _rapport_agregats(stations, debut_prec, fin_prec)
    ag_annee = _rapport_agregats(stations, debut_annee, fin)
    pannes_stats, pannes_liste = _rapport_pannes(stations, debut, fin)

    detail = []
    for s in stations:
        a = ag.get(s["nom"], _RAPPORT_VIDE)
        ind = _rapport_indicateurs(a, prix, heures_attendues, jours_periode)
        prec = _rapport_indicateurs(ag_prec.get(s["nom"], _RAPPORT_VIDE), prix, heures_prec, jours_prec)
        ind["variation_eau_recyclee_pct"] = _rapport_variation(ind["eau_recyclee_m3"], prec["eau_recyclee_m3"])
        ind["pannes"] = _rapport_pannes_indic([pannes_stats.get(s["nom"], {})])
        if a is _RAPPORT_VIDE:
            ind["motifs_incompletude"] = ["aucune donnée reçue ce mois"]
            ind["donnees_incompletes"] = True
        detail.append({"nom_automate": s["nom"], "lieu": s["lieu"], **ind})

    n = len(stations)
    total = _rapport_indicateurs(_rapport_somme(list(ag.values())), prix, heures_attendues, jours_periode, n)
    total.pop("motifs_incompletude")
    total["donnees_incompletes"] = any(d["donnees_incompletes"] for d in detail)
    total["stations_donnees_incompletes"] = [d["nom_automate"] for d in detail if d["donnees_incompletes"]]
    total["pannes"] = _rapport_pannes_indic(list(pannes_stats.values()))

    prec_tot = _rapport_indicateurs(_rapport_somme(list(ag_prec.values())), prix, heures_prec, jours_prec, n)
    cles_cmp = ("eau_recyclee_m3", "eau_renvoyee_m3", "economies_eur", "energie_kwh", "jours_avec_lavage")
    comparaison = {
        "mois": mois_prec,
        "aucune_donnee": prec_tot["heures_avec_donnees"] == 0,
        **{k: prec_tot[k] for k in cles_cmp + ("taux_recyclage_pct", "couverture_pct", "kwh_par_m3_recycle")},
        "variations_pct": {k: _rapport_variation(total[k], prec_tot[k]) for k in cles_cmp + ("kwh_par_m3_recycle",)},
        "ecart_taux_recyclage_points": (round(total["taux_recyclage_pct"] - prec_tot["taux_recyclage_pct"], 1)
                                        if total["taux_recyclage_pct"] is not None
                                        and prec_tot["taux_recyclage_pct"] is not None else None),
    }

    annee = _rapport_indicateurs(_rapport_somme(list(ag_annee.values())), prix, 0, 0, n)
    cumul = {
        "depuis": annee["premiere_donnee"][:10] if annee["premiere_donnee"] else None,
        "jusqu_au": (fin - dt.timedelta(days=1)).date().isoformat(),
        **{k: annee[k] for k in ("eau_recyclee_m3", "eau_renvoyee_m3", "taux_recyclage_pct", "economies_eur",
                                 "equivalent_piscines_olympiques", "energie_kwh")},
        "heures_ecartees": annee["heures_ecartees"],
    }

    return {
        "client": stations[0]["client"],
        "mois": f"{debut.year:04d}-{debut.month:02d}",
        "periode": {"debut": debut.isoformat(), "fin_exclue": fin.isoformat(),
                    "fuseau": "Europe/Paris (heure locale)", "mois_en_cours": mois_en_cours},
        "aucune_donnee": total["heures_avec_donnees"] == 0,
        "nb_stations": n,
        "total": total,
        "stations": detail,
        "comparaison_mois_precedent": comparaison,
        "cumul_annee": cumul,
        "pannes_du_mois": pannes_liste,
        "parametres": {
            "prix_m3_eur": prix,
            "piscine_olympique_m3": _RAPPORT_PISCINE_OLYMPIQUE_M3,
            "plage_ph": [_RAPPORT_PH_MIN, _RAPPORT_PH_MAX],
            "plage_chlore_mg_l": [_RAPPORT_CHLORE_MIN_MG_L, _RAPPORT_CHLORE_MAX_MG_L],
            "seuil_couverture_pct": _RAPPORT_SEUIL_COUVERTURE_PCT,
            "max_m3_par_heure": _RAPPORT_MAX_M3_HEURE,
            "max_kwh_par_heure": _RAPPORT_MAX_KWH_HEURE,
            "source": "mesures_aggregate (horaire)",
        },
        "genere_le": maintenant.replace(microsecond=0).isoformat(),
    }


# ----------------------------- Rendu HTML ------------------------------------
def _fr_nombre(v, nd=0) -> str:
    """1234567.8 -> '1 234 567,8' (espace fine insecable)."""
    if v is None:
        return "—"
    return f"{v:,.{nd}f}".replace(",", " ").replace(".", ",")


def _fr_pct(v, nd=1) -> str:
    return "—" if v is None else f"{_fr_nombre(v, nd)} %"


def _fr_eur(v, nd=0) -> str:
    return "—" if v is None else f"{_fr_nombre(v, nd)} €"


def _fr_var(v) -> str:
    if v is None:
        return '<span class="var neutre">—</span>'
    cls = "hausse" if v > 0 else ("baisse" if v < 0 else "neutre")
    signe = "+" if v > 0 else ("−" if v < 0 else "")
    return f'<span class="var {cls}">{signe}{_fr_nombre(abs(v), 1)} %</span>'


def _fr_date(iso) -> str:
    if not iso:
        return "—"
    try:
        return dt.datetime.fromisoformat(str(iso)).strftime("%d/%m/%Y")
    except ValueError:
        return _rapport_html.escape(str(iso))


def _fr_mois(mois: str) -> str:
    a, m = mois.split("-")
    return f"{_RAPPORT_MOIS_FR[int(m) - 1]} {a}"


def _fr_duree(h) -> str:
    if h is None:
        return "—"
    if h < 48:
        return f"{_fr_nombre(h, 1)} h"
    return f"{_fr_nombre(h / 24, 1)} j"


_RAPPORT_CSS = """
:root{--tq:#41AEAD;--tq-fonce:#2c7f7e;--tq-clair:#e8f5f5;--txt:#1d2b2b;--gris:#5d6b6b;--ligne:#d6e3e3;
--hausse:#1e7d4f;--baisse:#b23b3b;--alerte:#fff6e0;--alerte-bord:#e0b44c}
*{box-sizing:border-box}
html{background:#eef3f3}
body{margin:0;background:#eef3f3;color:var(--txt);font:14px/1.45 "Segoe UI",Roboto,Helvetica,Arial,sans-serif}
.page{max-width:210mm;margin:16px auto;background:#fff;padding:14mm 14mm 10mm;box-shadow:0 1px 6px rgba(0,0,0,.08)}
header{display:flex;justify-content:space-between;align-items:flex-end;border-bottom:3px solid var(--tq);padding-bottom:10px;margin-bottom:16px;gap:12px;flex-wrap:wrap}
.marque{font-size:26px;font-weight:700;color:var(--tq);letter-spacing:.5px}
.marque small{display:block;font-size:12px;font-weight:400;color:var(--gris);letter-spacing:0}
h1{font-size:20px;margin:0;text-align:right}
h1 small{display:block;font-size:14px;font-weight:400;color:var(--gris)}
h2{font-size:15px;color:var(--tq-fonce);margin:20px 0 8px;text-transform:uppercase;letter-spacing:.6px}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.kpi{background:var(--tq-clair);border-radius:8px;padding:10px 12px;break-inside:avoid}
.kpi .lib{font-size:12px;color:var(--gris)}
.kpi .val{font-size:22px;font-weight:700;color:var(--tq-fonce);white-space:nowrap}
.kpi .cmp{font-size:11.5px;color:var(--gris)}
.kpis.second .kpi{background:#f6f9f9}
.kpis.second .val{font-size:18px;color:var(--txt)}
.var{font-weight:600}.var.hausse{color:var(--hausse)}.var.baisse{color:var(--baisse)}.var.neutre{color:var(--gris)}
table{width:100%;border-collapse:collapse;font-size:12.5px;break-inside:auto}
th,td{padding:5px 6px;border-bottom:1px solid var(--ligne);text-align:right;vertical-align:top}
th{background:var(--tq);color:#fff;font-weight:600;font-size:11.5px}
th:first-child,td:first-child,th.g,td.g{text-align:left}
tr{break-inside:avoid}
tfoot td{font-weight:700;border-top:2px solid var(--tq)}
.nom{font-size:11px;color:var(--gris)}
.drapeau{color:#9a6a00;font-weight:700}
.alerte{background:var(--alerte);border-left:4px solid var(--alerte-bord);padding:8px 12px;margin:14px 0;font-size:12.5px;break-inside:avoid}
.alerte ul{margin:4px 0 0;padding-left:18px}
.vide{background:var(--tq-clair);padding:14px;border-radius:8px;text-align:center;color:var(--gris)}
.methode{font-size:11px;color:var(--gris);margin-top:22px;border-top:1px solid var(--ligne);padding-top:8px}
.methode p{margin:3px 0}
.actions{max-width:210mm;margin:12px auto 0;text-align:right}
.actions button{background:var(--tq);color:#fff;border:0;border-radius:6px;padding:8px 14px;font-size:14px;cursor:pointer}
.tableau{overflow-x:auto}
@media (max-width:700px){.page{padding:16px;margin:0}.kpis{grid-template-columns:repeat(2,1fr)}h1{text-align:left}
.actions{padding:0 16px}}
@page{size:A4;margin:12mm}
@media print{html,body{background:#fff}.page{box-shadow:none;margin:0;padding:0;max-width:none}.actions{display:none}
*{-webkit-print-color-adjust:exact;print-color-adjust:exact}h2{break-after:avoid}.tableau{overflow:visible}}
"""


def rapport_mensuel_html(r: dict) -> str:
    e = _rapport_html.escape
    t, cmp_, cum, par = r["total"], r["comparaison_mois_precedent"], r["cumul_annee"], r["parametres"]
    v = cmp_["variations_pct"]
    mois_txt = _fr_mois(r["mois"])
    prec_txt = _fr_mois(cmp_["mois"])

    def nom_station(s):
        if s["lieu"]:
            return f'{e(s["lieu"])}<div class="nom">{e(s["nom_automate"])}</div>'
        return e(s["nom_automate"])

    def kpi(lib, val, cmp_html=""):
        return (f'<div class="kpi"><div class="lib">{lib}</div><div class="val">{val}</div>'
                f'<div class="cmp">{cmp_html}</div></div>')

    vs = f"vs {e(prec_txt)} : "
    ecart = cmp_["ecart_taux_recyclage_points"]
    ecart_html = ('<span class="var neutre">—</span>' if ecart is None else
                  f'<span class="var {"hausse" if ecart > 0 else "baisse" if ecart < 0 else "neutre"}">'
                  f'{"+" if ecart > 0 else "−" if ecart < 0 else ""}{_fr_nombre(abs(ecart), 1)} pt</span>')
    piscines = t["equivalent_piscines_olympiques"]
    parts = []
    parts.append(f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Rapport Eaukey {e(mois_txt)}</title><style>{_RAPPORT_CSS}</style></head><body>
<div class="actions"><button onclick="window.print()">Imprimer / enregistrer en PDF</button></div>
<div class="page">
<header><div class="marque">Eaukey<small>Recyclage de l'eau des stations de lavage</small></div>
<h1>Rapport de performance – {e(mois_txt)}<small>{e(r["client"])} · {r["nb_stations"]} station{"s" if r["nb_stations"] > 1 else ""}</small></h1></header>
""")
    if r["periode"]["mois_en_cours"]:
        parts.append('<div class="alerte">Mois en cours : chiffres provisoires, arrêtés à la date de génération.</div>')
    if r["aucune_donnee"]:
        parts.append(f'<div class="vide">Aucune donnée n\'a été reçue de vos stations en {e(mois_txt)}.</div>')
    else:
        parts.append('<h2>Chiffres clés du mois</h2><div class="kpis">')
        parts.append(kpi("Eau recyclée", f"{_fr_nombre(t['eau_recyclee_m3'])} m³", vs + _fr_var(v["eau_recyclee_m3"])))
        parts.append(kpi("Taux de recyclage", _fr_pct(t["taux_recyclage_pct"]), vs + ecart_html))
        parts.append(kpi("Économies estimées", _fr_eur(t["economies_eur"]), vs + _fr_var(v["economies_eur"])))
        parts.append(kpi("Piscines olympiques", _fr_nombre(piscines, 2 if piscines < 10 else 1),
                         f"1 piscine = {_fr_nombre(par['piscine_olympique_m3'])} m³"))
        parts.append('</div><div class="kpis second" style="margin-top:10px">')
        parts.append(kpi("Énergie consommée", f"{_fr_nombre(t['energie_kwh'])} kWh", vs + _fr_var(v["energie_kwh"])))
        kpm = t["kwh_par_m3_recycle"]
        parts.append(kpi("kWh par m³ recyclé", "—" if kpm is None else _fr_nombre(kpm, 2)))
        parts.append(kpi("Jours avec lavage", _fr_nombre(t["jours_avec_lavage"]),
                         f"cumulés sur {r['nb_stations']} station{'s' if r['nb_stations'] > 1 else ''}"))
        parts.append(kpi("Données reçues", _fr_pct(t["couverture_pct"]), "des heures du mois"))
        parts.append('</div>')

    if t["donnees_incompletes"]:
        items = "".join(
            f'<li><b>{e(s["lieu"] or s["nom_automate"])}</b> : {e(" ; ".join(s["motifs_incompletude"]))}</li>'
            for s in r["stations"] if s["donnees_incompletes"])
        parts.append('<div class="alerte"><b>Données incomplètes</b> – les chiffres ci-dessous ne comptent que '
                     f'les périodes où les stations ont transmis leurs mesures (rien n\'est extrapolé).<ul>{items}</ul></div>')

    # Detail par station
    parts.append('<h2>Détail par station</h2><div class="tableau"><table><thead><tr><th>Station</th>'
                 '<th>Eau recyclée</th><th>Taux</th><th>Économies</th><th>kWh</th><th>Jours lavage</th>'
                 f'<th>Données</th><th>vs {e(prec_txt)}</th></tr></thead><tbody>')
    for s in r["stations"]:
        flag = ' <span class="drapeau" title="Données incomplètes">⚠</span>' if s["donnees_incompletes"] else ""
        parts.append(
            f'<tr><td>{nom_station(s)}</td><td>{_fr_nombre(s["eau_recyclee_m3"])} m³</td>'
            f'<td>{_fr_pct(s["taux_recyclage_pct"])}</td><td>{_fr_eur(s["economies_eur"])}</td>'
            f'<td>{_fr_nombre(s["energie_kwh"])}</td><td>{s["jours_avec_lavage"]}</td>'
            f'<td>{_fr_pct(s["couverture_pct"], 0)}{flag}</td><td>{_fr_var(s["variation_eau_recyclee_pct"])}</td></tr>')
    if r["nb_stations"] > 1:
        parts.append(
            f'<tfoot><tr><td>Total</td><td>{_fr_nombre(t["eau_recyclee_m3"])} m³</td>'
            f'<td>{_fr_pct(t["taux_recyclage_pct"])}</td><td>{_fr_eur(t["economies_eur"])}</td>'
            f'<td>{_fr_nombre(t["energie_kwh"])}</td><td>{t["jours_avec_lavage"]}</td>'
            f'<td>{_fr_pct(t["couverture_pct"], 0)}</td><td>{_fr_var(v["eau_recyclee_m3"])}</td></tr></tfoot>')
    parts.append('</tbody></table></div>')

    # Qualite de l'eau
    q = t["qualite"]
    parts.append(
        '<h2>Qualité de l\'eau</h2><div class="tableau"><table><thead><tr><th>Station</th><th>pH moyen</th>'
        f'<th>pH dans la plage ({_fr_nombre(par["plage_ph"][0], 1)}–{_fr_nombre(par["plage_ph"][1], 1)})</th>'
        '<th>Chlore moyen (mg/L)</th>'
        f'<th>Chlore dans la plage ({_fr_nombre(par["plage_chlore_mg_l"][0], 1)}–{_fr_nombre(par["plage_chlore_mg_l"][1], 1)} mg/L)</th>'
        '</tr></thead><tbody>')
    for s in r["stations"]:
        sq = s["qualite"]
        parts.append(f'<tr><td>{nom_station(s)}</td><td>{_fr_nombre(sq["ph_moyen"], 2)}</td>'
                     f'<td>{_fr_pct(sq["ph_pct_dans_plage"])}</td><td>{_fr_nombre(sq["chlore_moyen_mg_l"], 2)}</td>'
                     f'<td>{_fr_pct(sq["chlore_pct_dans_plage"])}</td></tr>')
    if r["nb_stations"] > 1:
        parts.append(f'<tfoot><tr><td>Ensemble</td><td>{_fr_nombre(q["ph_moyen"], 2)}</td>'
                     f'<td>{_fr_pct(q["ph_pct_dans_plage"])}</td><td>{_fr_nombre(q["chlore_moyen_mg_l"], 2)}</td>'
                     f'<td>{_fr_pct(q["chlore_pct_dans_plage"])}</td></tr></tfoot>')
    parts.append('</tbody></table></div>')

    # Interventions
    p = t["pannes"]
    parts.append('<h2>Interventions</h2><div class="kpis second">')
    parts.append(kpi("Pannes signalées", str(p["ouvertes_dans_le_mois"])))
    parts.append(kpi("Pannes résolues", str(p["resolues_dans_le_mois"])))
    parts.append(kpi("Délai moyen de résolution", _fr_duree(p["delai_moyen_resolution_h"])))
    parts.append(kpi("Déplacements sur site", str(p["deplacements"]),
                     f"{p['en_cours_fin_de_mois']} en cours en fin de mois"))
    parts.append('</div>')
    if r["pannes_du_mois"]:
        noms = {s["nom_automate"]: (s["lieu"] or s["nom_automate"]) for s in r["stations"]}
        parts.append('<div class="tableau" style="margin-top:10px"><table><thead><tr><th>Station</th><th class="g">Type</th>'
                     '<th>Début</th><th>Fin</th><th>Durée</th></tr></thead><tbody>')
        for pn in r["pannes_du_mois"]:
            parts.append(f'<tr><td>{e(noms.get(pn["nom_automate"], pn["nom_automate"]))}</td><td class="g">{e(pn["type"] or "—")}</td>'
                         f'<td>{_fr_date(pn["date_debut"])}</td><td>{_fr_date(pn["date_fin"]) if pn["date_fin"] else "en cours"}</td>'
                         f'<td>{_fr_duree(pn["duree_h"])}</td></tr>')
        parts.append('</tbody></table></div>')

    # Cumul annee
    parts.append(f'<h2>Depuis le début de l\'année {r["mois"][:4]}</h2><div class="kpis">')
    parts.append(kpi("Eau recyclée", f"{_fr_nombre(cum['eau_recyclee_m3'])} m³",
                     f"du {_fr_date(cum['depuis'])} au {_fr_date(cum['jusqu_au'])}" if cum["depuis"] else ""))
    parts.append(kpi("Taux de recyclage", _fr_pct(cum["taux_recyclage_pct"])))
    parts.append(kpi("Économies estimées", _fr_eur(cum["economies_eur"])))
    parts.append(kpi("Piscines olympiques", _fr_nombre(cum["equivalent_piscines_olympiques"], 1)))
    parts.append('</div>')

    parts.append(
        '<div class="methode">'
        f'<p><b>Méthode.</b> Eau recyclée = eau renvoyée vers le lavage − eau d\'appoint adoucie, mesurées par les compteurs '
        f'des stations. Taux de recyclage = eau recyclée / eau renvoyée. Économies estimées sur la base de '
        f'{_fr_nombre(par["prix_m3_eur"], 2)} € le m³ (eau + assainissement, prix indicatif). '
        'Données horaires, heure de Paris ; une heure sans transmission n\'est pas comptée.</p>'
        f'<p>Rapport généré le {_fr_date(r["genere_le"])}.</p></div>')
    parts.append('</div></body></html>')
    return "".join(parts)


@app.get("/rapport/mensuel")
def rapport_mensuel_json(
    request: Request,
    client: str = Query(..., description="Client (automate.client)"),
    mois: Optional[str] = Query(None, description="AAAA-MM (défaut : mois précédent, heure de Paris)"),
):
    """ADMIN : rapport de performance mensuel d'un client (JSON)."""
    _require_admin_or_super(request)
    return rapport_mensuel(client, mois or _rapport_mois_precedent())


@app.get("/rapport/mensuel/html", response_class=HTMLResponse)
def rapport_mensuel_page(
    request: Request,
    client: str = Query(..., description="Client (automate.client)"),
    mois: Optional[str] = Query(None, description="AAAA-MM (défaut : mois précédent, heure de Paris)"),
):
    """ADMIN : meme rapport en page HTML autonome, imprimable en PDF (A4)."""
    _require_admin_or_super(request)
    r = rapport_mensuel(client, mois or _rapport_mois_precedent())
    return HTMLResponse(rapport_mensuel_html(r), headers={"Cache-Control": "no-store"})


@app.get("/rapport/mensuel/clients")
def rapport_mensuel_clients(
    request: Request,
    mois: Optional[str] = Query(None, description="AAAA-MM (défaut : mois précédent, heure de Paris)"),
):
    """ADMIN : clients ayant au moins une station avec des donnees ce mois-ci
    (base de la future generation en lot)."""
    _require_admin_or_super(request)
    mois = mois or _rapport_mois_precedent()
    debut, fin = _rapport_bornes(mois)
    rows = _rapport_sql(
        """
        WITH actives AS (
            SELECT DISTINCT trim(nom_automate) AS nom
            FROM   mesures_aggregate
            WHERE  rounded_timestamp >= %s AND rounded_timestamp < %s
        )
        SELECT mode() WITHIN GROUP (ORDER BY trim(a.client)) AS client,
               count(DISTINCT trim(a.nom_automate)) AS stations_avec_donnees
        FROM   automate a
        JOIN   actives ON actives.nom = trim(a.nom_automate)
        WHERE  NULLIF(trim(a.client), '') IS NOT NULL
        GROUP  BY lower(trim(a.client))
        ORDER  BY 1
        """,
        (debut, fin),
    )
    return {"mois": f"{debut.year:04d}-{debut.month:02d}",
            "clients": [{"client": r[0], "stations_avec_donnees": int(r[1])} for r in rows]}


# =============================================================================
# REMONTEE DE DONNEES (page /parc, interne : admin / super_admin)
# =============================================================================
# Objectif : savoir en un coup d'oeil quels automates remontent encore des
# donnees. Lecture seule, aucune ecriture, aucun automate n'est modifie.
#
# ATTENTION PERF : `mesures` fait ~235M de lignes. La requete "derniere mesure
# de chaque automate" (ORDER BY horodatage DESC LIMIT 1) s'ecroule justement sur
# les automates silencieux, car le planner remonte l'index `horodatage` a
# l'envers en filtrant. On borne donc toujours la fenetre temporelle : un
# automate absent du resultat est un automate sans donnees dans la fenetre,
# ce qui est exactement l'information recherchee.
# Voir CLAUDE.md, section "Piege d'index sur mesures".

# ATTENTION FUSEAU : `mesures.horodatage` est un timestamp SANS fuseau qui
# contient de l'heure locale de Paris, alors que la base tourne en UTC. Comparer
# `horodatage` a `(now() AT TIME ZONE 'Europe/Paris')` introduit donc un decalage de 1h a 2h selon la saison
# (verifie le 27/07/2026 : (now() AT TIME ZONE 'Europe/Paris') = 12:29 UTC, max(horodatage) = 14:29). La bonne
# reference est `now() AT TIME ZONE 'Europe/Paris'`, qui gere aussi le passage
# heure d'ete / heure d'hiver.
_PARC_MAINTENANT = "(now() AT TIME ZONE 'Europe/Paris')"

_PARC_SEUIL_OK_MIN = 15      # vert  : vu il y a moins de 15 min
_PARC_SEUIL_ALERTE_MIN = 120  # orange jusqu'a 2h, rouge au-dela


@app.get("/parc/etat")
def parc_etat(request: Request):
    """Etat de remontee de donnees de tous les automates.

    Statuts : "ok" (< 15 min), "attention" (15 min a 2h), "muet" (> 2h ou jamais).
    """
    _require_admin_or_super(request)

    query = """
    WITH recent AS (
        SELECT nom_automate, max(horodatage) AS derniere
        FROM   mesures
        WHERE  horodatage > {maintenant} - INTERVAL '{fenetre} minutes'
        GROUP  BY nom_automate
    )
    SELECT a.nom_automate,
           a.client,
           a.lieu,
           r.derniere,
           round(extract(epoch FROM ({maintenant} - r.derniere)) / 60)::int AS minutes
    FROM   automate a
    -- trim() obligatoire : `automate.nom_automate` contient des espaces en trop
    -- pour certaines lignes (ex. '20260300.0 '), pas `mesures.nom_automate`. Sans
    -- trim la jointure echoue et un automate parfaitement actif s'affiche en muet.
    LEFT   JOIN recent r ON trim(r.nom_automate) = trim(a.nom_automate)
    ORDER  BY r.derniere DESC NULLS LAST, a.client, a.nom_automate;
    """.format(maintenant=_PARC_MAINTENANT, fenetre=_PARC_SEUIL_ALERTE_MIN)

    rows = executer_requete_sql(query)

    automates = []
    compteurs = {"ok": 0, "attention": 0, "muet": 0}
    for nom, client, lieu, derniere, minutes in rows:
        if derniere is None:
            statut = "muet"
            minutes = None
        elif minutes is not None and minutes <= _PARC_SEUIL_OK_MIN:
            statut = "ok"
        else:
            statut = "attention"
        compteurs[statut] += 1
        automates.append({
            "nom_automate": (nom or "").strip(),
            "client": (client or "").strip() or None,
            "lieu": (lieu or "").strip() or None,
            "derniere_mesure": derniere.isoformat() if derniere else None,
            "minutes": minutes,
            "statut": statut,
        })

    return {
        "automates": automates,
        "compteurs": compteurs,
        "total": len(automates),
        "seuils": {"ok_min": _PARC_SEUIL_OK_MIN, "alerte_min": _PARC_SEUIL_ALERTE_MIN},
    }


@app.get("/parc/etat/{nom_automate}/derniere")
def parc_derniere_donnee(request: Request,
                         nom_automate: str,
                         jours: int = Query(30, ge=1, le=90)):
    """Date de la derniere mesure d'UN automate, sur une fenetre bornee.

    Appelee a la demande (clic sur un automate muet), jamais en masse : la
    fenetre est bornee a 90 jours max pour rester dans les clous cote perf.
    """
    _require_admin_or_super(request)

    row = executer_requete_sql_one(
        """
        SELECT max(horodatage)
        FROM   mesures
        WHERE  trim(nom_automate) = %s
          AND  horodatage > {maintenant} - (%s || ' days')::interval;
        """.format(maintenant=_PARC_MAINTENANT),
        (nom_automate.strip(), jours),
    )

    derniere = row[0] if row else None
    return {
        "nom_automate": nom_automate,
        "derniere_mesure": derniere.isoformat() if derniere else None,
        "fenetre_jours": jours,
        # None + trouve=False : aucune donnee sur la fenetre, pas "jamais de donnee"
        "trouve": derniere is not None,
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8011"))
    uvicorn.run(app, host="0.0.0.0", port=port)
