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
    ("POST", "/canaux/email/entrant"),   # webhook du fournisseur e-mail : protégé par EMAIL_ENTRANT_SECRET
}
_ADMIN_ONLY_ROUTES = {
    ("POST", "/automate"),
    ("PUT", "/automate/{nom_automate}"),
    ("DELETE", "/automate/{nom_automate}"),
    ("GET", "/clients/{client_id}/contacts"),
    ("POST", "/clients/{client_id}/contacts"),
    ("DELETE", "/clients/{client_id}/contacts/{contact_id}"),
    ("GET", "/conversations"),
    ("POST", "/conversations"),
    ("GET", "/conversations/{conversation_id}/messages"),
    ("POST", "/conversations/{conversation_id}/messages"),
    ("POST", "/messages/{message_id}/panne"),
    ("POST", "/sav/fiches/proposer"),
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

# Relais d'envoi d'e-mails par Google Apps Script (compte Google d'Eaukey).
# Render (offre gratuite) bloque les ports SMTP sortants : aiosmtplib vers
# smtp.gmail.com échoue par délai dépassé. Le relais est appelé en HTTPS (autorisé) ;
# le script envoie l'e-mail depuis le Gmail d'Eaukey (adresse @eaukey.net, pas de spam).
# Code du script et mise en place : kit migrations_pending/2026-10-23_email_google/.
# Sans EMAIL_RELAIS_URL + EMAIL_RELAIS_SECRET, on garde l'envoi SMTP.
_EMAIL_RELAIS_URL = os.getenv("EMAIL_RELAIS_URL", "").strip()
_EMAIL_RELAIS_SECRET = os.getenv("EMAIL_RELAIS_SECRET", "").strip()
_EMAIL_RELAIS_NOM = os.getenv("EMAIL_RELAIS_NOM", "Eaukey")


def _mail_configure() -> bool:
    """Un moyen d'envoi est-il configuré (relais Google ou SMTP) ?"""
    return bool(_EMAIL_RELAIS_URL and _EMAIL_RELAIS_SECRET) or bool(_SMTP_HOST and _SMTP_USER and _SMTP_PASSWORD)


def _mail_parties(msg) -> tuple[str, str]:
    """(texte, html) d'un message MIMEText / MIMEMultipart."""
    texte, html_ = "", ""
    for part in msg.walk():
        if part.is_multipart():
            continue
        ctype = part.get_content_type()
        charset = part.get_content_charset() or "utf-8"
        contenu = part.get_payload(decode=True)
        contenu = contenu.decode(charset, "replace") if isinstance(contenu, bytes) else (contenu or "")
        if ctype == "text/plain" and not texte:
            texte = contenu
        elif ctype == "text/html" and not html_:
            html_ = contenu
    return texte, html_


async def _mail_envoyer(msg) -> None:
    """Envoie le message par le relais Google s'il est configuré, sinon en SMTP. Lève en cas d'échec."""
    if _EMAIL_RELAIS_URL and _EMAIL_RELAIS_SECRET:
        import httpx

        texte, html_ = _mail_parties(msg)
        payload = {
            "secret": _EMAIL_RELAIS_SECRET,
            "to": msg["To"],
            "subject": msg["Subject"],
            "text": texte,
            "html": html_,
            "replyTo": msg["Reply-To"] or "",
            "name": _EMAIL_RELAIS_NOM,
        }
        # Une application web Apps Script répond par une redirection vers googleusercontent.com.
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), follow_redirects=True) as client:
            r = await client.post(_EMAIL_RELAIS_URL, json=payload)
        try:
            data = r.json()
        except ValueError:
            data = {}
        if r.status_code != 200 or not data.get("ok"):
            raise RuntimeError(f"relais e-mail : HTTP {r.status_code}, {data.get('error') or 'réponse invalide'}")
        return
    await aiosmtplib.send(
        msg, hostname=_SMTP_HOST, port=_SMTP_PORT,
        username=_SMTP_USER, password=_SMTP_PASSWORD, start_tls=True,
    )
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
    if not _mail_configure():
        _AUTH_LOGGER.warning("E-mail sending not configured (EMAIL_RELAIS_* or SMTP_*), cannot send reset email")
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
        await _mail_envoyer(msg)
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
    return _pannes_types_liste()


def _pannes_types_liste() -> list:
    """Types de pannes connus (défauts + types déjà saisis), aussi donnés à l'agent SAV."""
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
    return _panne_inserer(p, email or "", _clean_opt(p.intervenant) or email or None)


def _panne_inserer(p: PanneIn, created_by: str, intervenant: Optional[str]) -> dict:
    """Validation + insertion d'une panne (POST /pannes et agent SAV)."""
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
            created_by,
            _clean_opt(p.cause),
            _clean_opt(p.action_realisee),
            _clean_opt(p.pieces_changees),
            p.deplacement,
            intervenant,
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


# ---------------------------------------------------------------------------
# ENDPOINTS ALERTES (ADMIN-ONLY) — tâches A1/A2, migrations/010_alertes.sql
# ---------------------------------------------------------------------------
# Les alertes sont écrites par l'API d'ingestion App Engine (POST /api/alertes,
# envoyé par les automates) : une ligne par station + code, les répétitions dans
# la fenêtre de regroupement incrémentent `occurrences`. Ici, l'équipe les
# consulte, les classe (panne, seuil, information, maintenance), suit leur
# statut (nouvelle, vue, traitee, ignoree) et peut en faire une panne
# (POST /alertes/{id}/panne, même logique que POST /pannes).

_ALERTE_STATUTS = ("nouvelle", "vue", "traitee", "ignoree")
_ALERTE_CLASSES = ("panne", "seuil", "information", "maintenance")
_ALERTE_NIVEAUX = ("info", "avertissement", "critique")

_ALERTE_COLS = (
    "a.id, a.nom_automate, am.client, am.lieu, a.code, a.libelle, a.niveau, a.classe, a.statut, "
    "a.valeur, a.unite, a.occurrences, a.premiere_at, a.derniere_at, a.recue_at, a.panne_id, "
    "a.commentaire, a.traitee_par, a.traitee_at"
)
_ALERTE_FROM = (
    "FROM alertes a LEFT JOIN LATERAL (SELECT client, lieu FROM automate "
    "WHERE automate.nom_automate = a.nom_automate LIMIT 1) am ON TRUE"
)


def _alerte_to_dict(r) -> dict:
    def _fmt(v):
        return v.isoformat() if hasattr(v, "isoformat") else v

    return {
        "id": r[0],
        "nom_automate": r[1],
        "client": r[2],
        "lieu": r[3],
        "code": r[4],
        "libelle": r[5],
        "niveau": r[6],
        "classe": r[7],
        "statut": r[8],
        "valeur": r[9],
        "unite": r[10],
        "occurrences": r[11],
        "premiere_at": _fmt(r[12]),
        "derniere_at": _fmt(r[13]),
        "recue_at": _fmt(r[14]),
        "panne_id": r[15],
        "commentaire": r[16],
        "traitee_par": r[17],
        "traitee_at": _fmt(r[18]),
    }


def _alerte_liste_param(valeur: Optional[str], autorises: tuple, nom: str) -> list:
    if not valeur:
        return []
    items = [v.strip().lower() for v in valeur.split(",") if v.strip()]
    for v in items:
        if v not in autorises:
            raise HTTPException(status_code=400, detail=f"{nom} invalide : {v}")
    return items


def _alerte_get(alerte_id: int):
    return executer_requete_sql_one(f"SELECT {_ALERTE_COLS} {_ALERTE_FROM} WHERE a.id = %s;", (alerte_id,))


@app.get("/alertes")
def lister_alertes(
    request: Request,
    statut: Optional[str] = Query(None, description="Un ou plusieurs statuts séparés par des virgules"),
    classe: Optional[str] = Query(None, description="Une ou plusieurs classes séparées par des virgules"),
    niveau: Optional[str] = Query(None, description="Un ou plusieurs niveaux séparés par des virgules"),
    nom_automate: Optional[str] = Query(None),
    client: Optional[str] = Query(None),
    du: Optional[date] = Query(None, description="Dernière occurrence à partir de ce jour (heure de Paris)"),
    au: Optional[date] = Query(None, description="Première occurrence jusqu'à ce jour inclus (heure de Paris)"),
    limit: int = Query(300, ge=1, le=1000),
):
    _require_admin(request)
    where, params = [], []
    for col, val, autorises, nom in (
        ("a.statut", statut, _ALERTE_STATUTS, "statut"),
        ("a.classe", classe, _ALERTE_CLASSES, "classe"),
        ("a.niveau", niveau, _ALERTE_NIVEAUX, "niveau"),
    ):
        items = _alerte_liste_param(val, autorises, nom)
        if items:
            where.append(f"{col} = ANY(%s)")
            params.append(items)
    if nom_automate:
        where.append("a.nom_automate = %s")
        params.append(nom_automate)
    if client:
        where.append("lower(am.client) = lower(%s)")
        params.append(client)
    if du:
        where.append("a.derniere_at >= (%s::date::timestamp AT TIME ZONE 'Europe/Paris')")
        params.append(du)
    if au:
        where.append("a.premiere_at < ((%s::date + 1)::timestamp AT TIME ZONE 'Europe/Paris')")
        params.append(au)
    sql = f"SELECT {_ALERTE_COLS} {_ALERTE_FROM}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY a.derniere_at DESC, a.id DESC LIMIT %s;"
    params.append(limit)
    return [_alerte_to_dict(r) for r in executer_requete_sql(sql, tuple(params))]


@app.get("/alertes/compteurs")
def compteurs_alertes(request: Request):
    """Compteurs pour le badge du menu et les onglets de la page Alertes."""
    _require_admin(request)
    rows = executer_requete_sql("SELECT statut, classe, count(*) FROM alertes GROUP BY statut, classe;")
    par_statut = {s: 0 for s in _ALERTE_STATUTS}
    nouvelles_par_classe = {c: 0 for c in _ALERTE_CLASSES}
    for statut, classe, n in rows:
        par_statut[statut] = par_statut.get(statut, 0) + n
        if statut == "nouvelle":
            nouvelles_par_classe[classe] = nouvelles_par_classe.get(classe, 0) + n
    return {
        "nouvelles": par_statut.get("nouvelle", 0),
        "a_traiter": par_statut.get("nouvelle", 0) + par_statut.get("vue", 0),
        "par_statut": par_statut,
        "nouvelles_par_classe": nouvelles_par_classe,
    }


class AlertesVuesIn(BaseModel):
    ids: Optional[List[int]] = Field(None, max_length=1000)


@app.post("/alertes/vues")
def marquer_alertes_vues(body: AlertesVuesIn, request: Request):
    """Passe en « vue » les alertes nouvelles (toutes, ou seulement `ids`)."""
    _require_admin(request)
    if body.ids is not None:
        rows = executer_requete_sql(
            "UPDATE alertes SET statut = 'vue', updated_at = now() "
            "WHERE statut = 'nouvelle' AND id = ANY(%s) RETURNING id;",
            (body.ids,),
        )
    else:
        rows = executer_requete_sql(
            "UPDATE alertes SET statut = 'vue', updated_at = now() WHERE statut = 'nouvelle' RETURNING id;"
        )
    return {"status": "ok", "modifiees": len(rows)}


@app.get("/alertes/{alerte_id}")
def detail_alerte(alerte_id: int, request: Request):
    _require_admin(request)
    row = _alerte_get(alerte_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Alerte introuvable")
    out = _alerte_to_dict(row)
    occ = executer_requete_sql(
        "SELECT horodatage, niveau, valeur, recue_at FROM alertes_occurrences "
        "WHERE alerte_id = %s ORDER BY horodatage DESC LIMIT 100;",
        (alerte_id,),
    )
    out["historique"] = [
        {"horodatage": h.isoformat(), "niveau": n, "valeur": v, "recue_at": r.isoformat()} for h, n, v, r in occ
    ]
    out["panne"] = None
    if out["panne_id"]:
        p = executer_requete_sql_one(f"SELECT {_PANNE_COLS} FROM pannes WHERE id = %s;", (out["panne_id"],))
        out["panne"] = _panne_to_dict(p) if p else None
    out["diagnostic"] = _alerte_diagnostic(alerte_id, out["code"], out["libelle"])
    return out


def _alerte_diagnostic(alerte_id: int, code: str, libelle: Optional[str]) -> dict:
    """Diagnostic probable d'une alerte (A3), sans modèle : ce que l'historique en dit.

    - causes : causes des pannes clôturées déjà liées à une alerte de MÊME code (toutes
      stations), les plus fréquentes d'abord — ce que les techniciens ont constaté ;
    - fiches : fiches de la Base SAV qui contiennent TOUS les mots du libellé (recherche
      stricte : un libellé court comme « Température haute » ne doit pas ramener
      « Pression filtre haute » sur le seul mot « haute »).
    Ne bloque jamais le détail de l'alerte (liste vide en cas d'erreur).
    """
    causes, fiches = [], []
    try:
        rows = executer_requete_sql(
            """
            SELECT lower(btrim(p.cause)) AS cle, min(btrim(p.cause)) AS cause, count(*) AS n,
                   max(p.date_fin) AS derniere,
                   (array_agg(p.action_realisee ORDER BY p.date_fin DESC)
                      FILTER (WHERE coalesce(btrim(p.action_realisee), '') <> ''))[1] AS action
              FROM alertes a JOIN pannes p ON p.id = a.panne_id
             WHERE a.code = %s AND a.id <> %s
               AND p.date_fin IS NOT NULL AND coalesce(btrim(p.cause), '') <> ''
             GROUP BY 1
             ORDER BY n DESC, derniere DESC
             LIMIT 3;
            """,
            (code, alerte_id),
        )
        causes = [
            {"cause": cause, "occurrences": int(n), "action": action,
             "derniere": derniere.isoformat() if hasattr(derniere, "isoformat") else derniere}
            for _cle, cause, n, derniere, action in rows
        ]
    except Exception as e:
        logging.getLogger("alertes").warning("Diagnostic (causes) indisponible : %s", e)
    try:
        if libelle:
            rows = executer_requete_sql(
                f"""
                SELECT id, titre, solution
                  FROM sav_fiches, (SELECT plainto_tsquery('french', %s) AS q) tsq
                 WHERE NOT archivee AND recherche @@ q
                   -- brouillons de l'assistant (colonne de la migration 014) exclus, avant comme après la 014
                   AND NOT coalesce((to_jsonb(sav_fiches) ->> 'brouillon')::boolean, false)
                 ORDER BY ts_rank(recherche, q) DESC, updated_at DESC
                 LIMIT 3;
                """,
                (libelle,),
            )
            fiches = [
                {"id": fid, "titre": titre,
                 "extrait": (" ".join((solution or "").split()))[:220]}
                for fid, titre, solution in rows
            ]
    except Exception as e:
        logging.getLogger("alertes").warning("Diagnostic (fiches) indisponible : %s", e)
    return {"causes": causes, "fiches": fiches}


class AlerteMajIn(BaseModel):
    statut: Optional[str] = None
    classe: Optional[str] = None
    commentaire: Optional[str] = Field(None, max_length=2000)


@app.patch("/alertes/{alerte_id}")
def maj_alerte(alerte_id: int, body: AlerteMajIn, request: Request):
    email = _require_admin(request)
    sets, params = [], []
    if body.statut is not None:
        statut = body.statut.strip().lower()
        if statut not in _ALERTE_STATUTS:
            raise HTTPException(status_code=400, detail="statut invalide")
        sets.append("statut = %s")
        params.append(statut)
        if statut in ("traitee", "ignoree"):
            sets.append("traitee_par = %s, traitee_at = now()")
            params.append(email or None)
        else:
            sets.append("traitee_par = NULL, traitee_at = NULL")
    if body.classe is not None:
        classe = body.classe.strip().lower()
        if classe not in _ALERTE_CLASSES:
            raise HTTPException(status_code=400, detail="classe invalide")
        sets.append("classe = %s")
        params.append(classe)
    if body.commentaire is not None:
        sets.append("commentaire = %s")
        params.append(_clean_opt(body.commentaire))
    if not sets:
        raise HTTPException(status_code=400, detail="Rien à modifier")
    row = executer_requete_sql_one(
        f"UPDATE alertes SET {', '.join(sets)}, updated_at = now() WHERE id = %s RETURNING id;",
        tuple(params) + (alerte_id,),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Alerte introuvable")
    return _alerte_to_dict(_alerte_get(alerte_id))


@app.post("/alertes/{alerte_id}/panne")
def alerte_vers_panne(alerte_id: int, p: PanneIn, request: Request):
    """Crée une panne à partir d'une alerte (même logique que POST /pannes) et lie l'alerte.

    Le formulaire de panne du front est pré-rempli avec la station, le libellé et
    l'horodatage de l'alerte ; l'utilisateur peut tout modifier sauf la station.
    """
    email = _require_admin(request)
    row = _alerte_get(alerte_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Alerte introuvable")
    alerte = _alerte_to_dict(row)
    if alerte["panne_id"]:
        raise HTTPException(status_code=409, detail=f"Cette alerte est déjà liée à la panne {alerte['panne_id']}")
    if str(p.nom_automate) != str(alerte["nom_automate"]):
        raise HTTPException(status_code=400, detail="La panne doit concerner la station de l'alerte")
    panne = creer_panne(p, request)
    lie = executer_requete_sql_one(
        "UPDATE alertes SET panne_id = %s, classe = 'panne', statut = 'traitee', traitee_par = %s, "
        "traitee_at = now(), updated_at = now() WHERE id = %s AND panne_id IS NULL RETURNING id;",
        (panne["id"], email or None, alerte_id),
    )
    if lie is None:
        # Course avec un autre utilisateur : on retire la panne qui vient d'être créée.
        executer_requete_sql("DELETE FROM pannes WHERE id = %s;", (panne["id"],))
        raise HTTPException(status_code=409, detail="Cette alerte vient d'être liée à une autre panne")
    return {"panne": panne, "alerte": _alerte_to_dict(_alerte_get(alerte_id))}


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
    canal: Optional[str] = None
    panne_id: Optional[int] = None
    panne_suggeree: Optional[dict] = None

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
        SELECT id, client_id, sender_id, content, file_url, created_at, is_read, escalade, escalade_motif, canal,
               panne_id, panne_suggeree
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
            "canal": r[9] or "app",
            "panne_id": r[10],
            # Panne détectée, station à préciser : note pour l'équipe, jamais renvoyée au client.
            "panne_suggeree": r[11] if is_admin else None,
        }
        for r in rows
    ]

@app.post("/messages/{client_id}", response_model=MessageOut)
def post_message_for_client(client_id: str, message: MessageIn, request: Request,
                            background_tasks: BackgroundTasks):
    """Ajoute un message pour un client.

    Réponse d'un admin à un client qui a écrit par e-mail : elle part aussi par
    e-mail, en tâche de fond (voir bloc « Canal e-mail »).
    """
    is_admin = _is_admin_user(_require_auth(request))
    if not is_admin and message.sender_id in ADMIN_SENDER_IDS:
        message.sender_id = CLIENT_SENDER_IDS[0]
    query = """
        INSERT INTO messages (client_id, sender_id, content, file_url, is_read) 
        VALUES (%s, %s, %s, %s, false) 
        RETURNING id, created_at
    """
    row = executer_requete_sql_one(query, (client_id, message.sender_id, message.content, message.file_url))
    if not row:
        raise HTTPException(status_code=500, detail="Failed to insert message")
    if is_admin and message.sender_id in ADMIN_SENDER_IDS and (message.content or "").strip():
        background_tasks.add_task(_email_relayer_reponse_equipe, client_id, row[0], message.content)

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

class MessagePanneIn(BaseModel):
    panne_id: int


@app.post("/messages/{message_id}/panne")
def lier_message_panne(message_id: int, payload: MessagePanneIn, request: Request):
    """Lie une panne (créée par l'équipe depuis une « panne suggérée ») à la réponse de
    l'assistant qui l'a détectée, et efface la suggestion (admin)."""
    _require_admin(request)
    msg = executer_requete_sql_one("SELECT client_id FROM messages WHERE id = %s;", (message_id,))
    if msg is None:
        raise HTTPException(status_code=404, detail="Message introuvable")
    panne = executer_requete_sql_one("SELECT client FROM pannes WHERE id = %s;", (payload.panne_id,))
    if panne is None:
        raise HTTPException(status_code=404, detail="Panne introuvable")
    if str(panne[0] or "").strip().lower() != str(msg[0] or "").strip().lower():
        raise HTTPException(status_code=400, detail="La panne ne concerne pas ce client")
    executer_requete_sql(
        "UPDATE messages SET panne_id = %s, panne_suggeree = NULL WHERE id = %s;",
        (payload.panne_id, message_id),
    )
    return {"status": "ok", "id": message_id, "panne_id": payload.panne_id}


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


# =============================================================================
# PROBLEMES FREQUENTS (tâche S4 « Recommandations aux équipes », page /problemes)
# =============================================================================
# Rapport interne (admin) : pannes, escalades de l'assistant SAV et alertes les
# plus fréquentes sur une période (7, 30 ou 90 jours, défaut 30), comparées à la
# période précédente de même durée, + recommandations produites par des règles
# simples et explicables (aucune IA). Lecture seule, aucune migration.
#   GET /rapport/problemes?jours=30        -> JSON (page /problemes)
#   GET /rapport/problemes/texte?jours=30  -> texte brut à coller dans un e-mail
# La table `alertes` (migrations/010_alertes.sql) peut ne pas encore exister :
# la section alertes est alors renvoyée vide avec disponible = false.
# Suite prévue : envoi régulier du texte par un agent Wardian planifié.
from datetime import timedelta, timezone
from zoneinfo import ZoneInfo
from fastapi.responses import PlainTextResponse

_ADMIN_ONLY_ROUTES.update({
    ("GET", "/rapport/problemes"),
    ("GET", "/rapport/problemes/texte"),
})

_PB_TOP = 5                    # longueur des listes « top »
_PB_STATION_MIN = 2            # station affichée à partir de 2 pannes
_PB_RECO_CAUSE_MIN = 2         # cause répétée -> fiche SAV
_PB_RECO_STATION_MIN = 3       # station -> visite préventive
_PB_RECO_MOTIF_MIN = 2         # motif d'escalade répété -> enrichir l'agent / la Base SAV
_PB_RECO_ALERTE_MIN = 10       # occurrences d'un code d'alerte sans panne -> revoir le seuil
_PB_RECO_HAUSSE_MIN = 3        # hausse des pannes signalée à partir de 3 pannes et +50 %
_PB_TZ = ZoneInfo("Europe/Paris")


def _pb_propre(texte) -> str:
    """Texte nettoyé pour l'affichage (espaces multiples réduits)."""
    return re.sub(r"\s+", " ", str(texte or "")).strip()


def _pb_cle(texte) -> str:
    """Clé de regroupement : minuscules, sans ponctuation, espaces réduits."""
    t = re.sub(r"[^\w\s]|_", " ", str(texte or "").lower())
    return re.sub(r"\s+", " ", t).strip()


def _pb_top(valeurs, n: int = _PB_TOP) -> list:
    """Regroupe des textes libres (casse / espaces / ponctuation ignorés).

    Libellé affiché = l'écriture la plus fréquente du groupe.
    """
    groupes = {}
    for v in valeurs:
        cle = _pb_cle(v)
        if not cle:
            continue
        g = groupes.setdefault(cle, {"nombre": 0, "ecritures": {}})
        g["nombre"] += 1
        e = _pb_propre(v)
        g["ecritures"][e] = g["ecritures"].get(e, 0) + 1
    res = []
    for g in groupes.values():
        libelle = max(g["ecritures"].items(), key=lambda kv: (kv[1], -len(kv[0])))[0]
        res.append({"libelle": libelle, "nombre": g["nombre"]})
    res.sort(key=lambda x: (-x["nombre"], x["libelle"].casefold()))
    return res[:n] if n else res


def _pb_variation(actuel: int, precedent: int) -> dict:
    pct = None if not precedent else round((actuel - precedent) * 100.0 / precedent)
    return {"actuel": actuel, "precedent": precedent, "ecart": actuel - precedent, "pct": pct}


def _pb_part(n: int, total: int):
    return None if not total else round(n * 100.0 / total)


def _pb_table_existe(nom: str) -> bool:
    row = executer_requete_sql_one("SELECT to_regclass(%s) IS NOT NULL;", (f"public.{nom}",))
    return bool(row and row[0])


def _pb_stations_noms() -> dict:
    """nom_automate (trimé) -> (client, lieu). trim() : espaces en trop dans automate."""
    rows = executer_requete_sql("SELECT trim(nom_automate), client, lieu FROM automate;")
    return {r[0]: (_pb_propre(r[1]) or None, _pb_propre(r[2]) or None) for r in rows if r and r[0]}


def _pb_fiche_couvre(texte: str) -> bool:
    """Une fiche SAV active couvre-t-elle ce texte (tous ses mots significatifs) ?"""
    row = executer_requete_sql_one(
        "SELECT count(*) FROM sav_fiches WHERE NOT archivee AND recherche @@ plainto_tsquery('french', %s) "
        # brouillons de l'assistant (colonne de la migration 014) exclus, avant comme après la 014
        "AND NOT coalesce((to_jsonb(sav_fiches) ->> 'brouillon')::boolean, false);",
        (texte,),
    )
    return bool(row and row[0])


def _pb_section_pannes(debut, debut_prec, fin, noms) -> dict:
    rows = executer_requete_sql(
        """
        SELECT id, client, lieu, trim(nom_automate), panne, cause, pieces_changees, deplacement,
               date_debut::timestamptz, date_fin::timestamptz
          FROM pannes
         WHERE date_debut::timestamptz >= %s OR date_fin::timestamptz >= %s OR date_fin IS NULL
         ORDER BY date_debut, id;
        """,
        (debut_prec, debut_prec),
    )
    signalees, signalees_prec, resolues, resolues_prec, en_cours = [], 0, [], 0, 0
    for r in rows:
        dd, df = r[8], r[9]
        if df is None:
            en_cours += 1
        if dd is not None and debut <= dd <= fin:
            signalees.append(r)
        elif dd is not None and debut_prec <= dd < debut:
            signalees_prec += 1
        if df is not None and debut <= df <= fin:
            resolues.append(r)
        elif df is not None and debut_prec <= df < debut:
            resolues_prec += 1

    # Stations : pannes signalées dans la période
    par_station = {}
    for r in signalees:
        nom = r[3] or ""
        s = par_station.setdefault(nom, {"nom_automate": nom, "client": None, "lieu": None, "nombre": 0, "types": []})
        client, lieu = noms.get(nom, (None, None))
        s["client"] = client or _pb_propre(r[1]) or None
        s["lieu"] = lieu or _pb_propre(r[2]) or None
        s["nombre"] += 1
        s["types"].append(r[4])
    stations = []
    for s in par_station.values():
        if s["nombre"] >= _PB_STATION_MIN:
            s["types"] = [t["libelle"] for t in _pb_top(s["types"], 3)]
            stations.append(s)
    stations.sort(key=lambda s: (-s["nombre"], (s["lieu"] or s["nom_automate"]).casefold()))

    # Résolution : pannes résolues dans la période
    durees = [(r[9] - r[8]).total_seconds() / 3600.0 for r in resolues if r[8] is not None and r[9] >= r[8]]
    renseignees = [r for r in resolues if r[7] is not None]
    avec_depl = sum(1 for r in renseignees if r[7])
    pieces = []
    for r in resolues:
        for p in re.split(r"[,;\n/+]|\bet\b", r[6] or ""):
            if _pb_cle(p):
                pieces.append(p)

    return {
        "signalees": _pb_variation(len(signalees), signalees_prec),
        "resolues": _pb_variation(len(resolues), resolues_prec),
        "en_cours": en_cours,
        "top_types": _pb_top([r[4] for r in signalees]),
        "top_causes": _pb_top([r[5] for r in resolues]),
        "toutes_causes": _pb_top([r[5] for r in resolues], 0),
        "causes_renseignees": sum(1 for r in resolues if _pb_cle(r[5])),
        "stations": stations[:10],
        "delai_moyen_heures": round(sum(durees) / len(durees), 1) if durees else None,
        "delai_base": len(durees),
        "deplacement": {"avec": avec_depl, "renseignees": len(renseignees), "pct": _pb_part(avec_depl, len(renseignees))},
        "top_pieces": _pb_top(pieces),
    }


def _pb_section_escalades(debut, debut_prec, fin) -> dict:
    # messages.created_at est un timestamp en prod (vérifié le 2026-10-21) ; le passage par
    # ::text garde la requête valable si la colonne est du texte, et écarte les valeurs non datées.
    rows = executer_requete_sql(
        """
        SELECT client_id, escalade_motif, quand
          FROM (SELECT client_id, escalade_motif,
                       CASE WHEN created_at::text ~ '^\\d{4}-\\d{2}-\\d{2}' THEN created_at::timestamptz END AS quand
                  FROM messages
                 WHERE escalade AND sender_id = -1) m
         WHERE quand >= %s;
        """,
        (debut_prec,),
    )
    periode = [r for r in rows if debut <= r[2] <= fin]
    prec = sum(1 for r in rows if debut_prec <= r[2] < debut)
    return {
        "nombre": _pb_variation(len(periode), prec),
        "top_motifs": _pb_top([r[1] for r in periode]),
        "sans_motif": sum(1 for r in periode if not _pb_cle(r[1])),
        "clients": len({r[0] for r in periode if r[0]}),
    }


def _pb_section_alertes(debut, debut_prec, fin, noms) -> dict:
    vide = {"disponible": False, "nombre": _pb_variation(0, 0), "occurrences": 0, "top_codes": [],
            "stations": [], "devenues_pannes": {"nombre": 0, "total": 0, "pct": None}}
    if not _pb_table_existe("alertes"):
        return vide
    # Alertes actives dans la période = dernière occurrence dans la période.
    rows = executer_requete_sql(
        """
        SELECT trim(nom_automate), code, libelle, occurrences, derniere_at, panne_id
          FROM alertes
         WHERE derniere_at >= %s
         ORDER BY derniere_at, id;
        """,
        (debut_prec,),
    )
    periode = [r for r in rows if debut <= r[4] <= fin]
    prec = sum(1 for r in rows if debut_prec <= r[4] < debut)

    codes = {}
    for nom, code, libelle, occ, _, panne_id in periode:
        c = codes.setdefault(code, {"code": code, "libelles": [], "alertes": 0, "occurrences": 0,
                                    "pannes": 0, "stations": set()})
        c["libelles"].append(libelle)
        c["alertes"] += 1
        c["occurrences"] += occ or 0
        c["pannes"] += 1 if panne_id else 0
        c["stations"].add(nom)
    top_codes = []
    for c in codes.values():
        top = _pb_top(c.pop("libelles"), 1)
        c["libelle"] = top[0]["libelle"] if top else c["code"]
        c["stations"] = len(c["stations"])
        top_codes.append(c)
    top_codes.sort(key=lambda c: (-c["occurrences"], str(c["code"])))

    stations = {}
    for nom, code, _, occ, _, _ in periode:
        client, lieu = noms.get(nom, (None, None))
        s = stations.setdefault(nom, {"nom_automate": nom, "client": client, "lieu": lieu,
                                      "alertes": 0, "occurrences": 0})
        s["alertes"] += 1
        s["occurrences"] += occ or 0
    stations = sorted(stations.values(), key=lambda s: (-s["occurrences"], s["nom_automate"]))

    pannes = sum(1 for r in periode if r[5])
    return {
        "disponible": True,
        "nombre": _pb_variation(len(periode), prec),
        "occurrences": sum(r[3] or 0 for r in periode),
        "top_codes": top_codes[:_PB_TOP],
        "tous_codes": top_codes,
        "stations": stations[:_PB_TOP],
        "devenues_pannes": {"nombre": pannes, "total": len(periode), "pct": _pb_part(pannes, len(periode))},
    }


def _pb_recommandations(jours: int, pannes: dict, escalades: dict, alertes: dict) -> list:
    recos = []

    def ajouter(regle, priorite, texte, pourquoi, chiffres):
        recos.append({"regle": regle, "priorite": priorite, "texte": texte,
                      "pourquoi": pourquoi, "chiffres": chiffres})

    # 1. Cause répétée sans fiche SAV qui la couvre
    fiches_ok = _pb_table_existe("sav_fiches")
    for c in pannes.get("toutes_causes", []):
        if c["nombre"] < _PB_RECO_CAUSE_MIN:
            continue
        if fiches_ok and _pb_fiche_couvre(c["libelle"]):
            continue
        ajouter("fiche_sav", "haute" if c["nombre"] >= 4 else "moyenne",
                f"Créer une fiche dans la Base SAV pour la cause « {c['libelle']} ».",
                f"Cette cause revient {c['nombre']} fois dans les pannes résolues sur {jours} jours "
                "et aucune fiche SAV ne la couvre.",
                {"cause": c["libelle"], "pannes": c["nombre"], "jours": jours})

    # 2. Station souvent en panne
    for s in pannes["stations"]:
        if s["nombre"] < _PB_RECO_STATION_MIN:
            continue
        nom = " — ".join(x for x in (s["client"], s["lieu"]) if x) or s["nom_automate"]
        ajouter("visite_preventive", "haute" if s["nombre"] >= 5 else "moyenne",
                f"Prévoir une visite préventive à {nom}.",
                f"La station a eu {s['nombre']} pannes en {jours} jours"
                + (f" ({', '.join(s['types'])})." if s["types"] else "."),
                {"nom_automate": s["nom_automate"], "pannes": s["nombre"], "jours": jours})

    # 3. Motif d'escalade répété
    for m in escalades["top_motifs"]:
        if m["nombre"] < _PB_RECO_MOTIF_MIN:
            continue
        ajouter("agent_sav", "haute" if m["nombre"] >= 5 else "moyenne",
                f"Enrichir les instructions de l'assistant SAV ou la Base SAV sur « {m['libelle']} ».",
                f"L'assistant a passé la main à l'équipe {m['nombre']} fois pour ce motif en {jours} jours.",
                {"motif": m["libelle"], "escalades": m["nombre"], "jours": jours})

    # 4. Code d'alerte qui se répète sans jamais devenir une panne
    for c in alertes.get("tous_codes", []):
        if c["occurrences"] < _PB_RECO_ALERTE_MIN or c["pannes"]:
            continue
        ajouter("seuil_alerte", "haute" if c["occurrences"] >= 50 else "moyenne",
                f"Revoir le seuil de l'alerte {c['code']} ({c['libelle']}).",
                f"{c['occurrences']} occurrences sur {c['stations']} station(s) en {jours} jours, "
                "sans qu'aucune ne devienne une panne.",
                {"code": c["code"], "occurrences": c["occurrences"], "stations": c["stations"], "jours": jours})

    # 5. Hausse nette des pannes
    v = pannes["signalees"]
    if v["actuel"] >= _PB_RECO_HAUSSE_MIN and v["pct"] is not None and v["pct"] >= 50:
        ajouter("hausse_pannes", "moyenne",
                "Les pannes sont en nette hausse : faire un point d'équipe sur les causes.",
                f"{v['actuel']} pannes signalées contre {v['precedent']} sur les {jours} jours précédents (+{v['pct']} %).",
                {"actuel": v["actuel"], "precedent": v["precedent"], "pct": v["pct"], "jours": jours})

    ordre = {"haute": 0, "moyenne": 1}
    recos.sort(key=lambda r: ordre[r["priorite"]])
    return recos


def rapport_problemes(jours: int) -> dict:
    fin = datetime.now(timezone.utc)
    debut = fin - timedelta(days=jours)
    debut_prec = debut - timedelta(days=jours)
    noms = _pb_stations_noms()
    pannes = _pb_section_pannes(debut, debut_prec, fin, noms)
    escalades = _pb_section_escalades(debut, debut_prec, fin)
    alertes = _pb_section_alertes(debut, debut_prec, fin, noms)
    recos = _pb_recommandations(jours, pannes, escalades, alertes)
    pannes.pop("toutes_causes", None)
    alertes.pop("tous_codes", None)
    return {
        "jours": jours,
        "debut": debut.isoformat(),
        "fin": fin.isoformat(),
        "precedente": {"debut": debut_prec.isoformat(), "fin": debut.isoformat()},
        "recommandations": recos,
        "pannes": pannes,
        "escalades": escalades,
        "alertes": alertes,
    }


def _pb_fmt_var(v: dict) -> str:
    if v["pct"] is None:
        return f"{v['actuel']} (période précédente : {v['precedent']})"
    signe = "+" if v["pct"] > 0 else ""
    return f"{v['actuel']} (période précédente : {v['precedent']}, {signe}{v['pct']} %)"


def _pb_fmt_delai(h) -> str:
    if h is None:
        return "non calculable"
    if h < 48:
        return f"{str(round(h, 1)).replace('.', ',')} h"
    return f"{str(round(h / 24, 1)).replace('.', ',')} jours"


def rapport_problemes_texte(r: dict) -> str:
    d = datetime.fromisoformat(r["debut"]).astimezone(_PB_TZ).strftime("%d/%m/%Y")
    f = datetime.fromisoformat(r["fin"]).astimezone(_PB_TZ).strftime("%d/%m/%Y")
    j = r["jours"]
    L = [f"PROBLÈMES FRÉQUENTS — {j} derniers jours (du {d} au {f})",
         f"Comparaison avec les {j} jours précédents.", ""]

    L.append("RECOMMANDATIONS")
    if not r["recommandations"]:
        L.append("Aucune recommandation sur la période.")
    for i, reco in enumerate(r["recommandations"], 1):
        L.append(f"{i}. [{reco['priorite'].upper()}] {reco['texte']}")
        L.append(f"   Pourquoi : {reco['pourquoi']}")
    L.append("")

    def top(titre, items, unite=""):
        L.append(f"{titre} :")
        if not items:
            L.append("  (aucun)")
        for it in items:
            L.append(f"  - {it['libelle']} : {it['nombre']}{unite}")

    p = r["pannes"]
    L.append("PANNES")
    L.append(f"- Signalées : {_pb_fmt_var(p['signalees'])}")
    L.append(f"- Résolues : {_pb_fmt_var(p['resolues'])}")
    L.append(f"- En cours aujourd'hui : {p['en_cours']}")
    L.append(f"- Délai moyen de résolution : {_pb_fmt_delai(p['delai_moyen_heures'])}"
             + (f" (sur {p['delai_base']} pannes)" if p["delai_base"] else ""))
    dep = p["deplacement"]
    if dep["renseignees"]:
        L.append(f"- Avec déplacement : {dep['pct']} % ({dep['avec']} sur {dep['renseignees']} renseignées)")
    else:
        L.append("- Avec déplacement : non renseigné")
    top("Types les plus fréquents", p["top_types"])
    top("Causes les plus fréquentes", p["top_causes"])
    top("Pièces les plus changées", p["top_pieces"])
    L.append(f"Stations les plus touchées (au moins {_PB_STATION_MIN} pannes) :")
    if not p["stations"]:
        L.append("  (aucune)")
    for s in p["stations"]:
        nom = " — ".join(x for x in (s["client"], s["lieu"]) if x) or s["nom_automate"]
        L.append(f"  - {nom} ({s['nom_automate']}) : {s['nombre']} pannes"
                 + (f" ({', '.join(s['types'])})" if s["types"] else ""))
    L.append("")

    e = r["escalades"]
    L.append("ESCALADES DE L'ASSISTANT SAV")
    L.append(f"- Nombre : {_pb_fmt_var(e['nombre'])}")
    L.append(f"- Clients concernés : {e['clients']}")
    top("Motifs les plus fréquents", e["top_motifs"])
    L.append("")

    a = r["alertes"]
    L.append("ALERTES DES AUTOMATES")
    if not a["disponible"]:
        L.append("Pas encore disponible (les alertes ne sont pas encore remontées dans l'appli).")
    else:
        L.append(f"- Alertes actives : {_pb_fmt_var(a['nombre'])}, {a['occurrences']} occurrences")
        dp = a["devenues_pannes"]
        L.append(f"- Devenues des pannes : {dp['nombre']} sur {dp['total']}"
                 + (f" ({dp['pct']} %)" if dp["pct"] is not None else ""))
        L.append("Codes les plus fréquents :")
        if not a["top_codes"]:
            L.append("  (aucun)")
        for c in a["top_codes"]:
            L.append(f"  - {c['code']} {c['libelle']} : {c['occurrences']} occurrences "
                     f"({c['stations']} station(s), {c['pannes']} panne(s))")
        L.append("Stations les plus bruyantes :")
        if not a["stations"]:
            L.append("  (aucune)")
        for s in a["stations"]:
            nom = " — ".join(x for x in (s["client"], s["lieu"]) if x) or s["nom_automate"]
            L.append(f"  - {nom} ({s['nom_automate']}) : {s['occurrences']} occurrences")
    L.append("")
    L.append("Rapport généré par la plateforme Eaukey (page Problèmes fréquents).")
    return "\n".join(L)


@app.get("/rapport/problemes")
def get_rapport_problemes(request: Request, jours: int = Query(30, ge=1, le=365)):
    _require_admin(request)
    return rapport_problemes(jours)


@app.get("/rapport/problemes/texte", response_class=PlainTextResponse)
def get_rapport_problemes_texte(request: Request, jours: int = Query(30, ge=1, le=365)):
    _require_admin(request)
    return PlainTextResponse(rapport_problemes_texte(rapport_problemes(jours)),
                             headers={"Cache-Control": "no-store"})


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
# Photos a verifier (super admin) : file de relecture des predictions IA
# ---------------------------------------------------------------------------
# Une photo par station et par heure (les photos d'une meme heure sont quasi identiques),
# jamais une heure deja validee. Trois motifs, dans cet ordre de priorite d'affichage :
#   - "alerte"      : le modele signale un probleme (bac vide, MES, indice de qualite bas) ;
#   - "incertain"   : le modele hesite (probabilite bac vide ou MES proche de 0,5) ;
#   - "echantillon" : ~10 % des heures tirees au hasard (regle fixe sur l'id), notees A L'AVEUGLE
#                     par l'admin (le front cache la prediction) : mesure honnete de l'erreur du modele.
# L'echantillon est tire avant les autres motifs : une photo d'alerte peut en faire partie.
# La validation passe par PUT /eau/photo/{id}/validation (route existante).
_VERIF_INDICE_ALERTE = 5.0     # indice (qualite + 10 - opacite) / 2 en dessous duquel on alerte
_VERIF_ECHANTILLON_PCT = 10    # part des heures tirees au hasard
# Regle d'echantillonnage (melange de l'id, stable dans le temps) : meme regle en SQL partout.
_VERIF_SQL_ECHANTILLON = "((u.id::bigint * 2654435761) %% 100) < %s"


def _verif_motif_sql() -> str:
    return f"""CASE
        WHEN {_VERIF_SQL_ECHANTILLON} THEN 'echantillon'
        WHEN u.pred_bac_vide_prob >= %s
          OR u.pred_matiere_prob >= 0.5
          OR (u.pred_qualite_eau + 10 - COALESCE(u.pred_opacite, 10 - u.pred_qualite_eau)) / 2 <= %s THEN 'alerte'
        WHEN u.pred_bac_vide_prob BETWEEN 0.3 AND 0.7
          OR u.pred_matiere_prob BETWEEN 0.3 AND 0.7 THEN 'incertain'
      END"""


@app.get("/eau/a_verifier")
def eau_a_verifier(request: Request,
                   jours: int = Query(30, ge=1, le=365, description="Photos des N derniers jours"),
                   limit: int = Query(20, ge=1, le=100)):
    """File des photos a verifier (super admin). JSON : {photos:[...], restantes}.
    Pour chaque photo : motif (alerte / incertain / echantillon), station, predictions."""
    _require_super_admin(request)
    query = f"""
      WITH relues AS (   -- heures (station, heure) dont au moins une photo est deja validee
        SELECT DISTINCT numero_automate, date_trunc('hour', timestamp) AS h
        FROM urls_images
        WHERE validated_at IS NOT NULL AND timestamp >= now() - make_interval(days => %s) - interval '1 hour'
      ), heures AS (
        SELECT DISTINCT ON (u.numero_automate, date_trunc('hour', u.timestamp))
               u.id, u.url, u.timestamp, u.numero_automate, u.pred_qualite_eau, u.pred_opacite,
               u.pred_matiere_prob, u.pred_bac_vide_prob, u.pred_modele
        FROM urls_images u
        LEFT JOIN relues r ON r.numero_automate = u.numero_automate AND r.h = date_trunc('hour', u.timestamp)
        WHERE u.timestamp >= now() - make_interval(days => %s)
          AND u.url IS NOT NULL
          AND u.pred_qualite_eau IS NOT NULL
          AND u.validated_at IS NULL
          AND r.h IS NULL
        ORDER BY u.numero_automate, date_trunc('hour', u.timestamp), u.timestamp DESC
      ), motifs AS (
        SELECT u.*, {_verif_motif_sql()} AS motif FROM heures u
      )
      SELECT m.id, m.url, m.timestamp, m.numero_automate, a.nom_automate, a.client, a.lieu,
             m.pred_qualite_eau, m.pred_opacite, m.pred_matiere_prob, m.pred_bac_vide_prob, m.pred_modele,
             m.motif, count(*) OVER () AS restantes
      FROM motifs m
      LEFT JOIN automate a ON a.nom_automate = m.numero_automate::text || '.0'
      WHERE m.motif IS NOT NULL
      ORDER BY CASE m.motif WHEN 'alerte' THEN 0 WHEN 'incertain' THEN 1 ELSE 2 END, m.timestamp DESC
      LIMIT %s;
    """
    rows = executer_requete_sql(query, (jours, jours, _VERIF_ECHANTILLON_PCT, _EAU_BAC_VIDE_SEUIL,
                                        _VERIF_INDICE_ALERTE, limit))
    f = lambda v: float(v) if v is not None else None
    photos = [{
        "id": r[0], "url": r[1], "timestamp": r[2].isoformat() if r[2] is not None else None,
        "numero_automate": r[3], "nom_automate": r[4] or (f"{r[3]}.0" if r[3] is not None else None),
        "client": r[5], "lieu": r[6],
        "pred_qualite": f(r[7]), "pred_opacite": f(r[8]),
        "pred_matiere": f(r[9]) * 10 if r[9] is not None else None,   # echelle /10 comme /eau/photos
        "bac_vide_prob": f(r[10]), "modele": r[11], "motif": r[12],
    } for r in rows]
    return {"photos": photos, "restantes": int(rows[0][13]) if rows else 0}


@app.get("/eau/a_verifier/stats")
def eau_a_verifier_stats(request: Request, jours: int = Query(90, ge=7, le=365)):
    """Suivi de la relecture (super admin) : photos validees sur 7 jours, et erreur moyenne du
    modele sur l'echantillon note a l'aveugle (ecart entre la note humaine et la prediction)."""
    _require_super_admin(request)
    rows = executer_requete_sql(f"""
      SELECT count(*) FILTER (WHERE u.validated_at >= now() - interval '7 days'),
             count(*) FILTER (WHERE {_VERIF_SQL_ECHANTILLON} AND u.corr_qualite_eau IS NOT NULL),
             AVG(abs(u.corr_qualite_eau - u.pred_qualite_eau)) FILTER (WHERE {_VERIF_SQL_ECHANTILLON}),
             AVG(abs(u.corr_opacite - u.pred_opacite)) FILTER (WHERE {_VERIF_SQL_ECHANTILLON})
      FROM urls_images u
      WHERE u.validated_at >= now() - make_interval(days => %s)
        AND u.pred_qualite_eau IS NOT NULL;
    """, (_VERIF_ECHANTILLON_PCT, _VERIF_ECHANTILLON_PCT, _VERIF_ECHANTILLON_PCT, jours))
    r = rows[0] if rows else (0, 0, None, None)
    return {
        "validees_7j": int(r[0] or 0),
        "echantillon_notees": int(r[1] or 0),
        "erreur_qualite": round(float(r[2]), 2) if r[2] is not None else None,
        "erreur_opacite": round(float(r[3]), 2) if r[3] is not None else None,
        "jours": jours,
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
#     prévenue par e-mail (une fois tant qu'aucun admin n'a répondu) ;
#   - quand l'échange révèle une vraie panne, l'agent ajoute [PANNE: type | constat] :
#     le marqueur est retiré, la panne est créée sur la station du client (ou liée à
#     la même panne encore ouverte) et rattachée au message (messages.panne_id) ; si la
#     station ne peut pas être déterminée, une « panne suggérée » est notée sur le
#     message pour l'équipe (messages.panne_suggeree, migrations/014).
# Flux SSE renvoyé au front : message (question enregistrée), token (texte au
# fil de l'eau, marqueur retiré), puis saved (réponse enregistrée, texte
# définitif) ou humain / error.

_CHAT_AGENT_PAUSE_H = int(os.getenv("CHAT_AGENT_PAUSE_H", "2"))
_SAV_ESCALADE_EMAIL = os.getenv("SAV_ESCALADE_EMAIL", "contact@eaukey.net")
_CHAT_LOGGER = logging.getLogger("chat_agent")
_ESCALADE_RE = re.compile(r"\[\s*ESCALADE\s*:?\s*([^\]]*)\]\s*", re.IGNORECASE)
# Le deux-points est obligatoire : « [Pannes fréquentes](lien) » n'est pas un marqueur.
_PANNE_RE = re.compile(r"\[\s*PANNE\s*:\s*([^\]]*)\]\s*", re.IGNORECASE)
# Débuts possibles d'un marqueur (sans espaces, en majuscules) : retenus pendant le flux.
_CHAT_MARQUEURS = ("[ESCALADE", "[PANNE:")
_PANNE_AGENT_AUTEUR = "assistant-sav"
_PANNE_AGENT_SUFFIXE = " (signalé par le client via l'assistant SAV)"
_PANNE_DOUBLON_H = 48       # même type, même station, encore ouverte : on lie au lieu de recréer
_PANNE_LIEU_FENETRE_H = 24  # messages du client relus pour trouver la station citée
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
Panne : si l'échange révèle une VRAIE panne de la machine (équipement en défaut, casse, fuite, alarme qui persiste) — pas une simple question, une explication ou un réglage —, ajoute en tête de ta réponse une ligne exacte « [PANNE: type | constat court] » (avant ou après la ligne ESCALADE s'il y en a une). Choisis le type dans cette liste, écrit à l'identique : <TYPES_PANNES> ; si aucun ne convient, écris « Autre ». Le constat décrit en une phrase ce que le client observe. Ne signale pas deux fois la même panne dans la conversation. Ce marqueur sert à l'équipe : ne promets jamais au client qu'une intervention ou un technicien est planifié.
[FIN DE LA CONSIGNE]"""

# Ligne ajoutée à la consigne quand le client a écrit par e-mail.
_CHAT_AGENT_CONSIGNE_EMAIL = (
    "Canal : e-mail. L'utilisateur t'a écrit par e-mail et lira ta réponse dans sa boîte mail, "
    "sans l'application : fais une réponse autonome, compréhensible sans le fil précédent, "
    "en texte simple (paragraphes courts, listes à tirets ; pas de tableau, de titre ni de mise en forme markdown complexe)."
)


def _chat_types_pannes_texte() -> str:
    try:
        types = _pannes_types_liste()
    except Exception as e:
        _CHAT_LOGGER.warning("Types de pannes indisponibles : %s", e)
        types = []
    return " ; ".join(types) or "Autre"


def _chat_agent_message(message: str, canal: str = "app") -> str:
    consigne = _CHAT_AGENT_CONSIGNE.replace("<TYPES_PANNES>", _chat_types_pannes_texte())
    if canal == "email":
        consigne = consigne.replace("[FIN DE LA CONSIGNE]", f"{_CHAT_AGENT_CONSIGNE_EMAIL}\n[FIN DE LA CONSIGNE]")
    return f"{consigne}\n\n{_sav_message_avec_contexte(message)}"


def _chat_extraire_escalade(texte: str) -> tuple[str, Optional[str]]:
    """Retire le marqueur [ESCALADE: motif] (où qu'il soit) ; renvoie (texte, motif ou None)."""
    m = _ESCALADE_RE.search(texte)
    if not m:
        return texte.strip(), None
    motif = (m.group(1) or "").strip()[:300] or "non précisé"
    return _ESCALADE_RE.sub("", texte).strip(), motif


def _chat_extraire_panne(texte: str) -> tuple[str, Optional[dict]]:
    """Retire le marqueur [PANNE: type | constat] (où qu'il soit) ; renvoie (texte, {type, constat} ou None)."""
    m = _PANNE_RE.search(texte)
    if not m:
        return texte.strip(), None
    type_panne, _, constat = (m.group(1) or "").partition("|")
    panne = {"type": " ".join(type_panne.split())[:200], "constat": " ".join(constat.split())[:500]}
    return _PANNE_RE.sub("", texte).strip(), panne


def _chat_marqueur_incomplet(fragment: str) -> bool:
    """Vrai si `fragment` (qui commence par « [ ») peut encore devenir un marqueur [ESCALADE…] / [PANNE: …]."""
    if "]" in fragment or len(fragment) >= 400:
        return False
    tete = "".join(fragment.split()).upper()
    return any(m.startswith(tete) or tete.startswith(m) for m in _CHAT_MARQUEURS)


def _chat_texte_diffusable(texte: str) -> str:
    """Partie de la réponse montrable pendant le flux : marqueurs complets retirés, et
    marqueur peut-être en cours d'écriture (fin du texte) retenu jusqu'à son « ] »."""
    i = texte.rfind("[")
    if i >= 0 and _chat_marqueur_incomplet(texte[i:]):
        texte = texte[:i]
    return _PANNE_RE.sub("", _ESCALADE_RE.sub("", texte)).lstrip()


def _chat_type_panne(propose: str, constat: str, question: str) -> tuple[str, str]:
    """Type connu (sinon « Autre », le type proposé passe dans le constat) et constat non vide."""
    connus = {t.casefold(): t for t in _pannes_types_liste()}
    type_panne = connus.get((propose or "").strip().casefold())
    if type_panne is None:
        if propose and propose.strip().casefold() != "autre":
            constat = f"{propose.strip()} : {constat}" if constat else propose.strip()
        type_panne = "Autre"
    if not constat:
        constat = " ".join((question or "").split())[:300] or type_panne
    return type_panne, constat


def _chat_sans_accents(texte: str) -> str:
    import unicodedata
    t = unicodedata.normalize("NFKD", texte or "")
    return "".join(c for c in t if not unicodedata.combining(c)).casefold()


def _chat_station_panne(client_id: str) -> Optional[tuple]:
    """(client, lieu, nom_automate) de la station concernée, ou None si on ne peut pas savoir.

    Une seule station pour ce client : c'est elle. Plusieurs : on relit les messages du
    client des dernières 24 h, du plus récent au plus ancien ; le premier qui cite un nom
    de lieu (ou d'automate) désigne la station s'il n'en désigne qu'une (sinon : ambigu).
    """
    stations = [
        s for s in executer_requete_sql(
            "SELECT client, lieu, nom_automate FROM automate "
            "WHERE lower(trim(client)) = lower(trim(%s)) ORDER BY nom_automate;",
            (client_id,),
        )
        if s and s[1] and s[2]
    ]
    if len(stations) <= 1:
        return stations[0] if stations else None
    noms = []  # (nom normalisé, station)
    for s in stations:
        for nom in (s[1], s[2]):
            n = _chat_sans_accents(" ".join(str(nom).split()))
            if len(n) >= 3:
                noms.append((n, s))
    msgs = executer_requete_sql(
        """
        SELECT content FROM messages
        WHERE client_id = %s AND sender_id = ANY(%s)
          AND created_at::timestamptz > now() - make_interval(hours => %s)
        ORDER BY created_at::timestamptz DESC, id DESC;
        """,
        (client_id, list(CLIENT_SENDER_IDS), _PANNE_LIEU_FENETRE_H),
    )
    for (content,) in msgs:
        texte = _chat_sans_accents(content or "")
        trouves = [(n, s) for n, s in noms if re.search(rf"(?<!\w){re.escape(n)}(?!\w)", texte)]
        # « Aix » ne compte pas quand « Aix Nord » est cité.
        trouves = [(n, s) for n, s in trouves if not any(n != m and n in m for m, _ in trouves)]
        retenues = {s[2]: s for _, s in trouves}
        if retenues:
            return next(iter(retenues.values())) if len(retenues) == 1 else None
    return None


def _chat_traiter_panne(client_id: str, message_id: int, panne: dict, question: str) -> Optional[int]:
    """Panne détectée par l'agent : création (ou panne ouverte identique) liée au message,
    sinon « panne suggérée » notée sur le message pour l'équipe. Ne lève jamais."""
    try:
        type_panne, constat = _chat_type_panne(panne.get("type", ""), panne.get("constat", ""), question)
        station = _chat_station_panne(client_id)
        panne_id = None
        if station is not None:
            client, lieu, nom_automate = station
            row = executer_requete_sql_one(
                """
                SELECT id FROM pannes
                WHERE nom_automate = %s AND date_fin IS NULL AND lower(trim(panne)) = lower(%s)
                  AND date_debut >= now() - make_interval(hours => %s)
                ORDER BY id DESC LIMIT 1;
                """,
                (nom_automate, type_panne, _PANNE_DOUBLON_H),
            )
            if row is not None:
                panne_id = row[0]
            else:
                try:
                    cree = _panne_inserer(
                        PanneIn(client=client, lieu=lieu, nom_automate=nom_automate, panne=type_panne,
                                probleme=constat + _PANNE_AGENT_SUFFIXE,
                                date_debut=datetime.now(dt.timezone.utc), partage_sav=False),
                        _PANNE_AGENT_AUTEUR, None,
                    )
                    panne_id = cree["id"]
                except HTTPException as e:
                    _CHAT_LOGGER.warning("Panne non créée (client %s) : %s", client_id, e.detail)
        if panne_id is not None:
            executer_requete_sql(
                "UPDATE messages SET panne_id = %s, panne_suggeree = NULL WHERE id = %s;", (panne_id, message_id)
            )
            return panne_id
        executer_requete_sql(
            "UPDATE messages SET panne_suggeree = %s::jsonb WHERE id = %s;",
            (json.dumps({"type": type_panne, "constat": constat}, ensure_ascii=False), message_id),
        )
    except Exception as e:
        _CHAT_LOGGER.error("Traitement de la panne détectée en echec (client %s) : %s", client_id, e)
    return None


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
                          escalade: bool = False, motif: Optional[str] = None,
                          canal: str = "app", adresse: Optional[str] = None):
    return executer_requete_sql_one(
        """
        INSERT INTO messages (client_id, sender_id, content, is_read, escalade, escalade_motif,
                              canal, canal_adresse)
        VALUES (%s, %s, %s, false, %s, %s, %s, %s)
        RETURNING id, created_at;
        """,
        (client_id, sender_id, content, escalade, motif, canal, adresse),
    )


async def _chat_prevenir_equipe(client_id: str, motif: str, question: str, reponse: str) -> None:
    """E-mail à l'équipe SAV. Ne lève jamais : l'alerte dans l'appli reste visible."""
    if not _mail_configure() or not _SAV_ESCALADE_EMAIL:
        _CHAT_LOGGER.warning("Envoi d'e-mail non configure (EMAIL_RELAIS_* ou SMTP_*) : escalade %s non envoyee", client_id)
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
        await _mail_envoyer(msg)
    except Exception as e:
        _CHAT_LOGGER.error("Echec e-mail d'escalade (client %s) : %s", client_id, e)


def _wardian_trame(frame: str) -> tuple:
    """(événement, données) d'une trame SSE Wardian ; (None, None) pour un commentaire / keepalive."""
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
        return None, None
    return ev, data


def _sse(event: str, data: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


class ChatAgentIn(BaseModel):
    content: str = Field(..., min_length=1, max_length=4000)


async def _chat_agent_evenements(client_id: str, content: str, canal: str = "app",
                                 adresse: Optional[str] = None):
    """Cœur de l'agent SAV dans le fil d'un client, partagé par les canaux (/chat, e-mail).

    La question du client est déjà enregistrée dans messages. Produit des couples
    (événement, données) : token (texte au fil de l'eau, marqueur retiré), puis
    saved (réponse enregistrée, texte définitif) ou humain / error. L'escalade
    (message marqué + e-mail à l'équipe) est gérée ici, pour tous les canaux.
    `canal` / `adresse` : canal de la réponse (consigne de l'agent, messages.canal).
    """
    import httpx

    # Un humain de l'équipe suit le fil : l'agent se tait.
    if _chat_humain_actif(client_id):
        yield ("humain", {"detail": "Un membre de l'équipe Eaukey suit votre conversation."})
        return
    if not _WARDIAN_API_KEY or not _WARDIAN_AGENT_ID:
        yield ("error", {"message": _CHAT_AGENT_INDISPO})
        return

    url = f"{_WARDIAN_API_BASE}/api/v1/agents/{_WARDIAN_AGENT_ID}/messages"
    headers = {"Authorization": f"Bearer {_WARDIAN_API_KEY}", "Content-Type": "application/json"}
    message = _chat_agent_message(content, canal)
    conv_id = _chat_conv_get(client_id)
    texte = ""        # réponse complète de l'agent
    diffuse = ""      # texte (marqueurs retirés) déjà transmis au front

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
                        yield ("error", {"message": _CHAT_AGENT_INDISPO})
                        return
                    buf = ""
                    async for chunk in up.aiter_text():
                        buf += chunk
                        *frames, buf = buf.split("\n\n")
                        for frame in frames:
                            ev, data = _wardian_trame(frame)
                            if not ev:
                                continue  # ": keepalive" et commentaires
                            if ev in ("conversation", "done") and data.get("conversation_id"):
                                if data["conversation_id"] != conv_id:
                                    conv_id = data["conversation_id"]
                                    _chat_conv_set(client_id, conv_id)
                            elif ev == "token":
                                texte += data.get("delta") or ""
                                # Marqueurs [ESCALADE…] / [PANNE: …] jamais montrés, même découpés en tokens.
                                visible = _chat_texte_diffusable(texte)
                                if len(visible) > len(diffuse) and visible.startswith(diffuse):
                                    yield ("token", {"delta": visible[len(diffuse):]})
                                    diffuse = visible
                            elif ev == "error":
                                _CHAT_LOGGER.warning("Wardian error (client %s) : %s", client_id, data)
                                yield ("error", {"message": _CHAT_AGENT_INDISPO})
                                return
                break
            except httpx.HTTPError as e:
                _CHAT_LOGGER.warning("Wardian injoignable (client %s) : %s", client_id, e)
                yield ("error", {"message": _CHAT_AGENT_INDISPO})
                return

    reponse, motif = _chat_extraire_escalade(texte)
    reponse, panne = _chat_extraire_panne(reponse)
    if not reponse:
        yield ("error", {"message": _CHAT_AGENT_INDISPO})
        return
    escalade = motif is not None
    deja_signale = escalade and _chat_escalade_en_attente(client_id)
    saved = _chat_inserer_message(client_id, AGENT_SENDER_ID, reponse, escalade, motif,
                                 canal, adresse)
    if panne is not None and saved:
        # Rien de nouveau pour le client : la panne (ou la suggestion) est visible de l'équipe.
        _chat_traiter_panne(client_id, saved[0], panne, content)
    yield ("saved", {
        "id": saved[0] if saved else None,
        "created_at": _iso(saved[1]) if saved else None,
        "content": reponse,
        "escalade": escalade,
    })
    if escalade and not deja_signale:
        await _chat_prevenir_equipe(client_id, motif, content, reponse)


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

    from fastapi.responses import StreamingResponse

    async def _flux():
        yield _sse("message", {"id": row[0], "created_at": _iso(row[1])})
        async for event, data in _chat_agent_evenements(client_id, content):
            yield _sse(event, data)

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
    "created_by, updated_by, created_at, updated_at, brouillon, source_client_id"
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
    # Brouillon proposé par l'assistant (migrations/014). PUT : absent = inchangé, false = publier.
    brouillon: Optional[bool] = None


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
        "brouillon": bool(r[12]),
        "source_client_id": r[13],
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
        f"SELECT {_SAV_FICHES_COLS} FROM sav_fiches ORDER BY brouillon DESC, archivee, updated_at DESC, id DESC;"
    )
    return [_sav_fiche_to_dict(r) for r in rows]


@app.post("/sav/fiches")
def creer_sav_fiche(f: SavFicheIn, request: Request):
    email = _require_admin(request) or ""
    row = executer_requete_sql_one(
        f"""
        INSERT INTO sav_fiches (titre, symptomes, solution, pieces, type_panne, mots_cles, archivee, created_by,
                                brouillon)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING {_SAV_FICHES_COLS};
        """,
        _sav_fiche_params(f) + (email, bool(f.brouillon)),
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
               mots_cles = %s, archivee = %s, updated_by = %s, updated_at = now(),
               brouillon = COALESCE(%s, brouillon)
         WHERE id = %s
        RETURNING {_SAV_FICHES_COLS};
        """,
        _sav_fiche_params(f) + (email, f.brouillon, fiche_id),
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


# ---------------------------------------------------------------------------
# Fiches proposées par l'assistant (brouillons) : POST /sav/fiches/proposer
# ---------------------------------------------------------------------------
# Un admin demande, depuis une conversation résolue, un brouillon de fiche à
# l'agent Wardian (appel non streamé, nouvelle conversation à chaque fois, sous
# un external_user_id dédié). Le brouillon n'est JAMAIS joint aux questions des
# clients (_sav_contexte exclut les brouillons) tant qu'un admin ne l'a pas publié.
# Suite prévue : proposition automatique à la clôture d'une conversation.

_SAV_REDACTION_USER = "eaukey-redaction-fiches"
_SAV_PROPOSITION_NB_MESSAGES = 30
_SAV_REDACTION_CONSIGNE = """[CONSIGNE RÉDACTION DE FICHE SAV — tâche interne pour l'équipe Eaukey : aucun client ne lira ta réponse]
Voici un échange de la messagerie SAV Eaukey entre un client, l'assistant SAV et l'équipe Eaukey. Rédige à partir de cet échange une fiche de dépannage réutilisable pour d'autres clients.
Règles :
- généralise : décris le problème et la solution pour n'importe quelle station, pas pour ce client ;
- retire tout nom de client, de lieu, de station, d'automate ou de personne, toute adresse, e-mail ou numéro de téléphone ;
- ne garde que les étapes réellement données dans l'échange (par l'assistant ou l'équipe) : n'invente aucune étape, valeur ni pièce ;
- si l'échange ne contient aucune solution exploitable, mets "solution" à "" (chaîne vide).
Réponds UNIQUEMENT par un objet JSON, sans texte autour, de la forme :
{"titre": "le problème en quelques mots", "symptomes": "ce que voit le client", "solution": "étapes numérotées, une par ligne", "pieces": "pièces concernées, ou chaîne vide", "type_panne": "un type de cette liste, ou chaîne vide", "mots_cles": "autres façons de décrire le problème, séparées par des virgules"}
Types de panne : <TYPES_PANNES>
[FIN DE LA CONSIGNE]"""


class SavPropositionIn(BaseModel):
    client_id: str = Field(..., min_length=1, max_length=200)


async def _wardian_reponse_complete(message: str, external_user_id: str) -> str:
    """Appel à l'agent Wardian sans flux : agrège les tokens SSE (nouvelle conversation).
    Lève HTTPException 503 (non configuré) ou 502 (injoignable / erreur)."""
    import httpx

    if not _WARDIAN_API_KEY or not _WARDIAN_AGENT_ID:
        raise HTTPException(status_code=503, detail="Assistant SAV non configure")
    url = f"{_WARDIAN_API_BASE}/api/v1/agents/{_WARDIAN_AGENT_ID}/messages"
    headers = {"Authorization": f"Bearer {_WARDIAN_API_KEY}", "Content-Type": "application/json"}
    body = {"message": message, "external_user_id": external_user_id, "conversation_id": None}
    texte = ""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0, connect=10.0)) as client:
            async with client.stream("POST", url, headers=headers, json=body) as up:
                if up.status_code != 200:
                    _CHAT_LOGGER.warning("Wardian HTTP %s (%s)", up.status_code, external_user_id)
                    raise HTTPException(status_code=502, detail="Assistant SAV indisponible")
                buf = ""
                async for chunk in up.aiter_text():
                    buf += chunk
                    *frames, buf = buf.split("\n\n")
                    for frame in frames:
                        ev, data = _wardian_trame(frame)
                        if ev == "token":
                            texte += data.get("delta") or ""
                        elif ev == "error":
                            _CHAT_LOGGER.warning("Wardian error (%s) : %s", external_user_id, data)
                            raise HTTPException(status_code=502, detail="Assistant SAV indisponible")
    except httpx.HTTPError as e:
        _CHAT_LOGGER.warning("Wardian injoignable (%s) : %s", external_user_id, e)
        raise HTTPException(status_code=502, detail="Assistant SAV injoignable")
    return texte


def _sav_extraire_json_fiche(texte: str) -> Optional[dict]:
    """Objet JSON de la réponse de l'agent : bloc ```json, ou du premier « { » au dernier « } »."""
    texte = texte or ""
    candidats = [m.group(1) for m in re.finditer(r"```(?:json)?\s*(.*?)```", texte, re.S | re.I)]
    debut, fin = texte.find("{"), texte.rfind("}")
    if debut >= 0 and fin > debut:
        candidats.append(texte[debut:fin + 1])
    for c in candidats:
        try:
            d = json.loads(c.strip())
        except ValueError:
            continue
        if isinstance(d, dict):
            return d
    return None


def _sav_champ_propose(v, max_len: int, sep: str = ", ") -> Optional[str]:
    if isinstance(v, (list, tuple)):
        v = sep.join(str(x).strip() for x in v if x is not None and str(x).strip())
    if v is None or isinstance(v, (dict, bool)):
        return None
    v = str(v).strip()
    return v[:max_len] or None


@app.post("/sav/fiches/proposer")
async def proposer_sav_fiche(payload: SavPropositionIn, request: Request):
    """Brouillon de fiche SAV rédigé par l'agent Wardian à partir des 30 derniers messages
    d'un fil /chat (admin). Enregistré `brouillon` : à relire et publier dans /base-sav."""
    _require_admin(request)
    client_id = payload.client_id.strip()
    rows = executer_requete_sql(
        """
        SELECT sender_id, content FROM messages
        WHERE client_id = %s AND coalesce(trim(content), '') <> ''
        ORDER BY created_at::timestamptz DESC, id DESC
        LIMIT %s;
        """,
        (client_id, _SAV_PROPOSITION_NB_MESSAGES),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Aucun message dans cette conversation")

    # Noms de clients / lieux / stations masqués avant envoi (l'agent doit aussi généraliser).
    pattern = _sav_noms_regex()
    lignes = []
    for sender_id, content in reversed(rows):
        qui = ("Assistant SAV" if sender_id == AGENT_SENDER_ID
               else "Équipe Eaukey" if sender_id in ADMIN_SENDER_IDS else "Client")
        lignes.append(f"{qui} : {_sav_masquer(str(content)[:2000], pattern)}")
    consigne = _SAV_REDACTION_CONSIGNE.replace("<TYPES_PANNES>", _chat_types_pannes_texte())
    message = f"{consigne}\n\n[ÉCHANGE]\n" + "\n\n".join(lignes) + "\n[FIN DE L'ÉCHANGE]"

    texte = await _wardian_reponse_complete(message, _SAV_REDACTION_USER)
    d = _sav_extraire_json_fiche(texte)
    if d is None:
        raise HTTPException(status_code=502, detail="Réponse de l'assistant illisible : aucune fiche créée")
    m = lambda v, n, sep=", ": _sav_masquer(_sav_champ_propose(v, n, sep), pattern) or None
    titre, solution = m(d.get("titre"), 200), m(d.get("solution"), 8000, "\n")
    if not titre or not solution:
        raise HTTPException(status_code=422, detail="L'échange ne contient pas de solution exploitable : aucune fiche créée")

    row = executer_requete_sql_one(
        f"""
        INSERT INTO sav_fiches (titre, symptomes, solution, pieces, type_panne, mots_cles, archivee,
                                created_by, brouillon, source_client_id)
        VALUES (%s, %s, %s, %s, %s, %s, false, %s, true, %s)
        RETURNING {_SAV_FICHES_COLS};
        """,
        (titre, m(d.get("symptomes"), 4000), solution, m(d.get("pieces"), 1000),
         m(d.get("type_panne"), 200), m(d.get("mots_cles"), 500), _PANNE_AGENT_AUTEUR, client_id),
    )
    if row is None:
        raise HTTPException(status_code=500, detail="Création du brouillon échouée")
    return _sav_fiche_to_dict(row)


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
         WHERE NOT archivee AND NOT brouillon AND recherche @@ q
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
# - Desinfection : avg_chlore = mediane horaire de mesures.chlore_mv, un potentiel
#   REDOX en millivolts (0..2000), pas une concentration : c'est ce que l'appli
#   affiche (« Chlore (mV) », taux de desinfection = mV / 3). Le rapport donne le
#   redox moyen en mV et le temps passe au-dessus du seuil de desinfection
#   (_RAPPORT_REDOX_SEUIL_MV, 300 mV = 100 % du taux de l'appli). 0 = sonde absente
#   / pas de mesure (l'ETL remplace les manques par 0) : heures exclues.
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
    ("GET", "/rapport/mensuel/lot"),
})
_RAPPORT_PRIX_M3_DEFAUT = _PRIX_EAU_M3     # EUR / m3 : même prix que les économies de l'appli (PRIX_EAU_M3), surchargeable par RAPPORT_PRIX_M3
_RAPPORT_PISCINE_OLYMPIQUE_M3 = 2500.0    # equivalence « piscine olympique »
_RAPPORT_PH_MIN, _RAPPORT_PH_MAX = 6.5, 8.5
_RAPPORT_REDOX_SEUIL_MV = 300.0           # desinfection suffisante au-dessus (= 100 % du taux de l'appli)
# --- Unites et filtres ---
_RAPPORT_PH_ECHELLE = 100.0               # avg_ph brut = pH x 100
_RAPPORT_PH_VALIDE = (2.0, 12.0)          # meme filtre que /ph/jour
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
       sum(cl_brut) FILTER (WHERE cl_ok)                              AS cl_somme,
       count(*) FILTER (WHERE cl_ok)                                  AS cl_n,
       count(*) FILTER (WHERE cl_ok AND cl_brut >= %(cl_seuil)s)      AS cl_n_plage,
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
        "cl_max": _RAPPORT_CHLORE_BRUT_MAX, "cl_seuil": _RAPPORT_REDOX_SEUIL_MV,
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
            "redox_moyen_mv": round(a["cl_somme"] / a["cl_n"]) if a["cl_n"] else None,
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

    res = {
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
            "seuil_redox_mv": _RAPPORT_REDOX_SEUIL_MV,
            "seuil_couverture_pct": _RAPPORT_SEUIL_COUVERTURE_PCT,
            "max_m3_par_heure": _RAPPORT_MAX_M3_HEURE,
            "max_kwh_par_heure": _RAPPORT_MAX_KWH_HEURE,
            "source": "mesures_aggregate (horaire)",
        },
        "genere_le": maintenant.replace(microsecond=0).isoformat(),
    }
    res.update(_rapport_analyse(res, stations, debut, fin))
    return res


# ------------------- Analyse : ce que le rapport peut affirmer -------------------
# Regles validees par Romain le 2026-10-27 (docs/RAPPORT_MENSUEL_ADAPTATIF.md) :
# - eau « fiable » pour une station = compteur renvoi > 0, compteur d'appoint > 0 et
#   appoint <= renvoi. Un appoint a zero donne un faux 100 % : la station est exclue
#   des chiffres d'eau (mais garde sa qualite d'eau) et le rapport passe « a relire ».
# - le titre de couverture est choisi par ordre de priorite (record, objectif,
#   progression...), une variation n'est donnee que si le mois precedent depasse
#   _RAPPORT_VOLUME_MIN_M3 ;
# - statut : « pret » (aucune alerte), « a_relire » (au moins une alerte), « pas_d_envoi »
#   (rien de fiable a montrer). Rien n'est envoye automatiquement.
_RAPPORT_OBJECTIF_TAUX = 80.0
_RAPPORT_VOLUME_MIN_M3 = 20.0          # en dessous : mois calme, pas de variation calculee
_RAPPORT_RECORD_MIN_M3 = 50.0
_RAPPORT_COUVERTURE_ALERTE_PCT = 70.0
_RAPPORT_QUALITE_BONNE_PCT = 90.0
_RAPPORT_QUALITE_MAUVAISE_PCT = 50.0
_RAPPORT_HISTO_MOIS = 6
_RAPPORT_EQUIV = {"douche_l": 60.0, "foyer_m3_an": 150.0, "lavage_l": 150.0, "bouteille_l": 1.5}

_RAPPORT_SQL_EAU_GRAIN = """
SELECT trim(nom_automate)                                  AS nom,
       date_trunc(%(grain)s, rounded_timestamp)            AS periode,
       COALESCE(sum(compteur_eau_renvoi)  FILTER (WHERE ok), 0) AS renvoi,
       COALESCE(sum(compteur_eau_adoucie) FILTER (WHERE ok), 0) AS adoucie
FROM (
    SELECT nom_automate, rounded_timestamp, compteur_eau_renvoi, compteur_eau_adoucie,
           COALESCE(compteur_eau_renvoi  BETWEEN 0 AND %(max_m3)s
                AND compteur_eau_adoucie BETWEEN 0 AND %(max_m3)s, false) AS ok
    FROM   mesures_aggregate
    WHERE  nom_automate = ANY(%(noms)s)
      AND  rounded_timestamp >= %(debut)s
      AND  rounded_timestamp <  %(fin)s
) x
GROUP BY 1, 2
"""


def _rapport_eau_par(stations: list, debut: dt.datetime, fin: dt.datetime, grain: str) -> dict:
    """{(station, periode naive): (renvoi, adoucie)} par mois ou par jour."""
    rows = _rapport_sql(_RAPPORT_SQL_EAU_GRAIN, {"grain": grain, "noms": _rapport_noms_sql(stations),
                                                 "debut": debut, "fin": fin, "max_m3": _RAPPORT_MAX_M3_HEURE})
    out = {}
    for nom, periode, renvoi, adoucie in rows:
        k = (nom, _rapport_naif(periode))
        r0, a0 = out.get(k, (0.0, 0.0))
        out[k] = (r0 + float(renvoi), a0 + float(adoucie))
    return out


def _rapport_eau_fiable(renvoi: float, adoucie: float) -> bool:
    return renvoi > 0 and adoucie > 0 and adoucie <= renvoi


def _rapport_analyse_station(s: dict) -> dict:
    """Fiabilite et alertes d'une station (dict de rapport_mensuel()['stations'])."""
    renvoi, adoucie = s["eau_renvoyee_m3"], s["eau_adoucie_m3"]
    q = s["qualite"]
    heures = s["heures_avec_donnees"]
    ph = bool(heures) and q["heures_mesure_ph"] >= 0.5 * heures
    redox = bool(heures) and q["heures_mesure_chlore"] >= 0.5 * heures
    q_bonne = (ph and (q["ph_pct_dans_plage"] or 0) >= _RAPPORT_QUALITE_BONNE_PCT) or \
              (redox and (q["chlore_pct_dans_plage"] or 0) >= _RAPPORT_QUALITE_BONNE_PCT)
    q_mauvaise = (ph and (q["ph_pct_dans_plage"] or 0) < _RAPPORT_QUALITE_MAUVAISE_PCT) or \
                 (redox and (q["chlore_pct_dans_plage"] or 0) < _RAPPORT_QUALITE_MAUVAISE_PCT)
    alertes = []
    if not heures:
        alertes.append("aucune donnée reçue ce mois")
    else:
        if s["couverture_pct"] is not None and s["couverture_pct"] < _RAPPORT_COUVERTURE_ALERTE_PCT:
            alertes.append(f"données reçues {_fr_nombre(s['couverture_pct'])} % du mois")
        if s["jours_sans_donnees"] > _RAPPORT_JOURS_MUETS_MAX:
            alertes.append(f"{s['jours_sans_donnees']} jours sans aucune donnée")
        if renvoi <= 0:
            alertes.append("compteur d'eau renvoyée à zéro : eau recyclée non mesurable")
        elif adoucie <= 0:
            alertes.append("compteur d'appoint à zéro : taux de 100 % douteux, eau exclue du rapport")
        elif adoucie > renvoi:
            alertes.append("appoint supérieur à l'eau renvoyée : compteurs incohérents")
        if s["heures_ecartees"]["eau"]:
            alertes.append(f"{s['heures_ecartees']['eau']} heure(s) de compteur d'eau écartée(s) (remise à zéro ?)")
        if q_mauvaise:
            alertes.append("pH ou désinfection hors plage la plupart du temps : à vérifier (sonde ou traitement)")
    return {"eau_fiable": _rapport_eau_fiable(renvoi, adoucie), "ph": ph, "redox": redox,
            "qualite_bonne": bool(q_bonne), "qualite_mauvaise": bool(q_mauvaise),
            "energie": s["energie_kwh"] > 0, "alertes": alertes}


def _rapport_analyse(r: dict, stations: list, debut: dt.datetime, fin: dt.datetime) -> dict:
    """Ajoute au rapport : analyse (statut, titre, blocs), historique 6 mois et jours du mois.
    Les chiffres d'eau de l'analyse ne comptent que les stations aux compteurs fiables."""
    prix = r["parametres"]["prix_m3_eur"]
    for s in r["stations"]:
        s["analyse"] = _rapport_analyse_station(s)
    fiables = [s for s in r["stations"] if s["analyse"]["eau_fiable"]]
    noms_fiables = {s["nom_automate"] for s in fiables}

    # Historique : 6 mois civils jusqu'au mois du rapport, stations-mois fiables seulement.
    a, m = debut.year, debut.month - (_RAPPORT_HISTO_MOIS - 1)
    while m < 1:
        a, m = a - 1, m + 12
    debut_histo = dt.datetime(a, m, 1)
    debut_annee = dt.datetime(debut.year, 1, 1)
    par_mois = _rapport_eau_par(stations, min(debut_histo, debut_annee), fin, "month")
    cumul_ren = cumul_ado = 0.0
    cumul_depuis = None
    for (nom, mm), (rv, av) in sorted(par_mois.items(), key=lambda kv: kv[0][1]):
        if mm >= debut_annee and _rapport_eau_fiable(rv, av):
            cumul_ren, cumul_ado = cumul_ren + rv, cumul_ado + av
            cumul_depuis = cumul_depuis or mm
    historique = []
    for i in range(_RAPPORT_HISTO_MOIS):
        mm = dt.datetime(a + (m - 1 + i) // 12, (m - 1 + i) % 12 + 1, 1)
        ren = ado = 0.0
        for s in stations:
            rv, av = par_mois.get((s["nom"], mm), (0.0, 0.0))
            if _rapport_eau_fiable(rv, av):
                ren, ado = ren + rv, ado + av
        historique.append({"mois": f"{mm.year:04d}-{mm.month:02d}",
                           "eau_recyclee_m3": round(ren - ado, 1) if ren else None,
                           "taux_recyclage_pct": round(100 * (ren - ado) / ren, 1) if ren else None})
    while historique and historique[0]["eau_recyclee_m3"] is None:
        historique.pop(0)

    # Jours du mois (stations fiables) pour le calendrier.
    par_jour = _rapport_eau_par([s for s in stations if s["nom"] in noms_fiables], debut, fin, "day")
    jours = []
    d = debut
    while d < fin:
        rec = sum(max(rv - av, 0.0) for (nom, jj), (rv, av) in par_jour.items() if jj == d)
        jours.append({"date": d.date().isoformat(), "eau_recyclee_m3": round(rec, 1)})
        d += dt.timedelta(days=1)

    # Chiffres d'eau fiables du mois.
    ren = sum(s["eau_renvoyee_m3"] for s in fiables)
    rec = sum(s["eau_renvoyee_m3"] - s["eau_adoucie_m3"] for s in fiables)
    taux = round(100 * rec / ren, 1) if ren else None
    prec = historique[-2]["eau_recyclee_m3"] if len(historique) >= 2 else None
    var = round(100 * (rec - prec) / prec, 1) if (fiables and prec and prec >= _RAPPORT_VOLUME_MIN_M3) else None
    prec_taux = historique[-2]["taux_recyclage_pct"] if len(historique) >= 2 and prec and prec >= _RAPPORT_VOLUME_MIN_M3 else None
    autres = [h["eau_recyclee_m3"] for h in historique[:-1] if h["eau_recyclee_m3"] is not None]
    record = bool(fiables) and len(autres) >= 2 and rec >= _RAPPORT_RECORD_MIN_M3 and rec > max(autres)
    kwh = sum(s["energie_kwh"] for s in fiables if s["analyse"]["energie"])
    rec_energie = sum(s["eau_renvoyee_m3"] - s["eau_adoucie_m3"] for s in fiables if s["analyse"]["energie"])

    avec_donnees = [s for s in r["stations"] if s["heures_avec_donnees"]]
    q_bonne = any(s["analyse"]["qualite_bonne"] for s in avec_donnees)
    q_mauvaise = any(s["analyse"]["qualite_mauvaise"] for s in avec_donnees)
    qualite = any(s["analyse"]["ph"] or s["analyse"]["redox"] for s in avec_donnees)

    if fiables and rec < _RAPPORT_VOLUME_MIN_M3:
        titre = "mois_calme"
    elif fiables and record:
        titre = "record"
    elif fiables and taux is not None and taux >= _RAPPORT_OBJECTIF_TAUX:
        titre = "objectif"
    elif fiables and var is not None and var >= 5:
        titre = "progression"
    elif fiables and var is not None and var <= -15:
        titre = "mois_calme"
    elif fiables:
        titre = "volume"
    elif q_bonne and not q_mauvaise:
        titre = "qualite"
    else:
        titre = None

    alertes = [{"nom_automate": s["nom_automate"], "lieu": s["lieu"], "alerte": al}
               for s in r["stations"] for al in s["analyse"]["alertes"]]
    statut = "pas_d_envoi" if titre is None or r["periode"]["mois_en_cours"] else ("a_relire" if alertes else "pret")
    raisons = []
    if r["periode"]["mois_en_cours"]:
        raisons.append("mois en cours : chiffres provisoires")
    if titre is None:
        raisons.append("aucune donnée fiable à montrer (ni eau recyclée, ni qualité d'eau satisfaisante)")

    objectif_suivant = None
    if taux is not None:
        objectif_suivant = _RAPPORT_OBJECTIF_TAUX if taux < _RAPPORT_OBJECTIF_TAUX else min(float(int(taux) + 2), 95.0)

    return {
        "historique": historique,
        "jours": jours,
        "analyse": {
            "statut": statut, "raisons": raisons, "titre": titre, "alertes": alertes,
            "stations_fiables": sorted(noms_fiables),
            "eau": {
                "eau_recyclee_m3": round(rec, 1), "eau_renvoyee_m3": round(ren, 1), "taux_recyclage_pct": taux,
                "economies_eur": round(rec * prix, 2), "variation_pct": var,
                "ecart_taux_points": round(taux - prec_taux, 1) if taux is not None and prec_taux is not None else None,
                "record": record, "energie_kwh": round(kwh, 1),
                "kwh_par_m3_recycle": round(kwh / rec_energie, 2) if kwh and rec_energie > 0 else None,
                "equivalences": {
                    "douches": round(rec * 1000 / _RAPPORT_EQUIV["douche_l"]),
                    "foyers_un_an": round(rec / _RAPPORT_EQUIV["foyer_m3_an"], 1),
                    "bouteilles": round(rec * 1000 / _RAPPORT_EQUIV["bouteille_l"]),
                    "lavages": round(rec * 1000 / _RAPPORT_EQUIV["lavage_l"]),
                    "piscines_olympiques": round(rec / _RAPPORT_PISCINE_OLYMPIQUE_M3, 2),
                    "reperes": dict(_RAPPORT_EQUIV),
                },
            } if fiables else None,
            "cumul_annee": {
                "depuis_mois": f"{cumul_depuis.year:04d}-{cumul_depuis.month:02d}" if cumul_depuis else None,
                "eau_recyclee_m3": round(cumul_ren - cumul_ado, 1),
                "economies_eur": round((cumul_ren - cumul_ado) * prix, 2),
                "piscines_olympiques": round((cumul_ren - cumul_ado) / _RAPPORT_PISCINE_OLYMPIQUE_M3, 1),
            },
            "objectif_taux": _RAPPORT_OBJECTIF_TAUX,
            "objectif_suivant_taux": objectif_suivant,
            "blocs": {
                "eau": bool(fiables),
                "evolution": bool(fiables) and sum(1 for h in historique if h["eau_recyclee_m3"] is not None) >= 2,
                "energie": bool(kwh),
                "stations": len(r["stations"]) > 1,
                "qualite": qualite,
                "calendrier": bool(fiables) and any(j["eau_recyclee_m3"] > 0 for j in jours),
                "interventions": True,
            },
        },
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
:root{color-scheme:dark;--abysse:#0a2425;--bassin:#10393a;--bassin-2:#174a4b;--ligne:#245657;--eau:#5cc3c2;
--eau-fonce:#2a8f8e;--ecume:#eef7f7;--brume:#a9caca;--corail:#f39a78;--hausse:#7fd9a4;--baisse:#f3a1a1;
--f:'Jost','Futura','Century Gothic','Avenir Next','Segoe UI',sans-serif;--r:18px}
*{box-sizing:border-box}
html,body{background:var(--abysse)}
body{margin:0;color:var(--ecume);font-family:var(--f);font-size:16px;line-height:1.5;padding:0 16px 48px}
h1,h2,h3,p{margin:0}h1,h2{font-weight:500;text-wrap:balance}
.num{font-variant-numeric:tabular-nums}
.wrap{max-width:900px;margin:0 auto}
.f{padding:44px 0;border-bottom:1px solid var(--ligne);display:flex;flex-direction:column;gap:28px}
.f:last-child{border-bottom:0}
.sur{font-size:12.5px;letter-spacing:.18em;text-transform:uppercase;color:var(--brume);font-weight:500}
.h2{font-size:clamp(28px,4.2vw,40px);line-height:1.15}.h2 em,h1 em{font-style:normal;color:var(--eau)}
.entete{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;padding-top:28px}
.marque{display:flex;align-items:center;gap:10px;color:var(--eau);font-size:24px}
.periode{text-align:right;font-size:13px;letter-spacing:.16em;text-transform:uppercase;color:var(--brume)}
.periode b{display:block;color:var(--ecume);font-size:15px;letter-spacing:.12em;font-weight:600}
.couv{display:grid;grid-template-columns:minmax(0,.9fr) minmax(0,1.1fr);gap:40px;align-items:center}
.goutte{width:100%;max-width:300px;justify-self:center}.goutte svg{width:100%;height:auto;display:block}
.couv h1{font-size:clamp(32px,5vw,50px);line-height:1.1;margin-top:10px}
.client{font-size:16px;color:var(--brume);margin-top:16px}.client b{color:var(--ecume);font-weight:500}
.pill{display:inline-flex;align-items:center;gap:10px;margin-top:22px;padding:8px 16px 8px 10px;border-radius:999px;background:var(--bassin);font-size:15px}
.pill i{width:26px;height:26px;border-radius:50%;background:var(--eau);color:var(--abysse);display:grid;place-items:center;font-style:normal;font-weight:700}
.pill.non i{background:var(--bassin-2);color:var(--ecume)}
.trio{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));border-top:1px solid var(--ligne);border-bottom:1px solid var(--ligne)}
.trio>div{padding:22px 22px 22px 0;display:flex;flex-direction:column;gap:6px}
.trio>div+div{padding-left:22px;border-left:1px solid var(--ligne)}
.gros{font-size:clamp(32px,5vw,46px);font-weight:600;line-height:1;letter-spacing:-.01em}.gros small{font-size:.55em;font-weight:500;margin-left:4px}
.lib{font-size:15px;color:var(--brume)}.lib b{color:var(--ecume);font-weight:600}
.var{font-size:14px;font-weight:600}.up{color:var(--hausse)}.down{color:var(--baisse)}.flat{color:var(--brume)}
.equiv{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
.carte{background:var(--bassin);border-radius:var(--r);padding:22px 24px;display:flex;flex-direction:column;gap:8px;min-width:0}
.eq{flex-direction:row;align-items:center;gap:18px}.eq .gros{font-size:34px}
.note{font-size:12.5px;color:var(--brume)}
.graph svg{display:block;width:100%;height:auto}.graph .g-etroit{display:none}
.legende{display:flex;gap:18px;flex-wrap:wrap;font-size:13px;color:var(--brume)}
.legende i{display:inline-block;width:12px;height:12px;border-radius:3px;margin-right:6px;vertical-align:-1px}
.deux{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:14px}
.trois{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
.st{background:var(--bassin);border-radius:var(--r);padding:20px 24px;display:grid;grid-template-columns:64px minmax(0,1.4fr) repeat(3,minmax(0,1fr));gap:18px;align-items:center}
.st.champion{border:2px solid var(--corail)}
.st h3{font-size:20px;font-weight:500}.st .code{font-size:13px;color:var(--brume)}
.badge{display:inline-block;margin-top:6px;font-size:12px;letter-spacing:.12em;text-transform:uppercase;font-weight:600;color:var(--abysse);background:var(--corail);border-radius:999px;padding:3px 10px}
.st .v{font-size:22px;font-weight:600;line-height:1.1}.st .l{font-size:13px;color:var(--brume)}
.st .indispo{grid-column:3 / -1;font-size:14px;color:var(--brume)}
.jauge{position:relative;height:14px;border-radius:7px;background:var(--bassin-2);margin-top:30px}
.jauge .plage{position:absolute;top:0;bottom:0;background:var(--eau-fonce);border-radius:7px}
.jauge .curseur{position:absolute;top:-30px;transform:translateX(-50%);font-size:14px;font-weight:600;white-space:nowrap;text-align:center}
.jauge .curseur::after{content:"";display:block;margin:4px auto 0;width:3px;height:22px;background:var(--ecume);border-radius:2px}
.grad{display:flex;justify-content:space-between;font-size:12px;color:var(--brume);margin-top:6px}
.barre{height:14px;border-radius:7px;background:var(--bassin-2);overflow:hidden}.barre span{display:block;height:100%;background:var(--eau)}
.cal{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:6px}
.cal .js{font-size:11.5px;color:var(--brume);text-align:center;letter-spacing:.08em;text-transform:uppercase}
.cal .j{height:42px;border-radius:8px;display:grid;place-items:center;font-size:13px}
.cal .rec{outline:2px solid var(--corail);outline-offset:1px}
.mot{background:var(--eau);color:var(--abysse);border-radius:var(--r);padding:30px 32px;display:flex;flex-direction:column;gap:14px}
.mot .cit{font-size:clamp(24px,3.4vw,32px);line-height:1.2;font-weight:500}.mot p{max-width:62ch}
.defi{border:2px dashed var(--corail);border-radius:var(--r);padding:20px 24px}.defi .sur{color:var(--corail)}
.contact{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;font-size:14px;color:var(--brume)}.contact b{color:var(--ecume);font-weight:500}
.methode{font-size:12.5px;color:var(--brume);max-width:80ch}
.admin{margin-top:20px;border-radius:12px;padding:14px 18px;font-size:14px;background:#3a2f12;color:#ffe7a8;border:1px solid #8a6d1f}
.admin.pret{background:#123a26;color:#c4f2d6;border-color:#2c7a52}.admin.pas_d_envoi{background:#3d1717;color:#ffd0d0;border-color:#8a2f2f}
.admin ul{margin:6px 0 0;padding-left:20px}.admin b{font-weight:600}
.actions{max-width:900px;margin:12px auto 0;text-align:right}
.actions button{background:var(--eau);color:var(--abysse);border:0;border-radius:8px;padding:9px 16px;font:600 14px var(--f);cursor:pointer}
@media (max-width:720px){.graph .g-large{display:none}.graph .g-etroit{display:block}.couv{grid-template-columns:1fr;gap:20px}.goutte{max-width:220px}
.trio{grid-template-columns:1fr}.trio>div,.trio>div+div{padding:16px 0;border-left:0}.trio>div+div{border-top:1px solid var(--ligne)}
.equiv,.deux,.trois{grid-template-columns:1fr}.periode{text-align:left}
.st{grid-template-columns:52px repeat(3,minmax(0,1fr));column-gap:12px;row-gap:12px;padding:18px}.st .nomst{grid-column:2 / -1}
.st svg{width:52px;height:52px;grid-row:span 2;align-self:start}.st .v{font-size:18px}.st .indispo{grid-column:2 / -1}.cal .j{height:36px;font-size:12px}}
@page{size:A4;margin:10mm}
@media print{body{padding:0}.actions,.admin{display:none}.f{break-inside:avoid-page}*{-webkit-print-color-adjust:exact;print-color-adjust:exact}}
"""

_RAPPORT_GOUTTE = "M135 8 C135 8 250 170 250 245 C250 312 198 362 135 362 C72 362 20 312 20 245 C20 170 135 8 135 8 Z"
_RAPPORT_ICONES = {
    "douche": '<path d="M4 20V9a5 5 0 0 1 10 0"/><path d="M11 9h6"/><path d="M12 13v1M15 13v1M18 13v1M13 17v1M16 17v1M19 17v1"/>',
    "maison": '<path d="M3 11l9-7 9 7"/><path d="M5 10v10h14V10"/><path d="M10 20v-5h4v5"/>',
    "bouteille": '<path d="M10 2h4v3l1 2v14a1 1 0 0 1-1 1h-4a1 1 0 0 1-1-1V7l1-2z"/><path d="M9 12h6"/>',
    "voiture": '<path d="M5 17h14l-1.5-5h-11z"/><path d="M7 12l1.5-4h7l1.5 4"/><circle cx="8" cy="18.5" r="1.5"/><circle cx="16" cy="18.5" r="1.5"/>',
}
_RAPPORT_JOURS_FR = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]


def _rapport_mois_court(mois: str) -> str:
    courts = ["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.", "déc."]
    return courts[int(mois[5:7]) - 1]


def _rapport_goutte_svg(pct: float) -> str:
    p = max(0.0, min(pct, 100.0)) / 100
    y = 362 - (362 - 8) * p
    vague = f"M-70 {y:.1f}" + " q17 -10 34 0 t34 0" * 13 + " L380 370 L-70 370 Z"
    return (f'<svg viewBox="0 0 270 370" role="img" aria-label="Goutte remplie à {round(pct)} %">'
            f'<defs><clipPath id="cg"><path d="{_RAPPORT_GOUTTE}"/></clipPath>'
            '<linearGradient id="ge" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#5cc3c2"/>'
            '<stop offset="1" stop-color="#2a8f8e"/></linearGradient></defs>'
            f'<path d="{_RAPPORT_GOUTTE}" fill="#10393a"/><g clip-path="url(#cg)"><path d="{vague}" fill="url(#ge)"/></g>'
            f'<path d="{_RAPPORT_GOUTTE}" fill="none" stroke="#5cc3c2" stroke-width="3"/>'
            f'<text x="135" y="258" text-anchor="middle" font-family="Jost, sans-serif" font-size="80" font-weight="600" fill="#eef7f7">{round(pct)}%</text></svg>')


def _rapport_graph_svg(hist: list, W: int = 840, classe: str = "") -> str:
    """Barres : eau recyclee (m3) ; courbe : taux (%). Echelles explicites, labels dans le cadre."""
    pts = [h for h in hist if h["eau_recyclee_m3"] is not None]
    H, gl, gr, gt, gb = 300, (60 if W > 600 else 44), (60 if W > 600 else 46), 30, 36
    iw, ih, n = W - gl - gr, H - gt - gb, len(pts)
    vmax = max(h["eau_recyclee_m3"] for h in pts)
    pas = 50 if vmax <= 200 else 100 if vmax <= 500 else 250 if vmax <= 1500 else 500 if vmax <= 4000 else 1000
    vmax = max(pas, -(-vmax // pas) * pas)
    taux = [h["taux_recyclage_pct"] for h in pts if h["taux_recyclage_pct"] is not None]
    tmin = max(0, min(taux) // 10 * 10 - 10) if taux else 0
    tmax = min(100, -(-max(taux) // 10) * 10 + 10) if taux else 100
    tmax = tmax if tmax > tmin else tmin + 10
    X = lambda i: gl + iw / n * (i + 0.5)
    Yv = lambda v: gt + ih - v / vmax * ih
    Yt = lambda t: gt + ih - (t - tmin) / (tmax - tmin) * ih
    bw = iw / n * 0.52
    s = [f'<svg class="{classe}" viewBox="0 0 {W} {H}" role="img" aria-label="Eau recyclée et taux de recyclage par mois">']
    v = 0
    while v <= vmax:
        s.append(f'<line x1="{gl}" x2="{W - gr}" y1="{Yv(v):.1f}" y2="{Yv(v):.1f}" stroke="#245657"/>'
                 f'<text x="{gl - 10}" y="{Yv(v) + 4:.1f}" text-anchor="end" font-size="12" fill="#a9caca">{_fr_nombre(v)}</text>')
        v += pas
    t = tmin
    while t <= tmax:
        s.append(f'<text x="{W - gr + 10}" y="{Yt(t) + 4:.1f}" font-size="12" fill="#f39a78">{int(t)} %</text>')
        t += 10
    for i, h in enumerate(pts):
        der = i == n - 1
        y = Yv(h["eau_recyclee_m3"])
        coul = "#eef7f7" if der else "#a9caca"
        s.append(f'<rect x="{X(i) - bw / 2:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{gt + ih - y:.1f}" rx="6" fill="{"#5cc3c2" if der else "#174a4b"}"/>'
                 f'<text x="{X(i):.1f}" y="{y - 8:.1f}" text-anchor="middle" font-size="13" font-weight="{600 if der else 400}" fill="{coul}">{_fr_nombre(h["eau_recyclee_m3"])}</text>'
                 f'<text x="{X(i):.1f}" y="{H - 12}" text-anchor="middle" font-size="13" fill="{coul}">{_rapport_mois_court(h["mois"])}</text>')
    lt = [(X(i), Yt(h["taux_recyclage_pct"])) for i, h in enumerate(pts) if h["taux_recyclage_pct"] is not None]
    if len(lt) >= 2:
        s.append('<polyline points="' + " ".join(f"{x:.1f},{y:.1f}" for x, y in lt)
                 + '" fill="none" stroke="#f39a78" stroke-width="2.5" stroke-linejoin="round"/>')
    for k, (x, y) in enumerate(lt):
        der = k == len(lt) - 1
        s.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{5.5 if der else 3.5}" fill="{"#f39a78" if der else "#0a2425"}" stroke="#f39a78" stroke-width="2"/>')
    s.append("</svg>")
    return "".join(s).replace("<text ", '<text font-family="Jost, sans-serif" ')


def _rapport_anneau_svg(pct) -> str:
    c = 2 * 3.14159 * 26
    arc = (f'<circle cx="32" cy="32" r="26" fill="none" stroke="#5cc3c2" stroke-width="7" stroke-linecap="round" '
           f'stroke-dasharray="{c * pct / 100:.1f} {c:.1f}" transform="rotate(-90 32 32)"/>') if pct is not None else ""
    txt = f"{round(pct)}%" if pct is not None else "—"
    return (f'<svg width="64" height="64" viewBox="0 0 64 64" role="img" aria-label="Taux de recyclage {txt}">'
            '<circle cx="32" cy="32" r="26" fill="none" stroke="#174a4b" stroke-width="7"/>' + arc +
            f'<text x="32" y="37" text-anchor="middle" font-size="15" font-weight="600" fill="#eef7f7" font-family="Jost, sans-serif">{txt}</text></svg>')


def rapport_mensuel_html(r: dict) -> str:
    """Rapport client « Impact » (fond eau profonde, charte Eaukey). Chaque bloc ne
    s'affiche que si l'analyse a des donnees fiables pour lui. Le bandeau de statut
    (pret / a relire / pas d'envoi) est reserve a l'equipe : masque a l'impression."""
    e = _rapport_html.escape
    an = r["analyse"]
    eau, blocs = an["eau"], an["blocs"]
    t = r["total"]
    mois_txt = _fr_mois(r["mois"])
    mois_nom = _RAPPORT_MOIS_FR[int(r["mois"][5:7]) - 1]
    hist = r["historique"]
    prec_court = _rapport_mois_court(hist[-2]["mois"]) if len(hist) >= 2 else None
    q = t["qualite"]
    n = r["nb_stations"]
    jours_lavage = sum(s["jours_avec_lavage"] for s in r["stations"] if s["analyse"]["eau_fiable"])
    plusieurs = len(an["stations_fiables"]) > 1
    out = []

    def var_html(v, inverse=False, suffixe=" %"):
        if v is None:
            return ""
        bon = v < 0 if inverse else v > 0
        cls = "flat" if v == 0 else ("up" if bon else "down")
        signe = "+" if v > 0 else ("−" if v < 0 else "")
        return f'<span class="var {cls}">{signe}{_fr_nombre(abs(v), 1).replace(",0", "")}{suffixe} vs {prec_court}</span>'

    out.append(f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Rapport Eaukey {e(mois_txt)} – {e(r["client"])}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Jost:wght@300;400;500;600;700&display=swap">
<style>{_RAPPORT_CSS}</style></head><body>
<div class="actions"><button onclick="window.print()">Imprimer / enregistrer en PDF</button></div>
<div class="wrap">""")

    # Bandeau equipe (jamais imprime)
    libelle = {"pret": "Prêt à envoyer", "a_relire": "À relire avant envoi", "pas_d_envoi": "Pas d'envoi possible"}[an["statut"]]
    items = "".join(f"<li>{e(x)}</li>" for x in an["raisons"])
    items += "".join(f"<li><b>{e(a['lieu'] or a['nom_automate'])}</b> : {e(a['alerte'])}</li>" for a in an["alertes"])
    out.append(f'<div class="admin {an["statut"]}"><b>{libelle}</b> (visible par l\'équipe Eaukey seulement, '
               f'non imprimé){"<ul>" + items + "</ul>" if items else ""}</div>')

    out.append(f"""<header class="entete"><div class="marque" aria-label="Eaukey">
<svg width="24" height="34" viewBox="0 0 26 36" aria-hidden="true"><path d="M13 2 L23 22 Q24 25 22 28 L18 34 L8 34 L4 28 Q2 25 3 22 Z" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linejoin="round"/><path d="M8 27 L13 31 L18 27" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
<span>Eaukey</span></div><div class="periode">Votre impact<b>{e(mois_txt)}</b></div></header>""")

    # 1. Couverture
    titre = an["titre"]
    if eau:
        m3 = _fr_nombre(eau["eau_recyclee_m3"])
        taux = eau["taux_recyclage_pct"]
        goutte = _rapport_goutte_svg(taux)
        h1 = {
            "record": ("Votre meilleur mois", f"Record battu : <em>{m3} m³</em> d'eau recyclée en {mois_nom}."),
            "objectif": ("Eau réutilisée ce mois-ci", ("Presque toute l'eau utilisée pour laver vos clients a <em>déjà servi</em>."
                         if taux >= 95 else f"{int(taux // 10)} litres sur 10 utilisés pour laver vos clients ont <em>déjà servi</em>.")),
            "progression": ("En progression", f"<em>+{_fr_nombre(eau['variation_pct'], 0)} %</em> d'eau recyclée par rapport au mois dernier."),
            "volume": ("Eau réutilisée ce mois-ci", f"<em>{m3} m³</em> d'eau recyclée, sans puiser dans le réseau."),
            "mois_calme": ("Un mois plus calme", f"Déjà <em>{_fr_nombre(an['cumul_annee']['eau_recyclee_m3'])} m³</em> recyclés depuis janvier."),
        }[titre]
        atteint = taux >= an["objectif_taux"]
        pill = (f'<div class="pill{"" if atteint else " non"}"><i aria-hidden="true">{"✓" if atteint else "→"}</i>'
                f'Objectif Eaukey {_fr_nombre(an["objectif_taux"])} % : <b>&nbsp;{"atteint" if atteint else "en route"}</b></div>')
        trio = (f'<div><span class="gros num">{m3}<small>m³</small></span><span class="lib">d\'eau recyclée</span>{var_html(eau["variation_pct"])}</div>'
                f'<div><span class="gros num">{_fr_nombre(eau["economies_eur"])}<small>€</small></span><span class="lib">économisés sur l\'eau</span>{var_html(eau["variation_pct"])}</div>'
                f'<div><span class="gros num">{_fr_nombre(taux, 1).replace(",0", "")}<small>%</small></span><span class="lib">taux de recyclage</span>'
                f'{var_html(eau["ecart_taux_points"], suffixe=" pt")}</div>')
    else:
        pct_q = q["chlore_pct_dans_plage"] if q["chlore_pct_dans_plage"] is not None else q["ph_pct_dans_plage"]
        goutte = _rapport_goutte_svg(pct_q or 0)
        h1 = ("Qualité de l'eau", "Une eau <em>saine et désinfectée</em>, tout le mois.") if titre == "qualite" else \
             ("Votre mois", "Le suivi de vos stations ce mois-ci.")
        pill = ""
        trio = (f'<div><span class="gros num">{_fr_nombre(q["ph_moyen"], 1) if q["ph_moyen"] else "—"}</span><span class="lib">pH moyen</span></div>'
                f'<div><span class="gros num">{_fr_nombre(q["chlore_pct_dans_plage"] or 0)}<small>%</small></span><span class="lib">du temps désinfectée</span></div>'
                f'<div><span class="gros num">{_fr_nombre(t["couverture_pct"] or 0)}<small>%</small></span><span class="lib">des heures suivies à distance</span></div>')
    out.append(f"""<section class="f"><div class="couv"><div class="goutte">{goutte}</div><div>
<p class="sur">{h1[0]}</p><h1>{h1[1]}</h1>
<p class="client"><b>{e(r["client"])}</b> · {n} station{"s" if n > 1 else ""}</p>{pill}</div></div>
<div class="trio">{trio}</div></section>""")

    # 2. Equivalences
    if blocs["eau"]:
        eq = eau["equivalences"]
        cartes = [(_fr_nombre(round(eq["douches"], -2)), "douches de 5 minutes", "douche"),
                  (_fr_nombre(eq["foyers_un_an"], 1), "foyers alimentés pendant un an", "maison"),
                  (_fr_nombre(round(eq["bouteilles"], -3)), "bouteilles d'eau de 1,5 L", "bouteille"),
                  (_fr_nombre(round(eq["lavages"], -1)), "lavages de voiture", "voiture")]
        out.append(f'<section class="f"><div><p class="sur">Pour se représenter</p><h2 class="h2">Ce que représentent '
                   f'<em>{_fr_nombre(eau["eau_recyclee_m3"])} m³</em></h2></div><div class="equiv">')
        for v, lib, ico in cartes:
            out.append(f'<div class="carte eq"><svg width="44" height="44" viewBox="0 0 24 24" fill="none" stroke="#5cc3c2" '
                       f'stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">{_RAPPORT_ICONES[ico]}</svg>'
                       f'<div><div class="gros num">{v}</div><div class="lib">{lib}</div></div></div>')
        rp = eq["reperes"]
        out.append(f'</div><p class="note">Repères : une douche de 5 minutes = {_fr_nombre(rp["douche_l"])} L ; un foyer = '
                   f'{_fr_nombre(rp["foyer_m3_an"])} m³ par an ; un lavage de voiture = {_fr_nombre(rp["lavage_l"])} L ; '
                   f'une bouteille = 1,5 L ; une piscine olympique = {_fr_nombre(_RAPPORT_PISCINE_OLYMPIQUE_M3)} m³ '
                   f'({_fr_nombre(eq["piscines_olympiques"], 2)} ce mois-ci).</p></section>')

    # 3. Evolution
    if blocs["evolution"] or blocs["energie"]:
        vals = [h["eau_recyclee_m3"] for h in hist if h["eau_recyclee_m3"] is not None]
        meilleur = eau and eau["record"]
        titre_evo = "Votre <em>meilleur mois</em> depuis {}".format("le début du suivi") if meilleur else \
                    "Votre recyclage, <em>mois après mois</em>"
        out.append(f'<section class="f"><div><p class="sur">Évolution</p><h2 class="h2">{titre_evo}</h2></div>')
        if blocs["evolution"]:
            out.append(f'<div class="carte graph">{_rapport_graph_svg(hist, 840, "g-large")}{_rapport_graph_svg(hist, 400, "g-etroit")}<div class="legende">'
                       '<span><i style="background:#5cc3c2"></i>Eau recyclée (m³)</span>'
                       '<span><i style="background:#f39a78"></i>Taux de recyclage (%)</span></div></div>')
        out.append('<div class="deux">')
        out.append(f'<div class="carte"><span class="sur">Économies du mois</span><span class="gros num">{_fr_nombre(eau["economies_eur"])}<small>€</small></span>'
                   f'<span class="lib">sur la base de {_fr_nombre(r["parametres"]["prix_m3_eur"], 2)} € le m³ d\'eau de ville</span></div>')
        if blocs["energie"]:
            out.append(f'<div class="carte"><span class="sur">Énergie</span><span class="gros num">{_fr_nombre(eau["kwh_par_m3_recycle"], 2)}<small>kWh/m³</small></span>'
                       f'<span class="lib">{_fr_nombre(eau["energie_kwh"])} kWh consommés pour recycler</span></div>')
        else:
            out.append(f'<div class="carte"><span class="sur">Lavages</span><span class="gros num">{jours_lavage}<small>jours</small></span>'
                       f'<span class="lib">avec de l\'eau recyclée{" (cumul de vos stations)" if plusieurs else ""}</span></div>')
        out.append('</div></section>')

    # 4. Stations
    if blocs["stations"]:
        fiables = [s for s in r["stations"] if s["analyse"]["eau_fiable"]]
        champion = max(fiables, key=lambda s: s["eau_recyclee_m3"]) if len(fiables) > 1 else None
        out.append('<section class="f"><div><p class="sur">Vos stations</p><h2 class="h2">Station par station</h2></div><div style="display:flex;flex-direction:column;gap:12px">')
        for s in sorted(r["stations"], key=lambda s: (not s["analyse"]["eau_fiable"], -s["eau_recyclee_m3"])):
            nom = e(s["lieu"] or s["nom_automate"])
            code = e(s["nom_automate"][:-2] if s["nom_automate"].endswith(".0") else s["nom_automate"])
            badge = '<span class="badge">Station du mois</span>' if s is champion else ""
            if s["analyse"]["eau_fiable"]:
                rec_s = s["eau_renvoyee_m3"] - s["eau_adoucie_m3"]
                taux_s = 100 * rec_s / s["eau_renvoyee_m3"]
                v = s["variation_eau_recyclee_pct"]
                corps = (f'<div><div class="v num">{_fr_nombre(rec_s)} m³</div><div class="l">recyclés</div></div>'
                         f'<div><div class="v num">{_fr_nombre(rec_s * r["parametres"]["prix_m3_eur"])} €</div><div class="l">économisés</div></div>'
                         f'<div><div class="v num">{s["jours_avec_lavage"]} j</div><div class="l">de lavage</div></div>')
                ring = _rapport_anneau_svg(taux_s)
            else:
                ring = _rapport_anneau_svg(None)
                qs = s["qualite"]
                det = f' · eau désinfectée {_fr_nombre(qs["chlore_pct_dans_plage"])} % du temps' if qs["chlore_pct_dans_plage"] is not None else ""
                corps = f'<div class="indispo">Mesure de l\'eau recyclée indisponible ce mois-ci{det}.</div>'
            out.append(f'<article class="st{" champion" if s is champion else ""}">{ring}<div class="nomst"><h3>{nom}</h3>'
                       f'<div class="code">Automate {code}</div>{badge}</div>{corps}</article>')
        out.append('</div></section>')

    # 5. Qualite, calendrier, interventions
    out.append('<section class="f"><div><p class="sur">Qualité et suivi</p><h2 class="h2">')
    out.append("Une eau <em>saine et désinfectée</em>" if (blocs["qualite"] and not any(
        s["analyse"]["qualite_mauvaise"] for s in r["stations"])) else "Le suivi de vos stations")
    out.append("</h2></div>")
    if blocs["qualite"]:
        cartes = []
        if q["ph_moyen"] is not None:
            pos = lambda v: max(0, min(100, (v - 5) / 5 * 100))
            cartes.append(f'<div class="carte"><span class="sur">pH moyen</span><div class="jauge">'
                          f'<span class="plage" style="left:{pos(_RAPPORT_PH_MIN):.0f}%;width:{pos(_RAPPORT_PH_MAX) - pos(_RAPPORT_PH_MIN):.0f}%"></span>'
                          f'<span class="curseur num" style="left:{pos(q["ph_moyen"]):.1f}%">{_fr_nombre(q["ph_moyen"], 1)}</span></div>'
                          '<div class="grad"><span>5</span><span>6</span><span>7</span><span>8</span><span>9</span><span>10</span></div>'
                          f'<span class="lib"><b class="num">{_fr_nombre(q["ph_pct_dans_plage"], 1)} %</b> du temps dans la plage idéale '
                          f'({_fr_nombre(_RAPPORT_PH_MIN, 1)} à {_fr_nombre(_RAPPORT_PH_MAX, 1)})</span></div>')
        if q["chlore_pct_dans_plage"] is not None:
            cartes.append(f'<div class="carte"><span class="sur">Désinfection</span><span class="gros num">{_fr_nombre(q["chlore_pct_dans_plage"])}<small>%</small></span>'
                          f'<div class="barre" aria-hidden="true"><span style="width:{q["chlore_pct_dans_plage"]:.0f}%"></span></div>'
                          f'<span class="lib">du temps au-dessus de {_fr_nombre(_RAPPORT_REDOX_SEUIL_MV)} mV (redox moyen '
                          f'<span class="num">{_fr_nombre(q["redox_moyen_mv"])}</span> mV)</span></div>')
        out.append(f'<div class="deux">{"".join(cartes)}</div>')
    if blocs["calendrier"]:
        jours = r["jours"]
        vmax = max(j["eau_recyclee_m3"] for j in jours)
        i_rec = next(i for i, j in enumerate(jours) if j["eau_recyclee_m3"] == vmax)
        d0 = dt.date.fromisoformat(jours[0]["date"])
        cases = "".join(f'<div class="js">{x[:3]}</div>' for x in _RAPPORT_JOURS_FR) + "<div></div>" * d0.weekday()
        for i, j in enumerate(jours):
            x = j["eau_recyclee_m3"]
            bg, fg = (("#5cc3c2", "#0a2425") if x >= vmax * 0.85 else ("#3f9a99", "#eef7f7") if x >= vmax * 0.6
                      else ("#2d6e6e", "#eef7f7") if x >= vmax * 0.35 else ("#174a4b", "#a9caca"))
            cases += f'<div class="j{" rec" if i == i_rec else ""}" style="background:{bg};color:{fg}">{i + 1}</div>'
        jr = dt.date.fromisoformat(jours[i_rec]["date"])
        out.append(f'<div class="carte"><span class="sur">Le mois jour par jour</span><div class="cal">{cases}</div>'
                   f'<span class="lib">Record le <b style="color:#f39a78">{_RAPPORT_JOURS_FR[jr.weekday()]} {jr.day}</b> avec '
                   f'{_fr_nombre(vmax)} m³ recyclés · {jours_lavage} jours de lavage'
                   f'{" (cumul de vos stations)" if plusieurs else ""}.</span></div>')
    p = t["pannes"]
    interv = (f'{p["ouvertes_dans_le_mois"]} panne{"s" if p["ouvertes_dans_le_mois"] > 1 else ""} signalée{"s" if p["ouvertes_dans_le_mois"] > 1 else ""} ce mois-ci'
              + (f' · {p["resolues_dans_le_mois"]} résolue{"s" if p["resolues_dans_le_mois"] > 1 else ""} en {_fr_duree(p["delai_moyen_resolution_h"])} en moyenne'
                 if p["resolues_dans_le_mois"] else ""))
    out.append(f'<div class="deux"><div class="carte"><span class="sur">Interventions</span><span class="gros num">{p["ouvertes_dans_le_mois"]}</span>'
               f'<span class="lib">{interv}</span></div><div class="carte"><span class="sur">Suivi à distance</span>'
               f'<span class="gros num">{_fr_nombre(t["couverture_pct"] or 0)}<small>%</small></span>'
               '<span class="lib">des heures du mois transmises à Eaukey</span></div></div></section>')

    # 6. Bilan
    c = an["cumul_annee"]
    out.append(f'<section class="f"><div><p class="sur">Depuis {"janvier" if (c["depuis_mois"] or "")[5:7] == "01" else "le début du suivi"}</p>'
               f'<h2 class="h2">Votre bilan <em>{r["mois"][:4]}</em></h2></div>')
    if c["eau_recyclee_m3"] > 0:
        out.append(f'<div class="trois"><div class="carte"><span class="gros num">{_fr_nombre(c["eau_recyclee_m3"])}<small>m³</small></span><span class="lib">d\'eau recyclée</span></div>'
                   f'<div class="carte"><span class="gros num">{_fr_nombre(c["economies_eur"])}<small>€</small></span><span class="lib">économisés</span></div>'
                   f'<div class="carte"><span class="gros num">{_fr_nombre(c["piscines_olympiques"], 1)}</span><span class="lib">piscine{"s" if c["piscines_olympiques"] >= 2 else ""} olympique{"s" if c["piscines_olympiques"] >= 2 else ""}</span></div></div>')
    phrase = []
    if eau:
        phrase.append(f"En {mois_nom}, vos stations ont recyclé {_fr_nombre(eau['eau_recyclee_m3'])} m³ d'eau")
        if eau["variation_pct"] is not None:
            v = eau["variation_pct"]
            phrase[-1] += f", {'soit ' + _fr_nombre(v, 0) + ' % de plus' if v > 0 else 'un peu moins'} qu'en {_RAPPORT_MOIS_FR[int(hist[-2]['mois'][5:7]) - 1]}"
        phrase[-1] += "."
        if eau["record"]:
            phrase.append("C'est votre meilleur mois depuis le début du suivi.")
        fiab = [s for s in r["stations"] if s["analyse"]["eau_fiable"]]
        if len(fiab) > 1:
            b = max(fiab, key=lambda s: s["eau_recyclee_m3"])
            phrase.append(f"La station de {e(b['lieu'] or b['nom_automate'])} arrive en tête.")
    elif blocs["qualite"]:
        phrase.append(f"En {mois_nom}, l'eau de vos stations a été suivie et désinfectée en continu.")
    phrase.append("Merci de votre confiance.")
    out.append(f'<div class="mot"><p class="cit">« L\'eau du futur vient des eaux d\'hier. »</p><p>{" ".join(phrase)}</p></div>')
    if an["objectif_suivant_taux"]:
        out.append(f'<div class="defi"><p class="sur">Objectif du mois prochain</p><p style="font-size:18px">'
                   f'Dépasser {_fr_nombre(an["objectif_suivant_taux"])} % de recyclage sur l\'ensemble de vos stations.</p></div>')
    out.append('<div class="contact"><span>Une question sur votre installation ? <b>Hotline 7 j/7</b> · <b>contact@eaukey.net</b></span><span>eaukey.net</span></div>')
    out.append(f'<p class="methode">Eau recyclée = eau renvoyée vers le lavage − eau d\'appoint, mesurées par les compteurs de vos stations '
               f'(seules les stations dont les deux compteurs ont fonctionné sont comptées). Taux de recyclage = eau recyclée / eau renvoyée. '
               f'Économies estimées à {_fr_nombre(r["parametres"]["prix_m3_eur"], 2)} € le m³ (eau + assainissement). Données horaires, '
               f'heure de Paris ; une heure sans transmission n\'est pas comptée. Rapport généré le {_fr_date(r["genere_le"])}.</p></section>')
    out.append("</div></body></html>")
    return "".join(out)


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


@app.get("/rapport/mensuel/lot")
def rapport_mensuel_lot(
    request: Request,
    mois: Optional[str] = Query(None, description="AAAA-MM (défaut : mois précédent, heure de Paris)"),
):
    """ADMIN : statut du rapport de chaque client du mois (pret / a_relire / pas_d_envoi),
    titre choisi et alertes, pour preparer l'envoi. Ne genere et n'envoie rien."""
    _require_admin_or_super(request)
    mois = mois or _rapport_mois_precedent()
    clients = rapport_mensuel_clients(request, mois)["clients"]
    out = []
    for c in clients:
        r = rapport_mensuel(c["client"], mois)
        an = r["analyse"]
        out.append({"client": r["client"], "nb_stations": r["nb_stations"], "statut": an["statut"],
                    "titre": an["titre"], "raisons": an["raisons"], "alertes": an["alertes"],
                    "eau_recyclee_m3": an["eau"]["eau_recyclee_m3"] if an["eau"] else None,
                    "taux_recyclage_pct": an["eau"]["taux_recyclage_pct"] if an["eau"] else None})
    ordre = {"pret": 0, "a_relire": 1, "pas_d_envoi": 2}
    out.sort(key=lambda x: (ordre[x["statut"]], x["client"].lower()))
    return {"mois": mois, "resume": {k: sum(1 for x in out if x["statut"] == k) for k in ordre}, "clients": out}


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


# ---------------------------------------------------------------------------
# Canal e-mail de la messagerie (M2) : une seule conversation, quel que soit le canal
# ---------------------------------------------------------------------------
# Le client écrit à l'adresse SAV (ex. sav@sav.eaukey.net). Le fournisseur e-mail
# (Brevo Inbound Parsing, Cloudflare Email Worker…) appelle le webhook
# POST /canaux/email/entrant, protégé par EMAIL_ENTRANT_SECRET (en-tête
# X-Eaukey-Webhook-Secret ou ?token=) ; sans secret configuré, la route est fermée (503).
#   - Réponses automatiques, listes, adresses noreply/mailer-daemon/postmaster et nos
#     propres adresses : ignorées (anti-boucle). Un e-mail rejoué (même Message-ID,
#     messages.external_id unique) n'est enregistré qu'une fois.
#   - Expéditeur reconnu (client_contacts, sinon e-mails des stations automate.email/2/3,
#     un seul client) : le message entre dans le fil /chat du client (canal 'email'),
#     puis même traitement que /chat (_chat_agent_evenements) ; la réponse de l'agent
#     repart par e-mail dans le même fil e-mail (Re: sujet, In-Reply-To, References).
#   - Expéditeur inconnu ou ambigu : AUCUNE réponse automatique (pas de spam en retour),
#     rien d'enregistré ; l'équipe reçoit l'adresse et le sujet pour rattacher le contact.
#   - Réponse d'un admin dans le fil (POST /messages) alors que le dernier message du
#     client est arrivé par e-mail il y a moins de _EMAIL_RELAIS_JOURS jours : elle part
#     aussi par e-mail à cette adresse.
# Traitement de l'agent + envoi en tâche de fond : le webhook répond tout de suite.

_EMAIL_ENTRANT_SECRET = os.getenv("EMAIL_ENTRANT_SECRET", "")
_EMAIL_ENTRANT_ADRESSE = os.getenv("EMAIL_ENTRANT_ADRESSE", "").strip()
_EMAIL_SAV_FROM = os.getenv("EMAIL_SAV_FROM", "").strip() or _SMTP_FROM
_EMAIL_LIEN_CHAT = os.getenv("EMAIL_LIEN_CHAT", "https://app.eaukey.net/chat")
_EMAIL_MAX_OCTETS = 2_000_000       # corps du webhook (pièces jointes non transmises par Brevo)
_EMAIL_TEXTE_MAX = 4000             # même limite que la messagerie /chat
_EMAIL_RELAIS_JOURS = 14
_EMAIL_ALERTE_INCONNU_S = 24 * 3600  # une alerte par adresse inconnue et par jour au plus
_EMAIL_LOGGER = logging.getLogger("canal_email")
_EMAIL_RE = re.compile(r"^[^@\s<>()\[\],;:\"]+@[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)+$")
_EMAIL_LOCAL_AUTOMATIQUE = re.compile(r"(no[-_.]?reply|do[-_.]?not[-_.]?reply|mailer[-_.]?daemon|postmaster)", re.IGNORECASE)
_EMAIL_PIED = f"Vous pouvez répondre à cet e-mail ou continuer sur {_EMAIL_LIEN_CHAT}"
_email_inconnus_alertes: dict = {}


def _email_normaliser_adresse(valeur) -> Optional[str]:
    """« Nom <Adresse@X.fr> » -> « adresse@x.fr » (None si invalide)."""
    from email.utils import parseaddr
    adresse = parseaddr(str(valeur or ""))[1].strip().lower()
    return adresse if adresse and len(adresse) <= 254 and _EMAIL_RE.match(adresse) else None


def _email_entetes(brut) -> dict:
    """En-têtes du fournisseur (dict, ou liste de {name, value} / [nom, valeur]) -> {nom en minuscules: valeur}."""
    out = {}
    if isinstance(brut, dict):
        paires = brut.items()
    elif isinstance(brut, list):
        paires = []
        for h in brut:
            if isinstance(h, dict):
                paires.append((h.get("name") or h.get("Name"), h.get("value") or h.get("Value")))
            elif isinstance(h, (list, tuple)) and len(h) == 2:
                paires.append((h[0], h[1]))
    else:
        return out
    for nom, val in paires:
        if not nom:
            continue
        if isinstance(val, list):
            val = val[0] if val else ""
        out[str(nom).strip().lower()] = str(val if val is not None else "").strip()
    return out


def _email_nettoyer_texte(texte: str) -> str:
    """Garde le nouveau message : retire les citations (« > », « Le … a écrit : », « On … wrote: »,
    en-têtes Outlook) et la signature « -- »."""
    t = (texte or "").replace("\r\n", "\n").replace("\r", "\n")
    coupures = [
        r"(?m)^[ \t]*(?:Le|On)\s[^\n]{0,200}(?:\n[^\n]{0,200})?\s(?:a\s+écrit|wrote)\s*:[ \t]*$",
        r"(?m)^[ \t]*-{2,}\s*(?:Original Message|Message d'origine|Message original|Forwarded message|Message transféré)\s*-{2,}",
        r"(?m)^[ \t]*(?:De|From)\s*:[^\n]*\n[ \t]*(?:Envoyé|Sent|Date)\s*:",
        r"(?m)^-- ?$",
    ]
    for motif in coupures:
        m = re.search(motif, t, re.IGNORECASE)
        if m:
            t = t[:m.start()]
    t = "\n".join(l for l in t.split("\n") if not l.lstrip().startswith(">"))
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    if len(t) > _EMAIL_TEXTE_MAX:
        t = t[:_EMAIL_TEXTE_MAX - 1].rstrip() + "…"
    return t


def _email_depuis_generique(d: dict) -> dict:
    entetes = _email_entetes(d.get("headers"))
    return {
        "adresse": _email_normaliser_adresse(d.get("from")),
        "nom": str(d.get("from_name") or "").strip()[:200],
        "sujet": str(d.get("subject") or "").strip(),
        "texte": str(d.get("text") or ""),
        "message_id": str(d.get("message_id") or entetes.get("message-id") or "").strip(),
        "in_reply_to": str(d.get("in_reply_to") or entetes.get("in-reply-to") or "").strip(),
        "entetes": entetes,
    }


def _email_depuis_brevo(item: dict) -> dict:
    expediteur = item.get("From") or {}
    if not isinstance(expediteur, dict):
        expediteur = {"Address": expediteur}
    entetes = _email_entetes(item.get("Headers"))
    # Brevo extrait déjà le nouveau message (sans citation) : on le préfère au texte brut.
    texte = item.get("ExtractedMarkdownMessage") or item.get("RawTextBody") or ""
    return {
        "adresse": _email_normaliser_adresse(expediteur.get("Address")),
        "nom": str(expediteur.get("Name") or "").strip()[:200],
        "sujet": str(item.get("Subject") or "").strip(),
        "texte": str(texte),
        "message_id": str(item.get("MessageId") or entetes.get("message-id") or "").strip(),
        "in_reply_to": str(item.get("InReplyTo") or entetes.get("in-reply-to") or "").strip(),
        "entetes": entetes,
    }


def _email_nos_adresses() -> set:
    nos = {_email_normaliser_adresse(a) for a in (_EMAIL_SAV_FROM, _SMTP_FROM, _EMAIL_ENTRANT_ADRESSE, _SAV_ESCALADE_EMAIL)}
    return {a for a in nos if a}


def _email_automatique(mail: dict) -> Optional[str]:
    """Motif pour lequel l'e-mail ne doit recevoir aucune réponse (anti-boucle), ou None."""
    h = mail["entetes"]
    auto = h.get("auto-submitted", "").lower()
    if auto and auto != "no":
        return "auto-submitted"
    if h.get("precedence", "").lower() in ("bulk", "junk", "list", "auto_reply"):
        return "precedence"
    if "x-autoreply" in h or "x-autorespond" in h:
        return "x-autoreply"
    adresse = mail["adresse"]
    if adresse in _email_nos_adresses():
        return "notre adresse"
    if _EMAIL_LOCAL_AUTOMATIQUE.search(adresse.split("@")[0]):
        return "adresse automatique"
    return None


def _email_identifier_client(adresse: str) -> Optional[str]:
    """client_id d'un expéditeur : client_contacts d'abord, puis e-mails des stations (un seul client)."""
    row = executer_requete_sql_one(
        "SELECT client_id FROM client_contacts WHERE canal = 'email' AND valeur = %s;", (adresse,)
    )
    if row:
        return row[0]
    rows = executer_requete_sql(
        "SELECT client, email, email2, email3 FROM automate WHERE client IS NOT NULL AND trim(client) <> '';"
    )
    trouves = {}
    for client, *emails in rows:
        for champ in emails:
            # Un champ peut contenir plusieurs adresses (séparées par ; ou ,)
            if adresse in {_email_normaliser_adresse(x) for x in re.split(r"[;,\s]+", champ or "") if x}:
                trouves.setdefault(client.strip().lower(), client)
    return next(iter(trouves.values())) if len(trouves) == 1 else None


def _email_deja_recu(external_id: str) -> bool:
    return executer_requete_sql_one(
        "SELECT 1 FROM messages WHERE external_id = %s LIMIT 1;", (external_id,)
    ) is not None


def _email_inserer_question(client_id: str, content: str, mail: dict):
    """Question reçue par e-mail dans le fil du client ; None si déjà enregistrée (rejeu)."""
    return executer_requete_sql_one(
        """
        INSERT INTO messages (client_id, sender_id, content, is_read, canal, external_id,
                              canal_adresse, canal_sujet)
        VALUES (%s, %s, %s, false, 'email', %s, %s, %s)
        ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING
        RETURNING id, created_at;
        """,
        (client_id, CLIENT_SENDER_IDS[0], content, mail["message_id"], mail["adresse"], mail["sujet"][:500]),
    )


def _email_texte_brut(md: str) -> str:
    """Markdown de l'agent -> texte brut lisible dans une boîte mail."""
    t = md or ""
    t = re.sub(r"```[a-zA-Z]*\n?", "", t)
    t = re.sub(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", r"\1 (\2)", t)
    t = re.sub(r"(?m)^\s{0,3}#{1,6}\s*", "", t)
    t = re.sub(r"(\*\*|__)(.+?)\1", r"\2", t)
    t = re.sub(r"(?<![\w*])\*(?!\s)([^*\n]+?)\*(?!\w)", r"\1", t)
    t = re.sub(r"`([^`]+)`", r"\1", t)
    t = re.sub(r"(?m)^(\s*)[*+]\s+", r"\1- ", t)
    t = re.sub(r"(?m)^\s*[-*_]{3,}\s*$", "", t)
    return re.sub(r"\n{3,}", "\n\n", t).strip()


def _email_sujet_re(sujet: str) -> str:
    sujet = re.sub(r"[\r\n]+", " ", sujet or "").strip()[:200] or "Votre message à Eaukey"
    return sujet if re.match(r"(?i)^re\s*:", sujet) else f"Re: {sujet}"


def _email_id_entete(valeur: str) -> str:
    v = re.sub(r"[\r\n\s]+", "", valeur or "")
    return v if not v or v.startswith("<") else f"<{v}>"


async def _email_envoyer(destinataire: str, sujet: str, texte: str, in_reply_to: str = "",
                         references: str = "", automatique: bool = True) -> bool:
    """Réponse par e-mail dans le fil e-mail du client. Ne lève jamais."""
    if not _mail_configure():
        _EMAIL_LOGGER.warning("Envoi d'e-mail non configure : reponse e-mail non envoyee")
        return False
    from email.utils import formatdate, make_msgid
    msg = MIMEText(f"{texte}\n\n--\n{_EMAIL_PIED}\n", "plain", "utf-8")
    msg["Subject"] = _email_sujet_re(sujet)
    msg["From"] = _EMAIL_SAV_FROM
    msg["To"] = destinataire
    if _EMAIL_ENTRANT_ADRESSE:
        msg["Reply-To"] = _EMAIL_ENTRANT_ADRESSE
    msg["Date"] = formatdate(localtime=True)
    domaine = (_email_normaliser_adresse(_EMAIL_SAV_FROM) or "eaukey.net").split("@")[-1]
    msg["Message-ID"] = make_msgid(domain=domaine)
    irt = _email_id_entete(in_reply_to)
    if irt:
        msg["In-Reply-To"] = irt
        refs = " ".join(r for r in re.split(r"\s+", references or "") if r)
        msg["References"] = f"{refs} {irt}".strip() if irt not in refs else refs
    if automatique:
        # RFC 3834 : réponse automatique -> les répondeurs du client ne nous répondent pas.
        msg["Auto-Submitted"] = "auto-replied"
    try:
        await _mail_envoyer(msg)
        return True
    except Exception as e:
        _EMAIL_LOGGER.error("Echec envoi e-mail SAV : %s", e)
        return False


async def _email_alerte_inconnu(adresse: str, sujet: str) -> None:
    """Expéditeur non reconnu : l'équipe reçoit l'adresse et le sujet (rien d'autre). Ne lève jamais."""
    maintenant = time.time()
    if maintenant - _email_inconnus_alertes.get(adresse, 0) < _EMAIL_ALERTE_INCONNU_S:
        return
    _email_inconnus_alertes[adresse] = maintenant
    if not _mail_configure() or not _SAV_ESCALADE_EMAIL:
        _EMAIL_LOGGER.warning("Envoi d'e-mail non configure : alerte expediteur inconnu non envoyee")
        return
    sujet = re.sub(r"[\r\n]+", " ", sujet or "").strip()[:200] or "(sans sujet)"
    msg = MIMEText(
        "Un e-mail est arrivé sur l'adresse SAV d'un expéditeur qui n'est rattaché à aucun client "
        "(ou à plusieurs). L'assistant n'a PAS répondu et le message n'a pas été enregistré.\n\n"
        f"Expéditeur : {adresse}\nSujet : {sujet}\n\n"
        "Pour le rattacher : ouvrez la conversation du client dans l'application (/chat), "
        "encart « Adresses e-mail reconnues », puis demandez-lui de renvoyer son message.\n",
        "plain", "utf-8",
    )
    msg["Subject"] = "[SAV Eaukey] E-mail d'un expéditeur inconnu"
    msg["From"] = _SMTP_FROM
    msg["To"] = _SAV_ESCALADE_EMAIL
    msg["Auto-Submitted"] = "auto-generated"
    try:
        await _mail_envoyer(msg)
    except Exception as e:
        _EMAIL_LOGGER.error("Echec alerte expediteur inconnu : %s", e)


async def _email_traiter(client_id: str, content: str, mail: dict) -> None:
    """Tâche de fond : réponse de l'agent (même cœur que /chat) puis envoi par e-mail."""
    try:
        saved = None
        async for event, data in _chat_agent_evenements(client_id, content, "email", mail["adresse"]):
            if event == "saved":
                saved = data
        # humain : l'équipe suit le fil, sa réponse sera relayée par e-mail ;
        # error : l'équipe voit la question dans la boîte de réception.
        if not saved:
            return
        ok = await _email_envoyer(
            mail["adresse"], mail["sujet"], _email_texte_brut(saved["content"]),
            in_reply_to=mail["message_id"], references=mail["entetes"].get("references", ""),
        )
        if not ok and saved.get("id"):
            # Réponse visible dans l'appli seulement : ne pas l'afficher « par e-mail ».
            executer_requete_sql(
                "UPDATE messages SET canal = 'app', canal_adresse = NULL WHERE id = %s;", (saved["id"],)
            )
    except Exception as e:
        _EMAIL_LOGGER.error("Traitement e-mail (client %s) en echec : %s", client_id, e)


async def _email_relayer_reponse_equipe(client_id: str, message_id: int, contenu: str) -> None:
    """Réponse d'un admin : part aussi par e-mail si le client écrit par e-mail (< 14 j). Ne lève jamais."""
    try:
        row = executer_requete_sql_one(
            """
            SELECT canal, canal_adresse, canal_sujet, external_id,
                   created_at::timestamptz > now() - make_interval(days => %s)
            FROM messages
            WHERE client_id = %s AND sender_id = ANY(%s)
            ORDER BY created_at::timestamptz DESC, id DESC
            LIMIT 1;
            """,
            (_EMAIL_RELAIS_JOURS, client_id, list(CLIENT_SENDER_IDS)),
        )
        if not row or row[0] != "email" or not row[1] or not row[4]:
            return
        ok = await _email_envoyer(
            row[1], row[2] or "", f"{contenu.strip()}\n\n— L'équipe Eaukey",
            in_reply_to=row[3] or "", automatique=False,
        )
        if ok:
            executer_requete_sql(
                "UPDATE messages SET canal = 'email', canal_adresse = %s WHERE id = %s;", (row[1], message_id)
            )
    except Exception as e:
        _EMAIL_LOGGER.error("Relais e-mail de la reponse equipe (client %s) en echec : %s", client_id, e)


def _email_recevoir(mail: dict, background_tasks: BackgroundTasks) -> str:
    """Traite un e-mail entrant normalisé ; renvoie le résultat (pour le journal du fournisseur)."""
    if not mail["adresse"]:
        return "ignore: expediteur invalide"
    motif = _email_automatique(mail)
    if motif:
        return f"ignore: {motif}"
    content = _email_nettoyer_texte(mail["texte"]) or _email_nettoyer_texte(mail["sujet"])
    if not content:
        return "ignore: vide"
    if not mail["message_id"]:
        # Sans Message-ID (format générique) : identifiant stable dérivé du contenu.
        empreinte = hashlib.sha256(
            "\n".join((mail["adresse"], mail["sujet"], content, mail["entetes"].get("date", ""))).encode("utf-8")
        ).hexdigest()[:40]
        mail["message_id"] = f"sans-id-{empreinte}"
        mail["sans_id"] = True
    mail["message_id"] = mail["message_id"][:500]
    if _email_deja_recu(mail["message_id"]):
        return "doublon"
    client_id = _email_identifier_client(mail["adresse"])
    if not client_id:
        background_tasks.add_task(_email_alerte_inconnu, mail["adresse"], mail["sujet"])
        return "inconnu"
    if not _email_inserer_question(client_id, content, mail):
        return "doublon"
    if mail.get("sans_id"):
        mail["message_id"] = ""  # pas d'In-Reply-To inventé
    background_tasks.add_task(_email_traiter, client_id, content, mail)
    return "enregistre"


@app.post("/canaux/email/entrant")
async def canal_email_entrant(request: Request, background_tasks: BackgroundTasks,
                              token: Optional[str] = None):
    """Webhook du fournisseur e-mail (route publique, protégée par EMAIL_ENTRANT_SECRET).

    Formats : générique {from, from_name, subject, text, message_id, in_reply_to, headers}
    ou Brevo Inbound Parsing {"items": [...]}. Répond 200 dès que l'e-mail est pris en
    compte (ou volontairement ignoré) : le fournisseur ne le renvoie pas.
    """
    if not _EMAIL_ENTRANT_SECRET:
        raise HTTPException(status_code=503, detail="Canal e-mail non configuré")
    fourni = request.headers.get("x-eaukey-webhook-secret") or token or ""
    if not fourni:
        raise HTTPException(status_code=401, detail="Secret manquant")
    if not hmac.compare_digest(fourni.encode("utf-8"), _EMAIL_ENTRANT_SECRET.encode("utf-8")):
        raise HTTPException(status_code=403, detail="Secret invalide")

    longueur = request.headers.get("content-length")
    if longueur and longueur.isdigit() and int(longueur) > _EMAIL_MAX_OCTETS:
        raise HTTPException(status_code=413, detail="E-mail trop volumineux")
    brut = await request.body()
    if len(brut) > _EMAIL_MAX_OCTETS:
        raise HTTPException(status_code=413, detail="E-mail trop volumineux")
    try:
        payload = json.loads(brut.decode("utf-8"))
    except ValueError:
        raise HTTPException(status_code=400, detail="JSON invalide")

    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        mails = [_email_depuis_brevo(i) for i in payload["items"][:20] if isinstance(i, dict)]
    elif isinstance(payload, dict):
        mails = [_email_depuis_generique(payload)]
    else:
        raise HTTPException(status_code=400, detail="Format non reconnu")

    resultats = [_email_recevoir(m, background_tasks) for m in mails]
    for r in resultats:
        _EMAIL_LOGGER.info("E-mail entrant : %s", r)
    return {"recus": len(mails), "resultats": resultats}


# --- Adresses reconnues d'un client (admin) ---------------------------------

class ClientContactIn(BaseModel):
    valeur: str = Field(..., min_length=3, max_length=254)
    canal: str = "email"


def _client_contact_to_dict(r) -> dict:
    return {"id": r[0], "canal": r[1], "valeur": r[2], "created_by": r[3], "created_at": _iso(r[4])}


@app.get("/clients/{client_id}/contacts")
def client_contacts_liste(client_id: str, request: Request):
    """Adresses e-mail reconnues pour un client : saisies par l'équipe + e-mails des stations."""
    _require_admin(request)
    rows = executer_requete_sql(
        """
        SELECT id, canal, valeur, created_by, created_at FROM client_contacts
        WHERE client_id = %s ORDER BY created_at, id;
        """,
        (client_id,),
    )
    stations = set()
    for r in executer_requete_sql(
        "SELECT email, email2, email3 FROM automate WHERE lower(trim(client)) = lower(trim(%s));", (client_id,)
    ):
        for champ in r:
            stations |= {a for a in (_email_normaliser_adresse(x) for x in re.split(r"[;,\s]+", champ or "") if x) if a}
    return {"contacts": [_client_contact_to_dict(r) for r in rows], "stations": sorted(stations)}


@app.post("/clients/{client_id}/contacts")
def client_contacts_ajouter(client_id: str, payload: ClientContactIn, request: Request):
    """Rattache une adresse e-mail à un client (elle ne peut appartenir qu'à un client)."""
    email_admin = _require_admin(request)
    if payload.canal != "email":
        raise HTTPException(status_code=400, detail="Seul le canal e-mail est disponible")
    adresse = _email_normaliser_adresse(payload.valeur)
    if not adresse or adresse != payload.valeur.strip().lower():
        raise HTTPException(status_code=400, detail="Adresse e-mail invalide")
    row = executer_requete_sql_one(
        """
        INSERT INTO client_contacts (client_id, canal, valeur, created_by)
        VALUES (%s, 'email', %s, %s)
        ON CONFLICT (canal, valeur) DO NOTHING
        RETURNING id, canal, valeur, created_by, created_at;
        """,
        (client_id, adresse, email_admin),
    )
    if row:
        return _client_contact_to_dict(row)
    existant = executer_requete_sql_one(
        "SELECT id, canal, valeur, created_by, created_at, client_id FROM client_contacts WHERE canal = 'email' AND valeur = %s;",
        (adresse,),
    )
    if existant and existant[5] == client_id:
        return _client_contact_to_dict(existant)
    if existant:
        raise HTTPException(status_code=409, detail="Adresse déjà rattachée à un autre client")
    raise HTTPException(status_code=500, detail="Enregistrement impossible")


@app.delete("/clients/{client_id}/contacts/{contact_id}")
def client_contacts_supprimer(client_id: str, contact_id: int, request: Request):
    _require_admin(request)
    row = executer_requete_sql_one(
        "DELETE FROM client_contacts WHERE id = %s AND client_id = %s RETURNING id;", (contact_id, client_id)
    )
    if not row:
        raise HTTPException(status_code=404, detail="Contact introuvable")
    return {"status": "success", "id": row[0]}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8011"))
    uvicorn.run(app, host="0.0.0.0", port=port)
