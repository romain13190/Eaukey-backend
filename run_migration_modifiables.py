"""Cree les tables donnees_modifiables et consignes_modifiables.

Idempotent : CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.
Si les tables existent deja, le script ne fait rien et l'indique.

Usage :
    cd eaukey_backend && source .venv/bin/activate
    python3 run_migration_modifiables.py
"""

import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

SQL = """
CREATE TABLE IF NOT EXISTS donnees_modifiables (
    horodatage      TIMESTAMP,
    numero_automate VARCHAR(9),
    nom_automate    VARCHAR(12),
    relevage_on   BOOLEAN,
    filtration_on BOOLEAN,
    renvoi_on     BOOLEAN,
    consigne_pompe_relevage                       INTEGER,
    consigne_debit_max_pompe_relevage_m3h         REAL,
    choix_pompe_relevage                          INTEGER,
    temps_ouverture_decanteur_min                 REAL,
    volume_relevage_entre_pause_decal             REAL,
    temps_pause_ms                                INTEGER,
    hauteur_cuve_traitement_demarrage_relevage_pc REAL,
    consigne_vitesse_pompe_filtration               INTEGER,
    consigne_pression_max_filtre_mbar               REAL,
    hauteur_stop_filtration_pc                      REAL,
    hauteur_relance_filtration_pc                   REAL,
    choix_pompe_filtration                          INTEGER,
    hauteur_cuve_traitement_demarrage_filtration_pc REAL,
    hauteur_min_remplissage_eau_adoucie_pc  REAL,
    hauteur_max_pc                          REAL,
    valeur_min_conductivite_us_cm2          REAL,
    valeur_max_conductivite_us_cm2          REAL,
    volume_actualisation_renvoi_dilution_m3 REAL,
    choix_pompe_renvoi                      INTEGER,
    consigne_vitesse_pompe_renvoi           INTEGER,
    consigne_pression_station_mbar          REAL,
    hysteresis_renvoi_mbar                  REAL,
    ouverture_electrovanne_station_mbar     REAL,
    fermeture_electrovanne_station_mbar     REAL,
    temps_cl_filtre_media          REAL,
    temps_cl_ca_filtre_transparent REAL,
    frequence_vidange_cuve         REAL,
    frequence_vidange_filtration   REAL,
    temps_dosage REAL
);

CREATE INDEX IF NOT EXISTS idx_donnees_modif_automate_horodatage
    ON donnees_modifiables (nom_automate, horodatage DESC);

CREATE TABLE IF NOT EXISTS consignes_modifiables (
    nom_automate    VARCHAR(12) PRIMARY KEY,
    numero_automate VARCHAR(9),
    relevage_on   BOOLEAN,
    filtration_on BOOLEAN,
    renvoi_on     BOOLEAN,
    consigne_pompe_relevage                       INTEGER,
    consigne_debit_max_pompe_relevage_m3h         REAL,
    choix_pompe_relevage                          INTEGER,
    temps_ouverture_decanteur_min                 REAL,
    volume_relevage_entre_pause_decal             REAL,
    temps_pause_ms                                INTEGER,
    hauteur_cuve_traitement_demarrage_relevage_pc REAL,
    consigne_vitesse_pompe_filtration               INTEGER,
    consigne_pression_max_filtre_mbar               REAL,
    hauteur_stop_filtration_pc                      REAL,
    hauteur_relance_filtration_pc                   REAL,
    choix_pompe_filtration                          INTEGER,
    hauteur_cuve_traitement_demarrage_filtration_pc REAL,
    hauteur_min_remplissage_eau_adoucie_pc  REAL,
    hauteur_max_pc                          REAL,
    valeur_min_conductivite_us_cm2          REAL,
    valeur_max_conductivite_us_cm2          REAL,
    volume_actualisation_renvoi_dilution_m3 REAL,
    choix_pompe_renvoi                      INTEGER,
    consigne_vitesse_pompe_renvoi           INTEGER,
    consigne_pression_station_mbar          REAL,
    hysteresis_renvoi_mbar                  REAL,
    ouverture_electrovanne_station_mbar     REAL,
    fermeture_electrovanne_station_mbar     REAL,
    temps_cl_filtre_media          REAL,
    temps_cl_ca_filtre_transparent REAL,
    frequence_vidange_cuve         REAL,
    frequence_vidange_filtration   REAL,
    temps_dosage REAL,
    updated_at TIMESTAMP DEFAULT now(),
    updated_by TEXT
);
"""

TABLES = ("donnees_modifiables", "consignes_modifiables")


def main() -> int:
    conninfo = " ".join(
        f"{k}={v}"
        for k, v in {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "connect_timeout": "5",
        }.items()
        if v
    )

    print(f"Connexion a {os.getenv('DB_HOST')}/{os.getenv('DB_NAME')} ...")
    with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name = ANY(%s)",
            (list(TABLES),),
        )
        before = {row[0] for row in cur.fetchall()}
        for t in TABLES:
            print(f"  - {t} : {'deja presente' if t in before else 'MANQUANTE'}")

        print("Execution du SQL (idempotent) ...")
        cur.execute(SQL)
        conn.commit()

        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name = ANY(%s)",
            (list(TABLES),),
        )
        after = {row[0] for row in cur.fetchall()}
        created = after - before
        print("Resultat :")
        for t in TABLES:
            if t in created:
                print(f"  - {t} : CREEE")
            elif t in after:
                print(f"  - {t} : OK (existait deja, aucune modification)")
            else:
                print(f"  - {t} : ECHEC (toujours absente)")
                return 1
    print("Termine.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
