-- Migration: Create donnees_modifiables table
-- Stocke les consignes / parametres modifiables envoyes via l'endpoint Flask /api/donnees_modifiables.
-- Table historisee : chaque POST ajoute une nouvelle ligne.
-- L'etat courant d'un automate = derniere ligne par horodatage DESC.
-- Types alignes sur la struct Flask (db_donnees_modifiables) dans /root/eaukey/api.

CREATE TABLE IF NOT EXISTS donnees_modifiables (
    horodatage TIMESTAMP,
    numero_automate VARCHAR(9),
    nom_automate VARCHAR(12),

    -- Sorties Binaires (On/Off)
    relevage_on BOOLEAN,
    filtration_on BOOLEAN,
    renvoi_on BOOLEAN,

    -- Sorties Numeriques : Relevage
    consigne_pompe_relevage INTEGER,                          -- /4200
    consigne_debit_max_pompe_relevage_m3h REAL,
    choix_pompe_relevage INTEGER,                             -- 1 a 75
    temps_ouverture_decanteur_min REAL,
    volume_relevage_entre_pause_decal REAL,
    temps_pause_ms INTEGER,
    hauteur_cuve_traitement_demarrage_relevage_pc REAL,

    -- Sorties Numeriques : Filtration / Traitement
    consigne_vitesse_pompe_filtration INTEGER,                -- /4200
    consigne_pression_max_filtre_mbar REAL,
    hauteur_stop_filtration_pc REAL,
    hauteur_relance_filtration_pc REAL,
    choix_pompe_filtration INTEGER,                           -- 1 a 75
    hauteur_cuve_traitement_demarrage_filtration_pc REAL,

    -- Sorties Numeriques : Renvoi
    hauteur_min_remplissage_eau_adoucie_pc REAL,
    hauteur_max_pc REAL,
    valeur_min_conductivite_us_cm2 REAL,
    valeur_max_conductivite_us_cm2 REAL,
    volume_actualisation_renvoi_dilution_m3 REAL,
    choix_pompe_renvoi INTEGER,                               -- 1 a 75
    consigne_vitesse_pompe_renvoi INTEGER,                    -- /4200
    consigne_pression_station_mbar REAL,
    hysteresis_renvoi_mbar REAL,
    ouverture_electrovanne_station_mbar REAL,
    fermeture_electrovanne_station_mbar REAL,

    -- Sorties Numeriques : Vidange
    temps_cl_filtre_media REAL,
    temps_cl_ca_filtre_transparent REAL,
    frequence_vidange_cuve REAL,
    frequence_vidange_filtration REAL,

    -- Sorties Numeriques : Pompe Doseuse
    temps_dosage REAL
);

CREATE INDEX IF NOT EXISTS idx_donnees_modif_automate_horodatage
    ON donnees_modifiables (nom_automate, horodatage DESC);
