-- Migration: Create consignes_modifiables table
-- Stocke la cible des consignes/parametres modifies depuis l'UI.
-- 1 ligne par automate (PK nom_automate) = la valeur "souhaitee".
-- L'automate poll /api/consignes_modifiables/<nom_automate>/diff toutes les 5 min :
--   - 0 si donnees_modifiables (derniere ligne recue) == consignes_modifiables
--   - 1 sinon -> l'automate va lire /api/consignes_modifiables/<nom_automate> et appliquer.
-- Une fois applique, l'automate renverra ces memes valeurs via son POST habituel
-- sur /api/donnees_modifiables, et le diff repassera a 0.
-- Types alignes sur la struct Flask db_donnees_modifiables (api.py).

CREATE TABLE IF NOT EXISTS consignes_modifiables (
    nom_automate VARCHAR(12) PRIMARY KEY,
    numero_automate VARCHAR(9),

    -- Sorties Binaires (On/Off)
    relevage_on BOOLEAN,
    filtration_on BOOLEAN,
    renvoi_on BOOLEAN,

    -- Sorties Numeriques : Relevage
    consigne_pompe_relevage INTEGER,
    consigne_debit_max_pompe_relevage_m3h REAL,
    choix_pompe_relevage INTEGER,
    temps_ouverture_decanteur_min REAL,
    volume_relevage_entre_pause_decal REAL,
    temps_pause_ms INTEGER,
    hauteur_cuve_traitement_demarrage_relevage_pc REAL,

    -- Sorties Numeriques : Filtration / Traitement
    consigne_vitesse_pompe_filtration INTEGER,
    consigne_pression_max_filtre_mbar REAL,
    hauteur_stop_filtration_pc REAL,
    hauteur_relance_filtration_pc REAL,
    choix_pompe_filtration INTEGER,
    hauteur_cuve_traitement_demarrage_filtration_pc REAL,

    -- Sorties Numeriques : Renvoi
    hauteur_min_remplissage_eau_adoucie_pc REAL,
    hauteur_max_pc REAL,
    valeur_min_conductivite_us_cm2 REAL,
    valeur_max_conductivite_us_cm2 REAL,
    volume_actualisation_renvoi_dilution_m3 REAL,
    choix_pompe_renvoi INTEGER,
    consigne_vitesse_pompe_renvoi INTEGER,
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
    temps_dosage REAL,

    updated_at TIMESTAMP DEFAULT now(),
    updated_by TEXT
);
