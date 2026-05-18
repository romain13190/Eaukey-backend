# Tuto automate — Paramètres modifiables

Document à destination du dev firmware de l'automate.
Objectif : permettre au client de **modifier des paramètres** depuis l'app web,
puis que l'automate **récupère et applique** ces modifications.

---

## 1. Contexte rapide

Aujourd'hui, l'automate envoie déjà ses **mesures brutes** via :

```
POST /api/automate    → table  `mesures`
```

On ajoute la même mécanique pour les **paramètres modifiables** (consignes,
on/off, seuils...), avec en plus une **boucle de retour** : le client peut
poser une nouvelle valeur depuis l'app, et l'automate doit aller la lire et
l'appliquer.

Trois nouveaux endpoints :

| # | Endpoint | Méthode | Fait par | Quand |
|---|---|---|---|---|
| A | `/api/donnees_modifiables` | **POST** | Automate | À chaque cycle, comme `/api/automate` |
| B | `/api/consignes_modifiables/<nom_automate>/diff` | **GET** | Automate | Poll régulier (toutes les 5 min) |
| C | `/api/consignes_modifiables/<nom_automate>` | **GET** | Automate | Seulement si le diff renvoie 1 |

---

## 2. Vue d'ensemble du flow

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │  APP CLIENT (web)                                                   │
 │  Le client ouvre /parametres, modifie des valeurs, clique           │
 │  "Enregistrer" → l'app POST la cible dans `consignes_modifiables`.  │
 └─────────────────────────────────────────────────────────────────────┘
                                  ↓
 ┌─────────────────────────────────────────────────────────────────────┐
 │  AUTOMATE (toutes les 5 min)                                        │
 │  1. GET .../diff  →  {"diff": 0 ou 1}                               │
 │     - 0 : rien à faire                                              │
 │     - 1 : une cible est en attente                                  │
 │                                                                     │
 │  2. Si diff=1 :                                                     │
 │     GET .../<nom_automate>  →  {"data": {...32 paramètres...}}      │
 │     L'automate applique localement ces valeurs.                     │
 │                                                                     │
 │  3. Au prochain cycle de mesures :                                  │
 │     POST /api/donnees_modifiables avec les NOUVELLES valeurs        │
 │     (= les valeurs que tu viens d'appliquer).                       │
 │                                                                     │
 │  → Le backend va comparer : donnees_modifiables == consignes        │
 │     → /diff repasse à 0 automatiquement. Plus rien à faire.         │
 └─────────────────────────────────────────────────────────────────────┘
```

**Important** : c'est le simple fait de repérer le POST avec les nouvelles
valeurs qui éteint le drapeau. Pas besoin d'appeler un endpoint
"j'ai appliqué". Le diff est calculé en SQL avec `IS DISTINCT FROM`.

---

## 3. Endpoint A — `POST /api/donnees_modifiables`

**Exactement le même principe que `POST /api/automate`** (le POST que tu
fais déjà pour les mesures), avec un body JSON différent.

À appeler **à chaque cycle** (même cadence que les mesures, ou moins
fréquemment si les paramètres bougent peu) avec l'**état actuel** des
paramètres lus dans l'automate.

### Exemple de body JSON

```json
{
  "horodatage": "2026-05-18T08:00:00",
  "numero_automate": "2022121.0",
  "nom_automate": "BODIN_TEST",

  "relevage_on": true,
  "filtration_on": true,
  "renvoi_on": false,

  "consigne_pompe_relevage": 2100,
  "consigne_debit_max_pompe_relevage_m3h": 12.5,
  "choix_pompe_relevage": 1,
  "temps_ouverture_decanteur_min": 5.0,
  "volume_relevage_entre_pause_decal": 2.0,
  "temps_pause_ms": 500,
  "hauteur_cuve_traitement_demarrage_relevage_pc": 30.0,

  "consigne_vitesse_pompe_filtration": 2800,
  "consigne_pression_max_filtre_mbar": 1800.0,
  "hauteur_stop_filtration_pc": 95.0,
  "hauteur_relance_filtration_pc": 60.0,
  "choix_pompe_filtration": 1,
  "hauteur_cuve_traitement_demarrage_filtration_pc": 40.0,

  "hauteur_min_remplissage_eau_adoucie_pc": 20.0,
  "hauteur_max_pc": 90.0,
  "valeur_min_conductivite_us_cm2": 100.0,
  "valeur_max_conductivite_us_cm2": 800.0,
  "volume_actualisation_renvoi_dilution_m3": 1.0,
  "choix_pompe_renvoi": 1,
  "consigne_vitesse_pompe_renvoi": 2500,
  "consigne_pression_station_mbar": 2400.0,
  "hysteresis_renvoi_mbar": 100.0,
  "ouverture_electrovanne_station_mbar": 1800.0,
  "fermeture_electrovanne_station_mbar": 2200.0,

  "temps_cl_filtre_media": 30.0,
  "temps_cl_ca_filtre_transparent": 30.0,
  "frequence_vidange_cuve": 7.0,
  "frequence_vidange_filtration": 14.0,

  "temps_dosage": 5.0
}
```

### Réponse attendue (succès)

```json
{"etat": "Donnees inserees avec success dans la table donnees_modifiables."}
```

> Comme pour les mesures, la table est **historisée** (1 ligne par POST).
> C'est la dernière ligne par `horodatage` qui fait foi.

---

## 4. Endpoint B — `GET /api/consignes_modifiables/<nom_automate>/diff`

C'est le **drapeau** "il y a quelque chose à appliquer".

### Quand l'appeler

À intervalle régulier, par exemple **toutes les 5 minutes**.
Léger (juste un booléen renvoyé), tu peux le faire plus souvent si besoin.

### Exemple d'appel

```
GET /api/consignes_modifiables/BODIN_TEST/diff
```

### Réponses possibles

```json
{"diff": 0}    // RAS, rien à faire
{"diff": 1}    // Une cible est en attente, va lire l'endpoint C
```

### Comportement attendu

- `diff = 0` → ne fais rien.
- `diff = 1` → enchaîne **immédiatement** sur l'endpoint C.

---

## 5. Endpoint C — `GET /api/consignes_modifiables/<nom_automate>`

À appeler **uniquement** quand le diff vaut 1.
Renvoie la **cible** posée par le client depuis l'app.

### Exemple d'appel

```
GET /api/consignes_modifiables/BODIN_TEST
```

### Exemple de réponse

```json
{
  "data": {
    "relevage_on": true,
    "filtration_on": true,
    "renvoi_on": false,
    "consigne_pompe_relevage": 2100,
    "consigne_debit_max_pompe_relevage_m3h": 12.5,
    "consigne_pression_station_mbar": 2800.0,
    "...": "... (tous les autres champs, 32 au total)"
  }
}
```

### Comportement attendu

1. Lis les valeurs.
2. **Applique chaque paramètre localement** dans l'automate.
3. Au **prochain cycle** de POST sur `/api/donnees_modifiables`, envoie ces
   nouvelles valeurs (= ton nouvel état après application).
4. Tu n'as **rien d'autre à faire** : le backend va voir que `donnees == consignes`
   et le diff repassera à 0 tout seul.

### Cas particuliers

- Si une valeur vaut `null` → garde la valeur actuelle de l'automate
  (ce paramètre n'a pas été touché par le client).
- Si la réponse est `{"data": null}` → cas anormal (diff=1 mais aucune cible
  trouvée), tu peux ignorer ce cycle.

---

## 6. Exemple complet — Le client veut passer la pression de 2400 à 2800 mbar

**État initial** dans `donnees_modifiables` :
```
consigne_pression_station_mbar = 2400
```

### Étape 1 : le client clique "Enregistrer" dans l'app

L'app pose la cible (rien à faire côté automate, c'est l'app qui le fait) :
```
consignes_modifiables :
  consigne_pression_station_mbar = 2800
  (les 31 autres valeurs sont identiques à l'état actuel)
```

### Étape 2 : l'automate poll (5 min plus tard)

```
GET /api/consignes_modifiables/BODIN_TEST/diff
→ {"diff": 1}
```

### Étape 3 : l'automate récupère la cible

```
GET /api/consignes_modifiables/BODIN_TEST
→ {"data": { "consigne_pression_station_mbar": 2800, ... }}
```

### Étape 4 : l'automate applique 2800 mbar en interne

### Étape 5 : prochain POST des paramètres

```
POST /api/donnees_modifiables
{
  "consigne_pression_station_mbar": 2800,
  ...
}
```

### Étape 6 : 5 min après, le poll suivant

```
GET /api/consignes_modifiables/BODIN_TEST/diff
→ {"diff": 0}   // Tout est aligné, plus rien à faire
```

---

## 7. Liste complète des 32 paramètres modifiables

Tous **optionnels** dans le POST : si un champ n'est pas envoyé, il sera
stocké comme `NULL`. Le mieux est de **toujours envoyer la valeur courante**
de chaque champ pour éviter les NULL parasites.

### Booléens (on/off)
| Nom | Type |
|---|---|
| `relevage_on` | bool |
| `filtration_on` | bool |
| `renvoi_on` | bool |

### Relevage
| Nom | Type | Unité / plage |
|---|---|---|
| `consigne_pompe_relevage` | int | / 4200 |
| `consigne_debit_max_pompe_relevage_m3h` | real | m³/h |
| `choix_pompe_relevage` | int | 1 à 75 |
| `temps_ouverture_decanteur_min` | real | min |
| `volume_relevage_entre_pause_decal` | real | m³ |
| `temps_pause_ms` | int | ms |
| `hauteur_cuve_traitement_demarrage_relevage_pc` | real | % |

### Filtration / Traitement
| Nom | Type | Unité / plage |
|---|---|---|
| `consigne_vitesse_pompe_filtration` | int | / 4200 |
| `consigne_pression_max_filtre_mbar` | real | mbar |
| `hauteur_stop_filtration_pc` | real | % |
| `hauteur_relance_filtration_pc` | real | % |
| `choix_pompe_filtration` | int | 1 à 75 |
| `hauteur_cuve_traitement_demarrage_filtration_pc` | real | % |

### Renvoi
| Nom | Type | Unité |
|---|---|---|
| `hauteur_min_remplissage_eau_adoucie_pc` | real | % |
| `hauteur_max_pc` | real | % |
| `valeur_min_conductivite_us_cm2` | real | µS/cm² |
| `valeur_max_conductivite_us_cm2` | real | µS/cm² |
| `volume_actualisation_renvoi_dilution_m3` | real | m³ |
| `choix_pompe_renvoi` | int | 1 à 75 |
| `consigne_vitesse_pompe_renvoi` | int | / 4200 |
| `consigne_pression_station_mbar` | real | mbar |
| `hysteresis_renvoi_mbar` | real | mbar |
| `ouverture_electrovanne_station_mbar` | real | mbar |
| `fermeture_electrovanne_station_mbar` | real | mbar |

### Vidange
| Nom | Type |
|---|---|
| `temps_cl_filtre_media` | real |
| `temps_cl_ca_filtre_transparent` | real |
| `frequence_vidange_cuve` | real |
| `frequence_vidange_filtration` | real |

### Pompe doseuse
| Nom | Type |
|---|---|
| `temps_dosage` | real |

---

## 8. Points d'attention

- **Types JSON** : booléens en `true` / `false` (pas `1` / `0`), entiers sans
  décimale, réels avec point décimal (`12.5`, pas `12,5`).
- **`null`** : ne JAMAIS appliquer une valeur `null` reçue dans l'endpoint C.
  Garde la valeur actuelle de l'automate pour ce paramètre.
- **Pas besoin de notifier "j'ai appliqué"** : le simple fait de POSTer les
  nouvelles valeurs sur `/api/donnees_modifiables` éteint le drapeau côté
  serveur.
- **Polling raisonnable** : 5 min suffit largement. Inutile de polliner toutes
  les secondes — la modif d'un paramètre n'est jamais urgente à la seconde.
- **Robustesse** : si un appel échoue (réseau, 5xx...), retente au cycle
  suivant. Le drapeau reste à 1 tant que la cible n'a pas été appliquée.
- **`nom_automate`** : utilise toujours le nom (`BODIN_TEST`...), pas le
  numéro (`2022121.0`), dans l'URL.

---

## 9. Récap des appels côté automate

```
TOUS LES CYCLES (mesures + paramètres) :
  POST /api/automate                              ← déjà en place (mesures)
  POST /api/donnees_modifiables                   ← NOUVEAU (paramètres actuels)

TOUTES LES 5 MIN :
  GET  /api/consignes_modifiables/<nom>/diff      ← NOUVEAU (le drapeau)
  
  Si diff == 1 :
    GET /api/consignes_modifiables/<nom>          ← NOUVEAU (récupère la cible)
    → appliquer les valeurs en local
    → le prochain POST /api/donnees_modifiables fera tomber le drapeau
```

C'est tout. Tu n'as donc qu'**un POST de plus** dans le cycle existant,
plus **un petit GET de poll** toutes les 5 min, et **un GET ponctuel** quand
le client modifie quelque chose.

---

## 10. Pour tester en local

Quand tout est déployé, tu peux simuler le flow complet avec `curl` :

```bash
# 1. POST l'état actuel
curl -X POST https://<URL_API>/api/donnees_modifiables \
  -H "Content-Type: application/json" \
  -d '{"horodatage":"2026-05-18T08:00:00","numero_automate":"2022121.0","nom_automate":"BODIN_TEST","consigne_pression_station_mbar":2400, ...}'

# 2. Vérifier qu'aucune cible n'est en attente
curl https://<URL_API>/api/consignes_modifiables/BODIN_TEST/diff
# → {"diff": 0}

# 3. (le client clique Enregistrer dans l'app, qui modifie consigne_pression_station_mbar à 2800)

# 4. Re-vérifier
curl https://<URL_API>/api/consignes_modifiables/BODIN_TEST/diff
# → {"diff": 1}

# 5. Récupérer la cible
curl https://<URL_API>/api/consignes_modifiables/BODIN_TEST
# → {"data": {"consigne_pression_station_mbar": 2800, ...}}

# 6. Appliquer en local puis re-POST l'état
curl -X POST https://<URL_API>/api/donnees_modifiables \
  -H "Content-Type: application/json" \
  -d '{"...","consigne_pression_station_mbar":2800, ...}'

# 7. Diff redevient 0
curl https://<URL_API>/api/consignes_modifiables/BODIN_TEST/diff
# → {"diff": 0}
```
