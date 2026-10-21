# Tests

Scripts de test autonomes, à lancer **uniquement contre une base Postgres jetable locale** :
ils suppriment et recréent les tables dont ils ont besoin, avec des données synthétiques.
Ne jamais les pointer vers la base de production.

## Lancer

```bash
# 1. Base jetable (exemple)
initdb -D /tmp/pg_test -U postgres -A trust
pg_ctl -D /tmp/pg_test -o "-p 55434 -k /tmp -c listen_addresses=''" start

# 2. Dépendances du backend (pip install -r requirements.txt) + httpx pour TestClient
export RAPPORT_TEST_PGHOST=/tmp RAPPORT_TEST_PGPORT=55434
python tests/test_rapport_mensuel.py

# 3. Arrêt
pg_ctl -D /tmp/pg_test stop
```

| Script | Ce qu'il vérifie |
|---|---|
| `test_rapport_mensuel.py` | Rapport mensuel par client (`/rapport/mensuel`, `/html`, `/clients`) : calculs, remise à zéro de compteur, trou de données, M-1, cumul annuel, pannes, isolation entre clients, droits |
| `test_canal_email.py` (PR canal e-mail) | Canal e-mail de la messagerie (`/canaux/email/entrant`, relais des réponses de l'équipe, `/clients/{client_id}/contacts`) : secret, formats générique et Brevo, anti-boucle, idempotence, expéditeur inconnu / ambigu, escalade, droits. Variables `CANAL_TEST_PGHOST` / `CANAL_TEST_PGPORT`. Faux Wardian, SMTP intercepté |
| `test_problemes_frequents.py` (PR problèmes fréquents) | Rapport des problèmes fréquents pour l'équipe (`/rapport/problemes`) |
