# Tests

Scripts de test autonomes, à lancer **uniquement contre une base Postgres jetable locale** :
ils suppriment et recréent les tables dont ils ont besoin, avec des données synthétiques.
Ne jamais les pointer vers la base de production.

## Lancer

```bash
# 1. Base jetable (exemple)
initdb -D /tmp/pg_test -U postgres -A trust
pg_ctl -D /tmp/pg_test -o "-p 55436 -k /tmp -c listen_addresses=''" start

# 2. Dépendances du backend (pip install -r requirements.txt) + httpx pour TestClient
export CANAL_TEST_PGHOST=/tmp CANAL_TEST_PGPORT=55436
python tests/test_canal_email.py

# 3. Arrêt
pg_ctl -D /tmp/pg_test stop
```

| Script | Ce qu'il vérifie |
|---|---|
| `test_canal_email.py` | Canal e-mail de la messagerie (`/canaux/email/entrant`, relais des réponses de l'équipe, `/clients/{client_id}/contacts`) : secret, formats générique et Brevo, citations retirées, anti-boucle, idempotence, expéditeur inconnu / ambigu, escalade, humain actif, droits. Faux Wardian, SMTP intercepté (aucun e-mail envoyé) |
