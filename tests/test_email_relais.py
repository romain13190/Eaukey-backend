"""Tests de l'envoi d'e-mails par le relais Google Apps Script (_mail_envoyer).

Aucune base ni réseau : le relais est simulé (httpx.MockTransport), le SMTP aussi
(aiosmtplib.send remplacé). AUCUN e-mail n'est envoyé.

    python tests/test_email_relais.py
"""
import asyncio
import json
import os
import sys

import aiosmtplib
import httpx

ICI = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(ICI))
os.environ.update({"DB_NAME": "x", "DB_USER": "x", "DB_HOST": "/nonexistent", "DB_PASSWORD": "x", "AUTH_SECRET": "t"})

import backend  # noqa: E402

ok = 0


def check(cond, label):
    global ok
    assert cond, label
    ok += 1
    print("  ok -", label)


APPELS, SMTP = [], []
REPONSE = {"mode": "ok"}


def relais(req: httpx.Request) -> httpx.Response:
    # Apps Script : POST /exec -> 302 vers googleusercontent -> JSON
    if req.url.host == "script.google.com":
        APPELS.append(json.loads(req.content))
        return httpx.Response(302, headers={"location": "https://script.googleusercontent.com/macros/echo?x=1"})
    if REPONSE["mode"] == "refus":
        return httpx.Response(200, json={"ok": False, "error": "secret"})
    if REPONSE["mode"] == "html":
        return httpx.Response(200, text="<html>Erreur Google</html>")
    return httpx.Response(200, json={"ok": True, "quota": 1499})


_Vrai = httpx.AsyncClient
httpx.AsyncClient = lambda **kw: _Vrai(transport=httpx.MockTransport(relais), **kw)


async def faux_smtp(msg, **kw):
    SMTP.append((msg["To"], kw.get("hostname")))

aiosmtplib.send = faux_smtp


def configurer(relais_ok: bool, smtp_ok: bool):
    backend._EMAIL_RELAIS_URL = "https://script.google.com/macros/s/ABC/exec" if relais_ok else ""
    backend._EMAIL_RELAIS_SECRET = "s3cret" if relais_ok else ""
    backend._SMTP_HOST, backend._SMTP_USER, backend._SMTP_PASSWORD = (("smtp.gmail.com", "u", "p") if smtp_ok else ("", "", ""))
    APPELS.clear()
    SMTP.clear()
    REPONSE["mode"] = "ok"


async def main():
    # 1. Mot de passe oublié par le relais (HTML + texte, redirection suivie)
    configurer(True, True)
    r = await backend._send_reset_email("client@exemple.fr", "jeton123")
    check(r is True and len(APPELS) == 1 and SMTP == [], "réinitialisation : relais Google appelé, SMTP non utilisé")
    a = APPELS[0]
    check(a["secret"] == "s3cret" and a["to"] == "client@exemple.fr" and a["name"] == "Eaukey", "secret, destinataire, nom d'expéditeur")
    check("jeton123" in a["text"] and "jeton123" in a["html"] and a["subject"], "parties texte et HTML transmises")

    # 2. Alerte d'escalade à l'équipe par le relais
    configurer(True, False)
    await backend._chat_prevenir_equipe("TEST EAUKEY", "pompe à remplacer", "Ma pompe est cassée", "L'équipe est prévenue.")
    check(len(APPELS) == 1 and APPELS[0]["to"] == backend._SAV_ESCALADE_EMAIL, "escalade : relais utilisé même sans SMTP")
    check("pompe à remplacer" in APPELS[0]["text"] and "TEST EAUKEY" in APPELS[0]["subject"], "motif et client dans l'e-mail")

    # 3. Le relais refuse (mauvais secret) -> échec propre, pas d'exception remontée
    configurer(True, True)
    REPONSE["mode"] = "refus"
    r = await backend._send_reset_email("client@exemple.fr", "jeton")
    check(r is False and SMTP == [], "refus du relais : envoi signalé en échec, pas de bascule silencieuse")
    REPONSE["mode"] = "html"
    try:
        await backend._mail_envoyer(_message("x@y.fr"))
        leve = False
    except RuntimeError:
        leve = True
    check(leve, "réponse non JSON du relais : erreur levée")

    # 4. Sans relais : SMTP comme avant
    configurer(False, True)
    r = await backend._send_reset_email("client@exemple.fr", "jeton")
    check(r is True and SMTP == [("client@exemple.fr", "smtp.gmail.com")] and APPELS == [], "sans relais : envoi SMTP inchangé")

    # 5. Rien de configuré
    configurer(False, False)
    check(backend._mail_configure() is False, "_mail_configure faux sans relais ni SMTP")
    r = await backend._send_reset_email("client@exemple.fr", "jeton")
    check(r is False and APPELS == [] and SMTP == [], "rien de configuré : aucun envoi")


def _message(to):
    from email.mime.text import MIMEText
    m = MIMEText("bonjour", "plain", "utf-8")
    m["To"], m["Subject"], m["From"] = to, "Test", "noreply@eaukey.net"
    return m


asyncio.run(main())
print(f"\n{ok} vérifications OK")
