#!/usr/bin/env python3
"""
Fatture in Cloud - OAuth2 Authorization Code Flow helper (CLI)

Prerequisiti:
  pip install requests

Config:
  - Crea/controlla l'app su Fatture in Cloud e recupera:
      CLIENT_ID
      CLIENT_SECRET
  - Imposta tra i Redirect URI dell'app:
      http://localhost:8000/callback

Cosa fa:
  1) Apre il browser per autorizzazione
  2) Cattura il ?code=... sul redirect locale
  3) Scambia code -> access_token + refresh_token
  4) Salva su fic_token.json

Riferimenti:
  - /oauth/authorize e /oauth/token su api-v2.fattureincloud.it
  - Scopes: https://developers.fattureincloud.it/docs/basics/scopes/
"""

import json
import os
from dotenv import load_dotenv
import threading
import time
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests

load_dotenv()

AUTH_BASE = "https://api-v2.fattureincloud.it"
AUTHORIZE_URL = f"{AUTH_BASE}/oauth/authorize"
TOKEN_URL = f"{AUTH_BASE}/oauth/token"

REDIRECT_URI = "http://localhost:8081/callback"
LISTEN_HOST = "localhost"
LISTEN_PORT = 8081

# "Tutti gli scope" (con :a dove consentito; alcuni sono solo :r)
# Lista basata sulla pagina Scopes ufficiale.
ALL_SCOPES = " ".join([
    "situation:r",
    "entity.clients:a",
    "entity.suppliers:a",
    "products:a",
    "stock:a",
    "issued_documents.invoices:a",
    "issued_documents.credit_notes:a",
    "issued_documents.quotes:a",
    "issued_documents.proformas:a",
    "issued_documents.receipts:a",
    "issued_documents.delivery_notes:a",
    "issued_documents.orders:a",
    "issued_documents.work_reports:a",
    "issued_documents.supplier_orders:a",
    "issued_documents.self_invoices:a",
    "received_documents:a",
    "receipts:a",
    "calendar:a",
    "archive:a",
    "taxes:a",
    "emails:r",
    "cashbook:a",
    "settings:a",
])

STATE = "fic_oauth_state_example"  # in produzione rendilo random + verifica

_received = {"code": None, "error": None}


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")
            return

        qs = urllib.parse.parse_qs(parsed.query)
        _received["code"] = (qs.get("code", [None])[0])
        _received["error"] = (qs.get("error", [None])[0])

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        if _received["code"]:
            self.wfile.write(b"<h2>OK, autorizzazione completata.</h2><p>Puoi tornare al terminale.</p>")
        else:
            self.wfile.write(b"<h2>Errore in autorizzazione.</h2><p>Controlla il terminale.</p>")

    def log_message(self, format, *args):
        # silenzia log HTTP
        return


def start_server():
    httpd = HTTPServer((LISTEN_HOST, LISTEN_PORT), CallbackHandler)
    httpd.handle_request()  # una sola richiesta (il callback)
    httpd.server_close()


def build_authorize_url(client_id: str, scope: str) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "scope": scope,
        "state": STATE,
    }
    return AUTHORIZE_URL + "?" + urllib.parse.urlencode(params)


def exchange_code_for_token(client_id: str, client_secret: str, code: str) -> dict:
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": REDIRECT_URI,
        "code": code,
    }
    r = requests.post(TOKEN_URL, json=payload, headers={"Accept": "application/json"})
    r.raise_for_status()
    return r.json()


def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> dict:
    payload = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }
    r = requests.post(TOKEN_URL, json=payload, headers={"Accept": "application/json"})
    r.raise_for_status()
    return r.json()


def main():
    client_id = os.getenv("FIC_CLIENT_ID") or input("CLIENT_ID: ").strip()
    client_secret = os.getenv("FIC_CLIENT_SECRET") or input("CLIENT_SECRET: ").strip()

    print("\nScope richiesti (ALL):")
    print(ALL_SCOPES)
    print("\nAvvio server locale per callback:", REDIRECT_URI)

    t = threading.Thread(target=start_server, daemon=True)
    t.start()

    url = build_authorize_url(client_id, ALL_SCOPES)
    print("\nAprendo browser per autorizzazione:\n", url)
    webbrowser.open(url)

    # attesa callback
    for _ in range(300):  # ~300s
        if _received["code"] or _received["error"]:
            break
        time.sleep(1)

    if _received["error"]:
        raise SystemExit(f"Autorizzazione negata/errore: {_received['error']}")
    if not _received["code"]:
        raise SystemExit("Timeout: non ho ricevuto il code. Controlla redirect URI e porta 8000.")

    code = _received["code"]
    print("\nRicevuto code. Scambio per token...")

    token = exchange_code_for_token(client_id, client_secret, code)

    with open("fic_token.json", "w", encoding="utf-8") as f:
        json.dump(token, f, indent=2, ensure_ascii=False)

    print("\n✅ Token salvato in fic_token.json")
    print("access_token:", token.get("access_token", "")[:20] + "...")
    print("refresh_token:", (token.get("refresh_token", "")[:20] + "...") if token.get("refresh_token") else "N/A")
    print("\nNota: conserva il refresh_token in modo sicuro (env/secrets manager).")


if __name__ == "__main__":
    main()

