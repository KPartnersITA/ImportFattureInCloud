import logging
import json
import os
from fattureincloud_python_sdk.rest import ApiException
import time
from datetime import date,datetime, timedelta
import calendar


CLIENTS_FILE = "fic_clients.json"
CACHE_DAYS = 5
  # ID azienda su FIC


def end_of_month(d: date) -> date:
    last_day = calendar.monthrange(d.year, d.month)[1]
    return date(d.year, d.month, last_day)

def log(msg,filename, level="notice"):
    logging.basicConfig(
        filename=filename,
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    level = level.lower()
    if level == "error":
        logging.error(f"ERROR: {msg}")
    elif level == "warning":
        logging.warning(f"WARNING: {msg}")
    else:
        logging.info(f"NOTICE: {msg}")

def load_all_fic_clients(api_instance,log_filename,company_id):
    # Controllo se esiste un file di cache valido
    if os.path.exists(CLIENTS_FILE):
        file_age_days = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(CLIENTS_FILE))).days
        if file_age_days <= CACHE_DAYS:
            log(f"Carico i clienti da cache locale ({CLIENTS_FILE})",log_filename, "notice")
            with open(CLIENTS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            log(f"Cache obsoleta (>{CACHE_DAYS} giorni), rigenero da API",log_filename, "warning")
            os.remove(CLIENTS_FILE)

    # Se arrivo qui → devo scaricare da API
    all_clients = {}
    page = 1
    while True:
        try:
            resp = api_instance.list_clients(
                company_id,
                fieldset="detailed",  # o "basic", come preferisci
                fields="id,name,vat_number,tax_code,certified_email,ei_code,address_street,address_zip,address_city,address_province,email,phone",
                per_page=100,
                page=page
            )
        except ApiException as e:
            log(f"Errore caricamento clienti FIC: {e}",log_filename, "error")
            break

        if not resp.data or len(resp.data) == 0:
            break

          
        for c in resp.data:
            if c.vat_number:
                # Per JSON serializzo come dict
                all_clients[c.vat_number] = {
                    "id": c.id,
                    "name": c.name,
                    "vat_number": c.vat_number,
                    'tax_code': c.tax_code or "",
                    "email": c.email or "",
                    "certified_email": c.certified_email or "",
                    "ei_code": c.ei_code or "",
                    "phone": c.phone or "",
                    "address_street": c.address_street or "",
                    'address_zip': c.address_postal_code  or "",
                    "address_city": c.address_city or "",
                    "address_province": c.address_province or "",
                }

        page += 1

    # Salvo i risultati in cache
    if all_clients:
        with open(CLIENTS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_clients, f, ensure_ascii=False, indent=2)
        log(f"Salvati {len(all_clients)} clienti in {CLIENTS_FILE}",log_filename, "notice")

    return all_clients
