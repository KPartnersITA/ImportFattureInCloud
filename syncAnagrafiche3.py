import os
import json
from dotenv import load_dotenv
from globalutils import log, load_all_fic_clients
import fattureincloud_python_sdk
from dbconn import getdbconn
from fattureincloud_python_sdk.rest import ApiException
import time
from datetime import datetime

load_dotenv()

log_filename = f"sync-{datetime.now().strftime('%Y%m%d')}.log"

configuration = fattureincloud_python_sdk.Configuration(
    host="https://api-v2.fattureincloud.it"
)
configuration.access_token = os.getenv("ACCESS_TOKEN")
company_id = int(os.getenv("COMPANY_ID"))

BATCH_FILE = "clients_batch.json"
BATCH_SIZE = 500

PM_CACHE_FILE = "payment_methods_cache.json"

# Campi usati per il confronto modifiche
COMPARE_FIELDS = [
    "name", "address_street", "address_city", "address_province",
    "address_zip", "email", "certified_email", "phone",
    "vat_number", "tax_code", "ei_code",
]

def get_clients_from_db():
    from dbconn import getdbconn
    connection = getdbconn()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT 
        account_no as code,
        accountname AS name,
        vacf.cf_898 AS address_street,
        vacf.cf_892 AS address_zip,
        vacf.cf_894 AS address_city,
        vacf.cf_900 AS address_province,
        vacf.cf_880 AS certified_email,
        email1 AS email,
        phone as phone,
        vacf.cf_878 AS vat_number,
        vacf.cf_1316 AS tax_code,
        vacf.cf_1963 AS default_payment_method,
        vacf.cf_1969 AS ei_code,
        vacf.cf_1568 AS notes,
        ownership AS contact_person
    FROM 
        vtiger_account va 
        LEFT JOIN vtiger_accountscf vacf ON (va.accountid = vacf.accountid)
        LEFT JOIN vtiger_crmentity vce ON (vce.crmid = va.accountid)
    WHERE
        vce.deleted = 0
        AND vacf.cf_878 IS NOT NULL 
        AND LENGTH(vacf.cf_878) = 11 
        AND va.account_type IN ('Ag. princ.', 'Ag. princ. collegata', 'Sub-A', 'SUB-E')
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results


def load_payment_methods_cached(info_api, company_id):
    """Carica i metodi di pagamento da cache locale (non cambiano mai)."""
    if os.path.exists(PM_CACHE_FILE):
        with open(PM_CACHE_FILE, "r", encoding="utf-8") as f:
            log(f"Metodi di pagamento caricati da cache ({PM_CACHE_FILE})", log_filename, "notice")
            return json.load(f)

    pm_resp = info_api.list_payment_methods(company_id)
    data = {m.name: m.id for m in pm_resp.data}
    with open(PM_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"Metodi di pagamento salvati in cache ({len(data)} voci)", log_filename, "notice")
    return data


def client_needs_update(existing: dict, new_data: dict) -> bool:
    """
    Confronta i campi rilevanti tra il cliente in FIC (cache) e quello del DB.
    Ritorna True se c'è almeno una differenza → serve la chiamata API.
    """
    for field in COMPARE_FIELDS:
        val_fic = str(existing.get(field) or "").strip()
        val_db  = str(new_data.get(field) or "").strip()
        if val_fic != val_db:
            return True
    return False


def sync_client(api_instance, existing, client, max_retries=4):
    client_data = fattureincloud_python_sdk.Client(
        name=client["name"],
        address_street=client.get("address_street"),
        address_city=client.get("address_city"),
        address_province=client.get("address_province"),
        address_zip=client.get("address_zip"),
        address_postal_code=client.get("address_zip"),
        type="company",
        address_country="IT",
        email=client.get("email"),
        certified_email=client.get("certified_email"),
        vat_number=client["vat_number"],
        ei_code=client.get("ei_code"),
        phone=client.get("phone"),
        tax_code=client.get("tax_code"),
        notes=client.get("notes"),
        contact_person=client.get("contact_person"),
        code=client.get("code"),
        default_payment_method=client.get("default_payment_method"),
    )

    for attempt in range(max_retries):
        try:
            if existing:
                client_id = existing["id"] if isinstance(existing, dict) else existing.id
                api_instance.modify_client(
                    company_id,
                    client_id,
                    fattureincloud_python_sdk.ModifyClientRequest(data=client_data)
                )
            else:
                api_instance.create_client(
                    company_id,
                    fattureincloud_python_sdk.CreateClientRequest(data=client_data)
                )
                log(f"Creato nuovo cliente: {client['name']} ({client['vat_number']})", log_filename, "notice")
            return  # successo → esci

        except ApiException as e:
            if e.status == 429 and attempt < max_retries - 1:
                wait = (2 ** attempt) + 0.5  # 1.5s, 2.5s, 4.5s, ...
                log(f"Rate limit 429, attendo {wait}s (tentativo {attempt+1}/{max_retries})...", log_filename, "warning")
                time.sleep(wait)
            else:
                log(f"Errore sync cliente {client['name']}: {e}", log_filename, "error")
                return


# --- MAIN ---
if __name__ == "__main__":

    if not os.path.exists(BATCH_FILE):
        db_clients = get_clients_from_db()
        with open(BATCH_FILE, "w", encoding="utf-8") as f:
            json.dump(db_clients, f, ensure_ascii=False, indent=2)

    with open(BATCH_FILE, "r", encoding="utf-8") as f:
        clients = json.load(f)

    if not clients:
        os.remove(BATCH_FILE)
        exit()

    current_batch = clients[:BATCH_SIZE]
    remaining = clients[BATCH_SIZE:]

    with fattureincloud_python_sdk.ApiClient(configuration) as api_client:
        info_api = fattureincloud_python_sdk.InfoApi(api_client)
        try:
            name_to_id = load_payment_methods_cached(info_api, company_id)
        except AttributeError:
            log("Il metodo list_payment_methods non esiste.", log_filename, "error")
            quit()

        api_instance = fattureincloud_python_sdk.ClientsApi(api_client)
        fic_clients = load_all_fic_clients(api_instance, log_filename, company_id)

        skipped = 0
        updated = 0
        created = 0
        errors  = 0

        for c in current_batch:
            vat = c.get("vat_number")
            if not vat:
                log(f"Cliente {c.get('name')} senza P.IVA, ignorato.", log_filename, "warning")
                continue

            # Mappa metodo di pagamento → oggetto SDK
            method_name = c.get("default_payment_method")
            method_id = name_to_id.get(method_name) if isinstance(method_name, str) else None
            if method_id:
                c["default_payment_method"] = fattureincloud_python_sdk.PaymentMethod(id=method_id)
            else:
                c["default_payment_method"] = None
                if method_name:
                    log(f"Metodo di pagamento '{method_name}' non trovato per {c['name']}.", log_filename, "warning")

            existing = fic_clients.get(vat)

            # ← PUNTO CHIAVE: salta se i dati sono identici
            if existing and not client_needs_update(existing, c):
                skipped += 1
                continue

            if existing:
                updated += 1
            else:
                created += 1

            sync_client(api_instance, existing, c)

        log(
            f"Batch completato: {updated} aggiornati, {created} creati, "
            f"{skipped} saltati (invariati), {errors} errori "
            f"su {len(current_batch)} clienti processati.",
            log_filename, "notice"
        )

    if remaining:
        with open(BATCH_FILE, "w", encoding="utf-8") as f:
            json.dump(remaining, f, ensure_ascii=False, indent=2)
    else:
        os.remove(BATCH_FILE)