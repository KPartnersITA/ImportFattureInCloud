import os
import json
from dotenv import load_dotenv
from globalutils import log, load_all_fic_clients
import fattureincloud_python_sdk
from dbconn import getdbconn
from fattureincloud_python_sdk.rest import ApiException
import time
from datetime import datetime, timedelta

load_dotenv()
# --- CONFIGURAZIONE LOGGING ---
log_filename = f"sync-{datetime.now().strftime('%Y%m%d')}.log"


# --- CONFIGURAZIONE FIC ---
configuration = fattureincloud_python_sdk.Configuration(
    host="https://api-v2.fattureincloud.it"
)
configuration.access_token = os.getenv("ACCESS_TOKEN")
company_id = int(os.getenv("COMPANY_ID"))   # ID azienda su FIC

BATCH_FILE = "clients_batch.json"
BATCH_SIZE = 280

# --- FUNZIONE: estrai anagrafiche dal gestionale ---
def get_clients_from_db():
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
vtiger_account va LEFT JOIN vtiger_accountscf vacf ON (va.accountid=vacf.accountid)
LEFT JOIN vtiger_crmentity vce ON (vce.crmid=va.accountid)
WHERE
vce.deleted=0
AND vacf.cf_878 IS NOT NULL 
AND LENGTH(vacf.cf_878) = 11 
AND va.account_type IN ("Ag. princ.","Ag. princ. collegata","Sub-A","SUB-E")
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results


def sync_client(api_instance, existing, client):
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
    except ApiException as e:
        log(f"Errore sync cliente {client['name']}: {e}", log_filename, "error")



# --- MAIN ---
if __name__ == "__main__":
    # Se il file batch non esiste → lo creo da DB
    if not os.path.exists(BATCH_FILE):
        db_clients = get_clients_from_db()
        with open(BATCH_FILE, "w", encoding="utf-8") as f:
            json.dump(db_clients, f, ensure_ascii=False, indent=2)
        
    # Carico batch dal file
    with open(BATCH_FILE, "r", encoding="utf-8") as f:
        clients = json.load(f)

    if not clients:
        os.remove(BATCH_FILE)
        exit()

    # Prendo il batch corrente
    current_batch = clients[:BATCH_SIZE]
    remaining = clients[BATCH_SIZE:]

    with fattureincloud_python_sdk.ApiClient(configuration) as api_client:
        info_api = fattureincloud_python_sdk.InfoApi(api_client)
        try:
            # Supponendo che esista un metodo per listare i metodi di pagamento...
            pm_resp = info_api.list_payment_methods(company_id)  # controlla sempre la firma del metodo!
        except AttributeError:
            log("Il metodo list_payment_methods non esiste. Controlla il nome esatto nella classe SettingsApi.",log_filename,"error")
            quit()
        
        
        api_instance = fattureincloud_python_sdk.ClientsApi(api_client)
       
        # Prima di sincronizzare i clienti, recupera i metodi di pagamento disponibili
        # Supponendo che il tuo client abbia un campo "payment_method_name", passi da nome a ID così
        name_to_id = {m.name: m.id for m in pm_resp.data}
        
        # carico tutti i clienti già presenti in FIC
        fic_clients = load_all_fic_clients(api_instance,log_filename,company_id)
        
        # processo il batch
        for c in current_batch:
            vat = c.get("vat_number")
            if not vat:
                log(f"Cliente {c.get('name')} senza P.IVA, ignorato.",log_filename,"warning")
                continue

            # mappo metodo di pagamento a ID
            method_name = c.get("default_payment_method")
            method_id = name_to_id.get(method_name)
            if method_id:
                c["default_payment_method"] = fattureincloud_python_sdk.PaymentMethod(id=method_id)
            else:
                c["default_payment_method"] = None
                if method_name:
                    log(f"Metodo di pagamento '{method_name}' non trovato per {c['name']}.",log_filename,"warning")

            existing = fic_clients.get(vat)
            sync_client(api_instance, existing, c)

    # aggiorno il file batch rimuovendo i già processati
    if remaining:
        with open(BATCH_FILE, "w", encoding="utf-8") as f:
            json.dump(remaining, f, ensure_ascii=False, indent=2)
    else:
        os.remove(BATCH_FILE)
        
