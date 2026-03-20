import os
import json
from dotenv import load_dotenv
import fattureincloud_python_sdk
from globalutils import log, load_all_fic_clients, end_of_month
from fattureincloud_python_sdk.api import issued_documents_api
from fattureincloud_python_sdk.models import (
    Entity,
    IssuedDocument,
    IssuedDocumentType,
    Currency,
    Language,
    IssuedDocumentItemsListItem,
    CreateIssuedDocumentRequest,
    GetNewIssuedDocumentTotalsRequest,
    IssuedDocumentOptions,
    IssuedDocumentPaymentsListItem,
    VatType,   # <-- per impostare l'aliquota 22%
)
# aggiungi in alto tra gli import
from fattureincloud_python_sdk.rest import ApiException
from dbconn import getdbconn
import time
from datetime import datetime, timedelta, date


configuration = fattureincloud_python_sdk.Configuration(
    host="https://api-v2.fattureincloud.it"
)
configuration.access_token = os.getenv("ACCESS_TOKEN")
company_id = int(os.getenv("COMPANY_ID"))   # ID azienda su FIC
log_filename = f"orders-{datetime.now().strftime('%Y%m%d')}.log"


def mese_corrente_su_vtiger():
    mesi = [
        "GENNAIO", "FEBBRAIO", "MARZO", "APRILE", "MAGGIO", "GIUGNO",
        "LUGLIO", "AGOSTO", "SETTEMBRE", "OTTOBRE", "NOVEMBRE", "DICEMBRE"
    ]
    oggi = date.today()
    numero_mese = oggi.month
    return f"{numero_mese:02d}-{mesi[numero_mese - 1]}"

def get_orders_of_the_month():
    month = mese_corrente_su_vtiger()
    connection = getdbconn()
    cursor = connection.cursor(dictionary=True)

    query = """
    SELECT 
    vacf.cf_878 AS vat_number,
    vacf.cf_1963 AS default_payment_method,
    so.salesorderid,
    so.`subject`,
    vs.service_no,
    ipr.sequence_no,
    vs.servicename,
    ipr.`comment`,
    ipr.quantity,
    ipr.listprice,
    COALESCE(ipr.discount_percent,0) AS discount,
    ipr.quantity * ipr.listprice * (100 - COALESCE(ipr.discount_percent,0))/100 AS net_price
    FROM 
    vtiger_account va LEFT JOIN vtiger_accountscf vacf ON (va.accountid=vacf.accountid)
    LEFT JOIN vtiger_crmentity vce ON (vce.crmid=va.accountid)
    LEFT JOIN vtiger_salesorder so ON (so.accountid=va.accountid)
    LEFT JOIN vtiger_crmentity vce2 ON (vce2.crmid=so.salesorderid)
    LEFT JOIN vtiger_inventoryproductrel ipr ON (so.salesorderid=ipr.id)
    LEFT JOIN vtiger_salesordercf socf ON (socf.salesorderid=so.salesorderid)
    LEFT JOIN vtiger_service vs ON (vs.serviceid=ipr.productid)
    LEFT JOIN vtiger_invoice_recurring_info vir ON (vir.salesorderid=so.salesorderid)
    WHERE
    vce.deleted=0
    AND vce2.deleted=0
    AND vacf.cf_878 IS NOT NULL 
    AND LENGTH(vacf.cf_878) = 11 
    AND va.account_type IN ("Ag. princ.","Ag. princ. collegata","Sub-A","SUB-E")
    and socf.cf_1252=%s AND socf.cf_1254='SI'
    AND vir.start_period<=NOW()
    ORDER BY so.salesorderid ASC,vat_number ASC , ipr.sequence_no ASC
    """
    cursor.execute(query, (month,))
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results
        
def get_orders_of_the_customer(vatid):
    month = mese_corrente_su_vtiger()
    connection = getdbconn()
    cursor = connection.cursor(dictionary=True)

    query = """
    SELECT
    vacf.cf_878 AS vat_number,
    vacf.cf_1963 AS default_payment_method,
    so.salesorderid,
    so.`subject`,
    vs.service_no,
    ipr.sequence_no,
    vs.servicename,
    ipr.`comment`,
    ipr.quantity,
    ipr.listprice,
    COALESCE(ipr.discount_percent,0) AS discount,
    ipr.quantity * ipr.listprice * (100 - COALESCE(ipr.discount_percent,0))/100 AS net_price
    FROM
    vtiger_account va LEFT JOIN vtiger_accountscf vacf ON (va.accountid=vacf.accountid)
    LEFT JOIN vtiger_crmentity vce ON (vce.crmid=va.accountid)
    LEFT JOIN vtiger_salesorder so ON (so.accountid=va.accountid)
    LEFT JOIN vtiger_crmentity vce2 ON (vce2.crmid=so.salesorderid)
    LEFT JOIN vtiger_inventoryproductrel ipr ON (so.salesorderid=ipr.id)
    LEFT JOIN vtiger_salesordercf socf ON (socf.salesorderid=so.salesorderid)
    LEFT JOIN vtiger_service vs ON (vs.serviceid=ipr.productid)
    LEFT JOIN vtiger_invoice_recurring_info vir ON (vir.salesorderid=so.salesorderid)
    WHERE
    
    vce.deleted=0
    AND vce2.deleted=0
    AND vacf.cf_878=%s
    AND LENGTH(vacf.cf_878) = 11
    AND va.account_type IN ("Ag. princ.","Ag. princ. collegata","Sub-A","SUB-E")
    and socf.cf_1252=%s AND socf.cf_1254='SI'
    AND vir.start_period<=NOW()
    ORDER BY so.salesorderid ASC,vat_number ASC , ipr.sequence_no ASC
    """
    cursor.execute(query, (vatid,month,))
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results

        
def  get_payment_method_id(api_client,company_id,payment_method):
    info_api = fattureincloud_python_sdk.InfoApi(api_client)
    try:
        pm_resp = info_api.list_payment_methods(company_id)
    except Exception as e:
        log(f"Il metodo list_payment_methods non funziona dettagli: {e}",log_filename,"error")

    name_to_id = {m.name: m.id for m in pm_resp.data}
    payment_method_id = name_to_id.get(payment_method)
    
    return payment_method_id 


# --- CONFIG BATCH/STATE ---
BATCH_SIZE = 30
STATE_FILE = "orders_state.json"

def _load_state():
    env = os.getenv("START_INDEX")
    if env is not None:
        try:
            return {
                "next_index": int(env),
                "completed_month": None
            }
        except ValueError:
            pass

    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return {
                    "next_index": int(data.get("next_index", 0)),
                    "completed_month": data.get("completed_month")
                }
    except Exception:
        pass

    return {
        "next_index": 0,
        "completed_month": None
    }


def _save_state(next_index=None, completed_month=None):
    state = _load_state()

    if next_index is not None:
        state["next_index"] = int(next_index)

    if completed_month is not None:
        state["completed_month"] = completed_month

    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception:
        pass


def _reset_state():
    try:
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
    except Exception:
        pass

def _load_checkpoint():
    # override da ENV al primo giro
    env = os.getenv("START_INDEX")
    if env is not None:
        try:
            return int(env)
        except ValueError:
            pass
    # fallback: leggi dal file
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return int(data.get("next_index", 0))
    except Exception:
        pass
    return 0

def _save_checkpoint(next_index):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"next_index": int(next_index)}, f)
    except Exception:
        pass

def _reset_checkpoint():
    try:
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
    except Exception:
        pass


if __name__ == "__main__":
    results = get_orders_of_the_month()
    current_month = date.today().strftime("%Y-%m")
    state = _load_state()

    if state.get("completed_month") == current_month:
        print("Ordini del mese già generati. Esco.")
        log("Ordini del mese già generati.", log_filename, "notice")
        raise SystemExit(0)

    if not results:
        log("Nessun ordine trovato.", log_filename, "warning")
        raise SystemExit(0)

    orders = []

    with fattureincloud_python_sdk.ApiClient(configuration) as api_client:
        clients_api = fattureincloud_python_sdk.ClientsApi(api_client)
        docs_api = issued_documents_api.IssuedDocumentsApi(api_client)

        fic_clients = load_all_fic_clients(clients_api, log_filename, company_id)

        # FIX 1: carica i metodi di pagamento UNA SOLA VOLTA, fuori dal loop
        try:
            info_api = fattureincloud_python_sdk.InfoApi(api_client)
            pm_resp = info_api.list_payment_methods(company_id)
            payment_method_cache = {m.name: m.id for m in pm_resp.data}
            log(f"Metodi di pagamento caricati: {list(payment_method_cache.keys())}", log_filename, "notice")
        except Exception as e:
            log(f"Impossibile caricare metodi di pagamento: {e}", log_filename, "warning")
            payment_method_cache = {}

        progressivo = int(date.today().strftime("%m") + "001")
        order_date = date.today().strftime("%Y-%m-%d")
        due_eom = end_of_month(date.today()).strftime("%Y-%m-%d")

        current_vat = None
        order = None
        skip_current_vat = False
        existing = None
        client_id = None

        for row in results:
            vat = (row["vat_number"] or "").strip()

            if skip_current_vat and vat == current_vat:
                continue

            if vat != current_vat:
                if order:
                    orders.append(order)
                    order = None

                current_vat = vat
                skip_current_vat = False
                progressivo += 1

                existing = fic_clients.get(current_vat)
                client_id = existing.get("id") if isinstance(existing, dict) else None

                if not client_id:
                    log(f"Cliente P.IVA {current_vat} non su FIC: salto.", log_filename, "warning")
                    skip_current_vat = True
                    order = None
                    continue

            if order is None:
                if not isinstance(existing, dict):
                    log(f"Dati cliente non disponibili per P.IVA {current_vat}.", log_filename, "warning")
                    skip_current_vat = True
                    continue

                ent = Entity(
                    id=client_id,
                    name=existing.get("name"),
                    address_street=existing.get("address_street"),
                    address_postal_code=existing.get("address_zip"),
                    address_city=existing.get("address_city"),
                    address_province=existing.get("address_province"),
                    certified_email=existing.get("certified_email"),
                    email=existing.get("email"),
                    tax_code=existing.get("tax_code"),
                    vat_number=existing.get("vat_number"),
                )

                # FIX 2: usa la cache, zero chiamate API aggiuntive
                pm_id = payment_method_cache.get(row["default_payment_method"])

                order = IssuedDocument(
                    payment_method=(fattureincloud_python_sdk.PaymentMethod(id=pm_id) if pm_id else None),
                    type=IssuedDocumentType("order"),
                    entity=ent,
                    date=order_date,
                    due_date=due_eom,
                    number=progressivo,
                    currency=Currency(id="EUR"),
                    language=Language(code="it", name="italiano"),
                    items_list=[],
                    show_payments=True,
                    show_payment_method=True
                )

            if order:
                order.items_list.append(
                    IssuedDocumentItemsListItem(
                        code=row["service_no"],
                        name=row["servicename"],
                        description=row["comment"],
                        net_price=float(row["listprice"]),
                        qty=float(row["quantity"]),
                        discount=float(row["discount"]),
                        vat=VatType(id=0)
                    )
                )

        if order:
            orders.append(order)

        if not orders:
            log("Nessun ordine costruito.", log_filename, "warning")
            raise SystemExit(0)

        total = len(orders)
        start = int(state.get("next_index", 0))

        if start >= total and state.get("completed_month") != current_month:
            start = 0

        end = min(start + BATCH_SIZE, total)
        batch = orders[start:end]

        log(f"Invio batch {start+1}-{end} su {total}", log_filename, "notice")
        print(f"Invio batch {start+1}-{end} su {total}")

        ok = 0
        ko = 0

        for i, od in enumerate(batch, start=start + 1):
            try:
                od.payments_list = [
                    IssuedDocumentPaymentsListItem(
                        amount=0.0,
                        due_date=due_eom,
                        status="not_paid"
                    )
                ]
                resp = docs_api.create_issued_document(
                    company_id,
                    create_issued_document_request=CreateIssuedDocumentRequest(
                        data=od,
                        options=IssuedDocumentOptions(fix_payments=True)
                    )
                )
                ok += 1
                log(
                    f"[{i}/{total}] Ordine creato: id={getattr(resp.data, 'id', None)} "
                    f"numero={getattr(resp.data, 'number', None)}",
                    log_filename, "notice"
                )
                # FIX 3: pausa tra chiamate per non saturare il pool HTTP
                time.sleep(0.15)

            except ApiException as e:
                ko += 1
                log(f"[{i}/{total}] Errore creazione ordine: {e}", log_filename, "error")
                # backoff più generoso su errore
                time.sleep(1.0)

        print(f"Batch completato. OK={ok}  KO={ko}")

        if end >= total:
            _save_state(next_index=0, completed_month=current_month)
            log("Tutti gli ordini processati. Stato completato.", log_filename, "notice")
        else:
            _save_state(next_index=end)
            log(f"Checkpoint: prossimo indice={end}, rimangono {total - end}.", log_filename, "notice")