
import os
import base64
import requests
from flask import Flask, request

# ===== Config =====
CLIENT_ID = os.getenv("CLIENT_ID")
AUTHORITY = "https://login.microsoftonline.com/consumers"  # cuentas personales
SCOPES = ["Files.Read", "offline_access"]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

EXCEL_SHARE_URL = os.getenv("EXCEL_SHARE_URL")
SHEET_NAME = os.getenv("SHEET_NAME")  # ej: "Hoja1"
TABLE_NAME = os.getenv("TABLE_NAME")  # ej: "Tabla1"

ONLY_TRIGGER_WORD = (os.getenv("ONLY_TRIGGER_WORD") or "OK").strip().upper()
USE_MARKDOWN = (os.getenv("USE_MARKDOWN") or "true").lower() == "true"

# ===== Flask =====
app = Flask(__name__)

# ===== MSAL (Device Code) =====
import msal
TOKEN_CACHE_PATH = "/tmp/msal_cache.json"

def _load_token_cache():
    cache = msal.SerializableTokenCache()
    if os.path.exists(TOKEN_CACHE_PATH):
        with open(TOKEN_CACHE_PATH, "r") as f:
            cache.deserialize(f.read())
    return cache

def _save_token_cache(cache):
    if cache.has_state_changed:
        with open(TOKEN_CACHE_PATH, "w") as f:
            f.write(cache.serialize())

def acquire_token():
    cache = _load_token_cache()
    app_auth = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)
    result = app_auth.acquire_token_silent(SCOPES, account=None)
    if not result:
        flow = app_auth.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            raise RuntimeError("No se pudo iniciar device flow.")
        print(f"[MSAL] Ve a {flow['verification_uri']} y usa el código: {flow['user_code']}")
        result = app_auth.acquire_token_by_device_flow(flow)
        if "access_token" not in result:
            raise RuntimeError(f"Error autenticación: {result}")
    _save_token_cache(cache)
    return result["access_token"]

# ===== Graph helpers =====
GRAPH = "https://graph.microsoft.com/v1.0"

def encode_sharing_url(u: str) -> str:
    b = base64.urlsafe_b64encode(u.encode("utf-8")).decode("utf-8").rstrip("=")
    return f"u!{b}"

def get_drive_item_from_share(sharing_url: str, access_token: str):
    encoded = encode_sharing_url(sharing_url)
    r = requests.get(
        f"{GRAPH}/shares/{encoded}/driveItem",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()  # contiene id, name, etc.

def get_used_range_values(item_id: str, sheet_name: str, access_token: str):
    # Devuelve el rango usado de la hoja como texto (más seguro para mensajes)
    url = f"{GRAPH}/drive/items/{item_id}/workbook/worksheets('{sheet_name}')/usedRange(valuesOnly=true)?$select=text,address"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("text", [])

def get_table_rows(item_id: str, table_name: str, access_token: str):
    url = f"{GRAPH}/drive/items/{item_id}/workbook/tables('{table_name}')/rows"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    rows = []
    for row in data.get("value", []):
        if row.get("values"):
            rows.append(row["values"][0])
    return rows

def format_row_message(headers, last_row):
    if USE_MARKDOWN:
        if headers and len(headers) == len(last_row):
            lines = [f"**{h}**: {v}" for h, v in zip(headers, last_row)]
            return "🟢 Último registro agregado:\n" + "\n".join(lines)
        return "🟢 Último registro agregado:\n" + ", ".join(map(str, last_row))
    else:
        if headers and len(headers) == len(last_row):
            lines = [f"{h}: {v}" for h, v in zip(headers, last_row)]
            return "Último registro agregado:\n" + "\n".join(lines)
        return "Último registro agregado:\n" + ", ".join(map(str, last_row))

def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown" if USE_MARKDOWN else None
    }
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

# ===== Acción principal: leer última fila y enviar =====
def read_last_row_and_message():
    token = acquire_token()
    item = get_drive_item_from_share(EXCEL_SHARE_URL, token)
    item_id = item.get("id")
    if not item_id:
        raise RuntimeError("No se obtuvo item.id del enlace compartido.")

    if TABLE_NAME:
        rows = get_table_rows(item_id, TABLE_NAME, token)
        if not rows:
            raise RuntimeError("La tabla no devolvió filas.")
        last_row = rows[-1]
        headers = rows[0] if len(rows) > 1 else []
    elif SHEET_NAME:
        values = get_used_range_values(item_id, SHEET_NAME, token)
        if not values:
            raise RuntimeError("La hoja no devolvió valores.")
        headers = values[0] if len(values) >= 1 else []
        last_row = values[-1]
    else:
        raise RuntimeError("Configura SHEET_NAME o TABLE_NAME.")

    msg = format_row_message(headers, last_row)
    send_telegram_message(msg)
    return msg

# ===== Webhook de Telegram: responde al 'OK' =====
@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    data = request.get_json(force=True) or {}
    msg = data.get("message") or data.get("edited_message") or {}
    text = (msg.get("text") or "").strip()

    if text.upper() == ONLY_TRIGGER_WORD:
        try:
            preview = read_last_row_and_message()
            return {"ok": True, "preview": preview}
        except Exception as e:
            # Si falla, responde al grupo con el error para depuración rápida
            try:
                send_telegram_message(f"⚠️ Error al leer/enviar: {e}")
            except:
                pass
            return {"ok": False, "error": str(e)}, 500
    else:
        # Ignora otros mensajes o responde algo breve
        return {"ok": True, "ignored": True}
