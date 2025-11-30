
import os
import base64
import requests
from flask import Flask, request

# ===== Config =====
CLIENT_ID = os.getenv("CLIENT_ID")
AUTHORITY = os.getenv("AUTHORITY") or "https://login.microsoftonline.com/consumers"  # cuentas personales por defecto
# Si editarás la hoja, Files.ReadWrite; si solo lees, Files.Read
SCOPES = ["Files.ReadWrite"]

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

def get_token_silent_only():
    """
    Usa SOLO el token existente en caché. Si no existe, falla.
    Evita iniciar Device Flow dentro del webhook para no bloquear workers.
    """
    cache = _load_token_cache()
    app_auth = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)
    result = app_auth.acquire_token_silent(SCOPES, account=None)
    if not result or "access_token" not in result:
        raise RuntimeError("Token no disponible. Ejecuta primero /init-auth y completa el Device Code.")
    return result["access_token"]

def start_device_flow_and_cache():
    """
    Inicia el Device Flow y bloquea esta llamada hasta que completes la autorización.
    Úsalo SOLO desde /init-auth (manual). Cachea el token para usos futuros.
    """
    cache = _load_token_cache()
    app_auth = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)

    # Intento silencioso por si ya está cacheado
    result = app_auth.acquire_token_silent(SCOPES, account=None)
    if result and "access_token" in result:
        _save_token_cache(cache)
        return "Token ya disponible en caché."

    flow = app_auth.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"No se pudo iniciar device flow. Revisa CLIENT_ID, AUTHORITY, 'Allow public client flows' en Azure, y los scopes. Detalle: {flow}")

    # Muestra el código en logs y también en la respuesta
    print(f"[MSAL] Ve a {flow['verification_uri']} y usa el código: {flow['user_code']}")
    # Bloquea hasta que completes
    result = app_auth.acquire_token_by_device_flow(flow)
    if "access_token" not in result:
        raise RuntimeError(f"Error de autenticación: {result}")

    _save_token_cache(cache)
    return "Autenticado y token cacheado."

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
    # Devuelve el rango usado de la hoja como texto (string), ideal para mensajes
    url = f"{GRAPH}/drive/items/{item_id}/workbook/worksheets('{sheet_name}')/usedRange(valuesOnly=true)?$select=text,address"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("text", [])  # [["h1","h2"], ["v1","v2"], ...]

def get_table_rows(item_id: str, table_name: str, access_token: str):
    url = f"{GRAPH}/drive/items/{item_id}/workbook/tables('{table_name}')/rows"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    rows = []
    for row in data.get("value", []):
        if row.get("values"):
            # Cada row["values"] puede tener múltiples filas, tomamos la primera
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
    }
    if USE_MARKDOWN:
        payload["parse_mode"] = "Markdown"
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

# ===== Acción principal: leer última fila y enviar =====
def read_last_row_and_message():
    # Usa SOLO el token silencioso del caché (no iniciar Device Flow aquí)
    token = get_token_silent_only()

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
            # Responde al grupo con el error para depuración rápida
            try:
                send_telegram_message(f"⚠️ Error al leer/enviar: {e}")
            except:
                pass
            return {"ok": False, "error": str(e)}, 500
    else:
        return {"ok": True, "ignored": True}

# ===== Endpoints de soporte para autenticación =====
@app.route("/init-auth", methods=["GET"])
def init_auth():
    """
    Inicia el Device Flow y cachea el token.
    Abre esta ruta manualmente en el navegador y completa el código que verás en logs.
    """
    try:
        msg = start_device_flow_and_cache()
        return {"ok": True, "message": msg}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

@app.route("/reset-auth", methods=["POST"])
def reset_auth():
    """
    Borra la caché de MSAL por si se corrompe. Luego llama /init-auth de nuevo.
    """
    try:
        if os.path.exists(TOKEN_CACHE_PATH):
            os.remove(TOKEN_CACHE_PATH)
        return {"ok": True, "message": "Cache borrado. Llama /init-auth para reautorizar."}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

@app.route("/healthz", methods=["GET"])
def healthz():
    return {"ok": True, "status": "up"}
