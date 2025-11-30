
import os
import base64
import threading
import requests
from flask import Flask, request

# ===== Config =====
CLIENT_ID = os.getenv("CLIENT_ID")
# Si tu OneDrive es personal, usa "consumers". Si es empresarial o no estás seguro, usa "common".
AUTHORITY = os.getenv("AUTHORITY") or "https://login.microsoftonline.com/consumers"
# Si sólo lees, puedes cambiar a ["Files.Read"]
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

_auth_thread = None
_auth_state = {"running": False, "message": None, "error": None}

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

def _public_client(cache=None):
    return msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)

def get_token_silent_only():
    """Usa SOLO el token del caché. Si no existe, falla (no iniciar device flow aquí)."""
    cache = _load_token_cache()
    app_auth = _public_client(cache)
    result = app_auth.acquire_token_silent(SCOPES, account=None)
    if not result or "access_token" not in result:
        raise RuntimeError("Token no disponible. Ejecuta primero /init-auth, autoriza el código y vuelve a intentar.")
    return result["access_token"]

def _auth_worker(flow, cache):
    """Hilo en segundo plano que bloquea hasta que completes el Device Flow."""
    global _auth_state
    try:
        app_auth = _public_client(cache)
        result = app_auth.acquire_token_by_device_flow(flow)  # bloquea hasta que completes
        if "access_token" not in result:
            _auth_state["error"] = f"Error de autenticación: {result}"
        else:
            _save_token_cache(cache)
            _auth_state["message"] = "Autenticado y token cacheado."
    except Exception as e:
        _auth_state["error"] = str(e)
    finally:
        _auth_state["running"] = False

def start_device_flow_async():
    """Arranca el Device Flow sin bloquear la petición, devuelve código y URL."""
    global _auth_thread, _auth_state
    if _auth_state.get("running"):
        return {"status": "in-progress", "message": "Device flow ya en progreso."}

    cache = _load_token_cache()
    app_auth = _public_client(cache)

    # Intento silencioso por si ya hay token
    result = app_auth.acquire_token_silent(SCOPES, account=None)
    if result and "access_token" in result:
        _save_token_cache(cache)
        return {"status": "ready", "message": "Token ya disponible en caché."}

    flow = app_auth.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(
            f"No se pudo iniciar device flow. Revisa CLIENT_ID, AUTHORITY ('consumers' vs 'common'), "
            f"y que tu app en Azure tenga 'Allow public client flows' habilitado. Detalle: {flow}"
        )

    # Arranca hilo que hará el polling sin bloquear este request
    _auth_state = {"running": True, "message": None, "error": None}
    _auth_thread = threading.Thread(target=_auth_worker, args=(flow, cache), daemon=True)
    _auth_thread.start()

    # Devuelve datos para que autorices ya
    return {
        "status": "started",
        "verification_uri": flow.get("verification_uri"),
        "user_code": flow.get("user_code"),
        "expires_in": flow.get("expires_in"),  # segundos
        "interval": flow.get("interval"),      # polling interval sugerido
        "note": "Ve a la URL y pega el código. Luego consulta /auth-status hasta ver 'Autenticado y token cacheado'.",
    }

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
    }
    if USE_MARKDOWN:
        payload["parse_mode"] = "Markdown"
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

# ===== Acción principal: leer última fila y enviar =====
def read_last_row_and_message_with_token(access_token: str):
    item = get_drive_item_from_share(EXCEL_SHARE_URL, access_token)
    item_id = item.get("id")
    if not item_id:
        raise RuntimeError("No se obtuvo item.id del enlace compartido.")

    if TABLE_NAME:
        rows = get_table_rows(item_id, TABLE_NAME, access_token)
        if not rows:
            raise RuntimeError("La tabla no devolvió filas.")
        last_row = rows[-1]
        headers = rows[0] if len(rows) > 1 else []
    elif SHEET_NAME:
        values = get_used_range_values(item_id, SHEET_NAME, access_token)
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
            token = get_token_silent_only()
            preview = read_last_row_and_message_with_token(token)
            return {"ok": True, "preview": preview}
        except Exception as e:
            try:
                send_telegram_message(f"⚠️ Error al leer/enviar: {e}")
            except:
                pass
            return {"ok": False, "error": str(e)}, 500
    else:
        return {"ok": True, "ignored": True}

# ===== Endpoints de soporte =====
@app.route("/init-auth", methods=["GET"])
def init_auth():
    """Inicia Device Flow en segundo plano y devuelve el código/URL de verificación."""
    try:
        payload = start_device_flow_async()
        return {"ok": True, **payload}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

@app.route("/auth-status", methods=["GET"])
def auth_status():
    """Consulta el estado del Device Flow / token cacheado."""
    global _auth_state
    status = {
        "running": _auth_state.get("running"),
        "message": _auth_state.get("message"),
        "error": _auth_state.get("error"),
    }
    # Además, intenta un silent para confirmar si ya hay token
    try:
        _ = get_token_silent_only()
        status["token_ready"] = True
    except Exception:
        status["token_ready"] = False
    return {"ok": True, "status": status}

@app.route("/reset-auth", methods=["POST"])
def reset_auth():
    """Borra la caché de MSAL. Luego llama /init-auth para reautorizar."""
    try:
        if os.path.exists(TOKEN_CACHE_PATH):
            os.remove(TOKEN_CACHE_PATH)
        # Reinicia estado
        global _auth_state
        _auth_state = {"running": False, "message": None, "error": None}
        return {"ok": True, "message": "Cache borrado. Llama /init-auth para reautorizar."}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

@app.route("/healthz", methods=["GET"])
def healthz():
    # Verifica variables básicas (sin revelar secretos)
    cfg = {
        "CLIENT_ID_set": bool(CLIENT_ID),
        "AUTHORITY": AUTHORITY,
        "TELEGRAM_BOT_TOKEN_set": bool(TELEGRAM_BOT_TOKEN),
        "TELEGRAM_CHAT_ID_set": bool(TELEGRAM_CHAT_ID),
        "EXCEL_SHARE_URL_set": bool(EXCEL_SHARE_URL),
        "SHEET_NAME": SHEET_NAME,
        "TABLE_NAME": TABLE_NAME,
    }
    return {"ok": True, "status": "up", "config": cfg}
