
import os
import base64
import threading
import requests
import unicodedata
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

def _normalize_header(h: str) -> str:
    h = (h or "").strip().lower()
    # quita acentos
    h = "".join(c for c in unicodedata.normalize("NFD", h) if unicodedata.category(c) != "Mn")
    # reemplaza espacios y signos comunes por _
    for ch in [" ", "-", "/", ".", ":", ";"]:
        h = h.replace(ch, "_")
    return h

def _public_client(cache=None):
    return msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)

def get_token_silent_only():
    """
    Usa el token del caché con account explícito.
    Si no hay accounts en caché o silent falla, lanza error claro.
    """
    cache = _load_token_cache()
    app_auth = _public_client(cache)
    accounts = app_auth.get_accounts()
    if not accounts:
        raise RuntimeError("No hay cuentas en caché. Ejecuta /init-auth y autoriza, luego intenta de nuevo.")
    # Intenta con el primer account (puedes iterar si necesitas)
    result = app_auth.acquire_token_silent(SCOPES, account=accounts[0])
    if not result or "access_token" not in result:
        raise RuntimeError("Silent token no disponible. Repite /init-auth o revisa AUTHORITY/SCOPES.")
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

# ===== MarkdownV2 escape util =====
MDV2_SPECIALS = set("_*~`>#+-=|{}.!")

def escape_markdown_v2(text: str) -> str:
    """
    Escapa caracteres especiales de MarkdownV2 para evitar errores de parseo en Telegram.
    (No escapamos emojis ni letras; tampoco escapamos saltos de línea.)
    """
    if not text:
        return ""
    out = []
    for ch in text:
        if ch in MDV2_SPECIALS:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)

# --- reemplaza send_telegram_message para aceptar chat destino y parse_mode ---
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(text: str, chat_id: str = None, parse_mode: str = None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id or TELEGRAM_CHAT_ID,
        "text": text,
    }
    # Si se pasa parse_mode explícito, úsalo; si no, respeta USE_MARKDOWN (MarkdownV2)
    if parse_mode:
        payload["parse_mode"] = parse_mode
    elif USE_MARKDOWN:
        payload["parse_mode"] = "MarkdownV2"

    r = requests.post(url, json=payload, timeout=20)
    try:
        r.raise_for_status()
    except Exception as e:
        # Envía el error al grupo para depuración rápida
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": f"⚠️ Error al enviar: {escape_markdown_v2(str(e))}\nResp: {escape_markdown_v2(r.text)}",
                    "parse_mode": "MarkdownV2" if USE_MARKDOWN else None
                },
                timeout=10
            )
        except:
            pass
        raise
    return r.json()

# --- confirmación basada en reply ---
def send_confirmation_from_reply(msg):
    """
    Envía al chat personal una confirmación basada en el mensaje al que se respondió con 'OK'.
    msg: dict del Update.message
    """
    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("Define TELEGRAM_CHAT_ID para enviar la confirmación personal.")
    reply = msg.get("reply_to_message") or {}
    # El texto del original puede venir en 'text' o 'caption' si era media
    original_text = reply.get("text") or reply.get("caption") or ""
    if not original_text:
        original_text = "(mensaje original sin texto)"
    # Quien confirmó
    from_user = msg.get("from", {}) or {}
    who = from_user.get("first_name") or from_user.get("username") or "alguien"

    # Escapar para MarkdownV2
    original_text_safe = escape_markdown_v2(str(original_text))
    who_safe = escape_markdown_v2(str(who))

    confirmation = f"✅ Tarea confirmada por {who_safe}. Márcala como terminada.\n\n{original_text_safe}"

    # Envía a tu chat personal con MarkdownV2
    send_telegram_message(confirmation, chat_id=TELEGRAM_CHAT_ID, parse_mode="MarkdownV2")
    return confirmation

# --- NUEVO: formato exacto del mensaje y menciones desde 'responsable' ---
def build_message_with_fields(headers, last_row):
    # mapea headers -> valores
    if headers and len(headers) == len(last_row):
        keys = [_normalize_header(h) for h in headers]
        row = {k: v for k, v in zip(keys, last_row)}
    else:
        # fallback: sin encabezados claros, usa índices
        row = {f"col_{i}": v for i, v in enumerate(last_row)}

    # lee campos (ajusta estos nombres si en tu Excel difieren)
    responsable     = row.get("responsable", "")
    agrupacion      = row.get("agrupacion", "")
    mes_planificado = row.get("mes_planificado", row.get("mes", ""))
    estatus         = row.get("estatus", row.get("status", ""))
    codigo_equipo   = row.get("codigo_de_equipo", row.get("codigo_equipo", ""))
    instrumento     = row.get("instrumento", "")
    ubicacion       = row.get("ubicacion", "")
    departamento    = row.get("departamento", "")
    actividad       = row.get("actividad", row.get("tarea", ""))

    # generar menciones tipo @K @D a partir de "K,D" o "K, D"
    mentions = []
    for token in (responsable or "").replace(";", ",").split(","):
        u = token.strip()
        if not u:
            continue
        if not u.startswith("@"):
            u = "@" + u
        # Dejar menciones en crudo para no romper el username
        mentions.append(u)
    mentions_line = " ".join(mentions) if mentions else ""

    # Escapa campos/etiquetas si USE_MARKDOWN
    esc = escape_markdown_v2 if USE_MARKDOWN else (lambda x: x)

    lines = []
    if mentions_line:
        # NOTA: no escapamos menciones_line para preservar la mención
        lines.append(mentions_line)
    lines.append(esc("Se tiene una tarea asignada:"))
    lines.append(f"📊 {esc('Agrupación')}: {esc(str(agrupacion))}")
    lines.append(f"🗓️ {esc('Mes planificado')}: {esc(str(mes_planificado))}")
    lines.append(f"✅ {esc('Estatus')}: {esc(str(estatus))}")
    lines.append("")  # línea en blanco
    lines.append(f"🔢 {esc('Código de equipo')}: {esc(str(codigo_equipo))}")
    lines.append(f"🎼 {esc('Instrumento')}: {esc(str(instrumento))}")
    lines.append(f"⚙️ {esc('Ubicación')}: {esc(str(ubicacion))}")
    lines.append(f"🏢 {esc('Departamento')}: {esc(str(departamento))}")
    lines.append(f"📝 {esc('Actividad')}: {esc(str(actividad))}")
    return "\n".join(lines)

# --- lectura de última fila y envío ---
def read_last_row_and_message():
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

    # mensaje con formato exacto (ya escapado internamente si USE_MARKDOWN)
    msg = build_message_with_fields(headers, last_row)

    # envía al grupo
    send_telegram_message(msg, chat_id=TELEGRAM_CHAT_ID)

    # confirmación a tu chat personal (si está configurado)
    if TELEGRAM_CHAT_ID:
        send_telegram_message("✅ Envío completado.\n\n" + msg, chat_id=TELEGRAM_CHAT_ID)

    return msg

# --- NUEVO: dedup por update_id ---
LAST_UPDATE_FILE = "/tmp/last_update_id"

def _is_duplicate_update(update_id: int) -> bool:
    try:
        if os.path.exists(LAST_UPDATE_FILE):
            with open(LAST_UPDATE_FILE, "r") as f:
                last = int(f.read().strip())
                if update_id <= last:
                    return True
        # guarda el nuevo id
        with open(LAST_UPDATE_FILE, "w") as f:
            f.write(str(update_id))
    except Exception:
        # si falla el archivo, no bloquees la ejecución
        pass
    return False

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

# --- webhook con dedup y manejo de 'message' ---
@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    data = request.get_json(force=True) or {}
    update_id = data.get("update_id")

    # Idempotencia: evita reprocesar el mismo update
    if isinstance(update_id, int) and _is_duplicate_update(update_id):
        return {"ok": True, "duplicate": True}

    msg = data.get("message") or {}  # solo 'message', ignoramos 'edited_message'
    text = (msg.get("text") or "").strip()

    if text.upper() == ONLY_TRIGGER_WORD:
        try:
            # Si es reply a un mensaje, enviamos confirmación personal con el contenido original
            if msg.get("reply_to_message"):
                confirmation_preview = send_confirmation_from_reply(msg)
                return {"ok": True, "preview": confirmation_preview}
            # Si NO es reply: leer última fila y publicar + confirmar
            preview = read_last_row_and_message()
            return {"ok": True, "preview": preview}
        except Exception as e:
            # Responde al grupo con el error para depuración rápida
            try:
                send_telegram_message(f"⚠️ Error al leer/enviar: {escape_markdown_v2(str(e))}")
            except:
                pass
            return {"ok": False, "error": str(e)}, 500
    else:
        # Ignora otros textos
        return {"ok": True, "ignored": True}
