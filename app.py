import os
import base64
import threading
import requests
import unicodedata
import json
import hashlib
from flask import Flask, request

# ===== Config =====
CLIENT_ID = os.getenv("CLIENT_ID")
AUTHORITY = os.getenv("AUTHORITY") or "https://login.microsoftonline.com/consumers"
SCOPES = ["Files.ReadWrite"]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # supergrupo destino
EXCEL_SHARE_URL = os.getenv("EXCEL_SHARE_URL")
SHEET_NAME = os.getenv("SHEET_NAME")  # ej: "Hoja1"
TABLE_NAME = os.getenv("TABLE_NAME")  # ej: "Tabla1"

ONLY_TRIGGER_WORD = (os.getenv("ONLY_TRIGGER_WORD") or "OK").strip().upper()

# ===== Persistencia local =====
LAST_UPDATE_FILE = "/tmp/last_update_id"
LAST_SENT_SIGNATURE_FILE = "/tmp/last_sent_signature"
LAST_GROUP_MESSAGE_ID_FILE = "/tmp/last_group_message_id"

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
    """Arranca el Device Flow sin bloquear la petición, devuelve el código y la URL."""
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
    _auth_state = {"running": True, "message": None, "error": None}
    _auth_thread = threading.Thread(target=_auth_worker, args=(flow, cache), daemon=True)
    _auth_thread.start()
    return {
        "status": "started",
        "verification_uri": flow.get("verification_uri"),
        "user_code": flow.get("user_code"),
        "expires_in": flow.get("expires_in"),
        "interval": flow.get("interval"),
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
    return r.json()

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

# ===== Telegram (texto plano, sin parse_mode) =====
TELEGRAM_PERSONAL_CHAT_ID = os.getenv("TELEGRAM_PERSONAL_CHAT_ID")

def send_telegram_message(text: str, chat_id: str = None, reply_to_message_id: int = None):
    """
    Envía texto plano al chat indicado. Permite responder (reply) a un message_id.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id or TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
        payload["allow_sending_without_reply"] = True

    r = requests.post(url, json=payload, timeout=20)
    try:
        r.raise_for_status()
    except Exception as e:
        # Error también en texto plano
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": f"⚠️ Error al enviar: {str(e)}\nResp: {r.text}",
                    "disable_web_page_preview": True,
                },
                timeout=10
            )
        except:
            pass
        raise
    return r.json()

# ===== Firma del último mensaje enviado =====
def _compute_signature_from_row(headers, last_row) -> str:
    """
    Crea una firma estable (hash) del contenido publicado.
    Si cambia cualquier celda, cambia la firma.
    """
    try:
        if headers and len(headers) == len(last_row):
            keys = [_normalize_header(h) for h in headers]
            row_dict = {k: ("" if v is None else str(v)) for k, v in zip(keys, last_row)}
        else:
            row_dict = {f"col_{i}": ("" if v is None else str(v)) for i, v in enumerate(last_row)}
        payload = json.dumps(row_dict, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    except Exception:
        return hashlib.sha256(("|".join(map(lambda x: "" if x is None else str(x), last_row))).encode("utf-8")).hexdigest()

def _load_last_signature() -> str:
    try:
        if os.path.exists(LAST_SENT_SIGNATURE_FILE):
            with open(LAST_SENT_SIGNATURE_FILE, "r") as f:
                return f.read().strip()
    except:
        pass
    return ""

def _save_last_signature(sig: str):
    try:
        with open(LAST_SENT_SIGNATURE_FILE, "w") as f:
            f.write(sig)
    except:
        pass

def _save_last_group_message_id(message_id: int):
    try:
        with open(LAST_GROUP_MESSAGE_ID_FILE, "w") as f:
            f.write(str(message_id))
    except:
        pass

def _load_last_group_message_id() -> int:
    try:
        if os.path.exists(LAST_GROUP_MESSAGE_ID_FILE):
            with open(LAST_GROUP_MESSAGE_ID_FILE, "r") as f:
                return int(f.read().strip())
    except:
        pass
    return 0

# ===== Formato del mensaje (texto plano) =====
def build_message_with_fields(headers, last_row):
    if headers and len(headers) == len(last_row):
        keys = [_normalize_header(h) for h in headers]
        row = {k: v for k, v in zip(keys, last_row)}
    else:
        row = {f"col_{i}": v for i, v in enumerate(last_row)}

    responsable     = row.get("responsable", "")
    agrupacion      = row.get("agrupacion", "")
    mes_planificado = row.get("mes_planificado", row.get("mes", ""))
    estatus         = row.get("estatus", row.get("status", ""))
    codigo_equipo   = row.get("codigo_de_equipo", row.get("codigo_equipo", ""))
    instrumento     = row.get("instrumento", "")
    ubicacion       = row.get("ubicacion", "")
    departamento    = row.get("departamento", "")
    actividad       = row.get("actividad", row.get("tarea", ""))

    # menciones
    mentions = []
    for token in (responsable or "").replace(";", ",").split(","):
        u = token.strip()
        if not u:
            continue
        if not u.startswith("@"):
            u = "@" + u
        mentions.append(u)
    mentions_line = " ".join(mentions) if mentions else ""

    lines = []
    if mentions_line:
        lines.append(mentions_line)
    lines.append("Se tiene una tarea asignada:")
    lines.append(f"📊 Agrupación: {agrupacion}")
    lines.append(f"🗓️ Mes planificado: {mes_planificado}")
    lines.append(f"✅ Estatus: {dictamen_estatus}")
    lines.append("")  # línea en blanco
    lines.append(f"🔢 Código de equipo: {codigo_de_equipo}")
    lines.append(f"🎼 Instrumento: {nombre_instrumento}")
    lines.append(f"⚙️ Ubicación: {equipo_sistema_ubicacion}")
    lines.append(f"🏢 Departamento: {departamento}")
    lines.append(f"📝 Actividad: {actividad}")

    return "\n".join(lines)

# ===== Lectura y envío con verificación de duplicado + reply =====
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

    # 1) Calcula firma del contenido actual
    current_sig = _compute_signature_from_row(headers, last_row)
    last_sig = _load_last_signature()

    if last_sig and current_sig == last_sig:
        # 2) Ya fue enviado: manda aviso SOLO al supergrupo respondiendo al último mensaje
        notice = "ℹ️ No hay actualizaciones. Revisa este último mensaje enviado."
        last_mid = _load_last_group_message_id()
        send_telegram_message(notice, chat_id=TELEGRAM_CHAT_ID, reply_to_message_id=(last_mid or None))
        return notice

    # 3) Es nuevo: construye mensaje y publícalo
    msg = build_message_with_fields(headers, last_row)
    send_resp = send_telegram_message(msg, chat_id=TELEGRAM_CHAT_ID)

    # Guarda el message_id y la firma
    try:
        # Bot API devuelve {"ok":true,"result":{"message_id":...}}
        message_id = int(send_resp.get("result", {}).get("message_id") or send_resp.get("message_id") or 0)
        if message_id:
            _save_last_group_message_id(message_id)
    except:
        pass
    _save_last_signature(current_sig)

# ===== Confirmación basada en reply (texto plano) =====
def send_confirmation_from_reply(msg):
    """
    Envía al chat personal una confirmación basada en el mensaje al que se respondió con 'OK'.
    """
    if not TELEGRAM_PERSONAL_CHAT_ID:
        raise RuntimeError("Define TELEGRAM_PERSONAL_CHAT_ID para enviar la confirmación personal.")
    reply = msg.get("reply_to_message") or {}
    original_text = reply.get("text") or reply.get("caption") or ""
    if not original_text:
        original_text = "(mensaje original sin texto)"
    from_user = msg.get("from", {}) or {}
    who = from_user.get("first_name") or from_user.get("username") or "alguien"

    confirmation = f"✅ Tarea confirmada por {who}. Márcala como terminada.\n\n{original_text}"
    send_telegram_message(confirmation, chat_id=TELEGRAM_PERSONAL_CHAT_ID)
    return confirmation

# ===== Dedup por update_id =====
def _is_duplicate_update(update_id: int) -> bool:
    try:
        if os.path.exists(LAST_UPDATE_FILE):
            with open(LAST_UPDATE_FILE, "r") as f:
                last = int(f.read().strip())
                if update_id <= last:
                    return True
        with open(LAST_UPDATE_FILE, "w") as f:
            f.write(str(update_id))
    except Exception:
        pass
    return False

# ===== Endpoints =====
@app.route("/init-auth", methods=["GET"])
def init_auth():
    try:
        payload = start_device_flow_async()
        return {"ok": True, **payload}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

@app.route("/auth-status", methods=["GET"])
def auth_status():
    global _auth_state
    status = {
        "running": _auth_state.get("running"),
        "message": _auth_state.get("message"),
        "error": _auth_state.get("error"),
    }
    try:
        _ = get_token_silent_only()
        status["token_ready"] = True
    except Exception:
        status["token_ready"] = False
    return {"ok": True, "status": status}

@app.route("/reset-auth", methods=["POST"])
def reset_auth():
    try:
        if os.path.exists(TOKEN_CACHE_PATH):
            os.remove(TOKEN_CACHE_PATH)
        global _auth_state
        _auth_state = {"running": False, "message": None, "error": None}
        return {"ok": True, "message": "Cache borrado. Llama /init-auth para reautorizar."}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

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
            # Si NO es reply: verificar cambios y publicar o avisar
            preview = read_last_row_and_message()
            return {"ok": True, "preview": preview}
        except Exception as e:
            # Responde al grupo con el error para depuración rápida (texto plano)
            try:
                send_telegram_message(f"⚠️ Error al leer/enviar: {str(e)}", chat_id=TELEGRAM_CHAT_ID)
            except:
                pass
            return {"ok": False, "error": str(e)}, 500
    else:
        # Ignora otros textos
        return {"ok": True, "ignored": True}
