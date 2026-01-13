import os
import base64
import threading
import requests
import unicodedata
import json
import hashlib
from urllib.parse import urlparse
from flask import Flask, request

# ===== Config =====
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID") or os.getenv("CLIENT_ID")  # compat con nombre anterior
# Enlace compartido de Excel (OneDrive/SharePoint)
EXCEL_SHARE_URL = os.getenv("EXCEL_SHARE_URL", "")
# TENANT_ID robusto: si está vacío en env, usa 'common'
TENANT_ID = os.getenv("TENANT_ID") or "common"
# Detecta si el share es de OneDrive personal (consumer: 1drv.ms / onedrive.live.com)
_share_host = urlparse(EXCEL_SHARE_URL).netloc.lower()
IS_CONSUMER = ("1drv.ms" in _share_host) or ("onedrive.live.com" in _share_host)
# Authority: consumidores para 1drv.ms; de lo contrario, usar TENANT_ID/common
AUTHORITY = f"https://login.microsoftonline.com/{'consumers' if IS_CONSUMER else TENANT_ID}"
# Scopes solo de Graph (sin openid/profile/offline_access)
SCOPES = ["User.Read", "Files.ReadWrite"]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # supergrupo destino
TELEGRAM_PERSONAL_CHAT_ID = os.getenv("TELEGRAM_PERSONAL_CHAT_ID")

SHEET_NAME = os.getenv("SHEET_NAME")  # ej: "Hoja1"
TABLE_NAME = os.getenv("TABLE_NAME")  # ej: "Tabla1"

ONLY_TRIGGER_WORD = (os.getenv("ONLY_TRIGGER_WORD") or "OK").strip().upper()

# ===== Persistencia local (ids/firmas). Los tokens van a Redis =====
LAST_UPDATE_FILE = "/tmp/last_update_id"
LAST_SENT_SIGNATURE_FILE = "/tmp/last_sent_signature"
LAST_GROUP_MESSAGE_ID_FILE = "/tmp/last_group_message_id"

# ===== Flask =====
app = Flask(__name__)

# ===== MSAL + caché persistente en Redis =====
import msal
from upstash_redis import Redis

REDIS = Redis.from_env()  # usa UPSTASH_REDIS_REST_URL y UPSTASH_REDIS_REST_TOKEN
CACHE_KEY = f"msal_cache:{AZURE_CLIENT_ID}:{'consumers' if IS_CONSUMER else TENANT_ID}"

_auth_thread = None
_auth_state = {"running": False, "message": None, "error": None}

def _load_token_cache():
    cache = msal.SerializableTokenCache()
    try:
        s = REDIS.get(CACHE_KEY)
        if s:
            cache.deserialize(s)
    except Exception as e:
        print(f"[WARN] No se pudo cargar caché desde Redis: {e}")
    return cache

def _save_token_cache(cache):
    try:
        if cache.has_state_changed:
            REDIS.set(CACHE_KEY, cache.serialize())
    except Exception as e:
        print(f"[WARN] No se pudo guardar caché en Redis: {e}")

def _public_client(cache=None):
    return msal.PublicClientApplication(
        client_id=AZURE_CLIENT_ID,
        authority=AUTHORITY,
        token_cache=cache,
    )

def try_get_token_silent():
    """Intenta obtener token solo en modo silencioso. Devuelve token o None."""
    cache = _load_token_cache()
    app_auth = _public_client(cache)
    accounts = app_auth.get_accounts()
    result = None
    if accounts:
        result = app_auth.acquire_token_silent(SCOPES, account=accounts[0])
    if not result or "access_token" not in result:
        result = app_auth.acquire_token_silent(SCOPES, account=None)
    if result and "access_token" in result:
        _save_token_cache(cache)
        return result["access_token"]
    return None

def get_token_silent_only():
    """Como antes: intenta silencioso, si falla inicia Device Flow y BLOQUEA hasta completar."""
    cache = _load_token_cache()
    app_auth = _public_client(cache)
    scopes = SCOPES
    # 1) Intento silencioso
    accounts = app_auth.get_accounts()
    result = None
    if accounts:
        result = app_auth.acquire_token_silent(scopes, account=accounts[0])
    if not result or "access_token" not in result:
        result = app_auth.acquire_token_silent(scopes, account=None)
    # 2) Fallback a Device Code (bloqueante)
    if not result or "access_token" not in result:
        flow = app_auth.initiate_device_flow(scopes=scopes)
        if "user_code" not in flow:
            raise RuntimeError(
                f"No se pudo iniciar device flow. Revisa AZURE_CLIENT_ID y AUTHORITY ('consumers' vs '{TENANT_ID}'). Detalle: {flow}"
            )
        try:
            df_msg = flow.get("message") or f"Visita {flow.get('verification_uri')} y usa el código {flow.get('user_code')}"
            if TELEGRAM_BOT_TOKEN and TELEGRAM_PERSONAL_CHAT_ID:
                _send_telegram_message(df_msg, chat_id=TELEGRAM_PERSONAL_CHAT_ID)
        except Exception:
            pass  # no interrumpas si Telegram falla
        # BLOQUEA hasta completar login
        result = app_auth.acquire_token_by_device_flow(flow)
        if not result or "access_token" not in result:
            raise RuntimeError("No se obtuvo access_token tras Device Code. Revisa configuración y vuelve a intentar.")
    _save_token_cache(cache)
    return result["access_token"]

def _auth_worker(flow, cache):
    """Hilo en segundo plano que bloquea hasta que completes el Device Flow."""
    global _auth_state
    try:
        app_auth = _public_client(cache)
        result = app_auth.acquire_token_by_device_flow(flow)  # bloquea hasta completar
        if "access_token" not in result:
            got_rt = "refresh_token" in result
            print(f"[INFO] DeviceFlow terminó sin access_token. refresh_token presente: {got_rt}")
            _save_token_cache(cache)
            _auth_state["error"] = f"Error de autenticación: {result}"
        else:
            print("[INFO] DeviceFlow completado con access_token.")
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
    # Intento silencioso por si ya hay token (redeploy, reinicio, etc.)
    accounts = app_auth.get_accounts()
    if accounts:
        result = app_auth.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            _save_token_cache(cache)
            return {"status": "ready", "message": "Token ya disponible en caché."}
    flow = app_auth.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(
            f"No se pudo iniciar device flow. Revisa AZURE_CLIENT_ID y AUTHORITY ('consumers' vs '{TENANT_ID}'), y habilita 'Allow public client flows' en la app. Detalle: {flow}"
        )
    # (Opcional) enviar el mensaje del device flow por Telegram para login inmediato
    try:
        df_msg = flow.get("message") or f"Visita {flow.get('verification_uri')} y usa el código {flow.get('user_code')}"
        if TELEGRAM_BOT_TOKEN and TELEGRAM_PERSONAL_CHAT_ID:
            _send_telegram_message(df_msg, chat_id=TELEGRAM_PERSONAL_CHAT_ID)
    except Exception as e:
        print(f"[WARN] No se pudo enviar mensaje de device flow por Telegram: {e}")
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
    """
    Resuelve el driveItem a partir de un enlace compartido (share URL) de OneDrive/SharePoint.
    - 'sharing_url' es el enlace tal cual (ej.: https://1drv.ms/... o https://<tenant>-my.sharepoint.com/...)
    - 'access_token' debe ser un JWT válido de Microsoft Graph.
    """
    if not access_token or not access_token.strip():
        raise RuntimeError("No hay access_token para llamar a Graph.")
    encoded = encode_sharing_url(sharing_url)
    r = requests.get(
        f"{GRAPH}/shares/{encoded}/driveItem",
        headers={
            "Authorization": f"Bearer {access_token}",
            # Redime el enlace compartido (similar a abrirlo en navegador para aceptar el gesto de compartir)
            "Prefer": "redeemSharingLink",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

def get_used_range_values(item_id: str, sheet_name: str, access_token: str):
    url = f"{GRAPH}/drive/items/{item_id}/workbook/worksheets('{sheet_name}')/usedRange(valuesOnly=true)?$select=text,address"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("text", [])  # matriz 2D

def get_table_headers(item_id: str, table_name: str, access_token: str):
    url = f"{GRAPH}/drive/items/{item_id}/workbook/tables('{table_name}')/headerRowRange?$select=text"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("text", [[]])[0]

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

def _send_telegram_message(text: str, chat_id: str = None, reply_to_message_id: int = None):
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN no está configurado.")
    target_chat = chat_id or TELEGRAM_CHAT_ID
    if not target_chat:
        raise RuntimeError("No hay chat_id destino (TELEGRAM_CHAT_ID).")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": target_chat,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
        payload["allow_sending_without_reply"] = True
    r = requests.post(url, json=payload, timeout=30)
    try:
        r.raise_for_status()
    except Exception as e:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHAT_ID or target_chat,
                    "text": f"⚠️ Error al enviar: {str(e)}\nResp: {r.text}",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
        except Exception:
            pass
        raise
    return r.json()

# ===== Normalización de encabezados =====

def _normalize_header(h: str) -> str:
    h = (h or "").strip().lower()
    h = "".join(c for c in unicodedata.normalize("NFD", h) if unicodedata.category(c) != "Mn")  # quita acentos
    for ch in [" ", "-", "/", ".", ":", ";"]:
        h = h.replace(ch, "_")
    return h

# ===== Alinear headers y row =====

def _align(headers, row):
    n = max(len(headers), len(row))
    hh = list(headers) + [""] * (n - len(headers))
    rr = list(row) + [""] * (n - len(row))
    return hh, rr

# ===== Firma del último mensaje enviado =====

def _compute_signature_from_row(headers, last_row) -> str:
    try:
        headers, last_row = _align(headers, last_row)
        keys = [_normalize_header(h) for h in headers]
        row_dict = {k: ("" if v is None else str(v)) for k, v in zip(keys, last_row)}
        payload = json.dumps(row_dict, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
    except Exception:
        return hashlib.sha256(("\n".join(map(lambda x: "" if x is None else str(x), last_row))).encode("utf-8")).hexdigest()

def _load_last_signature() -> str:
    try:
        if os.path.exists(LAST_SENT_SIGNATURE_FILE):
            with open(LAST_SENT_SIGNATURE_FILE, "r") as f:
                return f.read().strip()
    except Exception:
        pass
    return ""

def _save_last_signature(sig: str):
    try:
        with open(LAST_SENT_SIGNATURE_FILE, "w") as f:
            f.write(sig)
    except Exception:
        pass

def _save_last_group_message_id(message_id: int):
    try:
        with open(LAST_GROUP_MESSAGE_ID_FILE, "w") as f:
            f.write(str(message_id))
    except Exception:
        pass

def _load_last_group_message_id() -> int:
    try:
        if os.path.exists(LAST_GROUP_MESSAGE_ID_FILE):
            with open(LAST_GROUP_MESSAGE_ID_FILE, "r") as f:
                return int(f.read().strip())
    except Exception:
        pass
    return 0

# ===== Normalización de trigger =====

def _normalize_trigger(text: str) -> str:
    t = (text or "").strip().upper()
    # quita algunos adornos comunes
    t = t.replace("✅", "").replace("!", "").strip()
    return t

# ===== Última fila no vacía =====

def _last_nonempty_row(rows_2d):
    for row in reversed(rows_2d or []):
        if any((c or "").strip() for c in row):
            return row
    return []

# ===== Formato del mensaje (texto plano) =====

def build_message_with_fields(headers, last_row):
    headers, last_row = _align(headers or [], last_row or [])
    keys = [_normalize_header(h) for h in headers]
    row = {k: v for k, v in zip(keys, last_row)}

    responsable = row.get("responsable", "")
    agrupacion = row.get("agrupacion", "")
    mes_planificado = row.get("mes_planificado", row.get("mes", ""))
    dictamen_estatus = row.get("dictamen_estatus", row.get("status", ""))
    codigo_de_equipo = row.get("codigo_de_equipo", row.get("codigo_equipo", ""))
    nombre_instrumento = row.get("nombre_instrumento", "")
    equipo_sistema_ubicacion = row.get("equipo_sistema_ubicacion", "")
    departamento = row.get("departamento", "")
    actividad = row.get("actividad", row.get("tarea", ""))

    # menciones – solo usernames válidos (sin espacios)
    mentions = []
    for token in (responsable or "").replace(";", ",").split(","):
        u = token.strip()
        if not u:
            continue
        if u.startswith("@") and " " not in u:
            mentions.append(u)
        elif " " not in u:
            mentions.append("@" + u)
        # Si tiene espacios, lo omitimos para no romper el mensaje
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

def read_last_row_and_message(access_token: str):
    # 1) Resuelve el driveItem del enlace compartido
    item = get_drive_item_from_share(EXCEL_SHARE_URL, access_token)
    item_id = item.get("id")
    if not item_id:
        raise RuntimeError("No se obtuvo item.id del enlace compartido.")

    # 2) Lee filas desde tabla u hoja
    if TABLE_NAME:
        headers = get_table_headers(item_id, TABLE_NAME, access_token)
        rows = get_table_rows(item_id, TABLE_NAME, access_token)
        if not rows:
            raise RuntimeError("La tabla no devolvió filas.")
        last_row = rows[-1]
    elif SHEET_NAME:
        values = get_used_range_values(item_id, SHEET_NAME, access_token)
        if not values:
            raise RuntimeError("La hoja no devolvió valores.")
        headers = values[0] if len(values) >= 1 else []
        # tomar la última fila con contenido (excluyendo encabezados)
        last_row = _last_nonempty_row(values[1:] if len(values) >= 2 else values)
        if not last_row:
            raise RuntimeError("No se encontró última fila con contenido.")
    else:
        raise RuntimeError("Configura SHEET_NAME o TABLE_NAME.")

    # 3) Idempotencia por firma
    current_sig = _compute_signature_from_row(headers, last_row)
    last_sig = _load_last_signature()
    if last_sig and current_sig == last_sig:
        notice = "ℹ️ No hay actualizaciones. Revisa este último mensaje enviado."
        last_mid = _load_last_group_message_id()
        _send_telegram_message(
            notice,
            chat_id=TELEGRAM_CHAT_ID,
            reply_to_message_id=(last_mid or None),
        )
        return notice

    # 4) Construye y envía
    msg = build_message_with_fields(headers, last_row)
    send_resp = _send_telegram_message(msg, chat_id=TELEGRAM_CHAT_ID)
    # extracción robusta de message_id
    message_id = 0
    try:
        if isinstance(send_resp, dict):
            if "result" in send_resp and isinstance(send_resp["result"], dict):
                message_id = int(send_resp["result"].get("message_id", 0))
            elif "message_id" in send_resp:
                message_id = int(send_resp.get("message_id", 0))
    except Exception:
        pass
    if message_id:
        _save_last_group_message_id(message_id)

    _save_last_signature(current_sig)
    return "Mensaje enviado"

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
    _send_telegram_message(confirmation, chat_id=TELEGRAM_PERSONAL_CHAT_ID)
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
        _ = try_get_token_silent()
        status["token_ready"] = bool(_)
    except Exception:
        status["token_ready"] = False
    return {"ok": True, "status": status}

@app.route("/reset-auth", methods=["POST"])
def reset_auth():
    try:
        try:
            REDIS.delete(CACHE_KEY)
        except Exception as e:
            print(f"[WARN] No se pudo borrar cache en Redis: {e}")
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

    if _normalize_trigger(text) == ONLY_TRIGGER_WORD:
        try:
            # Si es reply a un mensaje, enviamos confirmación personal
            if msg.get("reply_to_message"):
                confirmation_preview = send_confirmation_from_reply(msg)
                return {"ok": True, "preview": confirmation_preview}

            # Si NO es reply: verificar token sin bloquear
            access_token = try_get_token_silent()
            if not access_token:
                payload = start_device_flow_async()
                note = payload.get("note") or "Autorización requerida."
                try:
                    _send_telegram_message(
                        f"🔐 {note}\nCódigo: {payload.get('user_code')}\nURL: {payload.get('verification_uri')}",
                        chat_id=TELEGRAM_CHAT_ID,
                    )
                except Exception:
                    pass
                return {"ok": True, "preview": "Se inició Device Flow. Intenta de nuevo cuando el token esté listo."}

            # Con token listo: procede
            preview = read_last_row_and_message(access_token)
            return {"ok": True, "preview": preview}
        except Exception as e:
            try:
                _send_telegram_message(f"⚠️ Error al leer/enviar: {str(e)}", chat_id=TELEGRAM_CHAT_ID)
            except Exception:
                pass
            return {"ok": False, "error": str(e)}, 500
    else:
        # Ignora otros textos
        return {"ok": True, "ignored": True}

@app.get("/me")
def who_am_i():
    access_token = get_token_silent_only()
    r = requests.get(
        f"{GRAPH}/me",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    try:
        r.raise_for_status()
    except Exception as e:
        return {"ok": False, "error": str(e), "body": r.text}, r.status_code
    return r.json(), 200

@app.get("/diag")
def diag():
    try:
        access_token = try_get_token_silent()
        return {
            "authority": AUTHORITY,
            "token_len": len(access_token) if access_token else 0,
            "scopes": SCOPES,
            "share_host": _share_host,
            "is_consumer": IS_CONSUMER,
            "token_ready": bool(access_token),
        }, 200
    except Exception as e:
        return {"authority": AUTHORITY, "error": str(e)}, 500

if __name__ == "__main__":
    # Para pruebas locales
    port = int(os.getenv("PORT") or 8000)
    app.run(host="0.0.0.0", port=port)
