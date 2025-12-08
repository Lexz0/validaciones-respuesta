
# auth.py
import os
import msal
from token_store import load_cache_string, save_cache_string

AUTHORITY = f"https://login.microsoftonline.com/{os.environ.get('TENANT_ID', 'common')}"
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]

# Incluye 'offline_access' para asegurar refresh tokens + los scopes que uses en Graph.
DEFAULT_SCOPES = ["offline_access", "User.Read"]  # añade otros: Files.ReadWrite, etc.

# 1) Cargar caché persistente desde Redis y dársela a MSAL
_cache = msal.SerializableTokenCache()
_cache_str = load_cache_string()
if _cache_str:
    _cache.deserialize(_cache_str)

app = msal.PublicClientApplication(
    client_id=CLIENT_ID,
    authority=AUTHORITY,
    token_cache=_cache,  # MSAL leerá y actualizará aquí
)

def _persist_cache_if_changed():
    if _cache.has_state_changed:
        save_cache_string(_cache.serialize())

def get_access_token(scopes: list[str] = None) -> str:
    scopes = scopes or DEFAULT_SCOPES

    # 2) Intento silencioso: usa la caché; si el AT está vencido, MSAL utiliza el RT automáticamente.
    accounts = app.get_accounts()
    result = None
    if accounts:
        result = app.acquire_token_silent(scopes, account=accounts[0])  # renovación automática
        # MSAL usa refresh token guardado en caché si el access token ya expiró.

    # 3) Si no hay caché válida, inicia Device Code una única vez
    if not result or "access_token" not in result:
        flow = app.initiate_device_flow(scopes=scopes)  # genera el mensaje con el código
        if "user_code" not in flow:
            raise RuntimeError("No se pudo iniciar device flow: " + str(flow))
        print(flow["message"])  # Muestra el texto para login (puedes enviarlo por Telegram a tu chat)
        result = app.acquire_token_by_device_flow(flow)  # bloquea hasta que completes el login

    # 4) Persistir cualquier cambio en la caché (access/refresh tokens nuevos)
    _persist_cache_if_changed()

    return result["access_token"]
