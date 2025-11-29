
from flask import Flask, request
import os, requests, pandas as pd, logging, base64

BOT_TOKEN = os.getenv("BOT_TOKEN")
DESTINO_CHAT_ID = os.getenv("DESTINO_CHAT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")

# Tu enlace compartido de OneDrive
ONEDRIVE_SHARE_URL = "https://1drv.ms/x/c/3fad8c902923be18/ETwxHKdkmdlLi7EPaoH6vXUB4R-qhJ3AMeMwEbek4LNong?e=b5SpNn"

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

def send_message(chat_id, texto):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": texto}, timeout=15)
    app.logger.info(f"sendMessage status={r.status_code}, resp={r.text}")

def get_access_token():
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }
    r = requests.post(token_url, data=data, timeout=20)
    if r.status_code != 200:
        app.logger.error(f"Token error {r.status_code}: {r.text}")
        raise RuntimeError("No se obtuvo token de Azure AD.")
    return r.json()["access_token"]

def share_id_from_url(url: str) -> str:
    encoded = base64.urlsafe_b64encode(url.encode("utf-8")).decode("utf-8").rstrip("=")
    return f"u!{encoded}"

def descargar_excel_por_share_url(share_url: str, destino_local="temp.xlsx"):
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    share_id = share_id_from_url(share_url)
    api = f"https://graph.microsoft.com/v1.0/shares/{share_id}/driveItem/content"
    r = requests.get(api, headers=headers, timeout=30)
    if r.status_code != 200:
        app.logger.error(f"Descarga error {r.status_code}: {r.text}")
        raise RuntimeError(f"No se pudo descargar el Excel (HTTP {r.status_code}).")
    with open(destino_local, "wb") as f:
        f.write(r.content)
    return destino_local

def formato_ultima_fila(ruta_local_excel: str) -> str:
    # Leer Excel con pandas y tomar la última fila "real"
    df = pd.read_excel(ruta_local_excel)  # requiere openpyxl para .xlsx
    # Quitar filas completamente vacías (si las hubiera)
    df_clean = df.dropna(how="all")
    if df_clean.empty:
        return "📊 El Excel no tiene datos."
    ultima = df_clean.tail(1).iloc[0]

    # Construir un mensaje con todas las columnas de la última fila
    lineas = []
    for col in df_clean.columns:
        val = ultima[col]
        # Convertir NaN a vacío y tipos a string bonitos
        if pd.isna(val):
            val = ""
        elif isinstance(val, float):
            # Quitar .0 cuando es entero
            val = int(val) if val.is_integer() else round(val, 4)
        lineas.append(f"- {col}: {val}")
    mensaje = "✅ Nueva actualización (última fila del Excel):\n" + "\n".join(lineas)
    return mensaje

@app.route("/", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "ok", 200

    texto = (msg.get("text") or "").strip()
    lower = texto.lower()

    # Tu lógica previa de "OK"
    if lower == "ok":
        original_text = msg.get("reply_to_message", {}).get("text", "")
        contenido = f"✅ La tarea se completó con éxito.\n\n{original_text}" if original_text else "✅ La tarea se completó con éxito."
        send_message(DESTINO_CHAT_ID, contenido)

    # Nuevo comando: /reporte -> última fila
    elif lower == "/reporte":
        try:
            local = descargar_excel_por_share_url(ONEDRIVE_SHARE_URL)
            mensaje = formato_ultima_fila(local)
            send_message(DESTINO_CHAT_ID, mensaje)
        except Exception as e:
            app.logger.exception("Error generando reporte")
            send_message(DESTINO_CHAT_ID, f"❌ Error generando reporte: {e}")

    return "ok", 200

@app.route("/", methods=["GET"])
def health():
    return "up", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
