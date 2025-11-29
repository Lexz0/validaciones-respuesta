
from flask import Flask, request
import os
import requests
import pandas as pd
import logging

# === Credenciales y configuración ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
DESTINO_CHAT_ID = os.getenv("DESTINO_CHAT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
EXCEL_PATH = os.getenv("EXCEL_PATH")  # Ej: "Documentos/Reporte.xlsx"

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# === Utilidades Telegram ===
def send_message(chat_id, texto):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": texto}
    r = requests.post(url, json=payload, timeout=15)
    app.logger.info(f"sendMessage status={r.status_code}, resp={r.text}")

# === OAuth2: client credentials para Graph ===
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
        raise RuntimeError("No se pudo obtener el access_token de Azure AD.")
    return r.json()["access_token"]

# === Descarga del Excel desde OneDrive (Graph) ===
def descargar_excel_a_archivo_local(token, one_drive_path, destino_local="temp.xlsx"):
    """
    Descarga el archivo de OneDrive usando Microsoft Graph:
    GET /me/drive/root:/{path}:/content  (personal/usuario actual)
    """
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{one_drive_path}:/content"
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        app.logger.error(f"Descarga error {r.status_code}: {r.text}")
        raise RuntimeError("No se pudo descargar el Excel desde OneDrive.")
    with open(destino_local, "wb") as f:
        f.write(r.content)
    return destino_local

# === Lógica de negocio: leer Excel y armar reporte ===
def generar_reporte_desde_excel(ruta_local_excel):
    """
    Ejemplo: toma la primera fila y arma un texto.
    Ajusta columnas según tu archivo.
    """
    df = pd.read_excel(ruta_local_excel)  # usa openpyxl para .xlsx
    if df.empty:
        return "📊 El Excel no tiene datos."
    fila = df.iloc[0]
    # Cambia 'Nombre' y 'Valor' por los nombres de columnas reales
    nombre_col = "Nombre" if "Nombre" in df.columns else df.columns[0]
    valor_col = "Valor" if "Valor" in df.columns else df.columns[1] if len(df.columns) > 1 else df.columns[0]
    reporte = (
        f"📊 Reporte automático:\n"
        f"- {nombre_col}: {fila.get(nombre_col, '')}\n"
        f"- {valor_col}: {fila.get(valor_col, '')}"
    )
    return reporte

# === Webhook Telegram ===
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

    # Nuevo comando: /reporte
    elif lower == "/reporte":
        try:
            token = get_access_token()
            local_path = descargar_excel_a_archivo_local(token, EXCEL_PATH)
            reporte = generar_reporte_desde_excel(local_path)
            send_message(DESTINO_CHAT_ID, reporte)
        except Exception as e:
            app.logger.exception("Error generando reporte")
            send_message(DESTINO_CHAT_ID, f"❌ Error generando reporte: {e}")

    return "ok", 200

# Salud del servicio (útil para pings tipo UptimeRobot)
@app.route("/", methods=["GET"])
def health():
    return "up", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
