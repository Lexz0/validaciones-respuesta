
from flask import Flask, request
import requests
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")  # Usaremos variables de entorno en Render
DESTINO_CHAT_ID = os.getenv("DESTINO_CHAT_ID")

app = Flask(__name__)

@app.route("/", methods=["POST"])
def webhook():
    data = request.json
    if "message" in data:
        msg = data["message"]
        # Verificar si el texto es "OK" y es respuesta a otro mensaje
        if msg.get("text") == "OK" and "reply_to_message" in msg:
            original_text = msg["reply_to_message"].get("text", "")
            texto = f"✅ La tarea se completó con éxito.\n\n{original_text}"
            # Enviar confirmación al destino
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={
                "chat_id": DESTINO_CHAT_ID,
                "text": texto
            })
    return "ok"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
