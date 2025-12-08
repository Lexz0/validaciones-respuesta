# token_store.py
from upstash_redis import Redis

# Carga credenciales UPSTASH_* desde variables de entorno automáticamente
redis = Redis.from_env()  # usa UPSTASH_REDIS_REST_URL y UPSTASH_REDIS_REST_TOKEN

CACHE_KEY = "msal_cache_default"  # si luego manejas varios usuarios, crea una clave por account

def load_cache_string() -> str | None:
    return redis.get(CACHE_KEY)

def save_cache_string(s: str) -> None:
    # Puedes añadir expiración si quieres forzar reconsent en X días:
    redis.set(CACHE_KEY, s)
