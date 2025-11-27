#!/usr/bin/env python3
import asyncio
import os
import sys
import logging
import traceback
import subprocess
import shutil
import socket
import time
from urllib.parse import urlparse

from websockets.asyncio.server import serve, ServerConnection
from websockets.exceptions import ConnectionClosed

###########################################################
# LOGGING
###########################################################
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("proxy")

logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.asyncio.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.http11").setLevel(logging.CRITICAL)

def info(msg: str):
    log.info(msg)


###########################################################
# GLOBALS (SEM GLOBAL DECLARATION)
###########################################################
REDIS_PROC = None  # (apenas uma variável global real)
IS_RENDER = bool(os.getenv("RENDER") or os.getenv("RENDER_SERVICE_ID"))
IS_LINUX = sys.platform.startswith("linux")

###########################################################
# CONFIG WEBSOCKET
###########################################################
PROXY_HOST = "0.0.0.0"
PROXY_PORT = int(os.getenv("PORT", "10000"))
WS_AUTH_TOKEN = os.getenv("WS_AUTH_TOKEN")

###########################################################
# CONFIG REDIS
###########################################################
RAW_URL = os.getenv("REDIS_URL")
EMBED_LOCAL = os.getenv("EMBED_LOCAL_REDIS", "").lower() in ("1", "true", "yes")

if RAW_URL:
    u = urlparse(RAW_URL)
    REDIS_HOST = u.hostname
    REDIS_PORT = u.port or 6379
    REDIS_SSL = u.scheme in ("rediss", "redis+ssl")
else:
    REDIS_HOST = "127.0.0.1"
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_SSL = False


###########################################################
# REDIS LOCAL MANAGER
###########################################################

def wait_redis_ready(host, port, timeout=10):
    info(f"[REDIS] Aguardando Redis subir em {host}:{port} ...")
    deadline = time.time() + timeout
    tents = 0
    while time.time() < deadline:
        tents += 1
        try:
            with socket.create_connection((host, port), timeout=0.4):
                info(f"[REDIS] Redis respondeu na tentativa {tents}")
                return True
        except OSError:
            time.sleep(0.3)
    info("[REDIS] Timeout ao subir Redis!")
    return False


def start_local_redis():
    global REDIS_PROC, REDIS_HOST, REDIS_PORT, REDIS_SSL

    # Se REDIS_URL foi fornecida, não inicia local
    if RAW_URL:
        info("[REDIS] Usando Redis externo via REDIS_URL")
        return

    # Render SEM REDIS_URL → usar Redis embutido
    if not (EMBED_LOCAL or IS_RENDER):
        info("[REDIS] Redis embutido desativado.")
        return

    info("[REDIS] Redis embutido ATIVADO")

    REDIS_HOST = "127.0.0.1"
    REDIS_PORT = 6379
    REDIS_SSL = False

    # Verifica se já há Redis rodando
    if wait_redis_ready(REDIS_HOST, REDIS_PORT, timeout=1):
        info("[REDIS] Redis já está rodando. Não vou subir outro.")
        return

    # Caminho para binário incluído no repo
    path = os.path.join(os.getcwd(), "bin", "redis-server")

    if not os.path.exists(path):
        info(f"[REDIS] Binário não encontrado: {path}")
        info("[REDIS] Certifique-se que bin/redis-server está no repositório.")
        return

    info(f"[REDIS] Iniciando redis-server pelo bin: {path}")

    cmd = [
        path,
        "--port", str(REDIS_PORT),
        "--bind", "127.0.0.1",
        "--appendonly", "no",
        "--save", "",
    ]

    REDIS_PROC = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    info(f"[REDIS] PID = {REDIS_PROC.pid}")

    if not wait_redis_ready(REDIS_HOST, REDIS_PORT, timeout=10):
        info("[REDIS] Falhou ao subir redis-server embutido.")
        return



###########################################################
# PROXY WS <-> REDIS
###########################################################
async def ws_handler(ws: ServerConnection):
    try:
        peer = ws.remote_address
    except:
        peer = None

    target = getattr(getattr(ws, "request", None), "target", "?")

    info(f"[WS] Nova conexão de {peer}, target={target}")

    # token
    if WS_AUTH_TOKEN:
        token = None
        req = getattr(ws, "request", None)
        if req and req.query:
            token = req.query.get("token")
        else:
            if "?" in target:
                qs = dict(p.split("=", 1) for p in target.split("?")[1].split("&"))
                token = qs.get("token")

        if token != WS_AUTH_TOKEN:
            info(f"[WS] Token inválido: {token}")
            await ws.send(b"-NOAUTH Invalid token\r\n")
            await ws.close(code=4001)
            return

    # conecta ao redis
    try:
        info(f"[WS] Conectando ao Redis {REDIS_HOST}:{REDIS_PORT} ssl={REDIS_SSL}")
        redis_reader, redis_writer = await asyncio.open_connection(
            REDIS_HOST, REDIS_PORT, ssl=REDIS_SSL or None
        )
        info("[WS] Conectado ao Redis backend")
    except Exception as e:
        msg = f"-ERR cannot connect to Redis backend {REDIS_HOST}:{REDIS_PORT}: {e}\r\n"
        await ws.send(msg.encode())
        await ws.close(code=1011)
        return

    async def ws_to_redis():
        try:
            async for msg in ws:
                if isinstance(msg, str):
                    data = msg.encode()
                else:
                    data = msg
                redis_writer.write(data)
                await redis_writer.drain()
        except Exception:
            pass
        finally:
            redis_writer.close()

    async def redis_to_ws():
        try:
            while True:
                data = await redis_reader.read(4096)
                if not data:
                    break
                await ws.send(data)
        except Exception:
            pass
        finally:
            await ws.close()

    t1 = asyncio.create_task(ws_to_redis())
    t2 = asyncio.create_task(redis_to_ws())

    await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)

    info(f"[WS] Conexão encerrada com {peer}")


###########################################################
# MAIN
###########################################################
async def main():
    info(f"Iniciando WS proxy em ws://0.0.0.0:{PROXY_PORT}")
    info(f"Redis alvo: {REDIS_HOST}:{REDIS_PORT}")

    async with serve(ws_handler, PROXY_HOST, PROXY_PORT):
        info("Servidor pronto. Aguardando conexões…")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        start_local_redis()
        asyncio.run(main())
    finally:
        if REDIS_PROC:
            info("[REDIS] Encerrando redis-server…")
            try:
                REDIS_PROC.terminate()
            except:
                pass
