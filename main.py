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
import threading
import signal
from urllib.parse import urlparse
from websockets.asyncio.server import serve, ServerConnection

###########################################################
# LOGGING
###########################################################
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("proxy")

# reduzir logs verbosos de websockets
logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.asyncio.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.http11").setLevel(logging.CRITICAL)

def info(msg: str):
    log.info(msg)

def debug(msg: str):
    log.debug(msg)

def error(msg: str):
    log.error(msg)

def exc(msg: str = None):
    if msg:
        log.exception(msg)
    else:
        log.exception("Exception")

###########################################################
# GLOBALS
###########################################################
REDIS_PROC = None
REDIS_MON_THREAD = None
IS_RENDER = bool(os.getenv("RENDER") or os.getenv("RENDER_SERVICE_ID"))
IS_LINUX = sys.platform.startswith("linux")
BIN_DIR = os.path.join(os.getcwd(), "bin")

###########################################################
# CONFIG WEBSOCKET & HTTP (health)
###########################################################
PROXY_HOST = "0.0.0.0"
PROXY_PORT = int(os.getenv("PORT", "10000"))  # Render fornece $PORT
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
# HELPERS
###########################################################
def wait_redis_ready(host, port, timeout=10):
    info(f"[REDIS] Aguardando Redis subir em {host}:{port} ...")
    deadline = time.time() + timeout
    tents = 0
    while time.time() < deadline:
        tents += 1
        try:
            with socket.create_connection((host, port), timeout=0.5):
                info(f"[REDIS] Redis respondeu na tentativa {tents}")
                return True
        except OSError:
            time.sleep(0.3)
    info("[REDIS] Timeout ao subir Redis!")
    return False

def _drain_stream_to_log(pipe, prefix):
    """Thread target: lê linhas do pipe e loga"""
    try:
        for raw in iter(pipe.readline, b""):
            if not raw:
                break
            try:
                line = raw.decode(errors="replace").rstrip()
            except Exception:
                line = str(raw)
            info(f"[REDIS][{prefix}] {line}")
    except Exception:
        info(f"[REDIS][{prefix}] leitura finalizada com erro.")
    finally:
        try:
            pipe.close()
        except Exception:
            pass

def _start_monitor_thread(proc):
    """Monitora saída do processo e encerra se necessário (thread)"""
    t_out = threading.Thread(target=_drain_stream_to_log, args=(proc.stdout, "OUT"), daemon=True)
    t_err = threading.Thread(target=_drain_stream_to_log, args=(proc.stderr, "ERR"), daemon=True)
    t_out.start()
    t_err.start()

    def wait_proc():
        try:
            rc = proc.wait()
            info(f"[REDIS] Processo redis-server finalizado com rc={rc}")
        except Exception as e:
            info(f"[REDIS] Erro ao aguardar redis-server: {e}")

    t_wait = threading.Thread(target=wait_proc, daemon=True)
    t_wait.start()
    return (t_out, t_err, t_wait)

###########################################################
# START LOCAL REDIS
###########################################################
def start_local_redis():
    global REDIS_PROC, REDIS_HOST, REDIS_PORT, REDIS_SSL, REDIS_MON_THREAD

    # Se REDIS_URL foi fornecida, não inicia local
    if RAW_URL:
        info("[REDIS] Usando Redis externo via REDIS_URL")
        return

    # Decide se sobe
    if not (EMBED_LOCAL or IS_RENDER):
        info("[REDIS] Redis embutido desativado.")
        return

    info("[REDIS] Redis embutido ATIVADO")

    REDIS_HOST = "127.0.0.1"
    REDIS_PORT = 6379
    REDIS_SSL = False

    if wait_redis_ready(REDIS_HOST, REDIS_PORT, timeout=1):
        info("[REDIS] Redis já está rodando. Não vou subir outro.")
        return

    # caminho para binário (repo/bin/redis-server)
    path = os.path.join(BIN_DIR, "redis-server")
    if not os.path.exists(path):
        info(f"[REDIS] Binário não encontrado em: {path}")
        info("[REDIS] Certifique-se de commitar bin/redis-server no repo.")
        return

    # garante permissão de execução (render faz clone sem +x)
    try:
        os.chmod(path, 0o755)
        info("[REDIS] chmod 0755 aplicado ao binário redis-server")
    except Exception as e:
        info(f"[REDIS] Falha ao aplicar chmod: {e}")

    info(f"[REDIS] Iniciando redis-server pelo bin: {path}")

    cmd = [
        path,
        "--port", str(REDIS_PORT),
        "--bind", "127.0.0.1",
        "--appendonly", "no",
        "--save", "",
    ]

    try:
        # subprocess sem shell, capturando pipes para log
        REDIS_PROC = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            close_fds=True,
        )
    except PermissionError as e:
        info("[REDIS] PermissionError ao iniciar binário - verifique chmod e se bin é executável: %s", e)
        return
    except FileNotFoundError as e:
        info("[REDIS] FileNotFoundError ao iniciar binário - caminho inválido: %s", e)
        return
    except Exception as e:
        info(f"[REDIS] Erro ao iniciar redis-server: {e}")
        exc()
        return

    info(f"[REDIS] PID = {REDIS_PROC.pid}")

    # inicia threads para drenar stdout/stderr e monitorar
    try:
        REDIS_MON_THREAD = _start_monitor_thread(REDIS_PROC)
    except Exception as e:
        info(f"[REDIS] Não consegui iniciar monitor de saída: {e}")

    # espera curto para validar startup
    if not wait_redis_ready(REDIS_HOST, REDIS_PORT, timeout=10):
        info("[REDIS] Falhou ao subir redis-server embutido.")
        # tenta ler último stderr (já sendo logado pelas threads)
        return

    info("[REDIS] redis-server embutido iniciado com sucesso")


###########################################################
# WebSocket <-> Redis proxy
###########################################################
async def ws_handler(ws: ServerConnection):
    try:
        peer = ws.remote_address
    except Exception:
        peer = None

    target = getattr(getattr(ws, "request", None), "target", "?")
    info(f"[WS] Nova conexão de {peer}, target={target}")

    # token (se configurado)
    if WS_AUTH_TOKEN:
        token = None
        req = getattr(ws, "request", None)
        if req and getattr(req, "query", None):
            token = req.query.get("token")
        else:
            if "?" in target:
                try:
                    qs = dict(p.split("=", 1) for p in target.split("?")[1].split("&"))
                    token = qs.get("token")
                except Exception:
                    token = None

        if token != WS_AUTH_TOKEN:
            info(f"[WS] Token inválido: {token}")
            try:
                await ws.send(b"-NOAUTH Invalid token\r\n")
            except Exception:
                pass
            await ws.close(code=4001)
            return

    # conecta ao redis (TCP)
    try:
        info(f"[WS] Conectando ao Redis {REDIS_HOST}:{REDIS_PORT} ssl={REDIS_SSL}")
        redis_reader, redis_writer = await asyncio.open_connection(
            REDIS_HOST, REDIS_PORT, ssl=REDIS_SSL or None
        )
        info("[WS] Conectado ao Redis backend")
    except Exception as e:
        msg = f"-ERR cannot connect to Redis backend {REDIS_HOST}:{REDIS_PORT}: {e}\r\n"
        info(f"[WS] {msg.strip()}")
        try:
            await ws.send(msg.encode())
        except Exception:
            pass
        await ws.close(code=1011)
        return

    async def ws_to_redis():
        try:
            async for msg in ws:
                if isinstance(msg, str):
                    data = msg.encode()
                else:
                    # already bytes or memoryview
                    data = bytes(msg)
                debug(f"[C→R] {len(data)} bytes")
                redis_writer.write(data)
                await redis_writer.drain()
        except Exception as e:
            debug(f"[C→R] exceção: {e}")
        finally:
            with suppress_close(redis_writer):
                try:
                    redis_writer.close()
                except Exception:
                    pass

    async def redis_to_ws():
        try:
            while True:
                data = await redis_reader.read(4096)
                if not data:
                    debug("[R→C] EOF")
                    break
                debug(f"[R→C] {len(data)} bytes")
                # enviar como binary frame
                await ws.send(data)
        except Exception as e:
            debug(f"[R→C] exceção: {e}")
        finally:
            try:
                await ws.close()
            except Exception:
                pass

    t1 = asyncio.create_task(ws_to_redis())
    t2 = asyncio.create_task(redis_to_ws())

    await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)

    info(f"[WS] Conexão encerrada com {peer}")

# small helper context manager to suppress close errors
class suppress_close:
    def __init__(self, obj):
        self.obj = obj
    def __enter__(self):
        return self.obj
    def __exit__(self, exc_type, exc, tb):
        try:
            if hasattr(self.obj, "wait_closed"):
                # asyncio stream writer has wait_closed
                pass
        except Exception:
            pass
        return False

###########################################################
# process_request -> responde HTTP GET / para healthcheck
###########################################################
async def process_request(path, request_headers):
    # Render fará GET / para confirmar que o serviço abriu HTTP.
    if path == "/" or path == "":
        body = b"OK"
        headers = [
            ("Content-Type", "text/plain"),
            ("Content-Length", str(len(body))),
        ]
        return 200, headers, body
    # Para outras rotas, não intercepta (retorna None para continuar upgrade)
    return None

###########################################################
# MAIN
###########################################################
async def main():
    info(f"Iniciando WS+HTTP em ws://{PROXY_HOST}:{PROXY_PORT} (health GET /)")
    info(f"Redis alvo: {REDIS_HOST}:{REDIS_PORT}")

    async with serve(ws_handler, PROXY_HOST, PROXY_PORT, process_request=process_request):
        info("Servidor pronto. Aguardando conexões…")
        await asyncio.Future()

if __name__ == "__main__":
    try:
        # start redis if necessary
        start_local_redis()

        # run main server (websocket + http health)
        asyncio.run(main())
    except KeyboardInterrupt:
        info("KeyboardInterrupt recebido, finalizando...")
    except Exception:
        exc("Erro no main")
    finally:
        if REDIS_PROC:
            try:
                info("[REDIS] Encerrando redis-server…")
                # tenta terminar graciosamente
                REDIS_PROC.terminate()
                try:
                    REDIS_PROC.wait(timeout=5)
                except Exception:
                    REDIS_PROC.kill()
                info("[REDIS] redis-server finalizado")
            except Exception:
                info("[REDIS] Erro ao encerrar redis-server")
