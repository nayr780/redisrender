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

# ======================================
# LOG BÁSICO
# ======================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ws-redis-proxy")

# Silenciar barulho do websockets (HEAD/healthcheck do Render)
logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.asyncio.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.http11").setLevel(logging.CRITICAL)

def info(msg: str) -> None:
    log.info(msg)


# ======================================
# AMBIENTE
# ======================================
IS_RENDER = bool(os.getenv("RENDER") or os.getenv("RENDER_SERVICE_ID"))
IS_LINUX = sys.platform.startswith("linux")

# Se quiser forçar ou desativar Redis embutido:
# EMBED_LOCAL_REDIS=1 => sempre tenta subir redis local
# EMBED_LOCAL_REDIS=0 => nunca sobe redis local (usa REDIS_URL / REDIS_HOST normal)
EMBED_LOCAL_REDIS_ENV = os.getenv("EMBED_LOCAL_REDIS", "").strip().lower()
EMBED_LOCAL_REDIS = EMBED_LOCAL_REDIS_ENV in ("1", "true", "yes")

# ======================================
# CONFIG – REDIS BACKEND (pode ser externo)
# ======================================

RAW_REDIS_URL = os.getenv("REDIS_URL")  # ex: redis://host:6379 ou rediss://...

if RAW_REDIS_URL:
    u = urlparse(RAW_REDIS_URL)
    REDIS_HOST = u.hostname or "127.0.0.1"
    REDIS_PORT = u.port or 6379
    REDIS_SSL = u.scheme in ("rediss", "redis+ssl")
else:
    # default: localhost:6379 sem SSL
    REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_SSL = os.getenv("REDIS_SSL", "").lower() in ("1", "true", "yes")

# Porta que o redis LOCAL vai usar (se for embutido)
LOCAL_REDIS_PORT = int(os.getenv("LOCAL_REDIS_PORT", str(REDIS_PORT)))

# ======================================
# CONFIG – WEBSOCKET
# ======================================

PROXY_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.getenv("PORT", os.getenv("PROXY_PORT", "10000")))

# Token opcional para proteger o WS (wss://host/?token=XXX)
WS_AUTH_TOKEN = os.getenv("WS_AUTH_TOKEN")  # se None, qualquer um conecta

# Para matar o redis local ao encerrar
REDIS_PROC = None


# ======================================
# FUNÇÕES PARA SUBIR REDIS LOCAL
# ======================================

def _try_install_redis_via_apt() -> bool:
    """
    Tenta instalar redis-server via apt-get (só Linux/Render).
    """
    info("[REDIS-LOCAL] Tentando instalar redis-server via apt-get...")
    try:
        subprocess.check_call(["apt-get", "update"])
        subprocess.check_call(["apt-get", "install", "-y", "redis-server"])
        info("[REDIS-LOCAL] redis-server instalado com sucesso via apt-get.")
        return True
    except Exception as e:
        info(f"[REDIS-LOCAL] ERRO ao instalar redis-server: {repr(e)}")
        traceback.print_exc()
        return False


def _wait_redis_ready(host: str, port: int, timeout_sec: int = 10) -> bool:
    """
    Tenta conectar TCP em host:port até timeout.
    Só pra garantir que o Redis subiu.
    """
    info(f"[REDIS-LOCAL] Aguardando Redis subir em {host}:{port} (até {timeout_sec}s)...")
    deadline = time.time() + timeout_sec
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            with socket.create_connection((host, port), timeout=0.5):
                info(f"[REDIS-LOCAL] Redis respondeu na tentativa {attempt}.")
                return True
        except OSError:
            time.sleep(0.3)
    info("[REDIS-LOCAL] Redis não respondeu dentro do tempo. Vou seguir mesmo assim.")
    return False


def start_embedded_redis_if_needed() -> None:
    """
    Se estivermos no Render e NÃO tiver REDIS_URL externo, tentamos subir
    um redis-server local em 127.0.0.1:LOCAL_REDIS_PORT.
    """
    global REDIS_HOST, REDIS_PORT, REDIS_SSL, REDIS_PROC

    # Decidir se vamos embutir Redis ou não
    use_embedded = EMBED_LOCAL_REDIS or (IS_RENDER and not RAW_REDIS_URL)
    if not use_embedded:
        info("[REDIS-LOCAL] Modo embutido DESATIVADO "
             "(usando REDIS_URL/REDIS_HOST configurados).")
        return

    info("[REDIS-LOCAL] Modo embutido ATIVADO.")

    # Sempre usa localhost sem SSL no modo embutido
    REDIS_HOST = "127.0.0.1"
    REDIS_PORT = LOCAL_REDIS_PORT
    REDIS_SSL = False

    # Já existe um redis-server acessível?
    info(f"[REDIS-LOCAL] Verificando se já existe Redis em {REDIS_HOST}:{REDIS_PORT}...")
    if _wait_redis_ready(REDIS_HOST, REDIS_PORT, timeout_sec=1):
        info("[REDIS-LOCAL] Já existe um Redis rodando nessa porta. "
             "Não vou subir outro.")
        return

    # Procurar binário redis-server
    path = shutil.which("redis-server")
    if not path:
        info("[REDIS-LOCAL] redis-server NÃO encontrado no PATH.")
        if IS_LINUX:
            info("[REDIS-LOCAL] Ambiente Linux detectado; vou tentar instalar.")
            if not _try_install_redis_via_apt():
                info("[REDIS-LOCAL] Não consegui instalar redis-server. "
                     "Você precisará configurá-lo manualmente.")
                return
            path = shutil.which("redis-server")
            if not path:
                info("[REDIS-LOCAL] Mesmo após apt-get, redis-server não apareceu no PATH.")
                return
        else:
            info("[REDIS-LOCAL] Ambiente não é Linux; não vou tentar apt-get. "
                 "Suba Redis manualmente.")
            return

    info(f"[REDIS-LOCAL] redis-server encontrado em: {path}")

    # Montar comando
    cmd = [
        path,
        "--port", str(REDIS_PORT),
        "--bind", "127.0.0.1",
        "--save", "",           # sem snapshots (pra ser mais leve)
        "--appendonly", "no",   # sem AOF
    ]
    info(f"[REDIS-LOCAL] Iniciando redis-server: {' '.join(cmd)}")

    try:
        # NÃO daemonize: queremos que ele seja filho do processo Python,
        # aí o Render mata tudo junto se o serviço cair.
        REDIS_PROC = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        info(f"[REDIS-LOCAL] redis-server PID={REDIS_PROC.pid}")
    except Exception as e:
        info(f"[REDIS-LOCAL] ERRO ao iniciar redis-server: {repr(e)}")
        traceback.print_exc()
        return

    # Esperar alguns segundos pro Redis ficar pronto
    _wait_redis_ready(REDIS_HOST, REDIS_PORT, timeout_sec=10)


# ======================================
# WS HANDLER – PROXY WS <-> REDIS
# ======================================

async def ws_redis_proxy(ws: ServerConnection) -> None:
    """
    Para cada conexão WS:
      - (opcional) valida token via querystring ?token=...
      - conecta no Redis REDIS_HOST:REDIS_PORT
      - faz pipe WS <-> Redis (bytes crus, protocolo RESP)
    """
    try:
        peer = ws.remote_address
    except Exception:
        peer = None

    # Pega request/target (websockets 15.x)
    try:
        req = ws.request
        target = getattr(req, "target", "?")  # ex: "/?token=abc"
    except Exception:
        req = None
        target = "?"

    info(f"[WS] Nova conexão de {peer}, target={target!r}")

    # ---------------------------------
    # 1) Auth por token (se configurado)
    # ---------------------------------
    if WS_AUTH_TOKEN:
        token = None
        try:
            # req.query normalmente é dict-like {'token': 'xxx'}
            token = req.query.get("token")
        except Exception:
            # fallback burro: parse manual
            if "?" in target:
                qs = target.split("?", 1)[1]
                params = dict(
                    p.split("=", 1)
                    for p in qs.split("&")
                    if "=" in p
                )
                token = params.get("token")

        if token != WS_AUTH_TOKEN:
            info(f"[WS] Token inválido de {peer}: {token!r}")
            try:
                await ws.send(b"-NOAUTH invalid token\r\n")
            except Exception:
                pass
            try:
                await ws.close(code=4001, reason="invalid token")
            except Exception:
                pass
            return

        info(f"[WS] Token OK para {peer}")

    # ---------------------------------
    # 2) Conecta no Redis backend
    # ---------------------------------
    try:
        info(f"[WS] Conectando no Redis {REDIS_HOST}:{REDIS_PORT} ssl={REDIS_SSL} ...")
        redis_reader, redis_writer = await asyncio.open_connection(
            REDIS_HOST,
            REDIS_PORT,
            ssl=REDIS_SSL or None,
        )
        info("[WS] Conectado no Redis backend.")
    except Exception as e:
        info(f"[WS] ERRO ao conectar no Redis: {repr(e)}")
        traceback.print_exc()
        msg = (
            f"-ERR cannot connect to Redis backend "
            f"{REDIS_HOST}:{REDIS_PORT}: {e}\r\n"
        )
        try:
            await ws.send(msg.encode("utf-8"))
        except Exception:
            pass
        try:
            await ws.close(code=1011, reason="backend connection failed")
        except Exception:
            pass
        return

    # ---------------------------------
    # 3) Pipe WS → Redis
    # ---------------------------------
    async def ws_to_redis():
        try:
            async for msg in ws:
                if isinstance(msg, str):
                    data = msg.encode("utf-8")
                    kind = "text"
                else:
                    data = msg
                    kind = "binary"

                info(f"[WS→Redis] {len(data)} bytes (tipo={kind})")
                redis_writer.write(data)
                await redis_writer.drain()
        except ConnectionClosed as e:
            info(f"[WS→Redis] WS fechado code={e.code} reason={e.reason!r}")
        except Exception as e:
            info(f"[WS→Redis] erro inesperado: {repr(e)}")
            traceback.print_exc()
        finally:
            try:
                redis_writer.close()
            except Exception:
                pass

    # ---------------------------------
    # 4) Pipe Redis → WS
    # ---------------------------------
    async def redis_to_ws():
        try:
            while True:
                data = await redis_reader.read(4096)
                if not data:
                    info("[Redis→WS] EOF do Redis (conexão fechada).")
                    break
                info(f"[Redis→WS] {len(data)} bytes")
                await ws.send(data)
        except ConnectionClosed as e:
            info(f"[Redis→WS] WS fechado code={e.code} reason={e.reason!r}")
        except Exception as e:
            info(f"[Redis→WS] erro inesperado: {repr(e)}")
            traceback.print_exc()
        finally:
            try:
                await ws.close()
            except Exception:
                pass

    t1 = asyncio.create_task(ws_to_redis(), name="ws_to_redis")
    t2 = asyncio.create_task(redis_to_ws(), name="redis_to_ws")

    done, pending = await asyncio.wait(
        [t1, t2],
        return_when=asyncio.FIRST_COMPLETED,
    )

    info(f"[WS] Tasks finalizadas: done={len(done)}, pending={len(pending)}")
    for t in pending:
        t.cancel()
        try:
            await t
        except Exception:
            pass

    info(f"[WS] Conexão encerrada com {peer}")


# ======================================
# MAIN
# ======================================

async def main() -> None:
    info(
        f"Iniciando Redis WS Proxy em ws://{PROXY_HOST}:{PROXY_PORT} "
        f"(Render → wss://redisrender.onrender.com)"
    )
    info(f"Redis alvo: {REDIS_HOST}:{REDIS_PORT} ssl={REDIS_SSL}")

    async with serve(
        ws_redis_proxy,
        PROXY_HOST,
        PROXY_PORT,
        max_size=None,
        max_queue=None,
    ):
        info("Servidor WS pronto. Aguardando conexões...")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        # 1) Sobe Redis local se preciso
        start_embedded_redis_if_needed()

        # 2) Sobe o proxy WS
        asyncio.run(main())
    except KeyboardInterrupt:
        info("Encerrando por KeyboardInterrupt...")
    finally:
        # 3) Mata o redis local se a gente tiver iniciado
        global REDIS_PROC
        if REDIS_PROC is not None:
            try:
                info(f"[REDIS-LOCAL] Encerrando redis-server PID={REDIS_PROC.pid}...")
                REDIS_PROC.terminate()
                try:
                    REDIS_PROC.wait(timeout=5)
                except Exception:
                    pass
            except Exception:
                pass
