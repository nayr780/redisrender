#!/usr/bin/env python3
import asyncio
import os
import logging
import traceback
from urllib.parse import urlparse

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

# ==========================
# LOG
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ws-redis-proxy")

# silencia barulho de handshake do websockets (HEAD etc.)
logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.asyncio.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.http11").setLevel(logging.CRITICAL)


def info(msg: str) -> None:
    log.info(msg)


# ==========================
# CONFIG DO BACKEND REDIS
# ==========================

# Você pode usar:
#   BACKEND_URL = redis://default:senha@host:port/0
BACKEND_URL = os.getenv("BACKEND_URL")

# Ou então setar HOST/PORT/PASSWORD separados
BACKEND_HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "6379"))
BACKEND_PASSWORD = os.getenv("BACKEND_PASSWORD")  # ex: test1234

# Se BACKEND_URL estiver definido, faz o parse
if BACKEND_URL:
    u = urlparse(BACKEND_URL)
    if u.hostname:
        BACKEND_HOST = u.hostname
    if u.port:
        BACKEND_PORT = u.port
    if u.password:
        BACKEND_PASSWORD = u.password

# Porta de escuta do serviço (Render seta PORT)
PROXY_HOST = "0.0.0.0"
PROXY_PORT = int(os.getenv("PORT", "10000"))


# ==========================
# HANDLER WS
# ==========================
async def ws_redis_proxy(ws) -> None:
    """
    Para cada conexão WebSocket:
      - conecta no Redis BACKEND_HOST:BACKEND_PORT
      - se tiver BACKEND_PASSWORD, manda AUTH
      - faz pipe WS <-> Redis (bytes crus RESP2)
    """
    try:
        peer = ws.remote_address
    except Exception:
        peer = None

    info(f"[WS] Nova conexão de {peer}")

    # 1) Conecta no Redis backend
    try:
        info(f"[WS] Conectando no Redis {BACKEND_HOST}:{BACKEND_PORT} ...")
        redis_reader, redis_writer = await asyncio.open_connection(
            BACKEND_HOST, BACKEND_PORT
        )
        info("[WS] Conectado no Redis backend.")
    except Exception as e:
        info(f"[WS] ERRO ao conectar no Redis: {repr(e)}")
        traceback.print_exc()
        msg = (
            f"-ERR cannot connect to Redis backend "
            f"{BACKEND_HOST}:{BACKEND_PORT}: {e}\r\n"
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

    # 2) AUTH opcional no backend
    if BACKEND_PASSWORD:
        try:
            pwd = BACKEND_PASSWORD
            auth_cmd = (
                f"*2\r\n"
                f"$4\r\nAUTH\r\n"
                f"${len(pwd)}\r\n{pwd}\r\n"
            ).encode("utf-8")
            info("[WS] Enviando AUTH para o Redis backend...")
            redis_writer.write(auth_cmd)
            await redis_writer.drain()

            auth_resp = await redis_reader.read(1024)
            info(f"[WS] Resposta AUTH: {auth_resp!r}")
        except Exception as e:
            info(f"[WS] ERRO durante AUTH no backend: {repr(e)}")
            traceback.print_exc()
            try:
                await ws.send(b"-ERR backend AUTH failed\r\n")
            except Exception:
                pass
            try:
                await ws.close(code=1011, reason="backend auth failed")
            except Exception:
                pass
            try:
                redis_writer.close()
            except Exception:
                pass
            return

    # 3) Pipes WS <-> Redis
    async def ws_to_redis():
        try:
            async for msg in ws:
                if isinstance(msg, str):
                    data = msg.encode("utf-8")
                    kind = "text"
                else:
                    data = msg
                    kind = "binary"
                info(f"[WS→Redis] {len(data)} bytes ({kind})")
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


# ==========================
# MAIN
# ==========================
async def main() -> None:
    info(f"Iniciando Redis WS Proxy em ws://{PROXY_HOST}:{PROXY_PORT} (Render → wss://redisrender.onrender.com)")
    info(f"Redis alvo: {BACKEND_HOST}:{BACKEND_PORT}")
    async with serve(
        ws_redis_proxy,
        PROXY_HOST,
        PROXY_PORT,
        max_size=None,
        max_queue=None,
    ):
        info("Servidor WS pronto. Aguardando conexões...")
        await asyncio.Future()  # roda pra sempre


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        info("Encerrando por KeyboardInterrupt...")
