#!/usr/bin/env python3
import asyncio
import os
import logging
import traceback

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

# ==========================
# LOG BÁSICO
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ws-redis-proxy")

# Silenciar barulho interno do websockets (HEAD / handshake, etc.)
logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.asyncio.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.http11").setLevel(logging.CRITICAL)

# ==========================
# CONFIG
# ==========================

# Redis REAL (no Render, aponte BACKEND_HOST/BACKEND_PORT pro Redis de verdade)
BACKEND_HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "6379"))

# Porta em que o serviço escuta.
#  - local: usa PROXY_PORT (default 5000)
#  - Render: ele seta PORT, então usamos PORT primeiro.
PROXY_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.getenv("PORT", os.getenv("PROXY_PORT", "5000")))

# Token opcional:
#  - Se definido, o PRIMEIRO frame do WebSocket tem que ser exatamente esse token (texto).
#  - Depois disso, o tráfego vira Redis RESP normal.
WS_AUTH_TOKEN = os.getenv("WS_AUTH_TOKEN")  # ex: "minha_senha_ws"


def info(msg: str) -> None:
    log.info(msg)


async def ws_redis_proxy(ws) -> None:
    """
    Para cada conexão WebSocket:
      - (opcional) valida um token no primeiro frame
      - conecta no Redis BACKEND_HOST:BACKEND_PORT
      - faz pipe WS <-> Redis (bytes crus, protocolo RESP)
    """
    try:
        peer = ws.remote_address
    except Exception:
        peer = None

    info(f"[WS] Nova conexão de {peer}")

    # -----------------------------------------------------------
    # 1) Auth simples via primeiro frame (se WS_AUTH_TOKEN estiver setado)
    # -----------------------------------------------------------
    if WS_AUTH_TOKEN:
        try:
            init_msg = await ws.recv()
        except ConnectionClosed as e:
            info(
                f"[AUTH] WS fechado antes do token: "
                f"code={e.code} reason={e.reason!r}"
            )
            return
        except Exception as e:
            info(f"[AUTH] erro ao ler token inicial: {repr(e)}")
            traceback.print_exc()
            return

        if isinstance(init_msg, bytes):
            token = init_msg.decode("utf-8", "replace")
        else:
            token = init_msg

        if token != WS_AUTH_TOKEN:
            info(f"[AUTH] Token inválido de {peer}")
            try:
                await ws.send(b"-NOAUTH invalid token\r\n")
            except Exception:
                pass
            try:
                await ws.close(code=4001, reason="invalid token")
            except Exception:
                pass
            return

        info(f"[AUTH] Token OK para {peer}")

    # -----------------------------------------------------------
    # 2) Conecta no Redis backend
    # -----------------------------------------------------------
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

    # -----------------------------------------------------------
    # 3) Pipes WS → Redis e Redis → WS
    # -----------------------------------------------------------

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
            info(
                f"[WS→Redis] WS fechado "
                f"code={e.code} reason={e.reason!r}"
            )
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
            info(
                f"[Redis→WS] WS fechado "
                f"code={e.code} reason={e.reason!r}"
            )
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


async def main() -> None:
    info(f"Iniciando Redis WS Proxy em ws://{PROXY_HOST}:{PROXY_PORT}/ (qualquer path)")
    info(f"Redis alvo: {BACKEND_HOST}:{BACKEND_PORT}")

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
        asyncio.run(main())
    except KeyboardInterrupt:
        info("Encerrando por KeyboardInterrupt...")
