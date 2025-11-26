#!/usr/bin/env python3
import asyncio
import os
import traceback
import logging

from websockets.asyncio.server import serve, ServerConnection
from websockets.exceptions import ConnectionClosed

# ==========================
# LOG BÁSICO
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ws-redis-proxy")

# (se quiser ver log interno da lib websockets, descomenta)
# logging.getLogger("websockets").setLevel(logging.DEBUG)

# ==========================
# CONFIG
# ==========================
BACKEND_HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "6379"))

PROXY_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.getenv("PROXY_PORT", "5000"))


def log(msg: str) -> None:
    logger.info(msg)


async def ws_redis_proxy(ws: ServerConnection) -> None:
    """
    Para cada conexão WS:
      - conecta no Redis BACKEND_HOST:BACKEND_PORT
      - faz pipe WS <-> Redis (bytes crus, protocolo Redis RESP)
    """
    try:
        peer = ws.remote_address
    except Exception:
        peer = None

    # path novo: ws.request.path (não existe mais ws.path)
    try:
        path = getattr(ws, "request", None)
        path_str = path.path if path is not None else "?"
    except Exception:
        path_str = "?"

    log(f"Nova conexão WS de {peer}, path={path_str}")

    # 1) Tenta conectar no Redis backend
    try:
        log(f"Tentando conectar no Redis backend {BACKEND_HOST}:{BACKEND_PORT} ...")
        redis_reader, redis_writer = await asyncio.open_connection(
            BACKEND_HOST, BACKEND_PORT
        )
        log("Conectado no Redis backend com sucesso.")
    except Exception as e:
        log(f"[ERRO BACKEND] Falha ao conectar no Redis: {repr(e)}")
        traceback.print_exc()
        # Tenta enviar um erro legível pro cliente antes de fechar
        err_msg = (
            f"-ERR cannot connect to Redis backend {BACKEND_HOST}:{BACKEND_PORT}: {e}\r\n"
        )
        try:
            await ws.send(err_msg.encode("utf-8"))
        except Exception:
            pass
        try:
            await ws.close(code=1011, reason="backend connection failed")
        except Exception:
            pass
        return

    async def ws_to_redis() -> None:
        """
        Lê frames do WebSocket e envia para o Redis.
        """
        try:
            async for msg in ws:
                if isinstance(msg, str):
                    data = msg.encode("utf-8")
                    kind = "text"
                else:
                    data = msg
                    kind = "binary"

                log(f"WS→Redis {len(data)} bytes (tipo={kind})")
                redis_writer.write(data)
                await redis_writer.drain()
        except ConnectionClosed as e:
            log(
                f"ws_to_redis: WS fechado "
                f"code={e.code} reason={e.reason!r}"
            )
        except Exception as e:
            log(f"ws_to_redis: erro inesperado: {repr(e)}")
            traceback.print_exc()
        finally:
            try:
                redis_writer.close()
            except Exception:
                pass

    async def redis_to_ws() -> None:
        """
        Lê bytes do Redis e manda de volta pro WebSocket.
        """
        try:
            while True:
                data = await redis_reader.read(4096)
                if not data:
                    log("redis_to_ws: EOF do Redis (conexão fechada).")
                    break
                log(f"Redis→WS {len(data)} bytes")
                await ws.send(data)  # envia como frame binário
        except ConnectionClosed as e:
            log(
                f"redis_to_ws: WS fechado "
                f"code={e.code} reason={e.reason!r}"
            )
        except Exception as e:
            log(f"redis_to_ws: erro inesperado: {repr(e)}")
            traceback.print_exc()
        finally:
            try:
                await ws.close()
            except Exception:
                pass

    # Cria as duas tasks de pipe
    t1 = asyncio.create_task(ws_to_redis(), name="ws_to_redis")
    t2 = asyncio.create_task(redis_to_ws(), name="redis_to_ws")

    done, pending = await asyncio.wait(
        [t1, t2],
        return_when=asyncio.FIRST_COMPLETED,
    )

    log(f"Tasks finalizadas: done={len(done)}, pending={len(pending)}")
    for t in pending:
        t.cancel()
        try:
            await t
        except Exception:
            pass

    log(f"Conexão encerrada com {peer}")


async def main() -> None:
    log(f"Iniciando WebSocket Redis Proxy em ws://{PROXY_HOST}:{PROXY_PORT}")
    log(f"Redis backend alvo: {BACKEND_HOST}:{BACKEND_PORT}")

    async with serve(
        ws_redis_proxy,
        PROXY_HOST,
        PROXY_PORT,
        max_size=None,
        max_queue=None,
    ) as server:
        log("Servidor WS pronto. Aguardando conexões...")
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Encerrando por KeyboardInterrupt...")
