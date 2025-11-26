#!/usr/bin/env python3
import asyncio
import os
import logging
import traceback

from websockets.asyncio.server import serve, ServerConnection
from websockets.exceptions import ConnectionClosed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("redis-ws-proxy")

# CONFIG -----------------------------
BACKEND_HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "6379"))
PROXY_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.getenv("PROXY_PORT", "5000"))

# ------------------------------------
# SERVIDOR HTTP PARA HEALTHCHECK
# ------------------------------------
async def http_handler(reader, writer):
    try:
        request = await reader.readline()
        if not request:
            writer.close()
            return

        method, path, _ = request.decode().split(" ")
        log.info(f"[HTTP] {method} {path}")

        # Healthcheck Render
        if method in ("GET", "HEAD"):
            body = b"PONG HTTP\n"
            headers = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: %d\r\n"
                b"Connection: close\r\n\r\n" % len(body)
            )

            writer.write(headers)
            if method == "GET":
                writer.write(body)

        else:
            writer.write(b"HTTP/1.1 405 METHOD NOT ALLOWED\r\n\r\n")

    except Exception as e:
        log.error(f"[HTTP] erro: {e}")
    finally:
        try:
            await writer.drain()
        except Exception:
            pass
        writer.close()


# ------------------------------------
# WEBSOCKET /ws  → Redis (TCP)
# ------------------------------------
async def ws_proxy(ws: ServerConnection):
    peer = ws.remote_address

    try:
        path = ws.request.path
    except:
        path = "?"

    log.info(f"[WS] Nova conexão de {peer}, path={path}")

    # Prevencion: só aceitar /ws
    if path != "/ws":
        log.warning(f"[WS] conexão rejeitada em {path}")
        await ws.close(code=4000, reason="use /ws")
        return

    # Conectar ao Redis
    try:
        redis_r, redis_w = await asyncio.open_connection(BACKEND_HOST, BACKEND_PORT)
        log.info("[WS] Conectado ao Redis")
    except Exception as e:
        log.error(f"[WS] ERRO Redis: {e}")
        await ws.send(f"-ERR Redis unavailable {e}".encode())
        await ws.close(code=1011, reason="redis error")
        return

    async def ws_to_redis():
        try:
            async for msg in ws:
                data = msg if isinstance(msg, bytes) else msg.encode()
                log.info(f"[WS→Redis] {len(data)} bytes")
                redis_w.write(data)
                await redis_w.drain()
        except Exception as e:
            log.error(f"[WS→Redis] erro: {e}")
        finally:
            try: redis_w.close()
            except: pass

    async def redis_to_ws():
        try:
            while True:
                data = await redis_r.read(4096)
                if not data:
                    log.info("[Redis] EOF")
                    break
                log.info(f"[Redis→WS] {len(data)} bytes")
                await ws.send(data)
        except Exception as e:
            log.error(f"[Redis→WS] erro: {e}")
        finally:
            try: await ws.close()
            except: pass

    t1 = asyncio.create_task(ws_to_redis())
    t2 = asyncio.create_task(redis_to_ws())

    await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
    log.info(f"[WS] Conexão encerrada: {peer}")


# ------------------------------------
# MAIN SERVER
# ------------------------------------
async def main():
    log.info(f"Iniciando Redis WS Proxy em ws://{PROXY_HOST}:{PROXY_PORT}/ws")
    log.info(f"Redis alvo: {BACKEND_HOST}:{BACKEND_PORT}")

    # HTTP server para Render healthcheck
    asyncio.create_task(asyncio.start_server(http_handler, PROXY_HOST, PROXY_PORT))

    # WebSocket somente em /ws
    async with serve(ws_proxy, PROXY_HOST, PROXY_PORT, max_size=None, max_queue=None):
        log.info("Servidor pronto.")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
