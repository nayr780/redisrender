#!/usr/bin/env python3
import asyncio
import os
import logging
import traceback
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

# ======================================
# CONFIG – REDIS BACKEND
# ======================================

RAW_REDIS_URL = os.getenv("REDIS_URL")  # ex: redis://host:6379 ou rediss://...
if RAW_REDIS_URL:
    u = urlparse(RAW_REDIS_URL)
    REDIS_HOST = u.hostname or "127.0.0.1"
    REDIS_PORT = u.port or 6379
    REDIS_SSL = u.scheme in ("rediss", "redis+ssl")
else:
    REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_SSL = os.getenv("REDIS_SSL", "").lower() in ("1", "true", "yes")

# ❗ IMPORTANTE: esse proxy NÃO faz AUTH automático.
# Se seu Redis tiver senha, o CLIENTE (worker, script, etc.)
# tem que mandar um comando AUTH via WS como faria num TCP normal.

# ======================================
# CONFIG – WEBSOCKET
# ======================================

PROXY_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
# No Render a porta vem em PORT
PROXY_PORT = int(os.getenv("PORT", os.getenv("PROXY_PORT", "10000")))

# Token opcional para proteger o WS (ws(s)://.../?token=XXX)
WS_AUTH_TOKEN = os.getenv("WS_AUTH_TOKEN")  # se None, qualquer um conecta


def info(msg: str) -> None:
    log.info(msg)


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
        asyncio.run(main())
    except KeyboardInterrupt:
        info("Encerrando por KeyboardInterrupt...")
