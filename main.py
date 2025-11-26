#!/usr/bin/env python3
import asyncio
import os
import logging
import traceback

from websockets.asyncio.server import serve, ServerConnection
from websockets.exceptions import ConnectionClosed

# ==========================
# LOG BÁSICO
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ws-redis-proxy")

# Silenciar barulho do websockets (erros de HEAD / handshake)
logging.getLogger("websockets").setLevel(logging.CRITICAL)
logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.asyncio.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.http11").setLevel(logging.CRITICAL)

# ==========================
# CONFIG
# ==========================

# Redis verdadeiro (Render: seta BACKEND_HOST/PORT pro seu Redis)
BACKEND_HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "6379"))

# Porta de escuta do serviço
# No Render, normalmente vem PORT no env
PROXY_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.getenv("PORT", os.getenv("PROXY_PORT", "5000")))

# Token opcional: ws://host/ws?token=SEU_TOKEN
WS_AUTH_TOKEN = os.getenv("WS_AUTH_TOKEN")  # se None, sem auth


def info(msg: str) -> None:
    log.info(msg)


async def ws_redis_proxy(ws: ServerConnection) -> None:
    """
    Para cada conexão WS:
      - (opcional) valida token via querystring
      - conecta no Redis BACKEND_HOST:BACKEND_PORT
      - faz pipe WS <-> Redis (bytes crus, protocolo Redis RESP)
    """
    # Algumas infos úteis
    try:
        peer = ws.remote_address
    except Exception:
        peer = None

    try:
        req = ws.request  # websockets 15.x
        path = req.path
        raw_query = req.query  # dict-like
    except Exception:
        path = "?"
        raw_query = {}

    info(f"[WS] Nova conexão de {peer}, path={path}")

    # ------------------------------------------------------------------
    # 1) Só aceitar em /ws
    # ------------------------------------------------------------------
    if path != "/ws":
        info(f"[WS] Rejeitando conexão em path {path}, esperado /ws")
        try:
            await ws.close(code=4000, reason="use /ws")
        except Exception:
            pass
        return

    # ------------------------------------------------------------------
    # 2) Auth por token (se configurado)
    # ------------------------------------------------------------------
    if WS_AUTH_TOKEN:
        # raw_query pode ser um dict ou Mapping[str, str]
        token = None
        try:
            token = raw_query.get("token")
        except Exception:
            # fallback manual se não for dict
            # req.target geralmente vem como "/ws?token=xxx"
            try:
                target = req.target  # tipo: "/ws?token=xxx"
                if "?" in target:
                    qs = target.split("?", 1)[1]
                    params = dict(
                        p.split("=", 1)
                        for p in qs.split("&")
                        if "=" in p
                    )
                    token = params.get("token")
            except Exception:
                token = None

        if token != WS_AUTH_TOKEN:
            info(f"[WS] Token inválido de {peer}")
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

    # ------------------------------------------------------------------
    # 3) Conecta no Redis backend
    # ------------------------------------------------------------------
    try:
        info(f"[WS] Conectando no Redis {BACKEND_HOST}:{BACKEND_PORT} ...")
        redis_reader, redis_writer = await asyncio.open_connection(
            BACKEND_HOST, BACKEND_PORT
        )
        info("[WS] Conectado no Redis backend.")
    except Exception as e:
        info(f"[WS] ERRO ao conectar no Redis: {repr(e)}")
        traceback.print_exc()
        msg = f"-ERR cannot connect to Redis backend {BACKEND_HOST}:{BACKEND_PORT}: {e}\r\n"
        try:
            await ws.send(msg.encode("utf-8"))
        except Exception:
            pass
        try:
            await ws.close(code=1011, reason="backend connection failed")
        except Exception:
            pass
        return

    # ------------------------------------------------------------------
    # 4) PIPES: WS → Redis e Redis → WS
    # ------------------------------------------------------------------

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
    info(f"Iniciando Redis WS Proxy em ws://{PROXY_HOST}:{PROXY_PORT}/ws")
    info(f"Redis alvo: {BACKEND_HOST}:{BACKEND_PORT}")

    # Só UM servidor na porta: o próprio websockets
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
