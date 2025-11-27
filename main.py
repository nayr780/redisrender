#!/usr/bin/env python3
import asyncio
import os
import websockets

WS_URL = os.getenv("REDIS_WS_URL", "ws://127.0.0.1:5000")
WS_AUTH_TOKEN = os.getenv("WS_AUTH_TOKEN")  # se setar no servidor, setar aqui também

def encode_resp(*parts: str) -> bytes:
    """
    Codifica comandos simples Redis em RESP2.
    Ex: encode_resp("SET", "foo", "bar")
    """
    out = f"*{len(parts)}\r\n"
    for p in parts:
        pb = p.encode("utf-8")
        out += f"${len(pb)}\r\n"
        out += p + "\r\n"
    return out.encode("utf-8")


async def send_cmd(ws, *parts: str) -> bytes:
    raw = encode_resp(*parts)
    print(f"\n[CLIENT] >> CMD {parts}")
    print(f"[CLIENT] >> RAW ({len(raw)} bytes): {raw[:60]!r}...")
    await ws.send(raw)
    resp = await ws.recv()
    if isinstance(resp, str):
        resp_b = resp.encode("utf-8")
    else:
        resp_b = resp
    print(f"[CLIENT] << RAW BYTES ({len(resp_b)}): {resp_b[:60]!r}...")
    print("[CLIENT] << DECODED:\n", resp_b.decode("utf-8", "replace"))
    return resp_b


async def main():
    print(f"🔌 Conectando ao WebSocket: {WS_URL}")
    try:
        async with websockets.connect(WS_URL, max_size=None) as ws:
            print("🟢 Conectado!")

            # 1) Token opcional
            if WS_AUTH_TOKEN:
                print("\n== AUTH TOKEN ==\n")
                await ws.send(WS_AUTH_TOKEN)
                print("[CLIENT] >> token enviado")

            # 2) PING
            print("\n== PING ==\n")
            await send_cmd(ws, "PING")

            # 3) Strings
            print("\n== STRINGS ==\n")
            await send_cmd(ws, "SET", "user", "ryan")
            await send_cmd(ws, "GET", "user")

            # 4) Counter
            print("\n== COUNTER (INCR/INCRBY) ==\n")
            await send_cmd(ws, "SET", "counter", "0")
            await send_cmd(ws, "INCR", "counter")
            await send_cmd(ws, "INCRBY", "counter", "10")
            await send_cmd(ws, "GET", "counter")

            # 5) List
            print("\n== LIST (LPUSH/LRANGE) ==\n")
            await send_cmd(ws, "DEL", "queue")
            await send_cmd(ws, "LPUSH", "queue", "a", "b", "c")
            await send_cmd(ws, "LRANGE", "queue", "0", "-1")

            # 6) Hash
            print("\n== HASH (HSET/HGETALL) ==\n")
            await send_cmd(ws, "HSET", "profile", "name", "Ryan", "age", "22")
            await send_cmd(ws, "HGETALL", "profile")

            # 7) Set
            print("\n== SET (SADD/SMEMBERS) ==\n")
            await send_cmd(ws, "SADD", "tags", "python", "redis", "ws")
            await send_cmd(ws, "SMEMBERS", "tags")

            # 8) Streams
            print("\n== STREAMS (XADD/XRANGE) ==\n")
            await send_cmd(ws, "XADD", "mystream", "*", "msg", "hello")
            await send_cmd(ws, "XRANGE", "mystream", "-", "+")

            # 9) KEYS
            print("\n== KEYS * ==\n")
            await send_cmd(ws, "KEYS", "*")

            # 10) DEL
            print("\n== DEL ==\n")
            await send_cmd(ws, "DEL", "user")
            await send_cmd(ws, "GET", "user")

            print("\n✅ TESTES FINALIZADOS")
    except websockets.ConnectionClosed as e:
        print(f"\n[CLIENT] 🔴 ConnectionClosed code={e.code} reason={e.reason!r}")
    except Exception as e:
        print(f"\n[CLIENT] 🔴 Erro inesperado: {repr(e)}")


if __name__ == "__main__":
    asyncio.run(main())
