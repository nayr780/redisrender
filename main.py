#!/usr/bin/env python3
import asyncio
import os
from contextlib import suppress

# ------------------------------
# Configuração via variáveis de ambiente
# ------------------------------

BACKEND_HOST = os.getenv("BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "6379"))

PROXY_HOST = os.getenv("PROXY_HOST", "0.0.0.0")
PROXY_PORT = int(os.getenv("PROXY_PORT", "5000"))

# Senha necessária para passar pelo proxy (Redis AUTH)
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

# Token para proteger o endpoint HTTP (se não setar, HTTP fica aberto)
HTTP_AUTH_TOKEN = os.getenv("HTTP_AUTH_TOKEN")

# Se "1", "true" ou "yes", repassa o AUTH para o Redis backend
FORWARD_AUTH_TO_BACKEND = os.getenv("FORWARD_AUTH_TO_BACKEND", "").lower() in {
    "1", "true", "yes"
}


def log(msg):
    print(f"[DEBUG] {msg}", flush=True)


# ------------------------------
# Funções auxiliares de RESP (Redis protocolo)
# ------------------------------

class AuthError(Exception):
    pass


async def read_exact(reader: asyncio.StreamReader, n: int) -> bytes:
    """Lê exatamente n bytes ou lança AuthError."""
    data = await reader.readexactly(n)
    if len(data) != n:
        raise AuthError("EOF inesperado ao ler dados")
    return data


async def read_line(reader: asyncio.StreamReader) -> bytes:
    """Lê uma linha terminada em \\r\\n."""
    line = await reader.readline()
    if not line:
        raise AuthError("EOF ao ler linha")
    if not line.endswith(b"\r\n"):
        raise AuthError("Linha sem terminador CRLF")
    return line


async def read_redis_auth_command(
    reader: asyncio.StreamReader,
    first_byte: bytes,
) -> tuple[str, bytes]:
    """
    Lê e valida o primeiro comando Redis, esperando que seja:
        *2\r\n
        $4\r\n
        AUTH\r\n
        $len\r\n
        senha\r\n

    Retorna (senha, bytes_crus_do_comando).
    Lança AuthError se algo não bater.
    """
    raw = bytearray()
    raw.extend(first_byte)

    # Espera array RESP: "*<num>\r\n"
    if first_byte != b"*":
        raise AuthError("Primeiro byte não é '*' (array RESP)")

    line = await read_line(reader)
    raw.extend(line)
    try:
        num_items = int(line.strip())
    except ValueError:
        raise AuthError("Não foi possível parsear número de elementos da array RESP")

    if num_items < 2:
        raise AuthError("Array RESP tem menos de 2 elementos para AUTH")

    items: list[str] = []

    for i in range(num_items):
        # Espera bulk string: $<len>\r\n<data>\r\n
        type_byte = await read_exact(reader, 1)
        raw.extend(type_byte)

        if type_byte != b"$":
            raise AuthError("Esperado bulk string ($) na posição %d" % i)

        len_line = await read_line(reader)
        raw.extend(len_line)
        try:
            blen = int(len_line.strip())
        except ValueError:
            raise AuthError("Comprimento de bulk string inválido na posição %d" % i)

        if blen < 0:
            # NULL bulk string (não faz sentido aqui para AUTH)
            items.append("")
            continue

        data = await read_exact(reader, blen)
        raw.extend(data)

        crlf = await read_exact(reader, 2)
        raw.extend(crlf)

        items.append(data.decode("utf-8", "replace"))

    if not items:
        raise AuthError("Comando vazio")

    cmd = items[0].upper()
    if cmd != "AUTH":
        raise AuthError("Primeiro comando não é AUTH")

    if len(items) < 2:
        raise AuthError("AUTH sem senha")

    password = items[1]
    return password, bytes(raw)


# ------------------------------
# Pipe genérico (proxy de bytes)
# ------------------------------

async def pipe(reader, writer, label):
    """Pipe full duplex: copia bytes sem interpretar."""
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                log(f"{label}: EOF")
                break
            log(f"{label}: {len(data)} bytes")
            writer.write(data)
            await writer.drain()
    except Exception as e:
        log(f"{label}: erro {e}")
    finally:
        with suppress(Exception):
            writer.close()


# ------------------------------
# Handler Redis com autenticação
# ------------------------------

async def handle_redis(reader, writer, first_byte: bytes):
    peer = writer.get_extra_info("peername")
    log(f"Nova conexão Redis de {peer}")

    # Se não tiver PROXY_PASSWORD, funciona como um proxy "aberto"
    if not PROXY_PASSWORD:
        log("ATENÇÃO: PROXY_PASSWORD não definido, rodando SEM autenticação!")
        await open_and_pipe_redis(reader, writer, first_byte)
        return

    try:
        client_password, auth_raw = await read_redis_auth_command(reader, first_byte)
    except AuthError as e:
        log(f"Falha ao ler comando AUTH: {e}")
        with suppress(Exception):
            writer.write(b"-NOAUTH Authentication required\r\n")
            writer.close()
        return
    except Exception as e:
        log(f"Erro inesperado ao analisar AUTH: {e}")
        with suppress(Exception):
            writer.write(b"-ERR Proxy internal error\r\n")
            writer.close()
        return

    if client_password != PROXY_PASSWORD:
        log(f"Senha incorreta de {peer}")
        with suppress(Exception):
            writer.write(b"-WRONGPASS invalid password\r\n")
            writer.close()
        return

    log(f"Autenticação bem-sucedida de {peer}")

    # Conecta no Redis backend
    try:
        backend_r, backend_w = await asyncio.open_connection(
            BACKEND_HOST, BACKEND_PORT
        )
    except Exception as e:
        log(f"ERRO conectando ao Redis backend: {e}")
        with suppress(Exception):
            writer.write(b"-ERR cannot connect to backend\r\n")
            writer.close()
        return

    # Opcionalmente repassa o AUTH para o backend
    if FORWARD_AUTH_TO_BACKEND:
        try:
            backend_w.write(auth_raw)
            await backend_w.drain()
        except Exception as e:
            log(f"Erro ao repassar AUTH para backend: {e}")
            with suppress(Exception):
                backend_w.close()
                writer.close()
            return

    # Depois do AUTH, tudo vira pipe transparente
    t1 = asyncio.create_task(pipe(reader, backend_w, "C→R"))
    t2 = asyncio.create_task(pipe(backend_r, writer, "R→C"))

    done, pending = await asyncio.wait([t1, t2],
                                       return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()

    backend_w.close()
    writer.close()


async def open_and_pipe_redis(reader, writer, first_byte: bytes):
    """
    Versão sem autenticação: se conecta diretamente ao backend e faz pipe.
    Usada quando PROXY_PASSWORD não está definido.
    """
    try:
        backend_r, backend_w = await asyncio.open_connection(
            BACKEND_HOST, BACKEND_PORT
        )
    except Exception as e:
        log(f"ERRO conectando ao Redis backend: {e}")
        writer.close()
        return

    # Envia o primeiro byte já lido
    backend_w.write(first_byte)
    await backend_w.drain()

    # Inicia pipes bidirecionais
    t1 = asyncio.create_task(pipe(reader, backend_w, "C→R"))
    t2 = asyncio.create_task(pipe(backend_r, writer, "R→C"))

    done, pending = await asyncio.wait([t1, t2],
                                       return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()

    backend_w.close()
    writer.close()


# ------------------------------
# Handler HTTP (status / healthcheck)
# ------------------------------

async def handle_http(reader, writer, first_chunk: bytes):
    log("HTTP mode")

    data = first_chunk
    # Lê até o final dos headers
    while b"\r\n\r\n" not in data:
        more = await reader.read(4096)
        if not more:
            break
        data += more

    # Se tiver proteção por token no HTTP
    if HTTP_AUTH_TOKEN:
        try:
            header_text = data.decode("latin-1", "ignore")
            lines = header_text.split("\r\n")
            headers = { }
            for line in lines[1:]:
                if not line:
                    continue
                if ":" in line:
                    k, v = line.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            token = headers.get("x-auth-token")
            if token != HTTP_AUTH_TOKEN:
                response = (
                    "HTTP/1.1 401 Unauthorized\r\n"
                    "Content-Length: 13\r\n"
                    "Content-Type: text/plain\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                    "UNAUTHORIZED\n"
                )
                writer.write(response.encode("latin-1"))
                await writer.drain()
                writer.close()
                return
        except Exception as e:
            log(f"Erro ao validar token HTTP: {e}")
            response = (
                "HTTP/1.1 400 Bad Request\r\n"
                "Content-Length: 11\r\n"
                "Content-Type: text/plain\r\n"
                "Connection: close\r\n"
                "\r\n"
                "BAD REQUEST"
            )
            writer.write(response.encode("latin-1"))
            await writer.drain()
            writer.close()
            return

    # Resposta simples
    body = b"PONG HTTP"
    response = (
        "HTTP/1.1 200 OK\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Content-Type: text/plain\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("latin-1") + body

    writer.write(response)
    await writer.drain()
    writer.close()


# ------------------------------
# Handler principal de conexões
# ------------------------------

async def connection_handler(reader, writer):
    first = await reader.read(1)

    if not first:
        writer.close()
        return

    # Se começar com uma letra (GET/POST/etc) assume HTTP
    if 65 <= first[0] <= 90:
        rest = await reader.read(3)
        await handle_http(reader, writer, first + rest)
    else:
        await handle_redis(reader, writer, first)


async def start_proxy():
    if PROXY_PASSWORD:
        log("Proxy Redis protegido por senha (AUTH obrigatório).")
    else:
        log("ATENÇÃO: PROXY_PASSWORD não definido! Proxy rodando SEM autenticação.")

    if HTTP_AUTH_TOKEN:
        log("Endpoint HTTP protegido por X-Auth-Token.")
    else:
        log("Endpoint HTTP sem proteção (ok para healthcheck).")

    log(f"Proxy rodando em {PROXY_HOST}:{PROXY_PORT}")
    server = await asyncio.start_server(connection_handler,
                                        PROXY_HOST, PROXY_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(start_proxy())
