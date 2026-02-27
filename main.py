# main.py — Playwright + Flask (controle remoto) com DEBUG pesado + multi-browser + FS tools + server logs
#
# Segurança prática:
# - Sem auth (como você pediu) = perigoso em público.
# - Então eu implementei "travamento de filesystem": só deixa mexer dentro de /tmp e /opt/render/project/src.
# - Sem execução de shell remota (pra não virar uma porta de ransomware acidental).
#
# Playwright sync + greenlet:
# - PRECISA rodar single-thread. Então: threaded=False e use_reloader=False
#
# Endpoints principais:
#   GET  /                      -> 200
#   GET  /health
#   GET  /sysinfo
#   GET  /server_logs           -> logs do servidor (ring buffer)
#   POST /server_logs/clear
#
# Browsers:
#   POST /browser/new           -> cria browser {browser_id}
#   GET  /browser/list          -> lista browsers
#   POST /browser/close         -> fecha browser {browser_id}
#   POST /ensure                -> garante browser default
#   POST /install               -> instala browser (chromium/firefox/webkit)
#
# Sessions:
#   POST /new                   -> cria sessão {sid} (aceita browser_id opcional)
#   GET  /sessions
#   GET  /logs?sid=...          -> logs da página (console/pageerror/requestfailed)
#   POST /close                 -> fecha sessão
#
# Exec:
#   POST /do                    -> step único ou steps
#
# Filesystem (restrito a roots seguros):
#   GET  /fs/list?path=...&max=200
#   GET  /fs/read?path=...&max_bytes=200000
#   GET  /fs/stat?path=...
#   POST /fs/write              -> {path, mode:"text|b64", content, append:bool}
#   POST /fs/delete             -> {path}
#
# Selftest:
#   POST /selftest              -> vai em example.com e retorna title + screenshot b64
#
# Requisitos:
#   flask
#   playwright

import os
import sys
import json
import time
import base64
import atexit
import signal
import shutil
import traceback
import subprocess
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List, Tuple

from flask import Flask, request, jsonify

from playwright.sync_api import (
    sync_playwright,
    Playwright,
    Browser,
    BrowserContext,
    Page,
    Error as PWError,
)

APP = Flask(__name__)

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "5000"))

BROWSERS_PATH = "/tmp/ms-playwright"
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_PATH

DEFAULT_TIMEOUT_MS = 30_000
DEFAULT_HEADLESS = True

# Sem auth (como você pediu). Mas ainda assim evitamos virar uma bomba acidental:
SAFE_FS_ROOTS = [
    "/tmp",
    "/opt/render/project/src",
]

# Ring buffer de logs do servidor (prints + exceptions + request log)
SERVER_LOG_LIMIT = 600

_lock = threading.RLock()

_pw: Optional[Playwright] = None

# multi-browser
BROWSERS: Dict[str, Browser] = {}  # browser_id -> Browser
BROWSER_META: Dict[str, Dict[str, Any]] = {}  # browser_id -> info


def _now() -> float:
    return time.time()


# ---------------------------
# Server logs (ring buffer)
# ---------------------------
@dataclass
class RingLog:
    limit: int = 400
    items: List[Dict[str, Any]] = field(default_factory=list)

    def add(self, kind: str, data: Dict[str, Any]) -> None:
        self.items.append({"ts": _now(), "kind": kind, **data})
        if len(self.items) > self.limit:
            self.items = self.items[-self.limit :]

    def dump(self) -> List[Dict[str, Any]]:
        return list(self.items)

    def clear(self) -> None:
        self.items.clear()


SERVER_LOG = RingLog(limit=SERVER_LOG_LIMIT)


def slog(kind: str, **data):
    """Log pro ring buffer + stdout (pra você ver no Render log)."""
    try:
        SERVER_LOG.add(kind, data)
    except Exception:
        pass
    try:
        # print “bonito” no stdout
        msg = f"[{kind}] " + " ".join([f"{k}={repr(v)}" for k, v in data.items()])
        print(msg, flush=True)
    except Exception:
        pass


@APP.before_request
def _log_request():
    slog("http_in", method=request.method, path=request.path, ip=request.remote_addr)


@APP.after_request
def _log_response(resp):
    slog("http_out", method=request.method, path=request.path, status=resp.status_code)
    return resp


@APP.errorhandler(Exception)
def _handle_exception(e: Exception):
    tb = traceback.format_exc(limit=20)
    slog("exception", error=str(e), traceback=tb)
    return jsonify({"ok": False, "error": str(e), "traceback": tb}), 500


# ---------------------------
# JSON parsing
# ---------------------------
def _json_body() -> Dict[str, Any]:
    if request.is_json:
        return request.get_json(silent=True) or {}
    raw = (request.data or b"").decode("utf-8", errors="ignore").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {"_raw": raw}


# ---------------------------
# Filesystem helpers (SAFE)
# ---------------------------
def _is_under_roots(abs_path: str) -> bool:
    abs_path = os.path.abspath(abs_path)
    for root in SAFE_FS_ROOTS:
        root_abs = os.path.abspath(root)
        if abs_path == root_abs or abs_path.startswith(root_abs + os.sep):
            return True
    return False


def _safe_path(p: str) -> str:
    if not isinstance(p, str) or not p.strip():
        raise ValueError("invalid_path")
    ap = os.path.abspath(p)
    if not _is_under_roots(ap):
        raise ValueError("path_outside_allowed_roots")
    return ap


def _fs_list(path: str, max_items: int = 200) -> Dict[str, Any]:
    ap = _safe_path(path)
    if not os.path.isdir(ap):
        raise ValueError("not_a_directory")
    items = []
    for name in sorted(os.listdir(ap))[: max_items]:
        full = os.path.join(ap, name)
        try:
            st = os.stat(full)
            items.append({
                "name": name,
                "path": full,
                "is_dir": os.path.isdir(full),
                "size": st.st_size,
                "mtime": st.st_mtime,
            })
        except Exception as e:
            items.append({"name": name, "path": full, "error": str(e)})
    return {"path": ap, "items": items}


def _fs_read(path: str, max_bytes: int = 200_000) -> Dict[str, Any]:
    ap = _safe_path(path)
    if not os.path.isfile(ap):
        raise ValueError("not_a_file")
    with open(ap, "rb") as f:
        data = f.read(max_bytes)
    # tenta decode como texto (pra debug rápido)
    try:
        text = data.decode("utf-8")
        return {"path": ap, "mode": "text", "text": text, "bytes": len(data)}
    except Exception:
        return {"path": ap, "mode": "b64", "b64": base64.b64encode(data).decode("utf-8"), "bytes": len(data)}


def _fs_stat(path: str) -> Dict[str, Any]:
    ap = _safe_path(path)
    st = os.stat(ap)
    return {
        "path": ap,
        "is_dir": os.path.isdir(ap),
        "is_file": os.path.isfile(ap),
        "size": st.st_size,
        "mtime": st.st_mtime,
        "mode": st.st_mode,
    }


def _fs_write(path: str, mode: str, content: str, append: bool = False) -> Dict[str, Any]:
    ap = _safe_path(path)
    os.makedirs(os.path.dirname(ap), exist_ok=True)

    if mode == "text":
        data = content.encode("utf-8")
    elif mode == "b64":
        data = base64.b64decode(content.encode("utf-8"))
    else:
        raise ValueError("mode_must_be_text_or_b64")

    file_mode = "ab" if append else "wb"
    with open(ap, file_mode) as f:
        f.write(data)
    return {"path": ap, "bytes_written": len(data), "append": append}


def _fs_delete(path: str) -> Dict[str, Any]:
    ap = _safe_path(path)
    if os.path.isdir(ap):
        # pra evitar apagar pasta inteira sem querer, exige vazio
        if os.listdir(ap):
            raise ValueError("directory_not_empty")
        os.rmdir(ap)
        return {"path": ap, "deleted": True, "type": "dir"}
    if os.path.isfile(ap):
        os.remove(ap)
        return {"path": ap, "deleted": True, "type": "file"}
    raise ValueError("path_not_found")


# ---------------------------
# Playwright / Browser management
# ---------------------------
def _ensure_dirs():
    os.makedirs(BROWSERS_PATH, exist_ok=True)


def _run(cmd: List[str], env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env or os.environ.copy(),
    )


def _install(browser: str = "chromium") -> Dict[str, Any]:
    _ensure_dirs()
    env = os.environ.copy()
    env["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_PATH
    cmd = [sys.executable, "-m", "playwright", "install", browser]
    proc = _run(cmd, env=env)
    slog("install", cmd=" ".join(cmd), returncode=proc.returncode)
    return {
        "ok": proc.returncode == 0,
        "cmd": " ".join(cmd),
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
        "browsers_path": BROWSERS_PATH,
        "python": sys.executable,
    }


def _ensure_pw_started() -> None:
    global _pw
    if _pw is None:
        _ensure_dirs()
        _pw = sync_playwright().start()
        slog("pw_start", ok=True)


def _launch_browser(browser: str, headless: bool) -> Browser:
    _ensure_pw_started()

    if browser == "chromium":
        return _pw.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
    if browser == "firefox":
        return _pw.firefox.launch(headless=headless)
    if browser == "webkit":
        return _pw.webkit.launch(headless=headless)
    raise ValueError("browser must be chromium|firefox|webkit")


def _browser_new(browser: str = "chromium", headless: bool = DEFAULT_HEADLESS, browser_id: Optional[str] = None) -> Dict[str, Any]:
    with _lock:
        _ensure_pw_started()
        if not browser_id:
            browser_id = base64.urlsafe_b64encode(os.urandom(10)).decode("utf-8").rstrip("=")

        if browser_id in BROWSERS:
            raise ValueError("browser_id_already_exists")

        try:
            b = _launch_browser(browser=browser, headless=headless)
        except PWError as e:
            # tenta instalar e relançar
            inst = _install(browser=browser)
            if not inst["ok"]:
                raise RuntimeError(f"install_failed: {inst['stderr_tail']}")
            b = _launch_browser(browser=browser, headless=headless)

        BROWSERS[browser_id] = b
        BROWSER_META[browser_id] = {
            "browser": browser,
            "headless": headless,
            "created_at": _now(),
        }
        slog("browser_new", browser_id=browser_id, browser=browser, headless=headless)
        return {"ok": True, "browser_id": browser_id, **BROWSER_META[browser_id]}


def _browser_get(browser_id: str) -> Browser:
    b = BROWSERS.get(browser_id)
    if not b:
        raise KeyError("browser_not_found")
    return b


def _browser_close(browser_id: str) -> Dict[str, Any]:
    with _lock:
        b = _browser_get(browser_id)
        try:
            b.close()
        finally:
            BROWSERS.pop(browser_id, None)
            meta = BROWSER_META.pop(browser_id, None) or {}
        slog("browser_close", browser_id=browser_id)
        return {"ok": True, "browser_id": browser_id, "meta": meta}


def _browser_list() -> List[Dict[str, Any]]:
    out = []
    for bid, meta in BROWSER_META.items():
        out.append({
            "browser_id": bid,
            **meta,
            "alive": bid in BROWSERS,
        })
    return out


def _ensure_default_browser(headless: bool, browser: str) -> Dict[str, Any]:
    # default browser_id = "default"
    with _lock:
        if "default" in BROWSERS:
            return {"ok": True, "status": "already_running", "browser_id": "default", **BROWSER_META.get("default", {})}
        return _browser_new(browser=browser, headless=headless, browser_id="default")


# ---------------------------
# Sessions + page logs
# ---------------------------
def _new_sid() -> str:
    return base64.urlsafe_b64encode(os.urandom(18)).decode("utf-8").rstrip("=")


@dataclass
class Session:
    sid: str
    browser_id: str
    context: BrowserContext
    page: Page
    created_at: float
    log: RingLog = field(default_factory=lambda: RingLog(limit=500))

    def close(self) -> None:
        self.context.close()


SESSIONS: Dict[str, Session] = {}


def _attach_listeners(sess: Session) -> None:
    p = sess.page

    def on_console(msg):
        try:
            sess.log.add("console", {"type": msg.type, "text": msg.text})
        except Exception:
            pass

    def on_page_error(err):
        try:
            sess.log.add("pageerror", {"error": str(err)})
        except Exception:
            pass

    def on_request_failed(req):
        try:
            sess.log.add("requestfailed", {"url": req.url, "failure": str(req.failure)})
        except Exception:
            pass

    p.on("console", on_console)
    p.on("pageerror", on_page_error)
    p.on("requestfailed", on_request_failed)


def _get_sess(sid: str) -> Session:
    s = SESSIONS.get(sid)
    if not s:
        raise KeyError("session_not_found")
    return s


def _summarize_session(s: Session) -> Dict[str, Any]:
    try:
        url = s.page.url
    except Exception:
        url = None
    try:
        title = s.page.title()
    except Exception:
        title = None
    return {
        "sid": s.sid,
        "browser_id": s.browser_id,
        "created_at": s.created_at,
        "age_s": round(_now() - s.created_at, 3),
        "url": url,
        "title": title,
        "log_items": len(s.log.items),
    }


# ---------------------------
# /do execution engine
# ---------------------------
def _is_safe_op(op: str) -> bool:
    if op.startswith("__"):
        return False
    if op in {"close"}:
        return False
    return True


def _jsonable(x: Any) -> Any:
    """Tenta tornar retorno serializável."""
    try:
        json.dumps(x)
        return x
    except Exception:
        return str(x)


def _get_target(sid: str, t: str, browser_id: Optional[str]) -> Any:
    if t == "browser":
        bid = browser_id or "default"
        return _browser_get(bid)

    s = _get_sess(sid)
    if t == "context":
        return s.context
    if t == "page":
        return s.page
    raise ValueError("target must be page|context|browser")


def _call_op(sid: str, t: str, op: str, args: List[Any], kwargs: Dict[str, Any], ret_mode: str, browser_id: Optional[str]) -> Any:
    if not _is_safe_op(op):
        raise ValueError("op_blocked")

    obj = _get_target(sid=sid, t=t, browser_id=browser_id)
    fn = getattr(obj, op, None)
    if fn is None or not callable(fn):
        raise ValueError(f"no_such_callable: {t}.{op}")

    # timeouts default
    if t == "page" and op in {"goto", "click", "dblclick", "fill", "type", "press", "hover", "focus", "wait_for_selector"}:
        kwargs = dict(kwargs)
        kwargs.setdefault("timeout", DEFAULT_TIMEOUT_MS)

    result = fn(*args, **kwargs)

    if ret_mode == "b64":
        if isinstance(result, (bytes, bytearray)):
            return base64.b64encode(bytes(result)).decode("utf-8")
        raise TypeError("return=b64 requires bytes result")

    if ret_mode == "str":
        return str(result)

    return _jsonable(result)


# ---------------------------
# Sysinfo helpers
# ---------------------------
def _disk_info(path: str = "/") -> Dict[str, Any]:
    try:
        u = shutil.disk_usage(path)
        return {"path": path, "total": u.total, "used": u.used, "free": u.free}
    except Exception as e:
        return {"path": path, "error": str(e)}


def _read_text(p: str, max_bytes: int = 200_000) -> Optional[str]:
    try:
        with open(p, "rb") as f:
            data = f.read(max_bytes)
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return None


def _cgroup_hints() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    cpu_max = _read_text("/sys/fs/cgroup/cpu.max")
    mem_max = _read_text("/sys/fs/cgroup/memory.max")
    if cpu_max:
        out["cpu.max"] = cpu_max.strip()
    if mem_max:
        out["memory.max"] = mem_max.strip()
    return out


# ---------------------------
# Routes
# ---------------------------
@APP.get("/")
def root():
    return jsonify({"ok": True, "hint": "use /health /sysinfo /server_logs /browser/list /new /do /sessions /logs"})


@APP.get("/health")
def health():
    return jsonify({
        "ok": True,
        "time": _now(),
        "port": PORT,
        "python_executable": sys.executable,
        "cwd": os.getcwd(),
        "browsers_path": BROWSERS_PATH,
        "pw_running": _pw is not None,
        "browsers": len(BROWSERS),
        "sessions": len(SESSIONS),
        "safe_fs_roots": SAFE_FS_ROOTS,
    })


@APP.get("/sysinfo")
def sysinfo():
    return jsonify({
        "ok": True,
        "time": _now(),
        "python_executable": sys.executable,
        "python_version": sys.version,
        "cwd": os.getcwd(),
        "pid": os.getpid(),
        "disk_root": _disk_info("/"),
        "disk_tmp": _disk_info("/tmp"),
        "cgroup": _cgroup_hints(),
        "env_keys_sample": sorted(list(os.environ.keys()))[:100],
        "browsers_path_exists": os.path.exists(BROWSERS_PATH),
        "browsers_path_list_sample": (os.listdir(BROWSERS_PATH)[:80] if os.path.isdir(BROWSERS_PATH) else []),
    })


@APP.get("/server_logs")
def server_logs():
    # pode passar ?tail=100
    try:
        tail = int(request.args.get("tail", "200"))
    except Exception:
        tail = 200
    logs = SERVER_LOG.dump()
    if tail > 0:
        logs = logs[-tail:]
    return jsonify({"ok": True, "count": len(SERVER_LOG.items), "tail": tail, "logs": logs})


@APP.post("/server_logs/clear")
def server_logs_clear():
    SERVER_LOG.clear()
    return jsonify({"ok": True})


# ---- Browsers ----
@APP.post("/install")
def install():
    body = _json_body()
    browser = body.get("browser", "chromium")
    res = _install(browser=browser)
    return jsonify(res), (200 if res["ok"] else 500)


@APP.post("/ensure")
def ensure():
    body = _json_body()
    headless = bool(body.get("headless", DEFAULT_HEADLESS))
    browser = body.get("browser", "chromium")
    res = _ensure_default_browser(headless=headless, browser=browser)
    return jsonify(res), (200 if res.get("ok") else 500)


@APP.post("/browser/new")
def browser_new():
    body = _json_body()
    browser = body.get("browser", "chromium")
    headless = bool(body.get("headless", DEFAULT_HEADLESS))
    browser_id = body.get("browser_id")
    res = _browser_new(browser=browser, headless=headless, browser_id=browser_id)
    return jsonify(res)


@APP.get("/browser/list")
def browser_list():
    return jsonify({"ok": True, "browsers": _browser_list()})


@APP.post("/browser/close")
def browser_close():
    body = _json_body()
    browser_id = body.get("browser_id")
    if not browser_id:
        return jsonify({"ok": False, "error": "missing browser_id"}), 400
    res = _browser_close(browser_id=browser_id)
    return jsonify(res)


# ---- Sessions ----
@APP.post("/new")
def new():
    body = _json_body()
    # escolhe browser
    browser_id = body.get("browser_id", "default")
    headless = bool(body.get("headless", DEFAULT_HEADLESS))
    browser_name = body.get("browser", "chromium")

    # garante default se pediram default
    if browser_id == "default" and "default" not in BROWSERS:
        _ensure_default_browser(headless=headless, browser=browser_name)

    with _lock:
        sid = _new_sid()
        b = _browser_get(browser_id)

        ctx_kwargs: Dict[str, Any] = {}

        viewport = body.get("viewport")
        if isinstance(viewport, dict) and "width" in viewport and "height" in viewport:
            ctx_kwargs["viewport"] = viewport
        if isinstance(body.get("user_agent"), str):
            ctx_kwargs["user_agent"] = body["user_agent"]
        if isinstance(body.get("locale"), str):
            ctx_kwargs["locale"] = body["locale"]
        if isinstance(body.get("timezone_id"), str):
            ctx_kwargs["timezone_id"] = body["timezone_id"]
        if isinstance(body.get("ignore_https_errors"), bool):
            ctx_kwargs["ignore_https_errors"] = body["ignore_https_errors"]

        context = b.new_context(**ctx_kwargs)
        context.set_default_timeout(DEFAULT_TIMEOUT_MS)
        context.set_default_navigation_timeout(DEFAULT_TIMEOUT_MS)

        page = context.new_page()

        sess = Session(
            sid=sid,
            browser_id=browser_id,
            context=context,
            page=page,
            created_at=_now(),
        )
        _attach_listeners(sess)
        SESSIONS[sid] = sess

        slog("session_new", sid=sid, browser_id=browser_id)

    return jsonify({"ok": True, "sid": sid, "session": _summarize_session(sess), "browser_id": browser_id})


@APP.get("/sessions")
def sessions():
    with _lock:
        return jsonify({"ok": True, "sessions": [_summarize_session(s) for s in SESSIONS.values()]})


@APP.get("/logs")
def logs():
    sid = (request.args.get("sid") or "").strip()
    if not sid:
        return jsonify({"ok": False, "error": "missing sid query param"}), 400
    with _lock:
        s = _get_sess(sid)
        # tail opcional
        try:
            tail = int(request.args.get("tail", "200"))
        except Exception:
            tail = 200
        items = s.log.dump()
        if tail > 0:
            items = items[-tail:]
        return jsonify({"ok": True, "sid": sid, "count": len(s.log.items), "tail": tail, "logs": items})


@APP.post("/close")
def close():
    body = _json_body()
    sid = (body.get("sid") or "").strip()
    if not sid:
        return jsonify({"ok": False, "error": "missing sid"}), 400

    with _lock:
        sess = SESSIONS.get(sid)
        if not sess:
            return jsonify({"ok": False, "error": "session_not_found"}), 404
        try:
            sess.close()
        finally:
            SESSIONS.pop(sid, None)
        slog("session_close", sid=sid)

    return jsonify({"ok": True, "closed": sid})


@APP.post("/stop")
def stop():
    with _lock:
        # fecha sessões
        for sid in list(SESSIONS.keys()):
            try:
                SESSIONS[sid].close()
            except Exception:
                pass
            SESSIONS.pop(sid, None)

        # fecha browsers
        for bid in list(BROWSERS.keys()):
            try:
                BROWSERS[bid].close()
            except Exception:
                pass
            BROWSERS.pop(bid, None)
            BROWSER_META.pop(bid, None)

        # fecha PW
        global _pw
        try:
            if _pw:
                _pw.stop()
        except Exception:
            pass
        _pw = None

    slog("stop_all")
    return jsonify({"ok": True, "status": "stopped"})


@APP.post("/do")
def do():
    """
    API flexível:
      - single:
        {"sid":"...","t":"page|context|browser","op":"goto","args":[...],"kwargs":{...},"return":"json|str|b64","browser_id":"..."}
      - multi:
        {"sid":"...","steps":[{...},{...}]}

    Extras:
      - step pode trazer browser_id (pra target=browser)
      - retorna dt_ms e traceback em erro
    """
    body = _json_body()
    top_sid = (body.get("sid") or "").strip()
    steps = body.get("steps")

    if steps is None:
        steps = [body]
    if not isinstance(steps, list) or not steps:
        return jsonify({"ok": False, "error": "steps must be a non-empty list"}), 400

    def exec_step(step: Dict[str, Any]) -> Dict[str, Any]:
        t0 = _now()
        sid = (step.get("sid") or top_sid or "").strip()
        t = str(step.get("t", step.get("target", "page"))).strip()
        op = str(step.get("op", "")).strip()
        args = step.get("args", [])
        kwargs = step.get("kwargs", {})
        ret_mode = str(step.get("return", "json")).strip().lower()
        browser_id = step.get("browser_id")

        if not op:
            raise ValueError("missing op")
        if t != "browser" and not sid:
            raise ValueError("missing sid")
        if not isinstance(args, list):
            raise ValueError("args must be a list")
        if not isinstance(kwargs, dict):
            raise ValueError("kwargs must be an object")
        if ret_mode not in {"json", "str", "b64"}:
            raise ValueError("return must be json|str|b64")

        result = _call_op(sid=sid, t=t, op=op, args=args, kwargs=kwargs, ret_mode=ret_mode, browser_id=browser_id)
        return {
            "ok": True,
            "sid": sid,
            "t": t,
            "op": op,
            "dt_ms": round((_now() - t0) * 1000, 3),
            "result": result,
        }

    out: List[Dict[str, Any]] = []
    with _lock:
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                return jsonify({"ok": False, "error": f"step {i} must be object"}), 400
            try:
                out.append(exec_step(step))
            except Exception as e:
                out.append({
                    "ok": False,
                    "step_index": i,
                    "error": str(e),
                    "traceback": traceback.format_exc(limit=15),
                    "step": step,
                })
                break

    ok = all(x.get("ok") for x in out)
    return jsonify({"ok": ok, "results": out}), (200 if ok else 500)


# ---- Filesystem endpoints ----
@APP.get("/fs/list")
def fs_list():
    path = request.args.get("path", "/opt/render/project/src")
    try:
        max_items = int(request.args.get("max", "200"))
    except Exception:
        max_items = 200
    res = _fs_list(path=path, max_items=max_items)
    return jsonify({"ok": True, **res})


@APP.get("/fs/read")
def fs_read():
    path = request.args.get("path", "")
    if not path:
        return jsonify({"ok": False, "error": "missing path"}), 400
    try:
        max_bytes = int(request.args.get("max_bytes", "200000"))
    except Exception:
        max_bytes = 200_000
    res = _fs_read(path=path, max_bytes=max_bytes)
    return jsonify({"ok": True, **res})


@APP.get("/fs/stat")
def fs_stat():
    path = request.args.get("path", "")
    if not path:
        return jsonify({"ok": False, "error": "missing path"}), 400
    res = _fs_stat(path=path)
    return jsonify({"ok": True, **res})


@APP.post("/fs/write")
def fs_write():
    body = _json_body()
    path = body.get("path", "")
    mode = body.get("mode", "text")
    content = body.get("content", "")
    append = bool(body.get("append", False))
    if not path:
        return jsonify({"ok": False, "error": "missing path"}), 400
    if not isinstance(content, str):
        return jsonify({"ok": False, "error": "content must be string"}), 400
    res = _fs_write(path=path, mode=mode, content=content, append=append)
    return jsonify({"ok": True, **res})


@APP.post("/fs/delete")
def fs_delete():
    body = _json_body()
    path = body.get("path", "")
    if not path:
        return jsonify({"ok": False, "error": "missing path"}), 400
    res = _fs_delete(path=path)
    return jsonify({"ok": True, **res})


# ---- Selftest ----
@APP.post("/selftest")
def selftest():
    body = _json_body()
    url = body.get("url", "https://example.com")
    headless = bool(body.get("headless", True))
    browser_name = body.get("browser", "chromium")

    # usa browser default
    _ensure_default_browser(headless=headless, browser=browser_name)

    sid = None
    with _lock:
        sid = _new_sid()
        b = _browser_get("default")
        ctx = b.new_context()
        ctx.set_default_timeout(DEFAULT_TIMEOUT_MS)
        ctx.set_default_navigation_timeout(DEFAULT_TIMEOUT_MS)
        page = ctx.new_page()

        sess = Session(sid=sid, browser_id="default", context=ctx, page=page, created_at=_now())
        _attach_listeners(sess)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
            title = page.title()
            shot = page.screenshot(full_page=True)
            shot_b64 = base64.b64encode(shot).decode("utf-8")
            logs_tail = sess.log.dump()[-50:]
        finally:
            try:
                sess.close()
            except Exception:
                pass

    return jsonify({"ok": True, "url": url, "title": title, "screenshot_b64": shot_b64, "logs_tail": logs_tail})


# ---------------------------
# shutdown hooks
# ---------------------------
def _on_exit(*_args):
    try:
        with _lock:
            for sid in list(SESSIONS.keys()):
                try:
                    SESSIONS[sid].close()
                except Exception:
                    pass
                SESSIONS.pop(sid, None)

            for bid in list(BROWSERS.keys()):
                try:
                    BROWSERS[bid].close()
                except Exception:
                    pass
                BROWSERS.pop(bid, None)
                BROWSER_META.pop(bid, None)

            global _pw
            if _pw:
                try:
                    _pw.stop()
                except Exception:
                    pass
            _pw = None
    except Exception:
        pass


atexit.register(_on_exit)
signal.signal(signal.SIGTERM, _on_exit)
signal.signal(signal.SIGINT, _on_exit)


if __name__ == "__main__":
    slog("boot", port=PORT, cwd=os.getcwd())
    # single-thread obrigatório pro Playwright sync
    APP.run(host=HOST, port=PORT, threaded=False, use_reloader=False)
