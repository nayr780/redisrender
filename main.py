# main.py — Render Playwright Remote (LITE)
#
# Objetivo:
# - Leve (1 browser apenas)
# - Flexível (API genérica /do)
# - Debugável (prints no log do Render + /sysinfo + /sessions + /fs/*)
# - Profile opcional via storage_state (path ou b64)
# - Sem UI, sem multi-browser, sem frescura
#
# Start no Render (recomendado):
#   gunicorn main:APP --workers 1 --threads 1 --timeout 180
#
# Requisitos:
#   flask
#   playwright
#   gunicorn
#
# Endpoints:
#   GET  /                   -> ok
#   GET  /health
#   GET  /sysinfo
#
#   POST /ensure             -> sobe Playwright+Browser (chromium|firefox|webkit)
#   POST /install            -> playwright install browser
#
#   POST /new                -> cria sessão
#   GET  /sessions           -> lista sessões
#   GET  /logs?sid=...       -> logs de página (console/pageerror/requestfailed)
#   POST /close              -> fecha sessão {sid}
#   POST /stop               -> fecha tudo (sessões+browser+playwright)
#
#   POST /do                 -> executa steps genéricos (page/context/browser)
#
#   FS (restrito):
#   GET  /fs/list?path=...&max=200
#   GET  /fs/read?path=...&max_bytes=200000
#   POST /fs/write           -> {path, mode:"text|b64", content, append:bool}
#   POST /fs/delete          -> {path}
#
# Notas:
# - Render free: cpu 0.15 + ram 512MB. Então: 1 browser, e fecha sessões sempre.
# - storage_state:
#     - "storage_state_path": "/tmp/state.json"
#     - OU "storage_state_b64": base64(json)
#   O server salva em /tmp e passa no new_context(storage_state=...).

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
from typing import Any, Dict, Optional, List

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
PORT = int(os.getenv("PORT", "10000"))  # Render usa PORT; default 10000

BROWSERS_PATH = "/tmp/ms-playwright"
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_PATH

DEFAULT_TIMEOUT_MS = 30_000
DEFAULT_HEADLESS = True

SAFE_FS_ROOTS = ["/tmp", "/opt/render/project/src"]

_lock = threading.RLock()

_pw: Optional[Playwright] = None
_browser: Optional[Browser] = None
_browser_meta: Dict[str, Any] = {}

# ---------------------------
# Utils
# ---------------------------
def _now() -> float:
    return time.time()

def slog(kind: str, **data: Any) -> None:
    # log em stdout (aparece no Render logs)
    try:
        msg = f"[{kind}] " + " ".join(f"{k}={repr(v)}" for k, v in data.items())
        print(msg, flush=True)
    except Exception:
        pass

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

def _ensure_dirs() -> None:
    os.makedirs(BROWSERS_PATH, exist_ok=True)

def _run(cmd: List[str], env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env or os.environ.copy(),
    )

def _read_text(path: str, max_bytes: int = 200_000) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            return f.read(max_bytes).decode("utf-8", errors="ignore").strip()
    except Exception:
        return None

def _disk_info(path: str = "/") -> Dict[str, Any]:
    try:
        u = shutil.disk_usage(path)
        return {"path": path, "total": u.total, "used": u.used, "free": u.free}
    except Exception as e:
        return {"path": path, "error": str(e)}

def _proc_status_mem() -> Dict[str, str]:
    # memória do processo (Linux)
    out: Dict[str, str] = {}
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith(("VmRSS:", "VmSize:", "VmPeak:", "Threads:")):
                    k, v = line.split(":", 1)
                    out[k.strip()] = v.strip()
    except Exception:
        pass
    return out

def _cgroup_hints() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    cpu_max = _read_text("/sys/fs/cgroup/cpu.max")
    mem_max = _read_text("/sys/fs/cgroup/memory.max")
    if cpu_max:
        out["cpu.max"] = cpu_max
    if mem_max:
        out["memory.max"] = mem_max
    # v1 fallback (se existir)
    mem_v1 = _read_text("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    if mem_v1:
        out["memory.limit_in_bytes"] = mem_v1
    return out

# ---------------------------
# FS (SAFE)
# ---------------------------
def _is_under_roots(abs_path: str) -> bool:
    abs_path = os.path.abspath(abs_path)
    for root in SAFE_FS_ROOTS:
        r = os.path.abspath(root)
        if abs_path == r or abs_path.startswith(r + os.sep):
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
    for name in sorted(os.listdir(ap))[:max_items]:
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
    try:
        return {"path": ap, "mode": "text", "text": data.decode("utf-8"), "bytes": len(data)}
    except Exception:
        return {"path": ap, "mode": "b64", "b64": base64.b64encode(data).decode("utf-8"), "bytes": len(data)}

def _fs_write(path: str, mode: str, content: str, append: bool) -> Dict[str, Any]:
    ap = _safe_path(path)
    os.makedirs(os.path.dirname(ap), exist_ok=True)
    if mode == "text":
        data = content.encode("utf-8")
    elif mode == "b64":
        data = base64.b64decode(content.encode("utf-8"))
    else:
        raise ValueError("mode_must_be_text_or_b64")
    with open(ap, "ab" if append else "wb") as f:
        f.write(data)
    return {"path": ap, "bytes_written": len(data), "append": append}

def _fs_delete(path: str) -> Dict[str, Any]:
    ap = _safe_path(path)
    if os.path.isdir(ap):
        if os.listdir(ap):
            raise ValueError("directory_not_empty")
        os.rmdir(ap)
        return {"path": ap, "deleted": True, "type": "dir"}
    if os.path.isfile(ap):
        os.remove(ap)
        return {"path": ap, "deleted": True, "type": "file"}
    raise ValueError("path_not_found")

# ---------------------------
# Playwright / Browser (1 only)
# ---------------------------
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

    # Render-friendly chromium flags (ainda leve)
    chromium_args = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
    ]

    if browser == "chromium":
        return _pw.chromium.launch(headless=headless, args=chromium_args)  # type: ignore
    if browser == "firefox":
        return _pw.firefox.launch(headless=headless)  # type: ignore
    if browser == "webkit":
        return _pw.webkit.launch(headless=headless)  # type: ignore
    raise ValueError("browser must be chromium|firefox|webkit")

def _ensure_browser(browser: str = "chromium", headless: bool = DEFAULT_HEADLESS) -> Dict[str, Any]:
    global _browser, _browser_meta
    with _lock:
        if _browser is not None:
            return {"ok": True, "status": "already_running", **_browser_meta}
        try:
            _browser = _launch_browser(browser=browser, headless=headless)
        except PWError:
            inst = _install(browser=browser)
            if not inst.get("ok"):
                return {"ok": False, "status": "install_failed", "install": inst}
            _browser = _launch_browser(browser=browser, headless=headless)
        _browser_meta = {
            "browser": browser,
            "headless": headless,
            "created_at": _now(),
        }
        slog("browser_ready", **_browser_meta)
        return {"ok": True, "status": "started", **_browser_meta}

def _stop_all_noexcept() -> None:
    global _pw, _browser, _browser_meta
    with _lock:
        # close sessions
        for sid in list(SESSIONS.keys()):
            try:
                SESSIONS[sid].close()
            except Exception:
                pass
            SESSIONS.pop(sid, None)

        # close browser
        try:
            if _browser:
                _browser.close()
        except Exception:
            pass
        _browser = None
        _browser_meta = {}

        # stop pw
        try:
            if _pw:
                _pw.stop()
        except Exception:
            pass
        _pw = None
    slog("stop_all", ok=True)

# ---------------------------
# Sessions + logs
# ---------------------------
def _new_id(nbytes: int = 18) -> str:
    return base64.urlsafe_b64encode(os.urandom(nbytes)).decode("utf-8").rstrip("=")

@dataclass
class RingLog:
    limit: int = 250
    items: List[Dict[str, Any]] = field(default_factory=list)

    def add(self, kind: str, data: Dict[str, Any]) -> None:
        self.items.append({"ts": _now(), "kind": kind, **data})
        if len(self.items) > self.limit:
            self.items = self.items[-self.limit:]

    def dump(self) -> List[Dict[str, Any]]:
        return list(self.items)

@dataclass
class Session:
    sid: str
    context: BrowserContext
    page: Page
    created_at: float
    log: RingLog = field(default_factory=lambda: RingLog(limit=300))

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

def _summ_sess(s: Session) -> Dict[str, Any]:
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
        "created_at": s.created_at,
        "age_s": round(_now() - s.created_at, 3),
        "url": url,
        "title": title,
        "log_items": len(s.log.items),
    }

# ---------------------------
# /do engine
# ---------------------------
def _is_safe_op(op: str) -> bool:
    if op.startswith("__"):
        return False
    if op in {"close"}:
        return False
    return True

def _jsonable(x: Any) -> Any:
    try:
        json.dumps(x)
        return x
    except Exception:
        return str(x)

def _get_target(sid: str, t: str) -> Any:
    if t == "browser":
        if _browser is None:
            raise RuntimeError("browser_not_started")
        return _browser
    s = _get_sess(sid)
    if t == "context":
        return s.context
    if t == "page":
        return s.page
    raise ValueError("target must be page|context|browser")

def _call_op(sid: str, t: str, op: str, args: List[Any], kwargs: Dict[str, Any], ret_mode: str) -> Any:
    if not _is_safe_op(op):
        raise ValueError("op_blocked")

    obj = _get_target(sid, t)
    fn = getattr(obj, op, None)
    if fn is None or not callable(fn):
        raise ValueError(f"no_such_callable: {t}.{op}")

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
# Routes
# ---------------------------
@APP.errorhandler(Exception)
def _handle_exc(e: Exception):
    tb = traceback.format_exc(limit=15)
    slog("exception", error=str(e))
    return jsonify({"ok": False, "error": str(e), "traceback": tb}), 500

@APP.get("/")
def root():
    return jsonify({"ok": True, "hint": "use /health /sysinfo /ensure /new /do /sessions /logs /fs/* /stop"})

@APP.get("/health")
def health():
    return jsonify({
        "ok": True,
        "time": _now(),
        "pid": os.getpid(),
        "cwd": os.getcwd(),
        "python": sys.version,
        "python_executable": sys.executable,
        "browsers_path": BROWSERS_PATH,
        "pw_running": _pw is not None,
        "browser_running": _browser is not None,
        "browser_meta": _browser_meta,
        "sessions": len(SESSIONS),
        "safe_fs_roots": SAFE_FS_ROOTS,
    })

@APP.get("/sysinfo")
def sysinfo():
    return jsonify({
        "ok": True,
        "time": _now(),
        "pid": os.getpid(),
        "cwd": os.getcwd(),
        "python_executable": sys.executable,
        "python_version": sys.version,
        "disk_root": _disk_info("/"),
        "disk_tmp": _disk_info("/tmp"),
        "cgroup": _cgroup_hints(),
        "process_mem": _proc_status_mem(),
        "browsers_path_exists": os.path.exists(BROWSERS_PATH),
        "browsers_path_list_sample": (os.listdir(BROWSERS_PATH)[:80] if os.path.isdir(BROWSERS_PATH) else []),
        "env_keys_sample": sorted(list(os.environ.keys()))[:80],
    })

@APP.post("/install")
def install():
    body = _json_body()
    browser = body.get("browser", "chromium")
    res = _install(browser=browser)
    return jsonify(res), (200 if res.get("ok") else 500)

@APP.post("/ensure")
def ensure():
    body = _json_body()
    browser = body.get("browser", "chromium")
    headless = bool(body.get("headless", DEFAULT_HEADLESS))
    res = _ensure_browser(browser=browser, headless=headless)
    return jsonify(res), (200 if res.get("ok") else 500)

@APP.post("/new")
def new():
    body = _json_body()
    browser = body.get("browser", "chromium")
    headless = bool(body.get("headless", DEFAULT_HEADLESS))

    ens = _ensure_browser(browser=browser, headless=headless)
    if not ens.get("ok"):
        return jsonify({"ok": False, "stage": "ensure", "ensure": ens}), 500

    # storage_state (path ou b64)
    storage_state_path = body.get("storage_state_path")
    storage_state_b64 = body.get("storage_state_b64")

    with _lock:
        sid = _new_id()
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

        # se veio b64, salvar em /tmp
        if isinstance(storage_state_b64, str) and storage_state_b64.strip():
            try:
                raw = base64.b64decode(storage_state_b64.encode("utf-8"))
                p = f"/tmp/storage_state_{sid}.json"
                with open(p, "wb") as f:
                    f.write(raw)
                ctx_kwargs["storage_state"] = p
                slog("storage_state_b64_saved", sid=sid, path=p, bytes=len(raw))
            except Exception as e:
                return jsonify({"ok": False, "error": f"bad storage_state_b64: {e}"}), 400
        elif isinstance(storage_state_path, str) and storage_state_path.strip():
            try:
                sp = _safe_path(storage_state_path)
                ctx_kwargs["storage_state"] = sp
                slog("storage_state_path", sid=sid, path=sp)
            except Exception as e:
                return jsonify({"ok": False, "error": f"bad storage_state_path: {e}"}), 400

        assert _browser is not None
        context = _browser.new_context(**ctx_kwargs)
        context.set_default_timeout(DEFAULT_TIMEOUT_MS)
        context.set_default_navigation_timeout(DEFAULT_TIMEOUT_MS)
        page = context.new_page()

        sess = Session(sid=sid, context=context, page=page, created_at=_now())
        _attach_listeners(sess)
        SESSIONS[sid] = sess

        slog("session_new", sid=sid, has_storage_state=("storage_state" in ctx_kwargs))

    return jsonify({"ok": True, "sid": sid, "session": _summ_sess(sess), "ensure": ens})

@APP.get("/sessions")
def sessions():
    with _lock:
        return jsonify({"ok": True, "sessions": [_summ_sess(s) for s in SESSIONS.values()]})

@APP.get("/logs")
def logs():
    sid = (request.args.get("sid") or "").strip()
    if not sid:
        return jsonify({"ok": False, "error": "missing sid query param"}), 400
    with _lock:
        s = _get_sess(sid)
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
    _stop_all_noexcept()
    return jsonify({"ok": True, "status": "stopped"})

@APP.post("/do")
def do():
    body = _json_body()
    top_sid = (body.get("sid") or "").strip()
    steps = body.get("steps")

    if steps is None:
        steps = [body]
    if not isinstance(steps, list) or not steps:
        return jsonify({"ok": False, "error": "steps must be a non-empty list"}), 400

    out: List[Dict[str, Any]] = []

    def exec_step(step: Dict[str, Any]) -> Dict[str, Any]:
        t0 = _now()
        sid = (step.get("sid") or top_sid or "").strip()
        t = str(step.get("t", step.get("target", "page"))).strip()
        op = str(step.get("op", "")).strip()
        args = step.get("args", [])
        kwargs = step.get("kwargs", {})
        ret_mode = str(step.get("return", "json")).strip().lower()

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

        result = _call_op(sid=sid, t=t, op=op, args=args, kwargs=kwargs, ret_mode=ret_mode)
        return {
            "ok": True,
            "sid": sid,
            "t": t,
            "op": op,
            "dt_ms": round((_now() - t0) * 1000, 3),
            "result": result,
        }

    with _lock:
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                return jsonify({"ok": False, "error": f"step {i} must be object"}), 400
            try:
                r = exec_step(step)
                out.append(r)
                slog("do_step", i=i, t=r["t"], op=r["op"], dt_ms=r["dt_ms"])
            except Exception as e:
                out.append({
                    "ok": False,
                    "step_index": i,
                    "error": str(e),
                    "traceback": traceback.format_exc(limit=12),
                    "step": step,
                })
                slog("do_error", i=i, error=str(e))
                break

    ok = all(x.get("ok") for x in out)
    return jsonify({"ok": ok, "results": out}), (200 if ok else 500)

# ---- FS routes ----
@APP.get("/fs/list")
def fs_list():
    path = request.args.get("path", "/tmp")
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

# ---------------------------
# shutdown hooks
# ---------------------------
def _on_exit(*_args):
    _stop_all_noexcept()

atexit.register(_on_exit)
signal.signal(signal.SIGTERM, _on_exit)
signal.signal(signal.SIGINT, _on_exit)

# NÃO use APP.run() no Render com gunicorn.
# Se você rodar local sem gunicorn, descomente:
# if __name__ == "__main__":
#     APP.run(host=HOST, port=PORT, threaded=False, use_reloader=False)
