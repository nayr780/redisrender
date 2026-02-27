# pw_remote.py  (single-file)
#
# Playwright + Flask com API "genérica" (máxima flexibilidade) + MUITO debug.
# ✅ Sem token, sem env obrigatória: rodou, subiu.
# ✅ Auto-instala o Chromium do Playwright quando precisar.
# ✅ Um endpoint /do que executa operações flexíveis (1 ou vários steps).
# ✅ Endpoints de debug: /sysinfo, /browsers, /sessions, /logs
# ✅ Logs (console, pageerror, requestfailed) por sessão (ring buffer).
# ✅ Modo cliente embutido pra testar: `python pw_remote.py client`
#
# ⚠️ Importante (sério):
# - Se você expor isso publicamente sem proteção, você tá basicamente oferecendo um “controle remoto” do navegador.
# - Eu deixei um "UNSAFE_MODE" (True por padrão) pra você ter controle total.
#   Se for expor na internet: põe UNSAFE_MODE = False e amplia allowlist só do que você quer.
#
# Requisitos:
#   pip install flask playwright
#
# Uso:
#   python pw_remote.py
#   # server em http://0.0.0.0:5000 (ou PORT se existir)
#
# Teste automático (com server já rodando):
#   python pw_remote.py client
#
# Exemplos:
#   POST /ensure
#     {"headless": true}
#
#   POST /new
#     {"headless": true, "viewport": {"width": 1280, "height": 720}}
#
#   POST /do
#     {
#       "sid": "...",
#       "steps": [
#         {"t":"page","op":"goto","args":["https://example.com"],"kwargs":{"wait_until":"domcontentloaded"}},
#         {"t":"page","op":"title"},
#         {"t":"page","op":"screenshot","kwargs":{"full_page": true}, "return":"b64"}
#       ]
#     }
#
#   GET /logs?sid=...
#   GET /sessions
#   GET /sysinfo
#   GET /browsers

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

# ----------------------------
# Config (sem env obrigatório)
# ----------------------------
APP = Flask(__name__)
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "5000"))  # se a plataforma setar, ele pega. Senão, 5000.

BROWSERS_PATH = "/tmp/ms-playwright"
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_PATH

DEFAULT_TIMEOUT_MS = 30_000
DEFAULT_HEADLESS = True

# 🔥 Controle total:
# - True: permite chamar métodos do Playwright sem allowlist (exceto dunder/perigosos óbvios)
# - False: só allowlist (mais seguro)
UNSAFE_MODE = True

# allowlist (usada quando UNSAFE_MODE=False)
ALLOWED = {
    "browser": {"new_context", "version"},
    "context": {
        "new_page",
        "add_cookies", "clear_cookies", "cookies",
        "set_default_timeout", "set_default_navigation_timeout",
        "storage_state",
        "clear_permissions", "grant_permissions",
        "set_extra_http_headers",
        "route",  # cuidado: pode exigir callbacks; geralmente não usar por API
        "unroute",
    },
    "page": {
        "goto", "reload", "go_back", "go_forward",
        "title", "content",
        "click", "dblclick", "fill", "type", "press",
        "check", "uncheck", "select_option",
        "hover", "focus",
        "wait_for_timeout", "wait_for_selector", "wait_for_load_state",
        "evaluate", "eval_on_selector",
        "set_viewport_size",
        "set_extra_http_headers",
        "screenshot",
        "pdf",  # funciona só em chromium e com certas opções
    },
}

_lock = threading.RLock()

_pw: Optional[Playwright] = None
_browser: Optional[Browser] = None


def _now() -> float:
    return time.time()


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


def _run(cmd: List[str], env: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env or os.environ.copy(),
    )


def _ensure_dirs() -> None:
    os.makedirs(BROWSERS_PATH, exist_ok=True)


def _install(browser: str = "chromium") -> Dict[str, Any]:
    _ensure_dirs()
    env = os.environ.copy()
    env["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_PATH
    cmd = [sys.executable, "-m", "playwright", "install", browser]
    proc = _run(cmd, env=env)
    return {
        "ok": proc.returncode == 0,
        "cmd": " ".join(cmd),
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
        "browsers_path": BROWSERS_PATH,
        "python": sys.executable,
    }


def _start_playwright(headless: bool = DEFAULT_HEADLESS, browser: str = "chromium") -> Dict[str, Any]:
    """Sobe Playwright + Browser; se faltar executável, instala e tenta de novo."""
    global _pw, _browser
    with _lock:
        if _pw and _browser:
            return {"ok": True, "status": "already_running", "browser": browser}

        _ensure_dirs()
        _pw = sync_playwright().start()

        def _launch() -> Browser:
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

        try:
            _browser = _launch()
            exe = None
            try:
                exe = getattr(getattr(_pw, browser), "executable_path", None)
            except Exception:
                exe = None
            return {
                "ok": True,
                "status": "started",
                "headless": headless,
                "browser": browser,
                "browsers_path": BROWSERS_PATH,
                "python": sys.executable,
                "executable_path": exe,
            }
        except PWError as e:
            inst = _install(browser=browser)
            try:
                _browser = _launch()
                exe = None
                try:
                    exe = getattr(getattr(_pw, browser), "executable_path", None)
                except Exception:
                    exe = None
                return {
                    "ok": True,
                    "status": "started_after_install",
                    "headless": headless,
                    "browser": browser,
                    "install": inst,
                    "browsers_path": BROWSERS_PATH,
                    "python": sys.executable,
                    "executable_path": exe,
                }
            except PWError as e2:
                _stop_all_noexcept()
                return {
                    "ok": False,
                    "status": "failed_to_start",
                    "browser": browser,
                    "error": str(e2),
                    "install": inst,
                }


def _stop_all_noexcept() -> None:
    global _pw, _browser, SESSIONS
    with _lock:
        try:
            for sid in list(SESSIONS.keys()):
                try:
                    SESSIONS[sid].close()
                except Exception:
                    pass
                SESSIONS.pop(sid, None)
        except Exception:
            pass

        try:
            if _browser:
                _browser.close()
        except Exception:
            pass
        _browser = None

        try:
            if _pw:
                _pw.stop()
        except Exception:
            pass
        _pw = None


# ----------------------------
# Session + logs
# ----------------------------
def _new_sid() -> str:
    return base64.urlsafe_b64encode(os.urandom(18)).decode("utf-8").rstrip("=")


@dataclass
class RingLog:
    limit: int = 300
    items: List[Dict[str, Any]] = field(default_factory=list)

    def add(self, kind: str, data: Dict[str, Any]) -> None:
        entry = {"ts": _now(), "kind": kind, **data}
        self.items.append(entry)
        if len(self.items) > self.limit:
            self.items = self.items[-self.limit :]

    def dump(self) -> List[Dict[str, Any]]:
        return list(self.items)


@dataclass
class Session:
    sid: str
    context: BrowserContext
    page: Page
    created_at: float
    headless: bool
    browser_name: str
    log: RingLog = field(default_factory=lambda: RingLog(limit=400))

    def close(self) -> None:
        try:
            self.context.close()
        finally:
            pass


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


def _get_target(sid: str, t: str) -> Any:
    if t == "browser":
        if not _browser:
            raise RuntimeError("browser_not_started")
        return _browser
    s = _get_sess(sid)
    if t == "context":
        return s.context
    if t == "page":
        return s.page
    raise ValueError("target must be page|context|browser")


def _is_safe_op(op: str) -> bool:
    # bloqueia métodos/attrs claramente perigosos/irrelevantes
    if op.startswith("__"):
        return False
    if op in {"close"}:
        return False  # fecha por endpoint próprio
    return True


def _call_op(sid: str, t: str, op: str, args: List[Any], kwargs: Dict[str, Any], ret_mode: str) -> Any:
    """
    ret_mode:
      - "json" (default): retorna o valor como estiver (se não serializar, vira string)
      - "str": força str(result)
      - "b64": espera bytes e converte base64
    """
    if not _is_safe_op(op):
        raise ValueError("op_blocked")

    if not UNSAFE_MODE:
        if op not in ALLOWED.get(t, set()):
            raise ValueError(f"op_not_allowed: {t}.{op}")

    obj = _get_target(sid, t)
    fn = getattr(obj, op, None)
    if fn is None or not callable(fn):
        raise ValueError(f"no_such_callable: {t}.{op}")

    # timeouts default onde ajuda
    if t == "page" and op in {"goto", "click", "dblclick", "fill", "type", "press", "hover", "focus", "wait_for_selector"}:
        kwargs = dict(kwargs)
        kwargs.setdefault("timeout", DEFAULT_TIMEOUT_MS)

    result = fn(*args, **kwargs)

    if ret_mode == "b64":
        if isinstance(result, (bytes, bytearray)):
            return base64.b64encode(bytes(result)).decode("utf-8")
        # screenshot/pdf retornam bytes; se não retornar bytes, erro explícito
        raise TypeError("return=b64 requires bytes result")

    if ret_mode == "str":
        return str(result)

    # ret_mode json
    try:
        json.dumps(result)
        return result
    except Exception:
        return str(result)


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
        "created_at": s.created_at,
        "age_s": round(_now() - s.created_at, 3),
        "headless": s.headless,
        "browser": s.browser_name,
        "url": url,
        "title": title,
        "log_items": len(s.log.items),
    }


# ----------------------------
# Debug helpers
# ----------------------------
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
    # Best-effort: pega umas pistas de quota/limite
    out = {}
    # cgroup v2
    cpu_max = _read_text("/sys/fs/cgroup/cpu.max")
    mem_max = _read_text("/sys/fs/cgroup/memory.max")
    if cpu_max:
        out["cpu.max"] = cpu_max.strip()
    if mem_max:
        out["memory.max"] = mem_max.strip()
    # cgroup v1 fallback
    cpu_quota = _read_text("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    cpu_period = _read_text("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    mem_limit = _read_text("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    if cpu_quota:
        out["cpu.cfs_quota_us"] = cpu_quota.strip()
    if cpu_period:
        out["cpu.cfs_period_us"] = cpu_period.strip()
    if mem_limit:
        out["memory.limit_in_bytes"] = mem_limit.strip()
    return out


def _list_tree(root: str, max_depth: int = 3, max_items: int = 200) -> List[str]:
    res = []
    root = os.path.abspath(root)
    for base, dirs, files in os.walk(root):
        depth = base[len(root):].count(os.sep)
        if depth > max_depth:
            dirs[:] = []
            continue
        for name in sorted(dirs + files):
            p = os.path.join(base, name)
            res.append(p)
            if len(res) >= max_items:
                return res
    return res


# ----------------------------
# Routes
# ----------------------------
@APP.get("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "time": _now(),
            "port": PORT,
            "python": sys.version,
            "python_executable": sys.executable,
            "cwd": os.getcwd(),
            "browsers_path": BROWSERS_PATH,
            "running": bool(_pw and _browser),
            "sessions": len(SESSIONS),
            "unsafe_mode": UNSAFE_MODE,
        }
    )


@APP.get("/sysinfo")
def sysinfo():
    # super útil pra entender o ambiente (cgroups, disco, etc.)
    return jsonify(
        {
            "ok": True,
            "time": _now(),
            "python_executable": sys.executable,
            "python_version": sys.version,
            "argv": sys.argv,
            "cwd": os.getcwd(),
            "pid": os.getpid(),
            "uid": os.getuid() if hasattr(os, "getuid") else None,
            "gid": os.getgid() if hasattr(os, "getgid") else None,
            "env_keys_sample": sorted(list(os.environ.keys()))[:60],
            "disk_root": _disk_info("/"),
            "disk_tmp": _disk_info("/tmp"),
            "cgroup": _cgroup_hints(),
            "browsers_path_exists": os.path.exists(BROWSERS_PATH),
            "browsers_path_list_sample": (os.listdir(BROWSERS_PATH)[:60] if os.path.isdir(BROWSERS_PATH) else []),
        }
    )


@APP.get("/browsers")
def browsers():
    # lista a árvore onde o Playwright baixa os browsers
    if not os.path.isdir(BROWSERS_PATH):
        return jsonify({"ok": True, "exists": False, "path": BROWSERS_PATH, "tree": []})
    tree = _list_tree(BROWSERS_PATH, max_depth=4, max_items=400)
    return jsonify({"ok": True, "exists": True, "path": BROWSERS_PATH, "tree": tree})


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
    res = _start_playwright(headless=headless, browser=browser)
    return jsonify(res), (200 if res.get("ok") else 500)


@APP.post("/new")
def new():
    body = _json_body()
    headless = bool(body.get("headless", DEFAULT_HEADLESS))
    browser = body.get("browser", "chromium")

    res = _start_playwright(headless=headless, browser=browser)
    if not res.get("ok"):
        return jsonify(res), 500

    with _lock:
        sid = _new_sid()

        ctx_kwargs: Dict[str, Any] = {}
        viewport = body.get("viewport")
        if isinstance(viewport, dict) and "width" in viewport and "height" in viewport:
            ctx_kwargs["viewport"] = viewport

        # extras úteis
        if isinstance(body.get("user_agent"), str):
            ctx_kwargs["user_agent"] = body["user_agent"]
        if isinstance(body.get("locale"), str):
            ctx_kwargs["locale"] = body["locale"]
        if isinstance(body.get("timezone_id"), str):
            ctx_kwargs["timezone_id"] = body["timezone_id"]
        if isinstance(body.get("ignore_https_errors"), bool):
            ctx_kwargs["ignore_https_errors"] = body["ignore_https_errors"]

        context = _browser.new_context(**ctx_kwargs)  # type: ignore
        context.set_default_timeout(DEFAULT_TIMEOUT_MS)
        context.set_default_navigation_timeout(DEFAULT_TIMEOUT_MS)
        page = context.new_page()

        sess = Session(
            sid=sid,
            context=context,
            page=page,
            created_at=_now(),
            headless=headless,
            browser_name=browser,
        )
        _attach_listeners(sess)

        SESSIONS[sid] = sess

    return jsonify({"ok": True, "sid": sid, "session": _summarize_session(sess), "ensure": res})


@APP.get("/sessions")
def sessions():
    with _lock:
        return jsonify({"ok": True, "sessions": [_summarize_session(s) for s in SESSIONS.values()]})


@APP.get("/logs")
def logs():
    sid = request.args.get("sid", "").strip()
    if not sid:
        return jsonify({"ok": False, "error": "missing sid query param"}), 400
    with _lock:
        try:
            s = _get_sess(sid)
        except KeyError:
            return jsonify({"ok": False, "error": "session_not_found"}), 404
        return jsonify({"ok": True, "sid": sid, "logs": s.log.dump()})


@APP.post("/close")
def close():
    body = _json_body()
    sid = body.get("sid", "")
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

    return jsonify({"ok": True, "closed": sid})


@APP.post("/stop")
def stop():
    _stop_all_noexcept()
    return jsonify({"ok": True, "status": "stopped"})


@APP.post("/do")
def do():
    """
    API flexível:
    - Um step:
      {"sid":"...","t":"page|context|browser","op":"goto","args":[...],"kwargs":{...},"return":"json|str|b64"}
    - Vários:
      {"sid":"...","steps":[{...},{...}]}
    Retorna:
      - results com timing e erro detalhado (traceback)
    """
    body = _json_body()
    sid = str(body.get("sid", "")).strip()

    def exec_step(step: Dict[str, Any]) -> Dict[str, Any]:
        t0 = _now()
        t = str(step.get("t", step.get("target", "page"))).strip()
        op = str(step.get("op", "")).strip()
        args = step.get("args", [])
        kwargs = step.get("kwargs", {})
        ret_mode = str(step.get("return", "json")).strip().lower()

        if not op:
            raise ValueError("missing op")
        if not isinstance(args, list):
            raise ValueError("args must be a list")
        if not isinstance(kwargs, dict):
            raise ValueError("kwargs must be an object")
        if ret_mode not in {"json", "str", "b64"}:
            raise ValueError("return must be json|str|b64")

        result = _call_op(sid, t, op, args, kwargs, ret_mode)
        return {
            "ok": True,
            "t": t,
            "op": op,
            "dt_ms": round((_now() - t0) * 1000, 3),
            "result": result,
        }

    steps = body.get("steps")
    if steps is None:
        steps = [body]
    if not isinstance(steps, list) or not steps:
        return jsonify({"ok": False, "error": "steps must be a non-empty list"}), 400

    out = []
    with _lock:
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                return jsonify({"ok": False, "error": f"step {i} must be object"}), 400
            try:
                r = exec_step(step)
                out.append(r)
            except Exception as e:
                tb = traceback.format_exc(limit=8)
                out.append(
                    {
                        "ok": False,
                        "step_index": i,
                        "error": str(e),
                        "traceback": tb,
                        "step": step,
                    }
                )
                # para no primeiro erro (pra debug rápido). Se quiser continuar, comenta o break.
                break

        ok = all(x.get("ok") for x in out)
        return jsonify({"ok": ok, "results": out}), (200 if ok else 500)


# ----------------------------
# Shutdown hooks
# ----------------------------
def _on_exit(*_args):
    _stop_all_noexcept()


atexit.register(_on_exit)
signal.signal(signal.SIGTERM, _on_exit)
signal.signal(signal.SIGINT, _on_exit)


# ----------------------------
# Client mode (test rápido)
# ----------------------------
def _http_json(url: str, payload: Optional[Dict[str, Any]] = None, method: str = "POST") -> Tuple[int, Dict[str, Any]]:
    # stdlib only (sem requests)
    import urllib.request

    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            code = resp.getcode()
            body = resp.read().decode("utf-8", errors="ignore")
            try:
                return code, json.loads(body)
            except Exception:
                return code, {"_raw": body}
    except Exception as e:
        return 0, {"ok": False, "error": str(e)}


def client_demo(base: str = "http://127.0.0.1:5000"):
    print("== client demo ==")
    code, h = _http_json(base + "/health", None, method="GET")
    print("health:", code, h)

    code, ens = _http_json(base + "/ensure", {"headless": True, "browser": "chromium"})
    print("ensure:", code, ens)

    code, nw = _http_json(base + "/new", {"headless": True, "viewport": {"width": 1280, "height": 720}})
    print("new:", code, {"ok": nw.get("ok"), "sid": nw.get("sid")})
    sid = nw.get("sid")
    if not sid:
        print("no sid; abort")
        return

    steps = [
        {"sid": sid, "t": "page", "op": "goto", "args": ["https://example.com"], "kwargs": {"wait_until": "domcontentloaded"}},
        {"sid": sid, "t": "page", "op": "title"},
        {"sid": sid, "t": "page", "op": "screenshot", "kwargs": {"full_page": True}, "return": "b64"},
    ]
    code, res = _http_json(base + "/do", {"sid": sid, "steps": steps})
    print("do:", code, {"ok": res.get("ok"), "steps": len(res.get("results", []))})

    # salva screenshot local
    try:
        b64 = res["results"][2]["result"]
        img = base64.b64decode(b64.encode("utf-8"))
        with open("demo.png", "wb") as f:
            f.write(img)
        print("saved demo.png")
    except Exception as e:
        print("could not save screenshot:", e)

    code, lg = _http_json(base + f"/logs?sid={sid}", None, method="GET")
    print("logs:", code, f"{len(lg.get('logs', []))} items")

    code, cl = _http_json(base + "/close", {"sid": sid})
    print("close:", code, cl)


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1].lower() == "client":
        # se você rodar em outra porta, passe: python pw_remote.py client http://127.0.0.1:XXXX
        base = sys.argv[2] if len(sys.argv) >= 3 else f"http://127.0.0.1:{PORT}"
        client_demo(base=base)
    else:
        APP.run(host=HOST, port=PORT, threaded=True)
