# main.py — Playwright + Flask (API genérica /do) com DEBUG pesado + auto-install + selftest
#
# Objetivo:
# - Máxima flexibilidade: você manda JSON dizendo target/op/args/kwargs e pronto.
# - Debugabilidade: endpoints de sysinfo/browsers/sessions/logs + traceback detalhado.
# - Agilidade: auto-instala Chromium se faltar.
#
# IMPORTANTE (o bug que você pegou):
# - Playwright SYNC (greenlet) NÃO pode ser usado atravessando threads.
# - Então este servidor roda SEM threads: threaded=False e use_reloader=False.
# - Isso resolve o "cannot switch to a different thread".
#
# Endpoints:
#   GET  /              -> 200 (pra healthcheck/probe)
#   GET  /health
#   GET  /sysinfo
#   GET  /browsers
#   GET  /sessions
#   GET  /logs?sid=...
#   POST /install       -> {"browser":"chromium|firefox|webkit"}  (default chromium)
#   POST /ensure        -> {"browser":"chromium|firefox|webkit", "headless": true/false}
#   POST /new           -> cria sessão
#   POST /do            -> executa 1 step ou vários
#   POST /close         -> {"sid":"..."}
#   POST /stop
#   POST /selftest      -> roda um teste completo e retorna resultado (inclusive screenshot b64)
#
# Modo cliente local (opcional):
#   python main.py client http://127.0.0.1:5000
#
# Requisitos:
#   pip install flask playwright
#
# Observação:
# - Isso é "expostão" de propósito, como você pediu. Se botar público, qualquer um controla um browser seu.

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
PORT = int(os.getenv("PORT", "5000"))  # se existir, usa; senão 5000.

BROWSERS_PATH = "/tmp/ms-playwright"
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = BROWSERS_PATH

DEFAULT_TIMEOUT_MS = 30_000
DEFAULT_HEADLESS = True

# True = super flexível (chama métodos sem allowlist, bloqueando só dunder e close)
UNSAFE_MODE = True

# allowlist (só usada se UNSAFE_MODE=False)
ALLOWED = {
    "browser": {"new_context", "version"},
    "context": {
        "new_page",
        "add_cookies", "clear_cookies", "cookies",
        "set_default_timeout", "set_default_navigation_timeout",
        "storage_state",
        "clear_permissions", "grant_permissions",
        "set_extra_http_headers",
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
        "pdf",
    },
}

# ⚠️ SEM THREADS aqui. Playwright sync + greenlet precisa disso.
# Como o Flask dev server vai rodar single-thread, o lock é mais para consistência.
import threading
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
        except PWError:
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


def _new_sid() -> str:
    return base64.urlsafe_b64encode(os.urandom(18)).decode("utf-8").rstrip("=")


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


@dataclass
class Session:
    sid: str
    context: BrowserContext
    page: Page
    created_at: float
    headless: bool
    browser_name: str
    log: RingLog = field(default_factory=RingLog)

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
    if op.startswith("__"):
        return False
    if op in {"close"}:
        return False
    return True


def _call_op(sid: str, t: str, op: str, args: List[Any], kwargs: Dict[str, Any], ret_mode: str) -> Any:
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
        raise TypeError("return=b64 requires bytes result")

    if ret_mode == "str":
        return str(result)

    # json
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


def _list_tree(root: str, max_depth: int = 4, max_items: int = 400) -> List[str]:
    res: List[str] = []
    root = os.path.abspath(root)
    for base, dirs, files in os.walk(root):
        depth = base[len(root):].count(os.sep)
        if depth > max_depth:
            dirs[:] = []
            continue
        for name in sorted(dirs + files):
            res.append(os.path.join(base, name))
            if len(res) >= max_items:
                return res
    return res


@APP.get("/")
def root():
    # evita 404 no healthcheck/probe da plataforma
    return jsonify({"ok": True, "hint": "use /health, /new, /do, /selftest"})


@APP.get("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "time": _now(),
            "port": PORT,
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
    return jsonify(
        {
            "ok": True,
            "time": _now(),
            "python_executable": sys.executable,
            "python_version": sys.version,
            "cwd": os.getcwd(),
            "pid": os.getpid(),
            "disk_root": _disk_info("/"),
            "disk_tmp": _disk_info("/tmp"),
            "cgroup": _cgroup_hints(),
            "browsers_path_exists": os.path.exists(BROWSERS_PATH),
            "browsers_path_list_sample": (os.listdir(BROWSERS_PATH)[:80] if os.path.isdir(BROWSERS_PATH) else []),
            "env_keys_sample": sorted(list(os.environ.keys()))[:80],
        }
    )


@APP.get("/browsers")
def browsers():
    if not os.path.isdir(BROWSERS_PATH):
        return jsonify({"ok": True, "exists": False, "path": BROWSERS_PATH, "tree": []})
    tree = _list_tree(BROWSERS_PATH, max_depth=5, max_items=600)
    return jsonify({"ok": True, "exists": True, "path": BROWSERS_PATH, "tree": tree})


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
        try:
            s = _get_sess(sid)
        except KeyError:
            return jsonify({"ok": False, "error": "session_not_found"}), 404
        return jsonify({"ok": True, "sid": sid, "logs": s.log.dump()})


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

    return jsonify({"ok": True, "closed": sid})


@APP.post("/stop")
def stop():
    _stop_all_noexcept()
    return jsonify({"ok": True, "status": "stopped"})


@APP.post("/do")
def do():
    """
    API flexível:
      - single:
        {"sid":"...","t":"page|context|browser","op":"goto","args":[...],"kwargs":{...},"return":"json|str|b64"}
      - multi:
        {"sid":"...","steps":[{...},{...}]}

    Regras:
      - "sid" pode ficar no topo e também dentro de step. Se step não tiver sid, usa o sid do topo.
      - Retorna traceback curto no erro.
    """
    body = _json_body()
    top_sid = (body.get("sid") or "").strip()

    def exec_step(step: Dict[str, Any]) -> Dict[str, Any]:
        t0 = _now()
        sid = (step.get("sid") or top_sid or "").strip()
        t = str(step.get("t", step.get("target", "page"))).strip()
        op = str(step.get("op", "")).strip()
        args = step.get("args", [])
        kwargs = step.get("kwargs", {})
        ret_mode = str(step.get("return", "json")).strip().lower()

        if not sid and t != "browser":
            raise ValueError("missing sid")
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
            "sid": sid,
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

    out: List[Dict[str, Any]] = []
    with _lock:
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                return jsonify({"ok": False, "error": f"step {i} must be object"}), 400
            try:
                out.append(exec_step(step))
            except Exception as e:
                out.append(
                    {
                        "ok": False,
                        "step_index": i,
                        "error": str(e),
                        "traceback": traceback.format_exc(limit=10),
                        "step": step,
                    }
                )
                break

    ok = all(x.get("ok") for x in out)
    return jsonify({"ok": ok, "results": out}), (200 if ok else 500)


@APP.post("/selftest")
def selftest():
    """
    Roda um teste completo no próprio runtime:
      - ensure chromium
      - new session
      - goto example.com
      - title
      - screenshot b64
      - close session
    """
    body = _json_body()
    browser = body.get("browser", "chromium")
    headless = bool(body.get("headless", True))
    url = body.get("url", "https://example.com")

    ensure_res = _start_playwright(headless=headless, browser=browser)
    if not ensure_res.get("ok"):
        return jsonify({"ok": False, "stage": "ensure", "ensure": ensure_res}), 500

    sid = None
    try:
        with _lock:
            sid = _new_sid()
            context = _browser.new_context()  # type: ignore
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

            page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
            title = page.title()
            shot = page.screenshot(full_page=True)
            shot_b64 = base64.b64encode(shot).decode("utf-8")

            logs_dump = sess.log.dump()[-50:]

            sess.close()
            SESSIONS.pop(sid, None)

        return jsonify(
            {
                "ok": True,
                "ensure": ensure_res,
                "url": url,
                "title": title,
                "screenshot_b64": shot_b64,
                "logs_tail": logs_dump,
            }
        )
    except Exception as e:
        tb = traceback.format_exc(limit=12)
        # cleanup best-effort
        with _lock:
            if sid and sid in SESSIONS:
                try:
                    SESSIONS[sid].close()
                except Exception:
                    pass
                SESSIONS.pop(sid, None)
        return jsonify({"ok": False, "stage": "run", "error": str(e), "traceback": tb}), 500


def _on_exit(*_args):
    _stop_all_noexcept()


atexit.register(_on_exit)
signal.signal(signal.SIGTERM, _on_exit)
signal.signal(signal.SIGINT, _on_exit)


# ----------------------------
# Client mode (pra testar do seu PC)
# ----------------------------
def _http_json(url: str, payload: Optional[Dict[str, Any]] = None, method: str = "POST") -> Tuple[int, Dict[str, Any]]:
    import urllib.request

    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            code = resp.getcode()
            body = resp.read().decode("utf-8", errors="ignore")
            try:
                return code, json.loads(body)
            except Exception:
                return code, {"_raw": body}
    except Exception as e:
        return 0, {"ok": False, "error": str(e)}


def client_demo(base: str):
    print("== client demo ==")
    c, h = _http_json(base + "/health", None, method="GET")
    print("health:", c, h)

    c, st = _http_json(base + "/selftest", {"browser": "chromium", "headless": True, "url": "https://example.com"})
    print("selftest:", c, {"ok": st.get("ok"), "title": st.get("title")})

    # salva screenshot local (se vier)
    if st.get("ok") and st.get("screenshot_b64"):
        try:
            img = base64.b64decode(st["screenshot_b64"])
            with open("selftest.png", "wb") as f:
                f.write(img)
            print("saved selftest.png")
        except Exception as e:
            print("failed saving image:", e)


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1].lower() == "client":
        base = sys.argv[2] if len(sys.argv) >= 3 else f"http://127.0.0.1:{PORT}"
        client_demo(base)
    else:
        # ✅ CRÍTICO: SEM threads e SEM reloader (senão dá greenlet/thread crash)
        APP.run(host=HOST, port=PORT, threaded=False, use_reloader=False)
