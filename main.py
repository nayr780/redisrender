import os
import pwd
import time
import socket
import platform
import datetime
from pathlib import Path

import psutil
from flask import Flask, render_template_string, request

app = Flask(__name__)

# =========================
# CONFIG
# =========================
SCAN_ROOT = os.getenv("MONITOR_SCAN_ROOT", ".")
MAX_TREE_DEPTH = int(os.getenv("MONITOR_MAX_TREE_DEPTH", "2"))
MAX_ITEMS_PER_DIR = int(os.getenv("MONITOR_MAX_ITEMS_PER_DIR", "50"))
SHOW_HIDDEN = os.getenv("MONITOR_SHOW_HIDDEN", "0") == "1"
SHOW_FULL_ENV = os.getenv("MONITOR_SHOW_FULL_ENV", "0") == "1"
PORT = int(os.getenv("PORT", "5000"))

SENSITIVE_ENV_KEYS = [
    "KEY", "TOKEN", "SECRET", "PASSWORD", "PASS", "PWD",
    "DATABASE_URL", "DB_URL", "API_KEY", "ACCESS_KEY",
    "PRIVATE", "JWT", "AUTH", "SESSION", "COOKIE"
]

# =========================
# HELPERS
# =========================
def format_bytes(num):
    """Converte bytes para formato legível."""
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if num < 1024:
            return f"{num:.2f} {unit}"
        num /= 1024
    return f"{num:.2f} EB"


def format_seconds(seconds):
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def safe_env_value(key, value):
    """Mascara variáveis sensíveis, a menos que SHOW_FULL_ENV esteja habilitado."""
    if SHOW_FULL_ENV:
        return value

    key_upper = key.upper()
    if any(word in key_upper for word in SENSITIVE_ENV_KEYS):
        if not value:
            return "[MASKED]"
        if len(value) <= 6:
            return "[MASKED]"
        return value[:3] + "*" * (len(value) - 6) + value[-3:]
    return value


def get_system_info():
    boot_time = datetime.datetime.fromtimestamp(psutil.boot_time())
    now = datetime.datetime.now()

    try:
        current_user = pwd.getpwuid(os.getuid()).pw_name
    except Exception:
        current_user = os.getenv("USER", "unknown")

    return {
        "os": platform.system(),
        "os_release": platform.release(),
        "os_version": platform.version(),
        "hostname": socket.gethostname(),
        "fqdn": socket.getfqdn(),
        "processor": platform.processor() or "N/A",
        "machine": platform.machine(),
        "architecture": " / ".join(platform.architecture()),
        "python_version": platform.python_version(),
        "current_user": current_user,
        "cwd": os.getcwd(),
        "executable": os.path.abspath(__file__),
        "boot_time": boot_time.strftime("%Y-%m-%d %H:%M:%S"),
        "uptime": format_seconds(time.time() - psutil.boot_time()),
        "server_time": now.strftime("%Y-%m-%d %H:%M:%S"),
    }


def get_cpu_info():
    try:
        load_avg = os.getloadavg()
        load_avg = f"{load_avg[0]:.2f}, {load_avg[1]:.2f}, {load_avg[2]:.2f}"
    except Exception:
        load_avg = "N/A"

    return {
        "cpu_percent": psutil.cpu_percent(interval=1),
        "cpu_count_logical": psutil.cpu_count(logical=True),
        "cpu_count_physical": psutil.cpu_count(logical=False),
        "cpu_freq": psutil.cpu_freq().current if psutil.cpu_freq() else None,
        "load_avg": load_avg,
        "per_cpu": psutil.cpu_percent(interval=0.2, percpu=True),
    }


def get_memory_info():
    ram = psutil.virtual_memory()
    swap = psutil.swap_memory()
    return {
        "ram_total": format_bytes(ram.total),
        "ram_used": format_bytes(ram.used),
        "ram_available": format_bytes(ram.available),
        "ram_percent": ram.percent,
        "swap_total": format_bytes(swap.total),
        "swap_used": format_bytes(swap.used),
        "swap_percent": swap.percent,
    }


def get_disk_info():
    disks = []

    try:
        partitions = psutil.disk_partitions(all=False)
    except Exception:
        partitions = []

    for part in partitions:
        try:
            usage = psutil.disk_usage(part.mountpoint)
            disks.append({
                "device": part.device,
                "mountpoint": part.mountpoint,
                "fstype": part.fstype,
                "total": format_bytes(usage.total),
                "used": format_bytes(usage.used),
                "free": format_bytes(usage.free),
                "percent": usage.percent,
            })
        except PermissionError:
            disks.append({
                "device": part.device,
                "mountpoint": part.mountpoint,
                "fstype": part.fstype,
                "total": "Permission denied",
                "used": "-",
                "free": "-",
                "percent": "-",
            })
        except Exception as e:
            disks.append({
                "device": part.device,
                "mountpoint": part.mountpoint,
                "fstype": part.fstype,
                "total": f"Error: {e}",
                "used": "-",
                "free": "-",
                "percent": "-",
            })

    return disks


def get_network_info():
    interfaces = []
    addrs = psutil.net_if_addrs()
    stats = psutil.net_if_stats()

    for iface, iface_addrs in addrs.items():
        iface_data = {
            "name": iface,
            "is_up": stats.get(iface).isup if iface in stats else False,
            "speed": stats.get(iface).speed if iface in stats else None,
            "mtu": stats.get(iface).mtu if iface in stats else None,
            "addresses": [],
        }

        for addr in iface_addrs:
            family = str(addr.family)
            if "AF_INET" in family:
                fam = "IPv4"
            elif "AF_INET6" in family:
                fam = "IPv6"
            elif "AF_PACKET" in family or "AF_LINK" in family:
                fam = "MAC"
            else:
                fam = family

            iface_data["addresses"].append({
                "family": fam,
                "address": addr.address,
                "netmask": addr.netmask,
                "broadcast": addr.broadcast,
            })

        interfaces.append(iface_data)

    io = psutil.net_io_counters()
    return {
        "interfaces": interfaces,
        "io": {
            "bytes_sent": format_bytes(io.bytes_sent),
            "bytes_recv": format_bytes(io.bytes_recv),
            "packets_sent": io.packets_sent,
            "packets_recv": io.packets_recv,
        }
    }


def get_listening_ports():
    ports = []
    try:
        connections = psutil.net_connections(kind="inet")
    except Exception as e:
        return [{"error": str(e)}]

    for conn in connections:
        if conn.status == psutil.CONN_LISTEN:
            pid = conn.pid
            process_name = "N/A"
            try:
                if pid:
                    process_name = psutil.Process(pid).name()
            except Exception:
                pass

            local_ip = conn.laddr.ip if conn.laddr else "-"
            local_port = conn.laddr.port if conn.laddr else "-"

            ports.append({
                "ip": local_ip,
                "port": local_port,
                "pid": pid,
                "process": process_name,
                "family": str(conn.family),
                "type": str(conn.type),
            })

    ports.sort(key=lambda x: (str(x.get("ip")), int(x.get("port", 0)) if str(x.get("port")).isdigit() else 0))
    return ports


def build_tree(path_str, depth=0, max_depth=2):
    """Monta uma árvore simples de arquivos/pastas."""
    result = []

    try:
        path = Path(path_str)
        if not path.exists():
            return [f"[NOT FOUND] {path_str}"]

        entries = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
        count = 0

        for entry in entries:
            if not SHOW_HIDDEN and entry.name.startswith("."):
                continue

            prefix = "    " * depth
            if entry.is_dir():
                result.append(f"{prefix}📁 {entry.name}/")
                if depth < max_depth:
                    result.extend(build_tree(entry, depth + 1, max_depth))
            else:
                try:
                    size = entry.stat().st_size
                    result.append(f"{prefix}📄 {entry.name} ({format_bytes(size)})")
                except Exception:
                    result.append(f"{prefix}📄 {entry.name}")

            count += 1
            if count >= MAX_ITEMS_PER_DIR:
                result.append(f"{prefix}... limite de {MAX_ITEMS_PER_DIR} itens atingido")
                break

    except PermissionError:
        result.append(f"{'    '*depth}[PERMISSION DENIED] {path_str}")
    except Exception as e:
        result.append(f"{'    '*depth}[ERROR] {path_str}: {e}")

    return result


def get_environment_variables():
    envs = []
    for key in sorted(os.environ.keys(), key=lambda x: x.lower()):
        value = os.environ.get(key, "")
        envs.append({
            "key": key,
            "value": safe_env_value(key, value)
        })
    return envs


def get_top_processes(limit=15):
    procs = []
    for proc in psutil.process_iter(["pid", "name", "username", "cpu_percent", "memory_percent"]):
        try:
            info = proc.info
            procs.append({
                "pid": info["pid"],
                "name": info["name"] or "N/A",
                "username": info["username"] or "N/A",
                "cpu_percent": info["cpu_percent"] or 0.0,
                "memory_percent": round(info["memory_percent"] or 0.0, 2),
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    procs.sort(key=lambda x: (x["cpu_percent"], x["memory_percent"]), reverse=True)
    return procs[:limit]


# =========================
# TEMPLATE
# =========================
TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <title>System Monitor Completo</title>
    <meta http-equiv="refresh" content="8">
    <style>
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: Arial, sans-serif;
            background: #0f1115;
            color: #e8e8e8;
        }
        header {
            background: #151922;
            padding: 20px;
            border-bottom: 1px solid #2b3240;
            position: sticky;
            top: 0;
            z-index: 10;
        }
        header h1 {
            margin: 0;
            color: #55e6c1;
            font-size: 28px;
        }
        header p {
            margin: 6px 0 0;
            color: #aab3c5;
        }
        .container {
            padding: 20px;
            max-width: 1600px;
            margin: 0 auto;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
            gap: 18px;
        }
        .box {
            background: #171b24;
            border: 1px solid #2a3140;
            border-radius: 12px;
            padding: 16px;
            box-shadow: 0 4px 18px rgba(0,0,0,0.25);
        }
        .box h2 {
            margin-top: 0;
            color: #7ee7ff;
            border-bottom: 1px solid #2a3140;
            padding-bottom: 10px;
        }
        .kv {
            display: grid;
            grid-template-columns: 180px 1fr;
            gap: 8px 10px;
            font-size: 14px;
        }
        .kv div:nth-child(odd) {
            color: #9eb0c7;
            font-weight: bold;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }
        th, td {
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #2a3140;
            vertical-align: top;
        }
        th {
            color: #9fe6ff;
            background: #1c2230;
            position: sticky;
            top: 0;
        }
        .mono {
            font-family: Consolas, Monaco, monospace;
            font-size: 12px;
            white-space: pre-wrap;
            word-break: break-word;
        }
        .scroll {
            max-height: 360px;
            overflow: auto;
            border: 1px solid #263041;
            border-radius: 8px;
            padding: 10px;
            background: #10151e;
        }
        .tag {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 999px;
            background: #233044;
            color: #d2ebff;
            font-size: 12px;
            margin-right: 6px;
            margin-bottom: 6px;
        }
        .ok { color: #6bf28c; }
        .warn { color: #ffd166; }
        .bad { color: #ff6b6b; }
        a {
            color: #7ee7ff;
            text-decoration: none;
        }
        .footer-note {
            margin-top: 20px;
            color: #93a0b5;
            font-size: 12px;
        }
    </style>
</head>
<body>
<header>
    <h1>Flask System Monitor Completo</h1>
    <p>Atualização automática a cada 8 segundos</p>
</header>

<div class="container">
    <div class="grid">

        <div class="box">
            <h2>Resumo do Sistema</h2>
            <div class="kv">
                <div>OS</div><div>{{ system.os }} {{ system.os_release }}</div>
                <div>Versão</div><div>{{ system.os_version }}</div>
                <div>Hostname</div><div>{{ system.hostname }}</div>
                <div>FQDN</div><div>{{ system.fqdn }}</div>
                <div>Processor</div><div>{{ system.processor }}</div>
                <div>Machine</div><div>{{ system.machine }}</div>
                <div>Arquitetura</div><div>{{ system.architecture }}</div>
                <div>Python</div><div>{{ system.python_version }}</div>
                <div>Usuário</div><div>{{ system.current_user }}</div>
                <div>Diretório atual</div><div class="mono">{{ system.cwd }}</div>
                <div>Arquivo atual</div><div class="mono">{{ system.executable }}</div>
                <div>Boot time</div><div>{{ system.boot_time }}</div>
                <div>Uptime</div><div>{{ system.uptime }}</div>
                <div>Hora do servidor</div><div>{{ system.server_time }}</div>
            </div>
        </div>

        <div class="box">
            <h2>CPU</h2>
            <div class="kv">
                <div>Uso total</div>
                <div class="{% if cpu.cpu_percent >= 85 %}bad{% elif cpu.cpu_percent >= 60 %}warn{% else %}ok{% endif %}">
                    {{ cpu.cpu_percent }}%
                </div>

                <div>CPUs lógicas</div><div>{{ cpu.cpu_count_logical }}</div>
                <div>CPUs físicas</div><div>{{ cpu.cpu_count_physical }}</div>
                <div>Frequência</div><div>{{ "%.2f MHz"|format(cpu.cpu_freq) if cpu.cpu_freq else "N/A" }}</div>
                <div>Load average</div><div>{{ cpu.load_avg }}</div>
            </div>

            <h3>Uso por núcleo</h3>
            <div>
                {% for item in cpu.per_cpu %}
                    <span class="tag">CPU {{ loop.index0 }}: {{ item }}%</span>
                {% endfor %}
            </div>
        </div>

        <div class="box">
            <h2>Memória</h2>
            <div class="kv">
                <div>RAM total</div><div>{{ memory.ram_total }}</div>
                <div>RAM usada</div>
                <div class="{% if memory.ram_percent >= 85 %}bad{% elif memory.ram_percent >= 60 %}warn{% else %}ok{% endif %}">
                    {{ memory.ram_used }} ({{ memory.ram_percent }}%)
                </div>
                <div>RAM disponível</div><div>{{ memory.ram_available }}</div>
                <div>Swap total</div><div>{{ memory.swap_total }}</div>
                <div>Swap usada</div><div>{{ memory.swap_used }} ({{ memory.swap_percent }}%)</div>
            </div>
        </div>

        <div class="box">
            <h2>Rede</h2>
            <div class="kv">
                <div>Bytes enviados</div><div>{{ network.io.bytes_sent }}</div>
                <div>Bytes recebidos</div><div>{{ network.io.bytes_recv }}</div>
                <div>Pacotes enviados</div><div>{{ network.io.packets_sent }}</div>
                <div>Pacotes recebidos</div><div>{{ network.io.packets_recv }}</div>
            </div>

            <h3>Interfaces</h3>
            <div class="scroll">
                {% for iface in network.interfaces %}
                    <div style="margin-bottom:14px;">
                        <b>{{ iface.name }}</b>
                        -
                        {% if iface.is_up %}
                            <span class="ok">UP</span>
                        {% else %}
                            <span class="bad">DOWN</span>
                        {% endif %}
                        | speed: {{ iface.speed if iface.speed is not none else "N/A" }} Mbps
                        | mtu: {{ iface.mtu if iface.mtu is not none else "N/A" }}

                        <div class="mono" style="margin-top:6px;">
{% for addr in iface.addresses %}
[{{ addr.family }}] {{ addr.address }} | netmask={{ addr.netmask }} | broadcast={{ addr.broadcast }}
{% endfor %}
                        </div>
                    </div>
                {% endfor %}
            </div>
        </div>

        <div class="box">
            <h2>Portas Abertas (LISTEN)</h2>
            <div class="scroll">
                <table>
                    <thead>
                        <tr>
                            <th>IP</th>
                            <th>Porta</th>
                            <th>PID</th>
                            <th>Processo</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% if open_ports and open_ports[0].get("error") %}
                            <tr>
                                <td colspan="4" class="bad">{{ open_ports[0]["error"] }}</td>
                            </tr>
                        {% else %}
                            {% for p in open_ports %}
                                <tr>
                                    <td>{{ p.ip }}</td>
                                    <td>{{ p.port }}</td>
                                    <td>{{ p.pid }}</td>
                                    <td>{{ p.process }}</td>
                                </tr>
                            {% endfor %}
                            {% if not open_ports %}
                                <tr><td colspan="4">Nenhuma porta em LISTEN encontrada.</td></tr>
                            {% endif %}
                        {% endif %}
                    </tbody>
                </table>
            </div>
        </div>

        <div class="box">
            <h2>Discos / Partições</h2>
            <div class="scroll">
                <table>
                    <thead>
                        <tr>
                            <th>Device</th>
                            <th>Mount</th>
                            <th>FS</th>
                            <th>Total</th>
                            <th>Usado</th>
                            <th>Livre</th>
                            <th>%</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for d in disks %}
                            <tr>
                                <td>{{ d.device }}</td>
                                <td>{{ d.mountpoint }}</td>
                                <td>{{ d.fstype }}</td>
                                <td>{{ d.total }}</td>
                                <td>{{ d.used }}</td>
                                <td>{{ d.free }}</td>
                                <td class="{% if d.percent != '-' and d.percent >= 90 %}bad{% elif d.percent != '-' and d.percent >= 75 %}warn{% else %}ok{% endif %}">
                                    {{ d.percent }}
                                </td>
                            </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>

        <div class="box">
            <h2>Arquivos e Pastas</h2>
            <p><b>Raiz analisada:</b> <span class="mono">{{ scan_root }}</span></p>
            <p><b>Profundidade:</b> {{ max_tree_depth }} | <b>Itens por pasta:</b> {{ max_items_per_dir }}</p>
            <div class="scroll mono">{% for line in file_tree %}{{ line }}
{% endfor %}</div>
        </div>

        <div class="box">
            <h2>Variáveis de Ambiente</h2>
            <p>
                {% if show_full_env %}
                    <span class="warn">Modo completo ativado: valores sem máscara.</span>
                {% else %}
                    <span class="ok">Valores sensíveis mascarados.</span>
                {% endif %}
            </p>
            <div class="scroll">
                <table>
                    <thead>
                        <tr>
                            <th>Chave</th>
                            <th>Valor</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for env in env_vars %}
                            <tr>
                                <td>{{ env.key }}</td>
                                <td class="mono">{{ env.value }}</td>
                            </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>

        <div class="box">
            <h2>Top Processos</h2>
            <div class="scroll">
                <table>
                    <thead>
                        <tr>
                            <th>PID</th>
                            <th>Nome</th>
                            <th>Usuário</th>
                            <th>CPU %</th>
                            <th>Mem %</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for p in processes %}
                            <tr>
                                <td>{{ p.pid }}</td>
                                <td>{{ p.name }}</td>
                                <td>{{ p.username }}</td>
                                <td>{{ p.cpu_percent }}</td>
                                <td>{{ p.memory_percent }}</td>
                            </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>

    </div>

    <div class="footer-note">
        Dica: use as variáveis MONITOR_SCAN_ROOT, MONITOR_MAX_TREE_DEPTH, MONITOR_MAX_ITEMS_PER_DIR,
        MONITOR_SHOW_HIDDEN e MONITOR_SHOW_FULL_ENV para controlar a visualização.
    </div>
</div>
</body>
</html>
"""


# =========================
# ROUTE
# =========================
@app.route("/")
def home():
    system = get_system_info()
    cpu = get_cpu_info()
    memory = get_memory_info()
    disks = get_disk_info()
    network = get_network_info()
    open_ports = get_listening_ports()
    file_tree = build_tree(SCAN_ROOT, depth=0, max_depth=MAX_TREE_DEPTH)
    env_vars = get_environment_variables()
    processes = get_top_processes(limit=15)

    return render_template_string(
        TEMPLATE,
        system=system,
        cpu=cpu,
        memory=memory,
        disks=disks,
        network=network,
        open_ports=open_ports,
        file_tree=file_tree,
        env_vars=env_vars,
        processes=processes,
        scan_root=os.path.abspath(SCAN_ROOT),
        max_tree_depth=MAX_TREE_DEPTH,
        max_items_per_dir=MAX_ITEMS_PER_DIR,
        show_full_env=SHOW_FULL_ENV,
        request_ip=request.remote_addr,
    )


if __name__ == "__main__":
    print(f"[INFO] Iniciando monitor em 0.0.0.0:{PORT}")
    print(f"[INFO] Scan root: {os.path.abspath(SCAN_ROOT)}")
    print(f"[INFO] Max depth: {MAX_TREE_DEPTH}")
    print(f"[INFO] Show hidden: {SHOW_HIDDEN}")
    print(f"[INFO] Show full env: {SHOW_FULL_ENV}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
