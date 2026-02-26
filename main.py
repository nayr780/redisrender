import os
import platform
import psutil
from flask import Flask, render_template_string

app = Flask(__name__)

TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>System Monitor</title>
    <meta http-equiv="refresh" content="5">
    <style>
        body { font-family: Arial; background:#111; color:#eee; padding:20px; }
        h1 { color:#00ffcc; }
        .box { background:#1e1e1e; padding:15px; margin-bottom:20px; border-radius:8px; }
        ul { max-height:300px; overflow:auto; }
        li { margin:3px 0; }
    </style>
</head>
<body>
    <h1>Flask System Monitor</h1>

    <div class="box">
        <h2>System Info</h2>
        <p><b>OS:</b> {{ system }}</p>
        <p><b>Hostname:</b> {{ hostname }}</p>
        <p><b>Processor:</b> {{ processor }}</p>
        <p><b>CPU Usage:</b> {{ cpu }}%</p>
        <p><b>RAM Usage:</b> {{ ram_used }} / {{ ram_total }} GB ({{ ram_percent }}%)</p>
        <p><b>Disk Usage:</b> {{ disk_used }} / {{ disk_total }} GB ({{ disk_percent }}%)</p>
    </div>

    <div class="box">
        <h2>Arquivos e Pastas</h2>
        <ul>
            {% for item in files %}
                <li>{{ item }}</li>
            {% endfor %}
        </ul>
    </div>

</body>
</html>
"""

@app.route("/")
def home():
    cpu = psutil.cpu_percent(interval=1)

    ram = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    files = os.listdir(".")

    return render_template_string(
        TEMPLATE,
        system=platform.system(),
        hostname=platform.node(),
        processor=platform.processor(),
        cpu=cpu,
        ram_used=round(ram.used / (1024**3), 2),
        ram_total=round(ram.total / (1024**3), 2),
        ram_percent=ram.percent,
        disk_used=round(disk.used / (1024**3), 2),
        disk_total=round(disk.total / (1024**3), 2),
        disk_percent=disk.percent,
        files=files
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
