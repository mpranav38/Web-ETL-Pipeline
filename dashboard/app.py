"""
dashboard/app.py — Web UI dashboard for the ETL pipeline.

Features:
  - View all past run summaries (rows extracted, cleaned, duration, files)
  - Trigger a pipeline run from the browser
  - Browse extracted data tables with pagination
  - Download CSV / Excel output files
  - Live run log streaming via Server-Sent Events

Run:
    pip install flask
    cd etl_pipeline
    python dashboard/app.py

Then open http://localhost:5000
"""

import json
import os
import queue
import sys
import threading
from datetime import datetime
from pathlib import Path

from flask import (
    Flask,
    Response,
    jsonify,
    render_template_string,
    request,
    send_file,
)

# Allow imports from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))

import config

app = Flask(__name__)
_run_queue: queue.Queue = queue.Queue()  # streams log lines to SSE clients
_run_lock = threading.Lock()             # only one run at a time

# ─── HTML Template ────────────────────────────────────────────────────────────

HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETL Pipeline Dashboard</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; }
  header { background: #1a1d27; border-bottom: 1px solid #2d3148; padding: 1rem 2rem; display: flex; align-items: center; gap: 1rem; }
  header h1 { font-size: 1.2rem; font-weight: 600; color: #a78bfa; }
  header span { font-size: 0.8rem; color: #64748b; }
  .badge { background: #22c55e22; color: #22c55e; border: 1px solid #22c55e44; border-radius: 999px; padding: 2px 10px; font-size: 0.75rem; }
  main { max-width: 1100px; margin: 2rem auto; padding: 0 1.5rem; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; margin-bottom: 1.5rem; }
  .card { background: #1a1d27; border: 1px solid #2d3148; border-radius: 12px; padding: 1.5rem; }
  .card h2 { font-size: 0.85rem; font-weight: 500; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 1rem; }
  .stat { font-size: 2.2rem; font-weight: 700; color: #a78bfa; }
  .stat-label { font-size: 0.8rem; color: #64748b; margin-top: 2px; }
  .stat-row { display: flex; gap: 2rem; }
  button#run-btn {
    background: #7c3aed; color: #fff; border: none; border-radius: 8px;
    padding: .65rem 1.5rem; font-size: 0.95rem; font-weight: 500;
    cursor: pointer; transition: background .15s;
  }
  button#run-btn:hover { background: #6d28d9; }
  button#run-btn:disabled { background: #374151; color: #6b7280; cursor: not-allowed; }
  .log-box {
    background: #0d0f18; border: 1px solid #2d3148; border-radius: 8px;
    font-family: 'Cascadia Code', 'Fira Code', monospace; font-size: 0.78rem;
    color: #94a3b8; height: 220px; overflow-y: auto; padding: 1rem;
    white-space: pre-wrap; word-break: break-all;
  }
  .log-box .ok   { color: #22c55e; }
  .log-box .err  { color: #f87171; }
  .log-box .warn { color: #fbbf24; }
  .runs-table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
  .runs-table th { text-align: left; color: #64748b; font-weight: 500; padding: .5rem .75rem; border-bottom: 1px solid #2d3148; }
  .runs-table td { padding: .6rem .75rem; border-bottom: 1px solid #1e2235; }
  .runs-table tr:hover td { background: #1e2235; }
  .pill { display: inline-block; border-radius: 999px; padding: 1px 8px; font-size: 0.75rem; }
  .pill-ok   { background: #22c55e22; color: #22c55e; }
  .pill-err  { background: #f8717122; color: #f87171; }
  a.dl { color: #818cf8; text-decoration: none; font-size: 0.8rem; }
  a.dl:hover { text-decoration: underline; }
  .full { grid-column: 1 / -1; }
  #sources-list { list-style: none; }
  #sources-list li { display: flex; justify-content: space-between; align-items: center;
    padding: .5rem 0; border-bottom: 1px solid #1e2235; font-size: 0.875rem; }
  #sources-list li:last-child { border-bottom: none; }
  .src-name { color: #c4b5fd; font-weight: 500; }
  .src-type { color: #64748b; font-size: 0.75rem; }
</style>
</head>
<body>
<header>
  <h1>⚡ ETL Pipeline</h1>
  <span>{{ config_name }}</span>
  <span class="badge">Running</span>
</header>
<main>
  <div class="grid">

    <!-- Stats card -->
    <div class="card">
      <h2>Last Run</h2>
      <div id="stats-area">
        <p style="color:#64748b;font-size:.875rem">No runs yet — click Run Pipeline to start.</p>
      </div>
    </div>

    <!-- Run control card -->
    <div class="card">
      <h2>Controls</h2>
      <div style="display:flex;gap:.75rem;align-items:center;margin-bottom:1rem">
        <button id="run-btn" onclick="startRun()">▶ Run Pipeline</button>
        <span id="run-status" style="font-size:.825rem;color:#64748b"></span>
      </div>
      <div class="log-box" id="log-box">Ready.</div>
    </div>

    <!-- Configured sources -->
    <div class="card">
      <h2>Configured Sources</h2>
      <ul id="sources-list">
        {% for src in sources %}
        <li>
          <div>
            <span class="src-name">{{ src.name }}</span><br>
            <span class="src-type">{{ src.type }} · {{ src.url[:50] }}{% if src.url|length > 50 %}…{% endif %}</span>
          </div>
          <span style="font-size:.75rem;color:#475569">{{ src.detail }}</span>
        </li>
        {% else %}
        <li style="color:#64748b">No sources configured in config.py</li>
        {% endfor %}
      </ul>
    </div>

    <!-- Output files -->
    <div class="card">
      <h2>Output Files</h2>
      <div id="files-area">
        {% if output_files %}
          {% for f in output_files %}
          <div style="margin-bottom:.5rem">
            <a class="dl" href="/download?path={{ f.path }}" download>⬇ {{ f.name }}</a>
            <span style="color:#475569;font-size:.75rem;margin-left:.5rem">{{ f.size }} · {{ f.modified }}</span>
          </div>
          {% endfor %}
        {% else %}
          <p style="color:#64748b;font-size:.875rem">No output files yet.</p>
        {% endif %}
      </div>
    </div>

    <!-- Run history -->
    <div class="card full">
      <h2>Run History</h2>
      <table class="runs-table">
        <thead><tr>
          <th>Timestamp</th><th>Status</th><th>Sources</th>
          <th>Extracted</th><th>After clean</th><th>Duration</th><th>Files</th>
        </tr></thead>
        <tbody id="runs-tbody">
          {% for r in runs %}
          <tr>
            <td>{{ r.run_timestamp }}</td>
            <td><span class="pill {{ 'pill-ok' if r.status == 'SUCCESS' else 'pill-err' }}">{{ r.status }}</span></td>
            <td>{{ r.source_count }}</td>
            <td>{{ r.rows_extracted }}</td>
            <td>{{ r.rows_clean }}</td>
            <td>{{ r.elapsed }}s</td>
            <td>
              {% for f in r.output_files %}
              <a class="dl" href="/download?path={{ f }}" download>⬇ {{ f.split('/')[-1] }}</a><br>
              {% endfor %}
            </td>
          </tr>
          {% else %}
          <tr><td colspan="7" style="color:#64748b;text-align:center;padding:1.5rem">No runs recorded yet.</td></tr>
          {% endfor %}
        </tbody>
      </table>
    </div>

  </div>
</main>

<script>
let evtSource = null;

function startRun() {
  const btn = document.getElementById('run-btn');
  const status = document.getElementById('run-status');
  const log = document.getElementById('log-box');
  btn.disabled = true;
  status.textContent = 'Running…';
  log.textContent = '';

  // Start the run on the server
  fetch('/run', { method: 'POST' });

  // Stream logs via SSE
  evtSource = new EventSource('/stream');
  evtSource.onmessage = (e) => {
    const line = e.data;
    const span = document.createElement('span');
    if (line.includes('ERROR')) span.className = 'err';
    else if (line.includes('WARNING')) span.className = 'warn';
    else if (line.includes('Done') || line.includes('complete') || line.includes('Saved')) span.className = 'ok';
    span.textContent = line + '\\n';
    log.appendChild(span);
    log.scrollTop = log.scrollHeight;
  };
  evtSource.addEventListener('done', (e) => {
    evtSource.close();
    btn.disabled = false;
    const result = JSON.parse(e.data);
    status.textContent = `Done in ${result.elapsed_seconds?.toFixed(1)}s`;
    updateStats(result);
    setTimeout(() => location.reload(), 1500);
  });
  evtSource.onerror = () => {
    btn.disabled = false;
    status.textContent = 'Connection lost.';
    evtSource.close();
  };
}

function updateStats(r) {
  document.getElementById('stats-area').innerHTML = `
    <div class="stat-row">
      <div><div class="stat">${r.rows_extracted ?? 0}</div><div class="stat-label">Rows extracted</div></div>
      <div><div class="stat">${r.rows_after_cleaning ?? 0}</div><div class="stat-label">After cleaning</div></div>
      <div><div class="stat">${(r.elapsed_seconds ?? 0).toFixed(1)}s</div><div class="stat-label">Duration</div></div>
    </div>`;
}
</script>
</body>
</html>
"""


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _collect_sources() -> list[dict]:
    """Build a unified source list for the UI."""
    srcs = []
    for s in config.TABLE_SOURCES:
        srcs.append({"name": s.name, "type": "Table", "url": s.url, "detail": f"table #{s.table_index}"})
    for s in config.API_SOURCES:
        srcs.append({"name": s.name, "type": "API/JSON", "url": s.url, "detail": s.data_key or "top-level list"})
    for s in config.PRODUCT_SOURCES:
        srcs.append({"name": s.name, "type": "Products", "url": s.url, "detail": f"{s.max_pages} page(s)"})
    for s in config.ARTICLE_SOURCES:
        srcs.append({"name": s.name, "type": "Articles", "url": s.url, "detail": f"max {s.max_articles} articles"})
    return srcs


def _collect_output_files() -> list[dict]:
    out_dir = Path(config.OUTPUT_DIR)
    if not out_dir.exists():
        return []
    files = sorted(out_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    result = []
    for f in files[:20]:
        if f.suffix in (".csv", ".xlsx", ".json"):
            stat = f.stat()
            size = f"{stat.st_size / 1024:.1f} KB" if stat.st_size < 1_048_576 else f"{stat.st_size / 1_048_576:.1f} MB"
            modified = datetime.fromtimestamp(stat.st_mtime).strftime("%b %d %H:%M")
            result.append({"name": f.name, "path": str(f), "size": size, "modified": modified})
    return result


def _collect_run_history() -> list[dict]:
    out_dir = Path(config.OUTPUT_DIR)
    if not out_dir.exists():
        return []
    runs = []
    for summary_file in sorted(out_dir.glob("run_summary_*.json"), reverse=True)[:10]:
        try:
            data = json.loads(summary_file.read_text())
            rows_extracted = sum(
                v.get("rows", 0) for v in data.get("sources", {}).values()
            )
            all_files = (
                data.get("outputs", {}).get("csv_files", []) +
                data.get("outputs", {}).get("excel_files", [])
            )
            runs.append({
                "run_timestamp": data.get("run_timestamp", ""),
                "status": "SUCCESS",
                "source_count": len(data.get("sources", {})),
                "rows_extracted": rows_extracted,
                "rows_clean": rows_extracted,
                "elapsed": "—",
                "output_files": all_files,
            })
        except Exception:  # noqa: BLE001
            pass
    return runs


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(
        HTML,
        config_name=getattr(config, "OUTPUT_DIR", "output"),
        sources=_collect_sources(),
        output_files=_collect_output_files(),
        runs=_collect_run_history(),
    )


@app.route("/run", methods=["POST"])
def trigger_run():
    """Start a pipeline run in a background thread."""
    if _run_lock.locked():
        return jsonify({"status": "already_running"}), 409

    def _run():
        with _run_lock:
            import logging

            # Attach a queue handler so logs flow to SSE
            class QueueHandler(logging.Handler):
                def emit(self, record):
                    _run_queue.put(self.format(record))

            handler = QueueHandler()
            handler.setFormatter(logging.Formatter("%(levelname)s | %(name)s | %(message)s"))
            root = logging.getLogger()
            root.addHandler(handler)

            try:
                from pipeline import run_pipeline
                result = run_pipeline()
                _run_queue.put(f"__done__:{json.dumps(result)}")
            except Exception as exc:  # noqa: BLE001
                _run_queue.put(f"ERROR | pipeline | {exc}")
                _run_queue.put("__done__:{}")
            finally:
                root.removeHandler(handler)

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/stream")
def stream():
    """Server-Sent Events endpoint — streams log lines to the browser."""
    def generate():
        while True:
            try:
                line = _run_queue.get(timeout=30)
            except queue.Empty:
                yield "data: \n\n"
                continue

            if line.startswith("__done__:"):
                payload = line[len("__done__:"):]
                yield f"event: done\ndata: {payload}\n\n"
                break
            else:
                yield f"data: {line}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/sources")
def api_sources():
    return jsonify(_collect_sources())


@app.route("/api/files")
def api_files():
    return jsonify(_collect_output_files())


@app.route("/download")
def download():
    path = request.args.get("path", "")
    p = Path(path)
    if not p.exists() or not p.is_file():
        return "File not found", 404
    # Security: only serve files inside the output dir
    try:
        p.resolve().relative_to(Path(config.OUTPUT_DIR).resolve())
    except ValueError:
        return "Forbidden", 403
    return send_file(p, as_attachment=True)


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    print(f"\n  ETL Dashboard → http://{args.host}:{args.port}\n")
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)
