# from fastapi import FastAPI, Form, BackgroundTasks, HTTPException
# from fastapi.responses import FileResponse, HTMLResponse
# import yt_dlp
# import os
# import uuid
# import asyncio
# from pathlib import Path

# app = FastAPI()
# DOWNLOAD_DIR = Path("downloads")
# DOWNLOAD_DIR.mkdir(exist_ok=True)

# # Simple homepage
# @app.get("/", response_class=HTMLResponse)
# async def home():
#     return """
#     <h1>Video Downloader</h1>
#     <form action="/download" method="post">
#         <input type="text" name="url" placeholder="Paste video URL" style="width:400px" required>
#         <button type="submit">Download in Highest Quality</button>
#     </form>
#     <p><small>High-res (bestvideo + bestaudio) • Server-side processing</small></p>
#     """

# @app.post("/download")
# async def start_download(url: str = Form(...), background_tasks: BackgroundTasks = None):
#     if not url.startswith(("http://", "https://")):
#         raise HTTPException(400, "Invalid URL")

#     task_id = str(uuid.uuid4())
#     output_path = DOWNLOAD_DIR / f"{task_id}.mp4"

#     ydl_opts = {
#         'format': 'bestvideo+bestaudio/best',
#         'outtmpl': str(output_path),
#         'noplaylist': True,
#         'quiet': True,
#         'no_warnings': True,
#         'merge_output_format': 'mp4',
#         'postprocessors': [{'key': 'FFmpegMetadata'}],
#     }

#     def download_video():
#         with yt_dlp.YoutubeDL(ydl_opts) as ydl:
#             ydl.download([url])

#     background_tasks.add_task(download_video)

#     return {
#         "message": "Download started – this may take a while for high-res videos",
#         "task_id": task_id,
#         "check_url": f"/status/{task_id}"
#     }

# @app.get("/status/{task_id}")
# async def check_status(task_id: str):
#     file_path = DOWNLOAD_DIR / f"{task_id}.mp4"
#     if file_path.exists():
#         return FileResponse(
#             file_path,
#             media_type="video/mp4",
#             filename="video.mp4",
#             headers={"Content-Disposition": f"attachment; filename=video_{task_id}.mp4"}
#         )
#     return {"status": "still processing or not found"}

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)


from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import os
import uuid
import threading
import time
from pathlib import Path
from typing import Optional
import json

app = FastAPI(title="VaultDL", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# In-memory task store: task_id -> task info dict
tasks: dict[str, dict] = {}


def sanitize_task_id(task_id: str) -> str:
    """Allow only alphanumeric and hyphens."""
    import re
    if not re.fullmatch(r"[a-f0-9\-]{36}", task_id):
        raise HTTPException(400, "Invalid task ID")
    return task_id


class ProgressLogger:
    # Captures yt-dlp progress and stores it in the tasks dict.
    def __init__(self, task_id: str):
        self.task_id = task_id

    def debug(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg):
        if self.task_id in tasks:
            tasks[self.task_id]["error"] = msg
            tasks[self.task_id]["status"] = "error"


def progress_hook(d, task_id: str):
    task = tasks.get(task_id)
    if not task:
        return

    status = d.get("status")
    if status == "downloading":
        total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
        downloaded = d.get("downloaded_bytes", 0)
        speed = d.get("speed") or 0
        eta = d.get("eta") or 0
        percent = round((downloaded / total) * 100, 1) if total else 0

        task["status"] = "downloading"
        task["percent"] = percent
        task["speed"] = _fmt_size(speed) + "/s" if speed else "—"
        task["eta"] = _fmt_seconds(eta)
        task["downloaded"] = _fmt_size(downloaded)
        task["total"] = _fmt_size(total) if total else "?"
        task["filename"] = d.get("filename", "")

    elif status == "finished":
        task["status"] = "processing"
        task["percent"] = 99
        task["speed"] = "—"
        task["eta"] = "Almost done…"


def _fmt_size(b: float) -> str:
    if b < 1024:
        return f"{b:.0f} B"
    elif b < 1024 ** 2:
        return f"{b/1024:.1f} KB"
    elif b < 1024 ** 3:
        return f"{b/1024**2:.1f} MB"
    return f"{b/1024**3:.2f} GB"


def _fmt_seconds(s: float) -> str:
    s = int(s)
    if s <= 0:
        return "—"
    m, sec = divmod(s, 60)
    return f"{m}m {sec:02d}s" if m else f"{sec}s"


def run_download(task_id: str, url: str, quality: str):
    task = tasks[task_id]
    output_path = DOWNLOAD_DIR / f"{task_id}.%(ext)s"

    format_map = {
        "best":   "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
        "1080":   "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]",
        "720":    "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720]",
        "audio":  "bestaudio[ext=m4a]/bestaudio",
    }
    fmt = format_map.get(quality, format_map["best"])
    is_audio = quality == "audio"
    ext = "m4a" if is_audio else "mp4"

    ydl_opts = {
        "format": fmt,
        "outtmpl": str(output_path),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "merge_output_format": ext,
        "logger": ProgressLogger(task_id),
        "progress_hooks": [lambda d: progress_hook(d, task_id)],
        "postprocessors": [{"key": "FFmpegMetadata"}],
        "socket_timeout": 30,
    }
    if is_audio:
        ydl_opts["postprocessors"].append({
            "key": "FFmpegExtractAudio",
            "preferredcodec": "m4a",
        })

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            task["title"] = info.get("title", "Unknown")
            task["thumbnail"] = info.get("thumbnail", "")
            task["duration"] = _fmt_seconds(info.get("duration", 0))
            task["uploader"] = info.get("uploader", "")
            task["status"] = "starting"
            ydl.download([url])

        # Find the output file
        for candidate in DOWNLOAD_DIR.glob(f"{task_id}.*"):
            if candidate.suffix in (".mp4", ".m4a", ".mkv", ".webm"):
                task["output_file"] = str(candidate)
                break

        task["status"] = "done"
        task["percent"] = 100
        task["eta"] = "Done!"
    except Exception as e:
        task["status"] = "error"
        task["error"] = str(e)


@app.get("/", response_class=HTMLResponse)
async def home():
    return HTMLResponse(content=HTML_PAGE)


@app.post("/api/download")
async def start_download(url: str = Form(...), quality: str = Form("best")):
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        raise HTTPException(400, "Invalid URL — must start with http:// or https://")
    if quality not in ("best", "1080", "720", "audio"):
        quality = "best"

    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "id": task_id,
        "url": url,
        "quality": quality,
        "status": "queued",
        "percent": 0,
        "speed": "—",
        "eta": "—",
        "downloaded": "—",
        "total": "—",
        "title": "",
        "thumbnail": "",
        "duration": "",
        "uploader": "",
        "output_file": "",
        "error": "",
        "started_at": time.time(),
    }

    thread = threading.Thread(target=run_download, args=(task_id, url, quality), daemon=True)
    thread.start()

    return {"task_id": task_id}


@app.get("/api/status/{task_id}")
async def get_status(task_id: str):
    task_id = sanitize_task_id(task_id)
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    # Return a clean snapshot (no file paths)
    return {k: v for k, v in task.items() if k != "output_file"}


@app.get("/api/file/{task_id}")
async def get_file(task_id: str):
    task_id = sanitize_task_id(task_id)
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    if task["status"] != "done":
        raise HTTPException(409, "File not ready yet")

    file_path = Path(task.get("output_file", ""))
    if not file_path.exists():
        raise HTTPException(404, "File missing on disk")

    ext = file_path.suffix
    media_type = "audio/mp4" if ext == ".m4a" else "video/mp4"
    safe_title = "".join(c for c in task.get("title", "video") if c.isalnum() or c in " -_")[:60]
    download_name = f"{safe_title}{ext}" if safe_title else f"download{ext}"

    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=download_name,
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )


@app.delete("/api/task/{task_id}")
async def delete_task(task_id: str):
    task_id = sanitize_task_id(task_id)
    task = tasks.pop(task_id, None)
    if task:
        fp = Path(task.get("output_file", ""))
        if fp.exists():
            fp.unlink(missing_ok=True)
    return {"deleted": True}


# ---------------------------------------------------------------------------
# Embedded HTML — served at /
# ---------------------------------------------------------------------------
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VaultDL</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:        #080b10;
    --surface:   #0e1420;
    --border:    #1d2535;
    --accent:    #4fffb0;
    --accent2:   #00c9ff;
    --danger:    #ff4f6a;
    --text:      #e8edf5;
    --muted:     #5a6480;
    --radius:    14px;
  }

  body {
    font-family: 'Syne', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 0 1rem 4rem;
    overflow-x: hidden;
  }

  /* Ambient glow */
  body::before {
    content: '';
    position: fixed;
    top: -200px; left: 50%;
    transform: translateX(-50%);
    width: 900px; height: 500px;
    background: radial-gradient(ellipse, rgba(79,255,176,.07) 0%, transparent 70%);
    pointer-events: none;
    z-index: 0;
  }

  /* ---------- Header ---------- */
  header {
    width: 100%; max-width: 680px;
    padding: 3.5rem 0 2rem;
    position: relative; z-index: 1;
  }
  .logo {
    display: flex; align-items: center; gap: .6rem;
    font-size: 1.7rem; font-weight: 800; letter-spacing: -.03em;
    color: var(--text);
    text-decoration: none;
  }
  .logo-icon {
    width: 36px; height: 36px;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
    border-radius: 10px;
    display: grid; place-items: center;
    font-size: .9rem;
    flex-shrink: 0;
  }
  .logo span { color: var(--accent); }
  .tagline {
    margin-top: .3rem;
    font-size: .8rem;
    color: var(--muted);
    font-family: 'DM Mono', monospace;
    letter-spacing: .04em;
  }

  /* ---------- Card ---------- */
  .card {
    width: 100%; max-width: 680px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 2rem;
    position: relative; z-index: 1;
    box-shadow: 0 24px 80px rgba(0,0,0,.4);
  }

  /* ---------- URL Input Row ---------- */
  .input-row {
    display: flex; gap: .6rem;
  }
  .url-wrap {
    flex: 1;
    position: relative;
    display: flex; align-items: center;
  }
  .url-icon {
    position: absolute; left: 14px;
    color: var(--muted);
    font-size: .95rem;
    pointer-events: none;
  }
  input[type="text"] {
    width: 100%;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 10px;
    color: var(--text);
    font-family: 'DM Mono', monospace;
    font-size: .82rem;
    padding: .85rem 1rem .85rem 2.4rem;
    outline: none;
    transition: border-color .2s, box-shadow .2s;
  }
  input[type="text"]:focus {
    border-color: var(--accent);
    box-shadow: 0 0 0 3px rgba(79,255,176,.1);
  }
  input[type="text"]::placeholder { color: var(--muted); }

  /* ---------- Quality Pills ---------- */
  .quality-row {
    display: flex; gap: .5rem;
    margin-top: 1rem; flex-wrap: wrap;
    align-items: center;
  }
  .quality-label {
    font-size: .7rem;
    color: var(--muted);
    font-family: 'DM Mono', monospace;
    letter-spacing: .06em;
    text-transform: uppercase;
    margin-right: .2rem;
  }
  .pill {
    padding: .35rem .85rem;
    border-radius: 99px;
    border: 1px solid var(--border);
    background: transparent;
    color: var(--muted);
    font-family: 'Syne', sans-serif;
    font-size: .75rem; font-weight: 600;
    cursor: pointer;
    transition: all .18s;
    letter-spacing: .02em;
  }
  .pill:hover { border-color: var(--accent); color: var(--accent); }
  .pill.active {
    background: var(--accent);
    border-color: var(--accent);
    color: #080b10;
  }

  /* ---------- Download Button ---------- */
  .btn-dl {
    background: var(--accent);
    color: #080b10;
    border: none;
    border-radius: 10px;
    font-family: 'Syne', sans-serif;
    font-size: .85rem; font-weight: 700;
    padding: 0 1.3rem;
    cursor: pointer;
    display: flex; align-items: center; gap: .4rem;
    white-space: nowrap;
    transition: opacity .18s, transform .12s;
    letter-spacing: .01em;
    min-height: 44px;
  }
  .btn-dl:hover:not(:disabled) { opacity: .88; transform: translateY(-1px); }
  .btn-dl:disabled { opacity: .45; cursor: not-allowed; }

  /* ---------- Divider ---------- */
  .divider {
    height: 1px; background: var(--border);
    margin: 1.6rem 0;
  }

  /* ---------- Tasks ---------- */
  #tasks { display: flex; flex-direction: column; gap: .9rem; }

  .task {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.1rem 1.2rem;
    animation: slideIn .3s ease;
    position: relative;
    overflow: hidden;
  }
  @keyframes slideIn {
    from { opacity: 0; transform: translateY(8px); }
    to   { opacity: 1; transform: translateY(0); }
  }

  /* Accent left bar */
  .task::before {
    content: '';
    position: absolute; left: 0; top: 0; bottom: 0;
    width: 3px;
    background: var(--accent);
    border-radius: 3px 0 0 3px;
    opacity: .5;
  }
  .task.error::before  { background: var(--danger); }
  .task.done::before   { background: var(--accent); opacity: 1; }

  .task-head {
    display: flex; align-items: flex-start; gap: .9rem;
  }
  .task-thumb {
    width: 72px; height: 44px;
    border-radius: 6px;
    object-fit: cover;
    background: var(--surface);
    border: 1px solid var(--border);
    flex-shrink: 0;
  }
  .task-thumb-ph {
    width: 72px; height: 44px;
    border-radius: 6px;
    background: var(--surface);
    border: 1px solid var(--border);
    flex-shrink: 0;
    display: grid; place-items: center;
    font-size: 1.2rem;
    color: var(--muted);
  }
  .task-meta { flex: 1; min-width: 0; }
  .task-title {
    font-size: .88rem; font-weight: 700;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .task-sub {
    font-size: .72rem; color: var(--muted);
    font-family: 'DM Mono', monospace;
    margin-top: .15rem;
  }
  .task-actions {
    display: flex; gap: .4rem; align-items: flex-start; flex-shrink: 0;
  }
  .btn-sm {
    padding: .3rem .7rem;
    border-radius: 7px;
    border: 1px solid var(--border);
    background: transparent;
    color: var(--text);
    font-family: 'Syne', sans-serif;
    font-size: .72rem; font-weight: 600;
    cursor: pointer;
    transition: all .15s;
  }
  .btn-sm:hover { border-color: var(--accent); color: var(--accent); }
  .btn-sm.primary {
    background: var(--accent);
    border-color: var(--accent);
    color: #080b10;
  }
  .btn-sm.primary:hover { opacity: .85; }
  .btn-sm.danger { border-color: var(--danger); color: var(--danger); }

  /* Progress bar */
  .progress-wrap {
    margin-top: .8rem;
  }
  .progress-track {
    height: 4px; background: var(--border);
    border-radius: 99px; overflow: hidden;
    margin-bottom: .5rem;
  }
  .progress-fill {
    height: 100%;
    background: linear-gradient(90deg, var(--accent), var(--accent2));
    border-radius: 99px;
    transition: width .4s ease;
  }
  .progress-fill.indeterminate {
    width: 40% !important;
    animation: slide 1.2s ease-in-out infinite;
  }
  @keyframes slide {
    0%   { transform: translateX(-100%); }
    100% { transform: translateX(350%); }
  }
  .progress-stats {
    display: flex; gap: 1rem; flex-wrap: wrap;
  }
  .stat {
    font-family: 'DM Mono', monospace;
    font-size: .68rem;
    color: var(--muted);
  }
  .stat strong { color: var(--text); font-weight: 500; }

  /* Status badge */
  .badge {
    display: inline-flex; align-items: center; gap: .3rem;
    padding: .15rem .55rem;
    border-radius: 99px;
    font-size: .65rem; font-weight: 700;
    text-transform: uppercase; letter-spacing: .06em;
  }
  .badge.queued    { background: rgba(90,100,128,.2); color: var(--muted); }
  .badge.starting  { background: rgba(0,201,255,.12); color: var(--accent2); }
  .badge.downloading { background: rgba(79,255,176,.1); color: var(--accent); }
  .badge.processing{ background: rgba(0,201,255,.12); color: var(--accent2); }
  .badge.done      { background: rgba(79,255,176,.15); color: var(--accent); }
  .badge.error     { background: rgba(255,79,106,.12); color: var(--danger); }

  .dot {
    width: 5px; height: 5px; border-radius: 50%;
    background: currentColor;
  }
  .dot.pulse { animation: pulse 1s ease-in-out infinite; }
  @keyframes pulse {
    0%,100% { opacity: 1; } 50% { opacity: .3; }
  }

  /* Error text */
  .error-msg {
    margin-top: .6rem;
    font-family: 'DM Mono', monospace;
    font-size: .72rem;
    color: var(--danger);
    background: rgba(255,79,106,.06);
    border: 1px solid rgba(255,79,106,.15);
    border-radius: 7px;
    padding: .5rem .75rem;
  }

  /* Empty state */
  .empty {
    text-align: center;
    padding: 2.5rem 0 .5rem;
    color: var(--muted);
    font-size: .82rem;
    line-height: 1.7;
  }
  .empty-icon { font-size: 2rem; margin-bottom: .6rem; }

  /* Toast */
  #toast {
    position: fixed; bottom: 2rem; left: 50%; transform: translateX(-50%);
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: .75rem 1.3rem;
    font-size: .82rem;
    display: none;
    z-index: 100;
    box-shadow: 0 8px 32px rgba(0,0,0,.4);
    animation: fadeUp .2s ease;
  }
  @keyframes fadeUp {
    from { opacity: 0; transform: translateX(-50%) translateY(6px); }
    to   { opacity: 1; transform: translateX(-50%) translateY(0); }
  }

  footer {
    margin-top: 3rem;
    font-size: .72rem;
    color: var(--muted);
    text-align: center;
    font-family: 'DM Mono', monospace;
    position: relative; z-index: 1;
  }
</style>
</head>
<body>

<header>
  <a href="/" class="logo">
    <div class="logo-icon">⬇</div>
    Vault<span>DL</span>
  </a>
  <p class="tagline">// high-res video &amp; audio downloader</p>
</header>

<div class="card">
  <!-- URL Row -->
  <div class="input-row">
    <div class="url-wrap">
      <span class="url-icon">🔗</span>
      <input type="text" id="urlInput" placeholder="Paste a YouTube, Vimeo, Twitter URL…" autocomplete="off" spellcheck="false">
    </div>
    <button class="btn-dl" id="dlBtn" onclick="startDownload()">
      <span>⬇</span> Grab
    </button>
  </div>

  <!-- Quality Pills -->
  <div class="quality-row">
    <span class="quality-label">Quality:</span>
    <button class="pill active" data-q="best" onclick="setQuality(this)">Best</button>
    <button class="pill" data-q="1080" onclick="setQuality(this)">1080p</button>
    <button class="pill" data-q="720" onclick="setQuality(this)">720p</button>
    <button class="pill" data-q="audio" onclick="setQuality(this)">🎵 Audio</button>
  </div>

  <div class="divider"></div>

  <!-- Task list -->
  <div id="tasks">
    <div class="empty" id="emptyState">
      <div class="empty-icon">📥</div>
      Paste a URL above and hit <strong>Grab</strong>.<br>
      Downloads appear here with live progress.
    </div>
  </div>
</div>

<div id="toast"></div>

<footer>Built on FastAPI + yt-dlp &nbsp;·&nbsp; VaultDL v2.0</footer>

<script>
let selectedQuality = 'best';
const pollers = {}; // task_id -> intervalId

function setQuality(el) {
  document.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
  el.classList.add('active');
  selectedQuality = el.dataset.q;
}

function toast(msg, duration = 2800) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.display = 'block';
  setTimeout(() => { t.style.display = 'none'; }, duration);
}

async function startDownload() {
  const urlEl = document.getElementById('urlInput');
  const url = urlEl.value.trim();
  if (!url) { toast('⚠ Paste a URL first.'); urlEl.focus(); return; }
  if (!url.startsWith('http://') && !url.startsWith('https://')) {
    toast('⚠ URL must start with http:// or https://'); return;
  }

  const btn = document.getElementById('dlBtn');
  btn.disabled = true;
  btn.innerHTML = '<span>⏳</span> Starting…';

  const fd = new FormData();
  fd.append('url', url);
  fd.append('quality', selectedQuality);

  try {
    const res = await fetch('/api/download', { method: 'POST', body: fd });
    const data = await res.json();
    if (!res.ok) { toast('❌ ' + (data.detail || 'Error starting download')); return; }
    urlEl.value = '';
    addTaskCard(data.task_id);
    pollTask(data.task_id);
  } catch (e) {
    toast('❌ Network error: ' + e.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<span>⬇</span> Grab';
  }
}

document.getElementById('urlInput').addEventListener('keydown', e => {
  if (e.key === 'Enter') startDownload();
});

// Paste & go
document.getElementById('urlInput').addEventListener('paste', () => {
  setTimeout(() => {
    const v = document.getElementById('urlInput').value.trim();
    if (v.startsWith('http://') || v.startsWith('https://')) startDownload();
  }, 50);
});

function addTaskCard(taskId) {
  const list = document.getElementById('tasks');
  const empty = document.getElementById('emptyState');
  if (empty) empty.remove();

  const card = document.createElement('div');
  card.className = 'task';
  card.id = `task-${taskId}`;
  card.innerHTML = taskHTML({ id: taskId, status: 'queued', percent: 0, title: 'Fetching info…', quality: selectedQuality });
  list.prepend(card);
}

function taskHTML(t) {
  const thumb = t.thumbnail
    ? `<img class="task-thumb" src="${escHtml(t.thumbnail)}" alt="" loading="lazy">`
    : `<div class="task-thumb-ph">🎬</div>`;

  const title = t.title || t.url || 'Loading…';
  const sub = [
    t.uploader ? `@${t.uploader}` : '',
    t.duration || '',
    qLabel(t.quality),
  ].filter(Boolean).join(' · ');

  const badge = badgeHTML(t.status);

  let progressBlock = '';
  if (['queued','starting','downloading','processing'].includes(t.status)) {
    const indeterminate = ['queued','starting','processing'].includes(t.status);
    progressBlock = `
      <div class="progress-wrap">
        <div class="progress-track">
          <div class="progress-fill ${indeterminate ? 'indeterminate' : ''}" style="width:${t.percent || 0}%"></div>
        </div>
        <div class="progress-stats">
          <span class="stat"><strong>${t.percent || 0}%</strong></span>
          ${t.speed && t.speed !== '—' ? `<span class="stat">⚡ <strong>${escHtml(t.speed)}</strong></span>` : ''}
          ${t.eta && t.eta !== '—' ? `<span class="stat">⏱ <strong>${escHtml(t.eta)}</strong></span>` : ''}
          ${t.downloaded && t.downloaded !== '—' ? `<span class="stat">${escHtml(t.downloaded)} / ${escHtml(t.total)}</span>` : ''}
        </div>
      </div>`;
  }

  let actions = '';
  if (t.status === 'done') {
    actions = `<a href="/api/file/${t.id}" download>
      <button class="btn-sm primary">⬇ Save</button></a>
      <button class="btn-sm danger" onclick="removeTask('${t.id}')">✕</button>`;
  } else if (t.status === 'error') {
    actions = `<button class="btn-sm danger" onclick="removeTask('${t.id}')">✕</button>`;
  }

  const errorBlock = t.status === 'error' && t.error
    ? `<div class="error-msg">⚠ ${escHtml(t.error)}</div>` : '';

  return `
    <div class="task-head">
      ${thumb}
      <div class="task-meta">
        <div class="task-title">${escHtml(title)}</div>
        <div class="task-sub">${escHtml(sub)} ${badge}</div>
      </div>
      <div class="task-actions">${actions}</div>
    </div>
    ${progressBlock}
    ${errorBlock}`;
}

function badgeHTML(status) {
  const pulse = ['downloading','starting','processing'].includes(status);
  const labels = { queued:'Queued', starting:'Starting', downloading:'Downloading', processing:'Muxing', done:'Done ✓', error:'Error' };
  return `<span class="badge ${status}"><span class="dot${pulse?' pulse':''}"></span>${labels[status]||status}</span>`;
}

function qLabel(q) {
  return { best:'Best quality', '1080':'1080p', '720':'720p', audio:'Audio only' }[q] || '';
}

function escHtml(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function pollTask(taskId) {
  if (pollers[taskId]) return;
  pollers[taskId] = setInterval(async () => {
    try {
      const res = await fetch(`/api/status/${taskId}`);
      if (!res.ok) return;
      const t = await res.json();
      updateTaskCard(t);
      if (['done','error'].includes(t.status)) {
        clearInterval(pollers[taskId]);
        delete pollers[taskId];
        if (t.status === 'done') toast('✅ Download ready — click Save!');
      }
    } catch {}
  }, 1000);
}

function updateTaskCard(t) {
  const card = document.getElementById(`task-${t.id}`);
  if (!card) return;
  card.className = `task ${t.status === 'error' ? 'error' : t.status === 'done' ? 'done' : ''}`;
  card.innerHTML = taskHTML(t);
}

async function removeTask(taskId) {
  clearInterval(pollers[taskId]);
  delete pollers[taskId];
  const card = document.getElementById(`task-${taskId}`);
  if (card) card.remove();
  try { await fetch(`/api/task/${taskId}`, { method: 'DELETE' }); } catch {}
  const list = document.getElementById('tasks');
  if (!list.querySelector('.task')) {
    list.innerHTML = `<div class="empty" id="emptyState">
      <div class="empty-icon">📥</div>
      Paste a URL above and hit <strong>Grab</strong>.<br>
      Downloads appear here with live progress.
    </div>`;
  }
}
</script>
</body>
</html>"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)


#     uvicorn main:app --reload