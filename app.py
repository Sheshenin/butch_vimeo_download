import json
import os
import queue
import subprocess
import re
import threading
import uuid
import zipfile

from flask import Flask, render_template, request, jsonify, Response, send_from_directory

app = Flask(__name__)

DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", "3"))

jobs = {}
jobs_lock = threading.Lock()


def active_job_count():
    with jobs_lock:
        return sum(1 for j in jobs.values() if j["status"] == "running")


def parse_showcase_url(url):
    """Validate that the URL looks like a Vimeo showcase/album/channel."""
    patterns = [
        r"https?://vimeo\.com/showcase/(\d+)",
        r"https?://vimeo\.com/channels/(\w+)",
        r"https?://vimeo\.com/album/(\d+)",
    ]
    for p in patterns:
        m = re.match(p, url.strip())
        if m:
            return url.strip()
    return None


def run_download(job_id, url, password=None):
    """Run yt-dlp on the full showcase URL and parse progress from output."""
    job = jobs[job_id]
    q = job["events"]

    def send(event_type, data):
        q.put(f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n")

    job_dir = os.path.join(DOWNLOAD_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    send("status", {"message": "Запускаю скачивание showcase..."})

    output_template = os.path.join(job_dir, "%(title)s.%(ext)s")
    dl_cmd = [
        "yt-dlp",
        "--newline",
        "--restrict-filenames",
        "--verbose",
        "-o", output_template,
        url,
    ]
    if password:
        dl_cmd.extend(["--video-password", password])

    try:
        proc = subprocess.Popen(
            dl_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
    except Exception as e:
        send("job_error", {"message": f"Не удалось запустить yt-dlp: {e}"})
        send("done", {"message": "Завершено с ошибкой"})
        job["status"] = "error"
        return

    current_video = None
    video_num = 0
    completed = 0
    last_lines = []

    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        last_lines.append(line)
        if len(last_lines) > 50:
            last_lines.pop(0)

        if job.get("cancelled"):
            proc.terminate()
            send("status", {"message": "Загрузка отменена"})
            job["status"] = "cancelled"
            send("done", {"message": "Отменено"})
            return

        # Detect new video: [download] Downloading item N of M
        item_match = re.search(
            r"\[download\]\s+Downloading\s+item\s+(\d+)\s+of\s+(\d+)", line, re.IGNORECASE
        )
        if item_match:
            video_num = int(item_match.group(1))
            total = int(item_match.group(2))
            job["total"] = total
            send("status", {"message": f"Видео {video_num} из {total}"})
            send("log", {"message": line})
            continue

        # Detect video title: [download] Destination: /path/to/Title.ext
        dest_match = re.search(r"\[download\]\s+Destination:\s+(.+)", line)
        if dest_match:
            filepath = dest_match.group(1)
            basename = os.path.basename(filepath)
            name_without_ext = os.path.splitext(basename)[0]
            current_video = name_without_ext
            send("progress", {
                "current": video_num or (completed + 1),
                "total": job.get("total", 0),
                "title": current_video,
                "percent": 0,
            })
            send("log", {"message": line})
            continue

        # Detect video info line: [VimeoShowcase] ... Downloading JSON metadata
        # or [Vimeo] <id>: Downloading ...
        vimeo_match = re.search(r"\[(?:Vimeo|VimeoShowcase)[^\]]*\]\s+(.+)", line)
        if vimeo_match:
            send("log", {"message": line})
            continue

        # Progress: [download]  45.2% of ~100MiB at 5.0MiB/s
        pct_match = re.search(r"\[download\]\s+([\d.]+)%", line)
        if pct_match:
            pct = float(pct_match.group(1))
            send("progress", {
                "current": video_num or (completed + 1),
                "total": job.get("total", 0),
                "title": current_video or f"Видео {video_num or completed + 1}",
                "percent": pct,
                "detail": line,
            })
            continue

        # Download complete: [download] 100% ...  or already downloaded
        if re.search(r"\[download\]\s+100%", line) or "has already been downloaded" in line:
            if "already been downloaded" in line:
                send("log", {"message": line})
            completed += 1
            job["completed"] = completed
            send("video_done", {
                "current": video_num or completed,
                "total": job.get("total", 0),
                "title": current_video or f"Видео {completed}",
            })
            current_video = None
            continue

        # Merger
        if "[merger]" in line.lower():
            send("log", {"message": line})
            # After merge, that video is done
            completed += 1
            job["completed"] = completed
            send("video_done", {
                "current": video_num or completed,
                "total": job.get("total", 0),
                "title": current_video or f"Видео {completed}",
            })
            current_video = None
            continue

        # Errors from yt-dlp
        if "error" in line.lower():
            send("log", {"message": line})
            continue

        # Log everything else so we can see what yt-dlp is doing
        send("log", {"message": line})

    proc.wait(timeout=7200)

    if proc.returncode == 0:
        # Count actual files downloaded
        files_count = sum(1 for f in os.listdir(job_dir) if os.path.isfile(os.path.join(job_dir, f)))
        send("done", {
            "message": f"Готово! Скачано {files_count} видео.",
            "path": job_dir,
        })
        job["status"] = "done"
    else:
        err_tail = "\n".join(last_lines[-10:])
        send("job_error", {"message": f"yt-dlp завершился с ошибкой:\n{err_tail}"})
        send("done", {"message": "Завершено с ошибкой"})
        job["status"] = "error"


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/download", methods=["POST"])
def start_download():
    data = request.get_json()
    url = (data.get("url") or "").strip()
    password = (data.get("password") or "").strip() or None

    validated = parse_showcase_url(url)
    if not validated:
        return jsonify({"error": "Неверная ссылка. Поддерживаются: vimeo.com/showcase/*, vimeo.com/album/*, vimeo.com/channels/*"}), 400

    if active_job_count() >= MAX_CONCURRENT_JOBS:
        return jsonify({"error": f"Сервер занят. Максимум {MAX_CONCURRENT_JOBS} одновременных загрузок. Попробуйте позже."}), 429

    job_id = uuid.uuid4().hex[:12]
    with jobs_lock:
        jobs[job_id] = {
            "status": "running",
            "events": queue.Queue(),
            "total": 0,
            "completed": 0,
            "videos": [],
        }

    thread = threading.Thread(target=run_download, args=(job_id, validated, password), daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/api/stream/<job_id>")
def stream(job_id):
    if job_id not in jobs:
        return jsonify({"error": "Job not found"}), 404

    def generate():
        q = jobs[job_id]["events"]
        while True:
            try:
                msg = q.get(timeout=30)
                yield msg
                if "event: done" in msg:
                    break
            except queue.Empty:
                yield "event: ping\ndata: {}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/cancel/<job_id>", methods=["POST"])
def cancel(job_id):
    if job_id in jobs:
        jobs[job_id]["cancelled"] = True
        return jsonify({"ok": True})
    return jsonify({"error": "Job not found"}), 404


@app.route("/api/files/<job_id>")
def list_files(job_id):
    job_dir = os.path.join(DOWNLOAD_DIR, job_id)
    if not os.path.isdir(job_dir):
        return jsonify({"files": []})
    files = []
    for f in sorted(os.listdir(job_dir)):
        fp = os.path.join(job_dir, f)
        if os.path.isfile(fp):
            files.append({"name": f, "size": os.path.getsize(fp)})
    return jsonify({"files": files})


@app.route("/api/zip/<job_id>")
def download_zip(job_id):
    """Create and serve a ZIP archive of all downloaded videos."""
    job_dir = os.path.join(DOWNLOAD_DIR, job_id)
    if not os.path.isdir(job_dir):
        return jsonify({"error": "Not found"}), 404

    zip_path = os.path.join(DOWNLOAD_DIR, f"{job_id}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
        for f in sorted(os.listdir(job_dir)):
            fp = os.path.join(job_dir, f)
            if os.path.isfile(fp):
                zf.write(fp, f)

    return send_from_directory(DOWNLOAD_DIR, f"{job_id}.zip", as_attachment=True,
                               download_name="vimeo_showcase.zip")


@app.route("/downloads/<job_id>/<filename>")
def download_file(job_id, filename):
    job_dir = os.path.join(DOWNLOAD_DIR, job_id)
    return send_from_directory(job_dir, filename, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000, threaded=True)
