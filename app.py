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

# Store active jobs: job_id -> {status, events_queue, videos, ...}
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
    """Run yt-dlp in a subprocess and stream progress via SSE."""
    job = jobs[job_id]
    q = job["events"]

    def send(event_type, data):
        q.put(f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n")

    job_dir = os.path.join(DOWNLOAD_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    # Step 1: get list of videos
    send("status", {"message": "Получаю список видео из showcase..."})

    list_cmd = ["yt-dlp", "--flat-playlist", "--dump-json", url]
    if password:
        list_cmd.extend(["--video-password", password])

    try:
        result = subprocess.run(list_cmd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired:
        send("job_error", {"message": "Таймаут при получении списка видео"})
        send("done", {"message": "Завершено с ошибкой"})
        job["status"] = "error"
        return

    if result.returncode != 0:
        err_detail = (result.stderr or result.stdout or "").strip()
        send("job_error", {"message": f"yt-dlp error: {err_detail[-500:]}"})
        send("done", {"message": "Завершено с ошибкой"})
        job["status"] = "error"
        return

    videos = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        try:
            info = json.loads(line)
            videos.append({
                "id": info.get("id", "unknown"),
                "title": info.get("title", "Без названия"),
            })
        except json.JSONDecodeError:
            continue

    if not videos:
        send("job_error", {"message": "Не найдено видео в showcase. Проверьте ссылку и пароль."})
        send("done", {"message": "Завершено с ошибкой"})
        job["status"] = "error"
        return

    job["total"] = len(videos)
    job["videos"] = videos
    send("playlist", {"total": len(videos), "videos": [v["title"] for v in videos]})

    # Step 2: download each video using playlist-items filter
    for idx, video in enumerate(videos):
        if job.get("cancelled"):
            send("status", {"message": "Загрузка отменена"})
            job["status"] = "cancelled"
            send("done", {"message": "Отменено"})
            return

        send("progress", {
            "current": idx + 1,
            "total": len(videos),
            "title": video["title"],
            "percent": 0,
        })

        output_template = os.path.join(job_dir, "%(title)s.%(ext)s")
        dl_cmd = [
            "yt-dlp",
            "--newline",
            "--no-warnings",
            "-o", output_template,
            "--restrict-filenames",
            "--playlist-items", str(idx + 1),
            url,
        ]
        if password:
            dl_cmd.extend(["--video-password", password])

        try:
            proc = subprocess.Popen(
                dl_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )

            for line in proc.stdout:
                line = line.strip()
                pct_match = re.search(r"\[download\]\s+([\d.]+)%", line)
                if pct_match:
                    pct = float(pct_match.group(1))
                    send("progress", {
                        "current": idx + 1,
                        "total": len(videos),
                        "title": video["title"],
                        "percent": pct,
                        "detail": line,
                    })
                elif "[download]" in line.lower() or "[merger]" in line.lower():
                    send("log", {"message": line})

            proc.wait(timeout=3600)

            if proc.returncode == 0:
                send("video_done", {
                    "current": idx + 1,
                    "total": len(videos),
                    "title": video["title"],
                })
                job["completed"] = job.get("completed", 0) + 1
            else:
                send("video_error", {
                    "current": idx + 1,
                    "total": len(videos),
                    "title": video["title"],
                    "message": "Ошибка при скачивании",
                })

        except Exception as e:
            send("video_error", {
                "current": idx + 1,
                "total": len(videos),
                "title": video["title"],
                "message": str(e),
            })

    send("done", {
        "message": f"Готово! Скачано {job.get('completed', 0)} из {len(videos)} видео.",
        "path": job_dir,
    })
    job["status"] = "done"


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
