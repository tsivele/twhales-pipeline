"""
T-WHALES 🐋  ·  Video-to-Video AI Pipeline  ·  v3.0
=====================================================
FastAPI backend — complete, production-ready
"""
from __future__ import annotations

import asyncio, aiohttp, aiofiles, base64, json, os, shutil
import subprocess, time, uuid, logging
from pathlib import Path
from typing import Optional, Any

from fastapi import FastAPI, BackgroundTasks, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("pipeline")

WS_KEY    = "wsk_live_9fa52fRA-LqiRiMrTRJX_W_LcN7JhGo7Bza3vhmxI4M"
APIFY_KEY = "apify_api_nGZAclYjIoPbEcvTFFVQMcIKCurD2J1uWF9C"

WS_BASE     = "https://api.wavespeed.ai/api/v3"
QWEN_MODEL  = "wavespeed-ai/qwen-image-2.0-pro/edit"
KLING_MODEL = "kwaivgi/kling-v3.0-pro/motion-control"

JOBS_DIR = Path("jobs");   JOBS_DIR.mkdir(exist_ok=True)
FACE_REF = Path("my_face.jpg")

_jobs = {}

def get_job(jid):
    if jid not in _jobs: raise HTTPException(404, f"Job {jid} not found")
    return _jobs[jid]

def upd(jid, **kw):
    _jobs[jid].update(kw)
    log.info(f"[{jid}] " + " | ".join(f"{k}={v}" for k,v in kw.items() if k != "base64"))

def jdir(jid):
    d = JOBS_DIR / jid;  d.mkdir(parents=True, exist_ok=True);  return d

MIME = {"jpg":"image/jpeg","jpeg":"image/jpeg","png":"image/png","mp4":"video/mp4","mp3":"audio/mpeg"}

def to_b64(p):
    mime = MIME.get(p.suffix.lstrip("."), "application/octet-stream")
    return f"data:{mime};base64,{base64.b64encode(p.read_bytes()).decode()}"

def ffmpeg(*args, timeout=300):
    cmd = ["ffmpeg", "-y", *[str(a) for a in args]]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode: raise RuntimeError(f"ffmpeg failed:
{r.stderr[-800:]}")

def probe_duration(mp4):
    r = subprocess.run(
        ["ffprobe","-v","error","-show_entries","format=duration",
         "-of","default=noprint_wrappers=1:nokey=1", str(mp4)],
        capture_output=True, text=True)
    try: return float(r.stdout.strip())
    except: return 0.0

WS_HDR = {"Authorization": f"Bearer {WS_KEY}", "Content-Type": "application/json"}
_http_timeout = aiohttp.ClientTimeout(total=60)

async def ws_submit(model, payload):
    async with aiohttp.ClientSession(timeout=_http_timeout) as s:
        async with s.post(f"{WS_BASE}/{model}", json=payload, headers=WS_HDR) as r:
            if r.status >= 400:
                txt = await r.text()
                raise RuntimeError(f"WaveSpeed {r.status}: {txt[:300]}")
            d = await r.json()
    return (d.get("data") or d).get("id") or d["id"]

async def ws_poll(pred_id, timeout_s=400):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
            async with s.get(f"{WS_BASE}/predictions/{pred_id}/result", headers=WS_HDR) as r:
                d = (await r.json()).get("data") or await r.json()
        status = d.get("status", "")
        if status == "completed":
            outs = d.get("outputs") or []
            if outs: return outs[0]
            raise RuntimeError("WaveSpeed: no outputs")
        if status in ("failed","cancelled"):
            raise RuntimeError(f"WaveSpeed {status}: {d.get('error','')}")
        await asyncio.sleep(3)
    raise TimeoutError(f"WaveSpeed timeout after {timeout_s}s")

async def dl_url(url, dest):
    hdrs = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180)) as s:
        async with s.get(url, headers=hdrs) as r:
            r.raise_for_status()
            async with aiofiles.open(dest, "wb") as f:
                async for chunk in r.content.iter_chunked(65536):
                    await f.write(chunk)

async def download_video(url, out):
    log.info(f"Downloading: {url}")
    try:
        r = subprocess.run([
            "yt-dlp","--no-warnings","--no-playlist",
            "-f","bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "--merge-output-format","mp4","--no-part","-o",str(out),url,
        ], capture_output=True, text=True, timeout=180)
        if r.returncode == 0 and out.exists() and out.stat().st_size > 50_000:
            log.info(f"yt-dlp OK: {out.stat().st_size:,} bytes"); return
        log.warning(f"yt-dlp failed: {r.stderr[-200:]}")
    except Exception as e: log.warning(f"yt-dlp exception: {e}")
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
            async with s.post("https://api.cobalt.tools/api/json",
                json={"url":url,"vQuality":"max","isNoTTWatermark":True},
                headers={"Accept":"application/json","Content-Type":"application/json"}) as r:
                d = await r.json()
        dl = d.get("url")
        if dl:
            await dl_url(dl, out)
            if out.exists() and out.stat().st_size > 50_000:
                log.info(f"Cobalt OK"); return
    except Exception as e: log.warning(f"Cobalt failed: {e}")
    is_tt = "tiktok.com" in url.lower()
    actor = "clockworks~tiktok-scraper" if is_tt else "apify~instagram-reel-scraper"
    inp = ({"postURLs":[url],"resultsType":"posts","resultsLimit":1} if is_tt
           else {"username":[url],"resultsLimit":1})
    params = {"token": APIFY_KEY}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
        async with s.post(f"https://api.apify.com/v2/acts/{actor}/runs",
                          params=params, json=inp) as r:
            text = await r.text()
            try: run = json.loads(text).get("data", {})
            except: raise RuntimeError(f"Apify start failed ({r.status}): {text[:200]}")
        run_id = run.get("id")
        if not run_id: raise RuntimeError("Apify: no run_id")
        for _ in range(60):
            await asyncio.sleep(3)
            async with s.get(f"https://api.apify.com/v2/actor-runs/{run_id}", params=params) as r:
                rd = (await r.json()).get("data", {})
            if rd.get("status") == "SUCCEEDED": break
            if rd.get("status") in ("FAILED","ABORTED","TIMED-OUT"):
                raise RuntimeError(f"Apify run {rd['status']}")
        ds_id = rd.get("defaultDatasetId")
        async with s.get(f"https://api.apify.com/v2/datasets/{ds_id}/items", params=params) as r:
            items = await r.json()
    video_url = None
    for item in items:
        video_url = (item.get("videoUrl") or item.get("video_url") or
                     (item.get("video") or {}).get("downloadAddr") or
                     item.get("webVideoUrl") or item.get("downloadUrl"))
        if video_url: break
    if not video_url: raise RuntimeError("Apify: no video URL in dataset")
    await dl_url(video_url, out)

async def run_pipeline(jid, url, frame_at):
    j = jdir(jid)
    try:
        upd(jid, status="downloading", progress=8, msg="Downloading video...")
        mp4 = j / "source.mp4"
        await download_video(url, mp4)
        upd(jid, status="extracting", progress=28, msg="Extracting audio & frame...")
        audio = j / "audio.mp3"
        ffmpeg("-i",mp4,"-vn","-q:a","0","-map","a:0",audio)
        frame = j / "frame.jpg"
        dur = probe_duration(mp4)
        ts = min(frame_at, max(0, dur-0.5)) if dur > 0 else frame_at
        ffmpeg("-ss",f"{ts:.2f}","-i",mp4,"-frames:v","1","-q:v","2",frame)
        upd(jid, status="faceswapping", progress=42, msg="Running AI faceswap...",
            frame_url=f"/files/{jid}/frame.jpg", video_dur=round(dur,1))
        ref = FACE_REF if FACE_REF.exists() else frame
        pred = await ws_submit(QWEN_MODEL, {
            "images": [to_b64(ref), to_b64(frame)],
            "prompt": "Seamlessly replace the face in image 2 with the face from image 1. Maintain original pose, expression, lighting and background. Photorealistic, no artifacts, 4K quality.",
            "seed": -1,
        })
        upd(jid, status="faceswapping", progress=58, msg=f"Generating faceswap... (pred={pred[:8]})")
        fs_url = await ws_poll(pred, 300)
        await dl_url(fs_url, j/"faceswap.jpg")
        upd(jid, status="awaiting_approval", progress=65, msg="Review the faceswap below",
            faceswap_url=f"/files/{jid}/faceswap.jpg?t={int(time.time())}", fs_remote=fs_url)
    except Exception as e:
        log.exception(f"[{jid}] error"); upd(jid, status="error", error=str(e), msg=f"Error: {str(e)[:200]}")

async def run_motion(jid):
    j = jdir(jid)
    try:
        upd(jid, status="generating", progress=72, msg="Running Kling motion control...")
        pred = await ws_submit(KLING_MODEL, {
            "image": to_b64(j/"faceswap.jpg"),
            "video": to_b64(j/"source.mp4"),
            "character_orientation": "video",
            "prompt": "Smooth cinematic motion, depth of field, warm color grading. Preserve original movement and scene. No text, no watermarks. Vertical 9:16.",
        })
        upd(jid, status="generating", progress=80, msg=f"Kling rendering... (pred={pred[:8]})")
        kling_url = await ws_poll(pred, 600)
        upd(jid, status="postprocessing", progress=90, msg="Post-processing: grain + audio...")
        await dl_url(kling_url, j/"kling.mp4")
        grain = j/"_grain.mp4"
        ffmpeg("-i",j/"kling.mp4","-vf","noise=alls=2:allf=t+u",
               "-c:v","libx264","-crf","18","-preset","fast","-an",grain,timeout=300)
        final = j/"final.mp4"
        ffmpeg("-i",grain,"-i",j/"audio.mp3","-c:v","copy","-c:a","aac","-b:a","192k","-shortest",final)
        grain.unlink(missing_ok=True)
        upd(jid, status="done", progress=100, msg="Pipeline complete!",
            final_url=f"/files/{jid}/final.mp4")
    except Exception as e:
        log.exception(f"[{jid}] motion error"); upd(jid, status="error", error=str(e), msg=f"Error: {str(e)[:200]}")

async def run_regen(jid):
    j = jdir(jid)
    try:
        ref = FACE_REF if FACE_REF.exists() else j/"frame.jpg"
        pred = await ws_submit(QWEN_MODEL, {
            "images": [to_b64(ref), to_b64(j/"frame.jpg")],
            "prompt": "Seamlessly replace the face in image 2 with the face from image 1. Photorealistic, no artifacts, 4K quality.",
            "seed": int(time.time()) % 999999,
        })
        upd(jid, status="faceswapping", progress=55, msg=f"Regenerating... (pred={pred[:8]})")
        fs_url = await ws_poll(pred, 300)
        await dl_url(fs_url, j/"faceswap.jpg")
        upd(jid, status="awaiting_approval", progress=65,
            faceswap_url=f"/files/{jid}/faceswap.jpg?t={int(time.time())}",
            msg="New faceswap ready for review")
    except Exception as e:
        upd(jid, status="error", error=str(e), msg=f"Regen error: {str(e)[:200]}")

app = FastAPI(title="T-WHALES Pipeline v3", docs_url="/docs")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class StartReq(BaseModel):
    url: str
    frame_at: float = 2.0

@app.get("/")
def root(): return FileResponse("index.html")

@app.get("/health")
def health(): return {"ok": True, "jobs": len(_jobs), "version": "3.0"}

@app.post("/api/start")
def start(req: StartReq, bg: BackgroundTasks):
    jid = uuid.uuid4().hex[:8]
    _jobs[jid] = {"id":jid,"url":req.url,"status":"queued","progress":0,"msg":"Queued...","created":int(time.time())}
    bg.add_task(run_pipeline, jid, req.url, req.frame_at)
    return {"job_id": jid}

@app.get("/api/status/{jid}")
def status(jid): return get_job(jid)

@app.post("/api/approve/{jid}")
def approve(jid, bg: BackgroundTasks):
    j = get_job(jid)
    if j["status"] != "awaiting_approval":
        raise HTTPException(400, f"Job is '{j['status']}', not 'awaiting_approval'")
    upd(jid, status="generating", progress=70, msg="Starting motion generation...")
    bg.add_task(run_motion, jid)
    return {"ok": True}

@app.post("/api/regenerate/{jid}")
def regenerate(jid, bg: BackgroundTasks):
    get_job(jid)
    upd(jid, status="faceswapping", progress=45, msg="Regenerating faceswap...", faceswap_url=None)
    bg.add_task(run_regen, jid)
    return {"ok": True}

@app.post("/api/upload-face")
async def upload_face(file: UploadFile = File(...)):
    data = await file.read()
    FACE_REF.write_bytes(data)
    return {"ok": True, "bytes": len(data)}

@app.get("/files/{jid}/{fname}")
def serve_file(jid, fname):
    fname = fname.split("?")[0]
    path = JOBS_DIR / jid / fname
    if not path.exists(): raise HTTPException(404, f"File not found: {fname}")
    media = MIME.get(path.suffix.lstrip("."), "application/octet-stream")
    return FileResponse(str(path), media_type=media, headers={"Cache-Control":"no-cache,no-store"})

@app.get("/api/jobs")
def list_jobs(): return sorted(_jobs.values(), key=lambda x: -x.get("created",0))

@app.delete("/api/jobs/{jid}")
def del_job(jid):
    _jobs.pop(jid, None); shutil.rmtree(JOBS_DIR/jid, ignore_errors=True); return {"ok":True}

@app.get("/api/events/{jid}")
async def sse(jid):
    async def gen():
        prev = ""
        for _ in range(400):
            j = _jobs.get(jid, {})
            txt = json.dumps(j)
            if txt != prev:
                yield f"data: {txt}\n\n"; prev = txt
            if j.get("status") in ("done","error"): break
            await asyncio.sleep(1.5)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
