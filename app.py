"""
T-WHALES 🐋  ·  Video-to-Video AI Pipeline  ·  v3.0
=====================================================
FastAPI backend — complete, production-ready
"""
from __future__ import annotations

import asyncio
import aiohttp
import aiofiles
import base64
import json
import os
import shutil
import subprocess
import time
import uuid
import logging
from pathlib import Path
from typing import Optional, Any

from fastapi import FastAPI, BackgroundTasks, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from pydantic import BaseModel

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("pipeline")

WS_KEY    = "wsk_live_9fa52fRA-LqiRiMrTRJX_W_LcN7JhGo7Bza3vhmxI4M"
APIFY_KEY = "apify_api_nGZAclYjIoPbEcvTFFVQMcIKCurD2J1uWF9C"

WS_BASE     = "https://api.wavespeed.ai/api/v3"
QWEN_MODEL  = "wavespeed-ai/qwen-image-2.0-pro/edit"
KLING_MODEL = "kwaivgi/kling-v3.0-pro/motion-control"

JOBS_DIR = Path("jobs")
JOBS_DIR.mkdir(exist_ok=True)
FACE_REF = Path("my_face.jpg")

_jobs = {}

def get_job(jid):
    if jid not in _jobs: 
        raise HTTPException(404, f"Job {jid} not found")
    return _jobs[jid]

def upd(jid, **kw):
    _jobs[jid].update(kw)
    log.info(f"[{jid}] " + " | ".join(f"{k}={v}" for k,v in kw.items() if k != "base64"))

def jdir(jid):
    d = JOBS_DIR / jid
    d.mkdir(parents=True, exist_ok=True)
    return d

MIME = {"jpg":"image/jpeg","jpeg":"image/jpeg","png":"image/png","mp4":"video/mp4","mp3":"audio/mpeg"}

def to_b64(p):
    mime = MIME.get(p.suffix.lstrip("."), "application/octet-stream")
    return f"data:{mime};base64,{base64.b64encode(p.read_bytes()).decode()}"

def ffmpeg(*args, timeout=300):
    cmd = ["ffmpeg", "-y", *[str(a) for a in args]]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode: 
        raise RuntimeError(f"ffmpeg failed:\n{r.stderr[-800:]}")

def probe_duration(mp4):
    r = subprocess.run(
        ["ffprobe","-v","error","-show_entries","format=duration",
         "-of","default=noprint_wrappers=1:nokey=1", str(mp4)],
        capture_output=True, text=True)
    try: 
        return float(r.stdout.strip())
    except: 
        return 0.0

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
            if outs: 
                return outs[0]
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
            log.info(f"yt-dlp OK: {out.stat().st_size:,} bytes")
            return
        log.warning(f"yt-dlp failed: {r.stderr[-200:]}")
    except Exception as e: 
        log.warning(f"yt-dlp exception: {e}")
        
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
                log.info(f"Cobalt OK")
                return
    except Exception as e: 
        log.warning(f"Cobalt failed: {e}")
        
    is_tt = "tiktok.com" in url.lower()
    actor = "clockworks~tiktok-scraper" if is_tt else "apify~instagram-reel-scraper"
    inp = ({"postURLs":[url],"resultsType":"posts","resultsLimit":1} if is_tt 
           else {"username":[url],"resultsLimit":1})
    params = {"token": APIFY_KEY}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
        async with s.post(f"https://api.apify.com/v2/acts/{actor}/runs", params=params, json=inp) as r:
            text = await r.text()
            try: 
                run = json.loads(text).get("data", {})
            except: 
                raise RuntimeError(f"Apify start failed ({r.status}): {text[:200]}")
        run_id = run.get("id")
        if not run_id: 
            raise RuntimeError("Apify: no run_id")
        for _ in range(60):
            await asyncio.sleep(3)
            async with s.get(f"https://api.apify.com/v2/actor-runs/{run_id}", params=params) as r:
                rd = (await r.json()).get("data", {})
            if rd.get("status") == "SUCCEEDED": 
                break
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
        if video_url: 
            break
    if not video_url: 
        raise RuntimeError("Apify: no video URL in dataset")
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
        log.exception(f"[{jid}] error")
        upd(jid, status="error", error=str(e), msg=f"Error: {str(e)[:200]}")

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
        upd(jid, status="done", progress=100, msg="Pipeline complete!", final_url=f"/files/{jid}/final.mp4")
    except Exception as e:
        log.exception(f"[{jid}] motion error")
        upd(jid, status="error", error=str(e), msg=f"Error: {str(e)[:200]}")

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

# --- HTML CONTENT SEPARATED SAFELY FROM PYTHON CODESG ---
HTML_CONTENT = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>T-WHALES 🐋 · AI Video Pipeline</title>
<script src="https://cdn.tailwindcss.com"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght=300;400;500;600;700;800;900&display=swap" rel="stylesheet"/>
<style>
*{font-family:'Inter',system-ui,sans-serif;box-sizing:border-box}
body{background:#080810;color:#e4e4e7;min-height:100vh}
.bg-glow{position:fixed;inset:0;z-index:-1;overflow:hidden;pointer-events:none}
.bg-glow::before{content:'';position:absolute;width:70vw;height:70vh;top:-20%;left:-15%;background:radial-gradient(ellipse,rgba(139,92,246,.1),transparent 65%)}
.bg-glow::after{content:'';position:absolute;width:50vw;height:50vh;bottom:-15%;right:-10%;background:radial-gradient(ellipse,rgba(0,229,255,.06),transparent 65%)}
.g-card{background:rgba(24,24,27,.9);border:1px solid rgba(63,63,70,.5);border-radius:16px;backdrop-filter:blur(12px)}
.btn{display:inline-flex;align-items:center;gap:8px;padding:12px 24px;border-radius:12px;font-weight:600;font-size:14px;cursor:pointer;border:none;transition:all .2s;white-space:nowrap}
.btn-primary{background:linear-gradient(135deg,#6d28d9,#8b5cf6);color:#fff}
.btn-primary:hover:not(:disabled){background:linear-gradient(135deg,#5b21b6,#7c3aed);box-shadow:0 0 24px rgba(139,92,246,.4);transform:translateY(-1px)}
.btn-green{background:linear-gradient(135deg,#065f46,#047857);color:#d1fae5}
.btn-green:hover:not(:disabled){box-shadow:0 0 20px rgba(16,185,129,.3);transform:translateY(-1px)}
.btn-yellow{background:rgba(234,179,8,.12);border:1px solid rgba(234,179,8,.35);color:#fcd34d}
.btn-yellow:hover:not(:disabled){background:rgba(234,179,8,.2)}
.btn-ghost{background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);color:#a1a1aa}
.btn-ghost:hover:not(:disabled){background:rgba(255,255,255,.1);color:#e4e4e7}
.btn:disabled{opacity:.38;cursor:not-allowed;transform:none!important}
.inp{background:rgba(9,9,11,.8);border:1px solid rgba(63,63,70,.6);border-radius:10px;color:#e4e4e7;padding:12px 16px;font-size:14px;outline:none;transition:border-color .2s}
.inp:focus{border-color:rgba(139,92,246,.6);box-shadow:0 0 0 3px rgba(139,92,246,.1)}
.inp::placeholder{color:#52525b}
.step-dot{width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;transition:all .4s}
.step-dot.pending{background:rgba(63,63,70,.4);border:2px solid rgba(63,63,70,.6);color:#52525b}
.step-dot.active{background:rgba(139,92,246,.2);border:2px solid #8b5cf6;color:#a78bfa;animation:pulse-v 2s infinite}
.step-dot.done{background:rgba(16,185,129,.15);border:2px solid #10b981;color:#34d399}
.step-line{flex:1;height:2px;background:rgba(63,63,70,.4);transition:background .4s}
.step-line.done{background:linear-gradient(to right,#8b5cf6,#00e5ff)}
@keyframes pulse-v{0%,100%{box-shadow:0 0 16px rgba(139,92,246,.4)}50%{box-shadow:0 0 28px rgba(139,92,246,.7)}}
.prog-bar{height:3px;border-radius:999px;background:rgba(63,63,70,.4);overflow:hidden}
.prog-fill{height:100%;border-radius:999px;background:linear-gradient(90deg,#6d28d9,#8b5cf6,#00e5ff);background-size:200%;animation:prog-anim 2s linear infinite;transition:width .5s}
@keyframes prog-anim{0%{background-position:100%}100%{background-position:0%}}
.shimmer{background:linear-gradient(90deg,#18181b 25%,#27272a 50%,#18181b 75%);background-size:400%;animation:shimmer 1.5s infinite}
@keyframes shimmer{0%{background-position:100%}100%{background-position:0%}}
.spin{width:18px;height:18px;border:2px solid rgba(139,92,246,.3);border-top-color:#8b5cf6;border-radius:50%;animation:spin .7s linear infinite;flex-shrink:0}
@keyframes spin{to{transform:rotate(360deg)}}
.compare{display:grid;grid-template-columns:1fr 1fr;gap:16px}
@media(max-width:600px){.compare{grid-template-columns:1fr}}
.tag{display:inline-flex;align-items:center;gap:4px;padding:3px 10px;border-radius:999px;font-size:11px;font-weight:600}
.tag-v{background:rgba(139,92,246,.15);color:#a78bfa;border:1px solid rgba(139,92,246,.3)}
.tag-g{background:rgba(16,185,129,.1);color:#34d399;border:1px solid rgba(16,185,129,.25)}
.tag-y{background:rgba(234,179,8,.1);color:#fbbf24;border:1px solid rgba(234,179,8,.25)}
.fade-in{animation:fadeIn .4s ease both}
@keyframes fadeIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-thumb{background:#3f3f46;border-radius:3px}
</style>
</head>
<body>
<div class="bg-glow"></div>
<div class="max-w-4xl mx-auto px-4 py-10 pb-24">
  <header class="flex items-center justify-between mb-10">
    <div class="flex items-center gap-3">
      <span class="text-4xl">🐋</span>
      <div><h1 class="text-2xl font-black tracking-tight text-white">T-WHALES</h1><p class="text-xs text-zinc-500">AI Video-to-Video Pipeline v3.0</p></div>
    </div>
    <label class="btn btn-ghost text-xs cursor-pointer">
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/></svg>
      <span id="face-lbl">Upload Face</span>
      <input type="file" class="hidden" id="face-file" accept="image/*" onchange="uploadFace(this)"/>
    </label>
  </header>
  <div class="g-card p-6 mb-6">
    <label class="block text-xs font-semibold text-zinc-500 uppercase tracking-widest mb-3">🔗 Paste TikTok / Instagram Reel URL</label>
    <div class="flex gap-3 flex-wrap">
      <input id="url" type="url" class="inp flex-1 min-w-0" placeholder="https://www.tiktok.com/@creator/video/... or https://www.instagram.com/reel/..."/>
      <div class="flex items-center gap-2 flex-shrink-0">
        <label class="text-xs text-zinc-500">Frame @</label>
        <input id="frame-at" type="number" value="2" min="0" step="0.5" class="inp w-20 text-center"/>
        <label class="text-xs text-zinc-500">sec</label>
      </div>
      <button id="go-btn" onclick="startPipeline()" class="btn btn-primary">
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 3l14 9-14 9V3z"/></svg>
        Run Pipeline
      </button>
    </div>
  </div>
  <div id="pipeline" class="hidden fade-in">
    <div class="flex items-center mb-6 px-2">
      <div class="flex flex-col items-center gap-1"><div id="s1" class="step-dot pending">1</div><span class="text-[10px] text-zinc-600">Download</span></div>
      <div id="l12" class="step-line mx-2 flex-1"></div>
      <div class="flex flex-col items-center gap-1"><div id="s2" class="step-dot pending">2</div><span class="text-[10px] text-zinc-600">Extract</span></div>
      <div id="l23" class="step-line mx-2 flex-1"></div>
      <div class="flex flex-col items-center gap-1"><div id="s3" class="step-dot pending">3</div><span class="text-[10px] text-zinc-600">Faceswap</span></div>
      <div id="l34" class="step-line mx-2 flex-1"></div>
      <div class="flex flex-col items-center gap-1"><div id="s4" class="step-dot pending">4</div><span class="text-[10px] text-zinc-600">Motion</span></div>
      <div id="l45" class="step-line mx-2 flex-1"></div>
      <div class="flex flex-col items-center gap-1"><div id="s5" class="step-dot pending">5</div><span class="text-[10px] text-zinc-600">Final</span></div>
    </div>
    <div class="g-card p-4 mb-5">
      <div class="flex items-center gap-3 mb-3">
        <div id="spin" class="spin"></div>
        <p id="msg" class="text-sm font-medium text-white flex-1">Initializing...</p>
        <span id="pct" class="text-xs font-bold text-violet-400">0%</span>
      </div>
      <div class="prog-bar"><div id="bar" class="prog-fill" style="width:0%"></div></div>
    </div>
    <div id="err-box" class="hidden g-card p-4 mb-5" style="border-color:rgba(153,27,27,.4);background:rgba(127,29,29,.12)">
      <p class="text-sm font-semibold text-red-400">❌ Pipeline Error</p>
      <p id="err-txt" class="mt-2 text-xs text-red-300/70 font-mono break-all"></p>
      <button onclick="reset()" class="mt-3 text-xs text-red-400 hover:underline">← Start over</button>
    </div>
    <div id="frame-box" class="hidden mb-5 fade-in">
      <p class="text-xs font-semibold text-zinc-500 uppercase tracking-widest mb-3">🎞 Extracted Frame</p>
      <div class="rounded-xl overflow-hidden border border-zinc-800 inline-block"><img id="frame-img" class="max-w-xs w-full h-auto" alt="frame"/></div>
    </div>
    <div id="fs-box" class="hidden mb-5 fade-in">
      <div class="flex items-center justify-between mb-4">
        <p class="text-xs font-semibold text-zinc-500 uppercase tracking-widest">🎭 Faceswap Review</p>
        <span class="tag tag-v">WaveSpeed Qwen-2.0-Pro</span>
      </div>
      <div class="compare mb-4">
        <div class="g-card overflow-hidden">
          <div class="px-3 py-2 border-b border-zinc-800"><span class="text-xs text-zinc-500 font-medium">Original Frame</span></div>
          <img id="fs-orig" class="w-full h-auto" alt="original"/>
        </div>
        <div class="g-card overflow-hidden" style="border-color:rgba(139,92,246,.3)">
          <div class="flex items-center justify-between px-3 py-2 border-b" style="border-color:rgba(139,92,246,.2)">
            <span class="text-xs text-violet-400 font-medium">AI Faceswap</span>
            <span id="fs-status" class="tag tag-y">Processing...</span>
          </div>
          <div id="fs-loading" class="shimmer" style="aspect-ratio:9/16;min-height:180px"></div>
          <img id="fs-img" class="hidden w-full h-auto" alt="faceswap"/>
        </div>
      </div>
      <div id="action-btns" class="hidden flex gap-3 flex-wrap fade-in">
        <button id="approve-btn" onclick="approve()" class="btn btn-green flex-1">
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"/></svg>
          Approve — Generate Video ✅
        </button>
        <button id="regen-btn" onclick="regen()" class="btn btn-yellow">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>
          Regenerate 🔄
        </button>
      </div>
    </div>
    <div id="final-box" class="hidden mb-5 fade-in">
      <div class="g-card p-6" style="border-color:rgba(16,185,129,.3)">
        <div class="flex items-center justify-between mb-4">
          <h3 class="font-bold text-white">🎬 Final Video Ready!</h3>
          <span class="tag tag-g">✓ Pipeline Complete</span>
        </div>
        <div class="rounded-xl overflow-hidden border border-zinc-800 bg-black mb-4">
          <video id="final-vid" controls playsinline class="w-full max-h-[520px]">Your browser does not support video.</video>
        </div>
        <div class="flex gap-3 flex-wrap">
          <a id="dl-btn" href="#" download="twhales_final.mp4" class="btn btn-primary">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 4v12m0 0l-4-4m4 4l4-4M4 20h16"/></svg>
            Download Final MP4
          </a>
          <button onclick="reset()" class="btn btn-ghost">← New Pipeline</button>
        </div>
        <div class="grid grid-cols-3 gap-4 mt-5 pt-4 border-t border-zinc-800/60">
          <div class="text-center"><p class="text-xs text-zinc-600 mb-1">Processing Time</p><p id="meta-time" class="text-base font-bold text-violet-400">-</p></div>
          <div class="text-center"><p class="text-xs text-zinc-600 mb-1">Motion Model</p><p class="text-xs text-zinc-300">Kling v3.0 Pro<br/>Motion Control</p></div>
          <div class="text-center"><p class="text-xs text-zinc-600 mb-1">Post-Processing</p><p class="text-xs text-zinc-300">Film Grain + Audio Remux</p></div>
        </div>
      </div>
    </div>
  </div>
  <div id="empty" class="text-center py-16 text-zinc-600">
    <div class="text-6xl mb-4">🐋</div>
    <p class="text-lg font-semibold text-zinc-500">Paste a URL to start the AI pipeline</p>
    <p class="text-sm mt-2 mb-8">TikTok · Instagram Reels · Any public video URL</p>
    <div class="grid grid-cols-3 gap-4 max-w-xs mx-auto text-xs">
      <div class="g-card p-3 text-center"><div class="text-2xl mb-1">⬇️</div>Download<br/><span class="text-zinc-600">yt-dlp</span></div>
      <div class="g-card p-3 text-center"><div class="text-2xl mb-1">🎭</div>Faceswap<br/><span class="text-zinc-600">Qwen 2.0</span></div>
      <div class="g-card p-3 text-center"><div class="text-2xl mb-1">🎬</div>Motion<br/><span class="text-zinc-600">Kling v3</span></div>
    </div>
  </div>
</div>
<script>
"use strict";
let jobId=null,t0=null,es=null;
const $=id=>document.getElementById(id);
async function uploadFace(inp){const f=inp.files[0];if(!f)return;const fd=new FormData();fd.append("file",f);try{await fetch("/api/upload-face",{method:"POST",body:fd});$("face-lbl").textContent=f.name.slice(0,12)+"...";}catch(e){alert("Upload failed: "+e.message);}}
async function startPipeline(){const url=$("url").value.trim();if(!url){alert("Please paste a video URL");return;}const frameAt=parseFloat($("frame-at").value)||2;resetUI();$("go-btn").disabled=true;$("empty").classList.add("hidden");$("pipeline").classList.remove("hidden");t0=Date.now();try{const r=await fetch("/api/start",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({url,frame_at:frameAt})});if(!r.ok)throw new Error((await r.json()).detail||r.statusText);const d=await r.json();jobId=d.job_id;startSSE();}catch(e){showError(e.message);$("go-btn").disabled=false;}}
function startSSE(){if(es)es.close();es=new EventSource("/api/events/"+jobId);es.onmessage=e=>{try{updateUI(JSON.parse(e.data));}catch(_){}};es.onerror=()=>{es.close();const t=setInterval(async()=>{try{const r=await fetch("/api/status/"+jobId);const d=await r.json();updateUI(d);if(["done","error"].includes(d.status))clearInterval(t);}catch(_){}},2000);};}
const STATUS_STEP={queued:1,downloading:1,extracting:2,faceswapping:3,awaiting_approval:3,generating:4,postprocessing:4,done:5,error:0};
function updateUI(job){const p=job.progress||0,msg=job.msg||job.status;$("bar").style.width=p+"%";$("pct").textContent=p+"%";$("msg").textContent=msg;const sp=$("spin");if(job.status==="done"){sp.className="text-emerald-400 text-xl";sp.textContent="✓";}else if(job.status==="error"){sp.className="text-red-400 text-xl";sp.textContent="✗";}else{sp.className="spin";}const active=STATUS_STEP[job.status]||1;for(let i=1;i<=5;i++){const d=$("s"+i);d.classList.remove("pending","active","done");if(i<active){d.classList.add("done");d.textContent="✓";}else if(i===active){d.classList.add("active");d.textContent=i;}else{d.classList.add("pending");d.textContent=i;}if(i<5){const l=document.getElementById("l"+i+(i+1));if(l)l.classList.toggle("done",i<active);}}if(job.frame_url){$("frame-box").classList.remove("hidden");const fi=$("frame-img");if(!fi.src.includes(job.frame_url.split("?")[0]))fi.src=job.frame_url;$("fs-orig").src=job.frame_url;}if(job.faceswap_url||["faceswapping","awaiting_approval"].includes(job.status))$("fs-box").classList.remove("hidden");if(job.faceswap_url){const img=$("fs-img"),newSrc=job.faceswap_url;if(img.src!==location.origin+newSrc.split("?")[0]){$("fs-loading").classList.remove("hidden");img.classList.add("hidden");img.src=newSrc;img.onload=()=>{$("fs-loading").classList.add("hidden");img.classList.remove("hidden");};}$("fs-status").className="tag tag-g";$("fs-status").textContent="Ready";}if(job.status==="awaiting_approval"){$("action-btns").classList.remove("hidden");$("approve-btn").disabled=false;$("regen-btn").disabled=false;}else if(!["faceswapping","awaiting_approval"].includes(job.status))$("action-btns").classList.add("hidden");if(job.status==="error"){showError(job.error||job.msg);if(es)es.close();$("go-btn").disabled=false;}if(job.status==="done"&&job.final_url){$("final-box").classList.remove("hidden");const vid=$("final-vid");if(!vid.src){vid.src=job.final_url;$("dl-btn").href=job.final_url;}const secs=Math.round((Date.now()-t0)/1000);$("meta-time").textContent=secs>60?Math.floor(secs/60)+"m "+secs%60+"s":secs+"s";if(es)es.close();$("go-btn").disabled=false;}}
async function approve(){$("approve-btn").disabled=true;$("regen-btn").disabled=true;$("action-btns").classList.add("hidden");try{const r=await fetch("/api/approve/"+jobId,{method:"POST"});if(!r.ok)throw new Error((await r.json()).detail);startSSE();}catch(e){showError(e.message);}}
async function regen(){$("regen-btn").disabled=true;$("approve-btn").disabled=true;$("action-btns").classList.add("hidden");$("fs-img").classList.add("hidden");$("fs-loading").classList.remove("hidden");$("fs-status").className="tag tag-y";$("fs-status").textContent="Regenerating...";try{const r=await fetch("/api/regenerate/"+jobId,{method:"POST"});if(!r.ok)throw new Error((await r.json()).detail);startSSE();}catch(e){showError(e.message);}}
function showError(msg){$("err-box").classList.remove("hidden");$("err-txt").textContent=msg;}
function resetUI(){["err-box","frame-box","fs-box","final-box","action-btns"].forEach(id=>$(id).classList.add("hidden"));$("bar").style.width="0%";$("pct").textContent="0%";$("msg").textContent="Initializing...";$("spin").className="spin";$("fs-img").classList.add("hidden");$("fs-loading").classList.remove("hidden");$("fs-status").className="tag tag-y";$("fs-status").textContent="Processing...";$("final-vid").src="";$("dl-btn").href="#";for(let i=1;i<=5;i++){const d=$("s"+i);d.classList.remove("active","done");d.classList.add("pending");d.textContent=i;const l=document.getElementById("l"+i+(i+1));if(l)l.classList.remove("done");}}
function reset(){if(es)es.close();jobId=null;t0=null;$("go-btn").disabled=false;$("url").value="";$("pipeline").classList.add("hidden");$("empty").classList.remove("hidden");resetUI();}
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def root(): 
    return HTMLResponse(content=HTML_CONTENT)

@app.get("/health")
def health(): 
    return {"ok": True, "jobs": len(_jobs), "version": "3.0"}

@app.post("/api/start")
def start(req: StartReq, bg: BackgroundTasks):
    jid = uuid.uuid4().hex[:8]
    _jobs[jid] = {"id":jid,"url":req.url,"status":"queued","progress":0,"msg":"Queued...","created":int(time.time())}
    bg.add_task(run_pipeline, jid, req.url, req.frame_at)
    return {"job_id": jid}

@app.get("/api/status/{jid}")
def status(jid): 
    return get_job(jid)

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
    if not path.exists(): 
        raise HTTPException(404, f"File not found: {fname}")
    media = MIME.get(path.suffix.lstrip("."), "application/octet-stream")
    return FileResponse(str(path), media_type=media, headers={"Cache-Control":"no-cache,no-store"})

@app.get("/api/jobs")
def list_jobs(): 
    return sorted(_jobs.values(), key=lambda x: -x.get("created",0))

@app.delete("/api/jobs/{jid}")
def del_job(jid):
    _jobs.pop(jid, None)
    shutil.rmtree(JOBS_DIR/jid, ignore_errors=True)
    return {"ok":True}

@app.get("/api/events/{jid}")
async def sse(jid):
    async def gen():
        prev = ""
        for _ in range(400):
            j = _jobs.get(jid, {})
            txt = json.dumps(j)
            if txt != prev:
                yield f"data: {txt}\n\n"
                prev = txt
            if j.get("status") in ("done","error"): 
                break
            await asyncio.sleep(1.5)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
