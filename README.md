# T-WHALES 🐋 · AI Video-to-Video Pipeline

**Full AI pipeline: TikTok/Instagram URL → Faceswap → Kling Motion → Final MP4**

## Quick Start
```bash
pip install -r requirements.txt
# Add your reference face photo:
cp /path/to/your/face.jpg my_face.jpg
# Start the server:
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
# Open: http://localhost:8000
```

## Pipeline Steps
1. **Download** — yt-dlp (no watermark) + Cobalt + Apify fallback
2. **Extract** — FFmpeg: `frame.jpg` at timestamp + `audio.mp3`  
3. **Faceswap** — WaveSpeed Qwen-Image-2.0-Pro (async poll)
4. **Review** — Approve ✅ or Regenerate 🔄 (human-in-the-loop)
5. **Motion** — WaveSpeed Kling-v3.0-Pro motion-control (async poll)
6. **Post-process** — Film grain (anti-AI-detect) + audio remux

## API Keys
Hardcoded in `main.py` (or set as env vars):
- `WAVESPEED_KEY` — WaveSpeed API
- `APIFY_KEY` — Apify scraper fallback

## Deploy to Railway / Render
Connect this GitHub repo and it deploys automatically via `Procfile`.

## Requirements
- Python 3.10+
- `ffmpeg` (system: `apt install ffmpeg`)
- `yt-dlp` (auto-installed via requirements.txt)

## Tech Stack
| Component | Technology |
|-----------|-----------|
| Backend | FastAPI + uvicorn |
| Download | yt-dlp + Cobalt + Apify |
| Frame Extract | FFmpeg |
| Face Swap | WaveSpeed Qwen-Image-2.0-Pro |
| Motion | WaveSpeed Kling-v3.0-Pro |
| Post-Process | FFmpeg (grain + remux) |
| Frontend | Tailwind CSS + SSE |
