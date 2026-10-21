const http = require('http');
const https = require('https');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

let ffmpegPath = 'ffmpeg';
try {
  ffmpegPath = require('ffmpeg-static') || 'ffmpeg';
} catch (e) {}

function downloadToFile(urlStr, destPath) {
  return new Promise((resolve, reject) => {
    const mod = urlStr.startsWith('https') ? https : http;
    mod.get(urlStr, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return downloadToFile(res.headers.location, destPath).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
      const ws = fs.createWriteStream(destPath);
      res.pipe(ws);
      ws.on('finish', () => ws.close(() => resolve(destPath)));
      ws.on('error', reject);
    }).on('error', reject);
  });
}

async function uploadFileInChunks(filePath, callbackBaseUrl) {
  const buf = fs.readFileSync(filePath);
  const CHUNK_SIZE = 3 * 1024 * 1024; // 3MB per chunk (vượt mọi giới hạn Vercel 4.5MB & PHP post_max_size)
  const totalChunks = Math.max(1, Math.ceil(buf.length / CHUNK_SIZE));
  const sep = callbackBaseUrl.includes('?') ? '&' : '?';

  for (let i = 0; i < totalChunks; i++) {
    const slice = buf.subarray(i * CHUNK_SIZE, Math.min(buf.length, (i + 1) * CHUNK_SIZE));
    const url = `${callbackBaseUrl}${sep}chunk=${i}&total=${totalChunks}`;
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/octet-stream' },
      body: slice
    });
    if (!r.ok) {
      throw new Error(`Upload chunk ${i + 1}/${totalChunks} failed: HTTP ${r.status}`);
    }
  }
  return buf.length;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }
  if (req.method !== 'POST') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({
      ok: true,
      service: 'GenCPU H3 Pro Cloud Upscale Worker (Lanczos + CAS + Unsharp)',
      ffmpeg: ffmpegPath
    }));
  }

  const chunks = [];
  for await (const c of req) chunks.push(c);
  const body = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
  const {
    jobId = 'video',
    sourceUrl,
    uploadCallbackUrl,
    target = '2K',
    targetW = 2560,
    targetH = 1440
  } = body;

  if (!sourceUrl) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: false, error: 'Missing sourceUrl' }));
  }

  const id = `${jobId}_${Date.now()}`;
  const inPath = path.join(os.tmpdir(), `${id}_in.mp4`);
  const outPath = path.join(os.tmpdir(), `${id}_out_${target}.mp4`);
  const t0 = Date.now();

  try {
    await downloadToFile(sourceUrl, inPath);
    const vf = `scale=${targetW}:${targetH}:flags=lanczos+accurate_rnd+full_chroma_int,cas=0.65,unsharp=5:5:0.8:3:3:0.4`;
    const args = [
      '-y', '-hide_banner',
      '-threads', '2',
      '-i', inPath,
      '-vf', vf,
      '-c:v', 'libx264',
      '-profile:v', 'high',
      '-pix_fmt', 'yuv420p',
      '-crf', '17',
      '-preset', 'superfast',
      '-threads', '2',
      '-c:a', 'copy',
      '-movflags', '+faststart',
      outPath
    ];

    const code = await new Promise((resolve) => {
      const proc = spawn(ffmpegPath, args, {
        stdio: ['ignore', 'ignore', 'pipe'],
        env: { ...process.env, MALLOC_ARENA_MAX: '2' }
      });
      proc.on('close', c => resolve(c ?? -1));
      proc.on('error', () => resolve(-1));
    });

    if (code !== 0 || !fs.existsSync(outPath)) {
      throw new Error('FFmpeg upscale failed on worker');
    }

    if (uploadCallbackUrl) {
      const totalBytes = await uploadFileInChunks(outPath, uploadCallbackUrl);
      const sizeMb = Number((totalBytes / (1024 * 1024)).toFixed(2));
      const elapsedSec = Number(((Date.now() - t0) / 1000).toFixed(1));
      try { fs.unlinkSync(inPath); } catch (e) {}
      try { fs.unlinkSync(outPath); } catch (e) {}
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({
        ok: true,
        uploaded: true,
        target,
        width: targetW,
        height: targetH,
        size: totalBytes,
        sizeMb,
        elapsedSec
      }));
    }

    const stat = fs.statSync(outPath);
    res.writeHead(200, {
      'Content-Type': 'video/mp4',
      'Content-Length': stat.size
    });
    const rs = fs.createReadStream(outPath);
    rs.on('close', () => {
      try { fs.unlinkSync(inPath); } catch (e) {}
      try { fs.unlinkSync(outPath); } catch (e) {}
    });
    rs.pipe(res);
  } catch (err) {
    try { fs.unlinkSync(inPath); } catch (e) {}
    try { fs.unlinkSync(outPath); } catch (e) {}
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false, error: err.message }));
  }
}

module.exports = handler;

if (require.main === module) {
  const port = process.env.PORT || 4010;
  http.createServer(handler).listen(port, () => {
    console.log('GenCPU Cloud Upscale Worker listening on port', port);
  });
}
