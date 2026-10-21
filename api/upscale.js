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

function downloadToFile(urlStr, destPath, timeoutMs = 25000) {
  return new Promise((resolve, reject) => {
    const mod = urlStr.startsWith('https') ? https : http;
    const req = mod.get(urlStr, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0' },
      timeout: timeoutMs
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return downloadToFile(res.headers.location, destPath, timeoutMs).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const ws = fs.createWriteStream(destPath);
      res.pipe(ws);
      ws.on('finish', () => ws.close(() => resolve(destPath)));
      ws.on('error', reject);
    });
    req.on('timeout', () => { req.destroy(new Error('Download timeout')); });
    req.on('error', reject);
  });
}

async function downloadParallelRanges(urlStr, destPath) {
  const headRes = await fetch(urlStr, { method: 'HEAD' });
  const totalSize = Number(headRes.headers.get('content-length') || 0);
  if (!headRes.ok || totalSize < 500000) {
    return downloadToFile(urlStr, destPath, 25000);
  }
  const PARTS = 8;
  const partSize = Math.ceil(totalSize / PARTS);
  const buffers = await Promise.all(
    Array.from({ length: PARTS }, async (_, idx) => {
      const start = idx * partSize;
      const end = Math.min(totalSize - 1, (idx + 1) * partSize - 1);
      const r = await fetch(urlStr, {
        headers: { Range: `bytes=${start}-${end}` }
      });
      if (!r.ok && r.status !== 206) throw new Error(`Range ${idx} HTTP ${r.status}`);
      return Buffer.from(await r.arrayBuffer());
    })
  );
  fs.writeFileSync(destPath, Buffer.concat(buffers));
  return destPath;
}

function probeVideoDimensions(filePath) {
  return new Promise((resolve) => {
    const proc = spawn(ffmpegPath, ['-hide_banner', '-i', filePath], {
      stdio: ['ignore', 'ignore', 'pipe'],
      env: { ...process.env, MALLOC_ARENA_MAX: '2' }
    });
    let errBuf = '';
    proc.stderr.on('data', d => { errBuf += d.toString(); });
    proc.on('close', () => {
      const m = errBuf.match(/Video:.*?\s(\d{3,5})x(\d{3,5})/);
      if (m) {
        resolve({ width: parseInt(m[1], 10), height: parseInt(m[2], 10) });
      } else {
        resolve({ width: 1920, height: 1080 });
      }
    });
    proc.on('error', () => resolve({ width: 1920, height: 1080 }));
  });
}

async function uploadFileInChunks(filePath, callbackBaseUrl) {
  const buf = fs.readFileSync(filePath);
  const CHUNK_SIZE = 3800 * 1024; // ~3.8MB per chunk (fits under 4.5MB limit, fewer chunks)
  const totalChunks = Math.max(1, Math.ceil(buf.length / CHUNK_SIZE));
  const sep = callbackBaseUrl.includes('?') ? '&' : '?';

  // Upload up to 8 chunks in a single parallel wave for maximum throughput
  const CONCURRENCY = 8;
  let assembledAutomatically = false;
  for (let start = 0; start < totalChunks; start += CONCURRENCY) {
    const batch = [];
    for (let i = start; i < Math.min(totalChunks, start + CONCURRENCY); i++) {
      const slice = buf.subarray(i * CHUNK_SIZE, Math.min(buf.length, (i + 1) * CHUNK_SIZE));
      const url = `${callbackBaseUrl}${sep}chunk=${i}&total=${totalChunks}&parallel=1`;
      batch.push(
        fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/octet-stream' },
          body: slice
        }).then(async r => {
          if (!r.ok) throw new Error(`Upload chunk ${i + 1}/${totalChunks} failed: HTTP ${r.status}`);
          const j = await r.json().catch(() => null);
          if (j && j.done) assembledAutomatically = true;
        })
      );
    }
    await Promise.all(batch);
  }

  // Fallback assemble call if not already auto-assembled on final chunk
  if (!assembledAutomatically) {
    const assembleUrl = `${callbackBaseUrl}${sep}assemble=1&total=${totalChunks}`;
    const finRes = await fetch(assembleUrl, { method: 'POST' });
    if (!finRes.ok) {
      throw new Error(`Assemble failed: HTTP ${finRes.status}`);
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
    target: rawTarget = '2K',
    force = false
  } = body;

  if (!sourceUrl) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: false, error: 'Missing sourceUrl' }));
  }

  const targetStr = String(rawTarget || '2K').trim().toUpperCase();
  let base = [2560, 1440];
  let targetLabel = '2K';
  if (targetStr.includes('1080')) {
    base = [1920, 1080];
    targetLabel = '1080p';
  } else if (targetStr.includes('4K') || targetStr.includes('2160')) {
    base = [3840, 2160];
    targetLabel = '4K';
  }

  // Kiểm tra nhanh nếu trên máy chủ chính đã có sẵn bản Upscale này -> trả về ngay lập tức (0.1s)
  if (!force && jobId && jobId !== 'video') {
    try {
      const checkUrl = `https://gencpu.com/api/v1/media/${encodeURIComponent(jobId)}.mp4?upscale=${encodeURIComponent(targetLabel)}&download=1`;
      const headRes = await fetch(checkUrl, { method: 'HEAD' });
      const len = Number(headRes.headers.get('content-length') || 0);
      const minBytes = 3000000;
      if (headRes.status === 200 && len >= minBytes) {
        const sizeMb = Number((len / (1024 * 1024)).toFixed(2));
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({
          ok: true,
          cached: true,
          target: targetLabel,
          size: len,
          sizeMb,
          downloadUrl: `/api/v1/media/${jobId}.mp4?upscale=${targetLabel}&download=1&v=${Date.now()}`,
          url: `/api/v1/media/${jobId}.mp4?upscale=${targetLabel}&download=1&v=${Date.now()}`
        }));
      }
    } catch (e) {}
  }

  const id = `${jobId}_${Date.now()}`;
  const inPath = path.join(os.tmpdir(), `${id}_in.mp4`);
  const outPath = path.join(os.tmpdir(), `${id}_out_${targetLabel}.mp4`);
  const t0 = Date.now();

  try {
    const tDl0 = Date.now();
    let downloaded = false;
    if (body.cdnUrl) {
      try {
        await downloadToFile(body.cdnUrl, inPath, 12000);
        if (fs.existsSync(inPath) && fs.statSync(inPath).size > 20000) downloaded = true;
      } catch (e) {}
    }
    if (!downloaded) {
      await downloadParallelRanges(sourceUrl, inPath);
    }
    const dlMs = Date.now() - tDl0;

    const dims = await probeVideoDimensions(inPath);
    const inW = dims.width || 1920;
    const inH = dims.height || 1080;

    let targetW = base[0];
    let targetH = base[1];
    if (inH > inW) {
      targetW = base[1];
      targetH = base[0]; // Dọc 9:16
    } else if (inW === inH) {
      targetW = base[1];
      targetH = base[1]; // Vuông 1:1
    }
    targetW = targetW - (targetW % 2);
    targetH = targetH - (targetH % 2);

    const is4K = targetLabel === '4K';
    const vf = `scale=${targetW}:${targetH}:flags=fast_bilinear,unsharp=3:3:0.8:3:3:0.0`;
    const crfVal = is4K ? '23' : '22';
    const maxRate = is4K ? '9M' : '6M';
    const bufSize = is4K ? '18M' : '12M';
    const args = [
      '-y', '-hide_banner', '-nostats', '-loglevel', 'error',
      '-threads', '0',
      '-i', inPath,
      '-vf', vf,
      '-c:v', 'libx264',
      '-profile:v', 'main',
      '-pix_fmt', 'yuv420p',
      '-crf', crfVal,
      '-maxrate', maxRate,
      '-bufsize', bufSize,
      '-preset', 'ultrafast',
      '-tune', 'fastdecode',
      '-x264-params', 'ref=1:subme=0:me=dia:trellis=0:aq-mode=0:scenecut=0:bframes=0:weightp=0:cabac=0',
      '-threads', '0',
      '-c:a', 'copy',
      '-movflags', '+faststart',
      outPath
    ];

    const code = await new Promise((resolve) => {
      const proc = spawn(ffmpegPath, args, {
        stdio: 'ignore',
        env: { ...process.env, MALLOC_ARENA_MAX: '2' }
      });
      proc.on('close', c => resolve(c ?? -1));
      proc.on('error', () => resolve(-1));
    });

    if (code !== 0 || !fs.existsSync(outPath)) {
      throw new Error('FFmpeg upscale failed on Vercel worker');
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
        offloaded: true,
        target: targetLabel,
        inWidth: inW,
        inHeight: inH,
        width: targetW,
        height: targetH,
        size: totalBytes,
        sizeMb,
        elapsedSec,
        downloadUrl: `/api/v1/media/${jobId}.mp4?upscale=${targetLabel}&download=1&v=${Date.now()}`,
        url: `/api/v1/media/${jobId}.mp4?upscale=${targetLabel}&download=1&v=${Date.now()}`
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
