import { Terminal } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import { WebglAddon } from '@xterm/addon-webgl';

// ---------------------------------------------------------------------------
const T_INIT = 0x01, T_DELTA = 0x02, T_KEYFRAME = 0x03, T_TITLE = 0x04,
      T_MODES = 0x05, T_CLIP = 0x06, T_PING = 0x07, T_EXIT = 0x08, T_PONG = 0x09,
      T_FILE = 0x0a;
const C_INIT = 0x01, C_INPUT = 0x02, C_RESIZE = 0x03, C_PONG = 0x04,
      C_PING = 0x05, C_RTT = 0x06;

const MOUSE_BTN = 1, MOUSE_MOTION = 2, MOUSE_ANY = 4, MOUSE_SGR = 8,
      MOUSE_X10 = 16, BRACKET_PASTE = 32, APP_CURSOR = 64, ALT_SCREEN = 128,
      CURSOR_VISIBLE = 256;

// tokyo-night-flavored palette
const THEME = {
  background: '#0f1216', foreground: '#c6cdf7',
  cursor: '#7aa2f7', cursorAccent: '#0f1216',
  selectionBackground: 'rgba(122,162,247,.30)',
  black: '#15161e', red: '#f7768e', green: '#9ece6a', yellow: '#e0af68',
  blue: '#7aa2f7', magenta: '#bb9af7', cyan: '#7dcfff', white: '#a9b1d6',
  brightBlack: '#414868', brightRed: '#ff7a93', brightGreen: '#b9f27c',
  brightYellow: '#ff9e64', brightBlue: '#7da6ff', brightMagenta: '#bb9af7',
  brightCyan: '#0db9d7', brightWhite: '#c0caf5',
};

const $ = id => document.getElementById(id);
const termEl = $('term'), statusEl = $('status'), statusLabel = $('status-label'),
      titleEl = $('term-title'), overlay = $('exit-overlay');

// ---------------------------------------------------------------------------
// terminal setup
export const term = new Terminal({
  fontFamily: "'JetBrains Mono', ui-monospace, monospace",
  fontSize: window.innerWidth < 480 ? 13 : 15,
  lineHeight: 1.08,
  cursorBlink: true,
  allowProposedApi: true,
  scrollback: 4000,
  theme: THEME,
});
const fit = new FitAddon();
term.loadAddon(fit);
term.open(termEl);
try {
  const gl = new WebglAddon();
  // headless/GPU-flaky environments: fall back to the DOM renderer mid-flight
  gl.onContextLoss?.(() => { try { gl.dispose(); } catch { /* already gone */ } });
  term.loadAddon(gl);
} catch { /* DOM renderer */ }

// ---------------------------------------------------------------------------
// escape-stream translation: packed cells -> minimal SGR runs

// color -> SGR param fragment list; fg=true uses 38-series codes
function colorParams(c, fg) {
  const base = fg ? 30 : 40, bright = fg ? 90 : 100;
  if (c === 0x01000000 || c === 0x01000001) return [fg ? 39 : 49]; // defaults
  if (c < 256) {
    return c < 8 ? [base + c] : c < 16 ? [bright + c - 8] : [fg ? 38 : 48, 5, c];
  }
  const rgb = c & 0xffffff;
  return [fg ? 38 : 48, 2, (rgb >> 16) & 255, (rgb >> 8) & 255, rgb & 255];
}

class SgrWriter {
  constructor() {
    this.out = '';
    this.fg = -1; this.bg = -1; this.flags = -1;
  }
  // flags bits match proto.GlyphAttr: reverse1 underline2 bold4 gfx8 italic16 blink32
  attr(cell) {
    const f = cell.attr;
    if (f !== this.flags) {
      // cheap correct path: flush with reset-style diffs
      const parts = [];
      if (!(f & 4) && (this.flags & 4)) parts.push(22);
      if (!(f & 16) && (this.flags & 16)) parts.push(23);
      if (!(f & 2) && (this.flags & 2)) parts.push(24);
      if (!(f & 32) && (this.flags & 32)) parts.push(25);
      if (!(f & 1) && (this.flags & 1)) parts.push(27);
      if (f & 4) parts.push(1);
      if (f & 16) parts.push(3);
      if (f & 2) parts.push(4);
      if (f & 32) parts.push(5);
      if (f & 1) parts.push(7);
      if (parts.length) this.out += `\x1b[${parts.join(';')}m`;
      this.flags = f;
    }
    if (cell.fg !== this.fg) { this.out += `\x1b[${colorParams(cell.fg, true).join(';')}m`; this.fg = cell.fg; }
    if (cell.bg !== this.bg) { this.out += `\x1b[${colorParams(cell.bg, false).join(';')}m`; this.bg = cell.bg; }
    this.out += cell.ch;
  }
}

// varint reader over Uint8Array
class CellReader {
  constructor(b, o) { this.b = b; this.o = o; }
  varint() { // signed
    let shift = 0, res = 0, byte;
    do {
      byte = this.b[this.o++];
      res |= (byte & 0x7f) << shift;
      shift += 7;
    } while (byte & 0x80);
    return (res >>> 1) ^ -(res & 1); // zigzag
  }
  uvarint() {
    let shift = 0, res = 0, byte;
    do {
      byte = this.b[this.o++];
      res |= (byte & 0x7f) << shift;
      shift += 7;
    } while (byte & 0x80);
    return res >>> 0;
  }
  cell() {
    return { ch: String.fromCodePoint(this.varint()), fg: this.uvarint(), bg: this.uvarint(), attr: this.b[this.o++] };
  }
}

const W = new SgrWriter();

function beginFramePaint() { W.out = ''; W.fg = -1; W.bg = -1; W.flags = -1; }

// ---------------------------------------------------------------------------
// transport

let ws = null;
let sid = localStorage.getItem('tether.sid') || '';
let lastSeq = 0;
let connected = false, everConnected = false;
let backoff = 250;
let inputQueue = [];
let modes = 0;
let pendingResize = null;
let pingN = 0; const pingAt = new Map();
let rttEma = 0;
let transport = 'ws';           // active screen-frame path: 'ws' | 'dc'

function setStatus(cls, label) {
  statusEl.className = 'status ' + cls;
  statusLabel.textContent = label
    + (rttEma > 0 ? ` · ${Math.round(rttEma)}ms` : '')
    + (transport === 'dc' ? ' · dc' : '');
}

function refreshStatus() { if (connected) setStatus('live', everConnected ? 'resumed' : 'live'); }

function encodeFrame(typ, body) {
  body = body || new Uint8Array(0);
  const out = new Uint8Array(9 + body.length);
  const dv = new DataView(out.buffer);
  out[0] = typ;
  dv.setUint32(1, 0, true);
  dv.setUint32(5, body.length, true);
  out.set(body, 9);
  return out;
}

function cinitBody(sidStr, last) {
  const enc = new TextEncoder().encode(sidStr);
  const out = new Uint8Array(2 + enc.length + 4);
  out[0] = 1; out[1] = enc.length;
  out.set(enc, 2);
  new DataView(out.buffer).setUint32(2 + enc.length, last, true);
  return out;
}

function sendRaw(bytes) {
  if (ws && ws.readyState === 1) ws.send(bytes);
}


function connect() {
  setStatus('connecting', 'connecting');
  const proto_ = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto_}//${location.host}/ws`);
  ws.binaryType = 'arraybuffer';

  ws.onopen = () => {
    sendRaw(encodeFrame(C_INIT, cinitBody(sid, lastSeq)));
    // flush queued keystrokes typed while offline
    if (inputQueue.length) {
      const blob = new Blob(inputQueue);
      inputQueue = [];
      blob.arrayBuffer().then(b => sendRaw(encodeFrame(C_INPUT, new Uint8Array(b))));
    }
    connected = true;
    setStatus('live', everConnected ? 'resumed' : 'live');
    everConnected = true;
    backoff = 250;
    scheduleFit();
    initRTC();
  };

  ws.onmessage = ev => {
    if (typeof ev.data === 'string') { rtcSignal(ev.data); return; }
    handleFrame(new Uint8Array(ev.data));
  };

  ws.onclose = () => {
    connected = false;
    rtcTeardown();
    if (exited) return;
    setStatus('offline', 'reconnecting…');
    setTimeout(connect, backoff + Math.random() * 150);
    backoff = Math.min(backoff * 1.8, 5000);
  };
  ws.onerror = () => { /* close handler drives retry */ };
}

// ---------------------------------------------------------------------------
// WebRTC datagram transport (mosh-style): screen frames ride an unreliable,
// unordered DataChannel so packet loss costs freshness, never correctness.
// The WebSocket stays connected for input, pongs, and keyframe resync.

let rtc = null;                 // { pc, dc, pendingIce }
let rtcOff = localStorage.getItem('rtc-off') === '1';

function rtcSend(obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function rtcSignal(text) {
  let m;
  try { m = JSON.parse(text); } catch { return; }
  if (!rtc) return;
  if (m.type === 'answer' && m.sdp) {
    rtc.pc.setRemoteDescription({ type: 'answer', sdp: m.sdp })
      .then(() => { // flush candidates that arrived before the answer
        for (const c of rtc.pendingIce.splice(0)) rtc.pc.addIceCandidate(c).catch(() => {});
      })
      .catch(e => console.log('rtc[ERR-answer]: ' + e.message));
  } else if (m.type === 'ice' && m.candidate && m.candidate.candidate) {
    if (rtc.pc.remoteDescription && rtc.pc.remoteDescription.type) {
      rtc.pc.addIceCandidate(m.candidate).catch(() => {});
    } else {
      rtc.pendingIce.push(m.candidate);
    }
  }
}

let iceConfig = null; // fetched once from /ice; STUN defaults + server-provided TURN
async function initRTC() {
  if (rtcOff || new URLSearchParams(location.search).has('nortc')) return;
  rtcTeardown();
  try {
    if (!iceConfig) {
      iceConfig = fetch('ice', { cache: 'no-store' })
        .then(r => r.ok ? r.json() : { iceServers: [] })
        .catch(() => ({ iceServers: [] }));
    }
    const { iceServers } = await iceConfig;
    const pc = new RTCPeerConnection({
      iceServers: [
        { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] },
        { urls: 'stun:stun.cloudflare.com:3478' },
        ...iceServers,
      ],
      iceCandidatePoolSize: 4,
    });
    const dc = pc.createDataChannel('tether', { ordered: false, maxRetransmits: 0 });
    dc.binaryType = 'arraybuffer';
    rtc = { pc, dc, pendingIce: [] };
    dc.onopen = () => { transport = 'dc'; console.log('rtc: datagram path open'); refreshStatus(); };
    dc.onclose = () => { transport = 'ws'; refreshStatus(); };
    dc.onmessage = ev => handleFrame(new Uint8Array(ev.data));
    pc.onicecandidate = e => { if (e.candidate) rtcSend({ type: 'ice', candidate: e.candidate.toJSON() }); };
    pc.onconnectionstatechange = () => {
      if (pc.connectionState === 'failed' && transport === 'dc') {
        transport = 'ws';
        refreshStatus();
        rtcSend({ type: 'kf' });
      }
    };
    pc.createOffer()
      .then(o => pc.setLocalDescription(o))
      .then(() => rtcSend({ type: 'offer', sdp: pc.localDescription.sdp }))
      .catch(e => console.log('rtc[ERR]: ' + e.message));
  } catch (e) { console.log('rtc[ERR-sync]: ' + e.message); }
}

function rtcTeardown() {
  transport = 'ws';
  if (!rtc) return;
  try { rtc.dc.close(); } catch {}
  clearTimeout(kfTimer);
}

// Gap detector: on the datagram path a lost delta leaves a seq hole; ask the
// server for a fresh keyframe (debounced so bursts cost one request).
function noteGap() {
  if (transport !== 'dc') return;
  clearTimeout(kfTimer);
  kfTimer = setTimeout(() => rtcSend({ type: 'kf' }), 30);
}

setInterval(() => { // staleness watchdog
  if (!connected || transport !== 'dc' || !lastStateFrameAt) return;
  const idle = performance.now() - lastStateFrameAt;
  if (idle > 8000) rtcSend({ type: 'kf' });
  if (idle > 12000) {
    // datagram path looks dead: force a resync on whichever transport lives
    transport = 'ws';
    sendRaw(encodeFrame(C_INIT, cinitBody(sid, lastSeq)));
  }
}, 2000);

// ---------------------------------------------------------------------------
// file transfer: downloads arrive as T_FILE offers (OSC 1337 emitted by
// anything on the VPS); uploads go out-of-band via authenticated POST /upload.

const dlChip = $('dl-chip'), dlName = $('dl-name');
const toastEl = $('toast');
let pendingFile = null, toastTimer = null;

function fmtBytes(n) {
  if (n < 1024) return n + ' B';
  if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
  if (n < 1073741824) return (n / 1048576).toFixed(1) + ' MB';
  return (n / 1073741824).toFixed(2) + ' GB';
}

function toast(msg, ms = 4000) {
  toastEl.textContent = msg;
  toastEl.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { toastEl.hidden = true; }, ms);
}

let docTitle = document.title;
let titleFlashTimer = null;

function stopTitleFlash() {
  clearInterval(titleFlashTimer);
  document.title = docTitle;
}

function handleFileOffer(body) {
  const nameLen = body[0] | (body[1] << 8);
  const name = new TextDecoder().decode(body.subarray(2, 2 + nameLen));
  const off = 2 + nameLen;
  const size = body[off] | (body[off + 1] << 8) | (body[off + 2] << 16) | (body[off + 3] << 24);
  const data = body.subarray(off + 4, off + 4 + size);
  pendingFile = { name, blob: new Blob([data]) };
  dlName.textContent = `${name} · ${fmtBytes(size)}`;
  dlChip.hidden = false;
  // attention routing: the offer may land in a viewer buried under other
  // windows (herdr layouts); flash the title so it's visible from anywhere
  clearInterval(titleFlashTimer);
  let flip = false;
  titleFlashTimer = setInterval(() => {
    flip = !flip;
    document.title = flip ? `⬇ ${pendingFile.name}` : docTitle;
  }, 900);
}

function savePendingFile() {
  if (!pendingFile) return;
  const url = URL.createObjectURL(pendingFile.blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = pendingFile.name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 30000);
  dlChip.hidden = true;
  pendingFile = null;
  stopTitleFlash();
}

dlChip.querySelector('#dl-save').addEventListener('click', savePendingFile);
dlChip.querySelector('#dl-dismiss').addEventListener('click', () => { dlChip.hidden = true; pendingFile = null; stopTitleFlash(); });

// uploads -------------------------------------------------------------------

function uploadFile(file) {
  return new Promise(resolve => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/upload');
    xhr.setRequestHeader('X-Filename', encodeURIComponent(file.name));
    xhr.upload.onprogress = e => {
      if (e.lengthComputable) toast(`↑ ${file.name} ${Math.round((e.loaded / e.total) * 100)}%`, 1500);
    };
    xhr.onload = () => {
      if (xhr.status === 200) {
        let disp = `saved ${file.name}`;
        try { const j = JSON.parse(xhr.responseText); if (j.disp) disp = 'saved ' + j.disp; } catch {}
        toast(disp, 8000);
      } else if (xhr.status === 413) {
        toast(`${file.name}: exceeds server size cap`);
      } else {
        toast(`${file.name}: upload failed (${xhr.status})`);
      }
      resolve(xhr.status === 200);
    };
    xhr.onerror = () => { toast(`${file.name}: upload failed`); resolve(false); };
    xhr.send(file);
  });
}

async function uploadFiles(files) {
  for (const f of files) {
    if (f.size === 0) { toast(`${f.name}: empty file`); continue; }
    await uploadFile(f);
  }
}

// paste with pure file/image flavors (never intercepts text paste)
function fileFlavors(dt) {
  return [...(dt?.items || [])]
    .filter(it => it.kind === 'file' && !it.type.startsWith('text/'))
    .map(it => it.getAsFile())
    .filter(Boolean);
}

window.addEventListener('paste', e => {
  const files = fileFlavors(e.clipboardData);
  if (!files.length) return;
  e.preventDefault();
  uploadFiles(files);
});

// drag & drop: prevent browser navigation; upload dropped files
window.addEventListener('dragover', e => e.preventDefault());
window.addEventListener('drop', e => {
  e.preventDefault();
  const files = [...(e.dataTransfer?.files || [])];
  if (files.length) uploadFiles(files);
});

// ---------------------------------------------------------------------------
// frame application

let exited = false;
let prevAlt = false;

function handleFrame(buf) {
  const typ = buf[0];
  const seq = buf[1] | (buf[2] << 8) | (buf[3] << 16) | (buf[4] << 24);
  const len = buf[5] | (buf[6] << 8) | (buf[7] << 16) | (buf[8] << 24);
  const body = buf.subarray(9, 9 + len);

  if (typ === T_DELTA || typ === T_KEYFRAME || typ === T_MODES || typ === T_INIT) {
    lastStateFrameAt = performance.now();
    if (typ === T_DELTA && transport === 'dc' && lastSeq > 0 && seq > lastSeq + 1) noteGap();
  }
  if (typ === T_FILE) { handleFileOffer(body); return; }
  switch (typ) {
    case T_INIT: {
      const dv = new DataView(body.buffer, body.byteOffset);
      const n = body[0];
      const newSid = new TextDecoder().decode(body.subarray(1, 1 + n));
      if (sid && newSid !== sid) {
        lastSeq = 0;
        fullReset();
        exited = false;
        overlay.hidden = true;
      }
      sid = newSid;
      localStorage.setItem('tether.sid', sid);
      const cols = dv.getUint16(1 + n, true), rows = dv.getUint16(3 + n, true);
      applyModes(dv.getUint32(5 + n, true));
      prevAlt = !!(modes & ALT_SCREEN);
      // our size wins; tell the PTY
      scheduleFit(true);
      void cols; void rows;
      break;
    }
    case T_DELTA:
      if (seq > lastSeq) { applyDelta(body); lastSeq = seq; }
      break;
    case T_KEYFRAME:
      if (seq > lastSeq) { fullReset(); applyKeyframe(body); lastSeq = seq; }
      break;
    case T_TITLE: {
      const t = new TextDecoder().decode(body);
      titleEl.textContent = t;
      docTitle = t ? `${t} · tether` : 'tether';
      if (!pendingFile) document.title = docTitle;
      break;
    }
    case T_MODES:
      if (seq > lastSeq) {
        applyModes(body[0] | (body[1] << 8) | (body[2] << 16) | (body[3] << 24));
        const alt = !!(modes & ALT_SCREEN);
        if (seq > lastSeq) lastSeq = seq;
        if (alt !== prevAlt) {
          prevAlt = alt;
          // the keyframe following this frame repaints the new buffer fully
        }
      }
      break;
    case T_CLIP:
      navigator.clipboard?.writeText(new TextDecoder().decode(body)).catch(() => {});
      break;
    case T_PING:
      // liveness only: ping seq lives in its own namespace and must NOT
      // touch the session state sequence
      { const f = encodeFrame(C_PONG, null); new DataView(f.buffer).setUint32(1, seq, true); sendRaw(f); }
      break;
    case T_PONG: {
      const t0 = pingAt.get(seq);
      if (t0) {
        pingAt.delete(seq);
        const sample = performance.now() - t0;
        rttEma = rttEma ? rttEma * 0.75 + sample * 0.25 : sample;
        if (everConnected && connected) setStatus('live', 'live');
        if (pingAt.size === 0 && rttEma > 0) {
          const b = new Uint8Array(2);
          new DataView(b.buffer).setUint16(0, Math.min(65535, rttEma | 0), true);
          sendRaw(encodeFrame(C_RTT, b));
        }
      }
      break;
    }
    case T_EXIT:
      exited = true;
      overlay.hidden = false;
      setStatus('offline', 'ended');
      break;
  }
}

function fullReset() { term.write('\x1bc'); reapplyModes(); }

function reapplyModes() {
  let s = '';
  if (modes & BRACKET_PASTE) s += '\x1b[?2004h';
  if (modes & APP_CURSOR) s += '\x1b[?1h';
  if (modes & CURSOR_VISIBLE) s += '\x1b[?25h'; else s += '\x1b[?25l';
  if (s) term.write(s);
}

function applyModes(m) {
  modes = m;
}

// DELTA body: cx u16, cy u16, flags u8, scrollN u16, rowCount u16,
//             then rows { y u16, spanCount u16, spans { x u16, n u16, cells } }
function applyDelta(b) {
  beginFramePaint();
  const dv = new DataView(b.buffer, b.byteOffset, b.byteLength);
  const cx = dv.getUint16(0, true), cy = dv.getUint16(2, true);
  const vis = !!(b[4] & 1);
  const scrollN = dv.getUint16(5, true);
  const rowCount = dv.getUint16(7, true);
  const r = new CellReader(b, 9);

  if (scrollN > 0) {
    const rows = term.rows;
    W.out += `\x1b[${rows};1H` + '\n'.repeat(scrollN);
  }
  for (let i = 0; i < rowCount; i++) {
    const y = dv.getUint16(r.o, true); r.o += 2;
    const spanCount = dv.getUint16(r.o, true); r.o += 2;
    for (let sIdx = 0; sIdx < spanCount; sIdx++) {
      const x = dv.getUint16(r.o, true); r.o += 2;
      const n = dv.getUint16(r.o, true); r.o += 2;
      W.out += `\x1b[${y + 1};${x + 1}H`;
      for (let k = 0; k < n; k++) W.attr(r.cell());
    }
  }
  W.out += `\x1b[${cy + 1};${cx + 1}H\x1b[?25` + (vis ? 'h' : 'l');
  term.write(W.out);
}

// KEYFRAME body: cols,rows,cx,cy u16 each, flags u8, then cols*rows cells
function applyKeyframe(b) {
  const dv = new DataView(b.buffer, b.byteOffset, b.byteLength);
  const cols = dv.getUint16(0, true), rows = dv.getUint16(2, true);
  const cx = dv.getUint16(4, true), cy = dv.getUint16(6, true);
  const vis = !!(b[8] & 1);
  beginFramePaint();
  const r = new CellReader(b, 9);
  for (let y = 0; y < rows; y++) {
    W.out += `\x1b[${y + 1};1H`;
    for (let x = 0; x < cols; x++) W.attr(r.cell());
  }
  W.out += `\x1b[${cy + 1};${cx + 1}H\x1b[?25` + (vis ? 'h' : 'l');
  term.write(W.out);
  void cols; void rows;
}

// ---------------------------------------------------------------------------
// mouse reporting

function reportCoords(e) {
  const el = term.element;
  if (!el) return null;
  const core = term._core;
  const dim = core._renderService?.dimensions?.css?.cell;
  if (!dim) return null;
  const rect = el.getBoundingClientRect();
  const col = Math.max(0, Math.min(term.cols - 1, Math.floor((e.clientX - rect.left) / dim.width)));
  const row = Math.max(0, Math.min(term.rows - 1, Math.floor((e.clientY - rect.top) / dim.height)));
  return { col, row };
}

function mouseBytes(kind, e) {
  const p = reportCoords(e);
  if (!p) return;
  let btn = 0;
  switch (kind) {
    case 'down': case 'up':
      btn = e.button === 1 ? 1 : e.button === 2 ? 2 : 0;
      break;
    case 'move': btn = 32; break; // motion with any-button
    case 'wheel': btn = e.deltaY < 0 ? 64 : 65; break;
  }
  const release = kind === 'up';
  if (modes & MOUSE_SGR) {
    sendInput(`\x1b[<${btn};${p.col + 1};${p.row + 1}${release ? 'm' : 'M'}`);
  } else {
    const cb = release ? 3 : btn;
    const xb = Math.min(255, p.col + 33), yb = Math.min(255, p.row + 33);
    sendInput(`\x1b[M${String.fromCharCode(32 + cb, xb, yb)}`);
  }
}

function sendInput(data) {
  // sticky Ctrl applies to single printable characters from any source
  if (ctrlSticky && typeof data === 'string' && data.length === 1) {
    const code = data.toUpperCase().charCodeAt(0);
    if (code >= 64 && code <= 95) {
      data = String.fromCharCode(code & 31);
      ctrlSticky = false;
      $('ctrl-btn').classList.remove('sticky');
    }
  }
  const arr = typeof data === 'string' ? new TextEncoder().encode(data) : data;
  if (!connected) {
    if (arr.length) {
      inputQueue.push(arr);
      const total = inputQueue.reduce((n, a) => n + a.length, 0);
      while (total > 64 * 1024 && inputQueue.length > 1) inputQueue.shift();
    }
    return;
  }
  sendRaw(encodeFrame(C_INPUT, arr));
}

let mouseOff = localStorage.getItem('mouse-off') === '1';
function mouseForwarding() {
  return !mouseOff && !!(modes & (MOUSE_BTN | MOUSE_MOTION | MOUSE_ANY));
}

function onMouse(kind) {
  return e => {
    if (e.shiftKey) return; // Shift always means native selection
    if (!mouseForwarding()) return;
    if (kind === 'move') {
      if (!(modes & MOUSE_ANY) && !(modes & MOUSE_MOTION)) return;
      if (!(modes & MOUSE_ANY) && !(e.buttons & 7)) return; // motion needs a button
    }
    e.preventDefault?.();
    mouseBytes(kind, e);
  };
}

termEl.addEventListener('mousedown', onMouse('down'));
termEl.addEventListener('mouseup', onMouse('up'));
termEl.addEventListener('mousemove', onMouse('move'));
let wheelAcc = 0;
termEl.addEventListener('wheel', e => {
  if (e.shiftKey || mouseForwarding()) {
    onMouse('wheel')(e); // app owns the wheel: forward as mouse report
    return;
  }
  // buffer scroll: accumulate pixel deltas so tiny trackpad gestures move
  // one line at a time instead of flinging the viewport
  e.preventDefault();
  const px = e.deltaMode === 1 ? e.deltaY * 16 : e.deltaY;
  wheelAcc += px;
  let lines = 0;
  while (wheelAcc >= 48) { lines -= 1; wheelAcc -= 48; }
  while (wheelAcc <= -48) { lines += 1; wheelAcc += 48; }
  if (lines) term.scrollLines(lines);
}, { passive: false });

// Touch: phones have no wheel events, so translate swipes into the same
// decision wheel makes — hand the gesture to the app when it owns the mouse
// (omp transcript scrolling), otherwise scroll the local buffer.
let touchY = null;
termEl.addEventListener('touchstart', e => {
  touchY = e.touches[0].clientY;
}, { passive: true });
termEl.addEventListener('touchmove', e => {
  if (touchY === null) return;
  e.preventDefault(); // keep the page itself from rubber-banding
  const y = e.touches[0].clientY;
  const dy = touchY - y; // swipe up = scroll down (positive)
  touchY = y;
  const lines = Math.trunc(dy / 16);
  if (!lines) return;
  if (mouseForwarding()) {
    // hand the gesture to the app: synthesize wheel reports at the finger
    const t = e.touches[0];
    const ev = { deltaY: lines > 0 ? 120 : -120, clientX: t.clientX, clientY: t.clientY, button: 0, buttons: 0 };
    mouseBytes('wheel', ev);
    for (let i = 1; i < Math.abs(lines); i++) {
      setTimeout(() => mouseBytes('wheel', { ...ev }), i * 30);
    }
  } else {
    term.scrollLines(lines);
  }
}, { passive: false });
termEl.addEventListener('touchend', () => { touchY = null; }, { passive: true });

// ---------------------------------------------------------------------------
// keyboard

let ctrlSticky = false;

term.onData(d => {
  if (ctrlSticky && d.length === 1) {
    const code = d.toUpperCase().charCodeAt(0);
    if (code >= 64 && code <= 95) { // @A-Z[\]^_
      d = String.fromCharCode(code & 31);
      ctrlSticky = false;
      $('ctrl-btn').classList.remove('sticky');
    }
  }
  sendInput(d);
});

term.attachCustomKeyEventHandler(e => {
  if (e.type !== 'keydown') return true;
  if (e.ctrlKey && !e.shiftKey && (e.key === 'c' || e.key === 'C') && term.hasSelection()) {
    copySelection();
    return false;
  }
  if ((e.ctrlKey || e.metaKey) && (e.key === 'v' || e.key === 'V')) {
    navigator.clipboard?.readText?.().then(paste).catch(() => {});
    return false;
  }
  return true;
});

function paste(text) {
  if (!text) return;
  const t = text.replace(/\r\n/g, '\r').replace(/\n/g, '\r');
  sendInput(modes & BRACKET_PASTE ? `\x1b[200~${t}\x1b[201~` : t);
}

function copySelection() {
  const sel = term.getSelection();
  if (sel) navigator.clipboard?.writeText(sel).catch(() => {});
}

// selection: auto-copy to the real clipboard
let selTimer = null;
term.onSelectionChange(() => {
  clearTimeout(selTimer);
  if (term.hasSelection()) {
    selTimer = setTimeout(() => copySelection(), 350); // settle after drag ends
  }
});

// ---------------------------------------------------------------------------
// on-screen key bar

const SEQ = {
  up:   () => `\x1b${modes & APP_CURSOR ? 'O' : '['}A`,
  down: () => `\x1b${modes & APP_CURSOR ? 'O' : '['}B`,
  right:() => `\x1b${modes & APP_CURSOR ? 'O' : '['}C`,
  left: () => `\x1b${modes & APP_CURSOR ? 'O' : '['}D`,
};
const KEYS = {
  esc: '\x1b', tab: '\t',
  home: () => `\x1b${modes & APP_CURSOR ? 'O' : '['}H`,
  end:  () => `\x1b${modes & APP_CURSOR ? 'O' : '['}F`,
  pgup: '\x1b[5~', pgdn: '\x1b[6~',
  pipe: '|', tilde: '~', dash: '-',
};

document.querySelectorAll('#bar button[data-key], #bar button[data-seq]').forEach(btn => {
  btn.addEventListener('pointerdown', e => {
    e.preventDefault(); // keep soft-keyboard state untouched
    const k = btn.dataset.key, s = btn.dataset.seq;
    if (k === 'ctrl') {
      ctrlSticky = !ctrlSticky;
      btn.classList.toggle('sticky', ctrlSticky);
      return;
    }
    if (s) sendInput(SEQ[s]());
    else if (KEYS[k]) sendInput(typeof KEYS[k] === 'function' ? KEYS[k]() : KEYS[k]);
  });
});

const mouseBtn = $('bar').querySelector('[data-act="mouse"]');
mouseBtn.addEventListener('pointerdown', e => {
  e.preventDefault();
  mouseOff = !mouseOff;
  localStorage.setItem('mouse-off', mouseOff ? '1' : '0');
  mouseBtn.classList.toggle('off', mouseOff);
});
mouseBtn.classList.toggle('off', mouseOff); // initial persisted state

const dcBtn = $('bar').querySelector('[data-act="dc"]');
function applyDcBtn() {
  dcBtn.classList.toggle('off', rtcOff);
  dcBtn.title = rtcOff
    ? 'datagram transport off; tap to enable mosh-style WebRTC'
    : 'datagram transport enabled; tap to fall back to WebSocket only';
}
dcBtn.addEventListener('pointerdown', e => {
  e.preventDefault();
  rtcOff = !rtcOff;
  localStorage.setItem('rtc-off', rtcOff ? '1' : '0');
  applyDcBtn();
  if (rtcOff) {
    rtcTeardown();                       // also clears the staleness watchdog
    transport = 'ws';
    if (connected) {
      rtcSend({ type: 'kf' }); // reliable repaint after the switch
      refreshStatus();
    }
  } else if (connected && !exited) {
    initRTC();                           // negotiate immediately, no reload
  }
});
applyDcBtn();

$('bar').querySelector('[data-act="copy"]').addEventListener('pointerdown', e => {
  e.preventDefault();
  copySelectionOrView();
  term.clearSelection();
});

function visibleText() {
  const buf = term.buffer.active;
  const y0 = buf.viewportY, lines = [];
  for (let y = y0; y < Math.min(buf.length, y0 + term.rows); y++) {
    lines.push(buf.getLine(y) ? buf.getLine(y).translateToString(true) : '');
  }
  return lines.join('\n');
}

function copySelectionOrView() {
  const sel = term.getSelection();
  const text = sel || visibleText();
  if (text) navigator.clipboard?.writeText(text).catch(() => {});
}
$('bar').querySelector('[data-act="paste"]').addEventListener('pointerdown', e => {
  e.preventDefault();
  navigator.clipboard?.readText?.().then(paste).catch(() => {});
});

$('bar-toggle').addEventListener('click', () => {
  const bar = $('bar');
  bar.classList.toggle('hidden');
  document.documentElement.style.setProperty('--bar-h', bar.classList.contains('hidden') ? '22px' : '46px');
  scheduleFit();
});

$('restart').addEventListener('click', () => {
  exited = false;
  overlay.hidden = true;
  localStorage.removeItem('tether.sid');
  sid = ''; lastSeq = 0; everConnected = false;
  fullReset();
  connect();
});

// ---------------------------------------------------------------------------
// resize plumbing

let fitTimer = 0, lastCols = 0, lastRows = 0;

function doFit(force) {
  try { fit.fit(); } catch { return; }
  if (term.cols !== lastCols || term.rows !== lastRows || force) {
    lastCols = term.cols; lastRows = term.rows;
    if (connected) {
      const b = new Uint8Array(4);
      new DataView(b.buffer).setUint16(0, term.cols, true);
      new DataView(b.buffer).setUint16(2, term.rows, true);
      sendRaw(encodeFrame(C_RESIZE, b));
    }
  }
}

function scheduleFit(force) {
  clearTimeout(fitTimer);
  fitTimer = setTimeout(() => doFit(force), 60);
}

new ResizeObserver(() => scheduleFit()).observe(termEl);
window.addEventListener('orientationchange', () => setTimeout(() => scheduleFit(true), 250));

// ---------------------------------------------------------------------------
// RTT probes
setInterval(() => {
  if (!connected) return;
  const seq = ++pingN;
  const f = encodeFrame(C_PING, null);
  new DataView(f.buffer).setUint32(1, seq, true); // encodeFrame leaves seq 0
  pingAt.set(seq, performance.now());
  sendRaw(f);
}, 4000);

// keep soft keyboard usable: tapping terminal focuses it
term.focus();

connect();


// automation / debugging hooks
window.__t = {
  term,
  sendInput,
  paste,
  copySelectionOrView,
  get modes() { return modes; },
  get ws() { return ws; },
  get sid() { return sid; },
  get lastSeq() { return lastSeq; },
  get rtcState() { return { transport, pc: rtc ? rtc.pc.connectionState : null, dc: rtc ? rtc.dc.readyState : null }; },
  get rtcOff() { return rtcOff; },
  toggleRTC() { dcBtn.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true })); return !rtcOff; },
  get lastFile() { return pendingFile && { name: pendingFile.name, size: pendingFile.blob.size }; },
  testPaste(dt) { const files = fileFlavors(dt); if (files.length) uploadFiles(files); return files.length; },
};
