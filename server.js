// เซิร์ฟเวอร์ศึกมวยไลฟ์
// รับเหตุการณ์จาก TikTok LIVE -> แปลงเป็นคำสั่ง -> ส่งให้หน้าเกมผ่าน WebSocket
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
let TikTokLiveConnection, WebcastEvent, ControlEvent; // โหลดตอนเริ่ม (ไลบรารีเป็น ES Module)

const config = JSON.parse(fs.readFileSync(path.join(__dirname, 'config.json'), 'utf8'));
const DEMO = process.argv.includes('--demo');
const PORT = Number(process.env.PORT || config.port || 3000);

/* ---------- เว็บเซิร์ฟเวอร์ (เสิร์ฟหน้าเกม) ---------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.json': 'application/json' };
const STATUS = { pages: 0, username: '', connected: false, roomId: null, lastError: null, events: 0, lastEvent: null, startedAt: new Date().toISOString() };
const server = http.createServer((req, res) => {
  if ((req.url || '').startsWith('/reconnect')) {
    if (retryTimer) { clearTimeout(retryTimer); retryTimer = null; }
    connectTikTok();
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end('กำลังต่อ TikTok ใหม่… รอ 10 วินาทีแล้วเปิด /status ดู');
  }
  if ((req.url || '').startsWith('/status')) {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    STATUS.pages = wss.clients.size; STATUS.secondsSinceData = STATUS.lastDataAt ? Math.round((Date.now() - STATUS.lastDataAt) / 1000) : null; STATUS.remembered = ROSTER.size; STATUS.champ = CHAMP ? CHAMP.who.name + ' (ป้องกัน ' + CHAMP.defenses + ')' : null; delete STATUS.host;
    return res.end(JSON.stringify(STATUS, null, 2));
  }
  let url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/') url = '/index.html';
  const file = path.join(__dirname, 'public', path.normalize(url).replace(/^(\.\.[\/\\])+/, ''));
  if (!file.startsWith(path.join(__dirname, 'public'))) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
});

/* ---------- WebSocket ไปหน้าเกม ---------- */
const wss = new WebSocketServer({ server, path: '/ws' });
// จำคนในห้องและคะแนน เพื่อส่งให้หน้าเกมตอนโหลดใหม่ (ฉากจะไม่โล่ง)
const ROSTER = new Map(), SCORES = new Map();
let HOST = null; // เจ้าภาพ: ส่งของขวัญครั้งเดียวมากที่สุด
const GIFT_COINS = { jab: 1, kick: 5, elbow: 20, knee: 100, incense: 1, stick: 5, lantern: 20, bless: 100 };
let CHAMP = null; // แชมป์เวที (หน้าเกมส่งมาบอก)
function remember(msg) {
  if (!msg.who || !msg.who.id) return;
  ROSTER.delete(msg.who.id); ROSTER.set(msg.who.id, { who: msg.who, last: Date.now() });
  while (ROSTER.size > 40) ROSTER.delete(ROSTER.keys().next().value);
  if (GIFT_COINS[msg.type]) {
    const s = SCORES.get(msg.who.id) || { who: msg.who, score: 0 };
    const c = msg.coins || GIFT_COINS[msg.type];
    s.who = msg.who; s.score += c; SCORES.set(msg.who.id, s);
    if (!HOST || c > HOST.coins) HOST = { who: msg.who, coins: c };
  }
}
function snapshot() {
  const cut = Date.now() - 30 * 60 * 1000;
  const users = [...ROSTER.values()].filter(r => r.last > cut).slice(-20).map(r => r.who);
  const scores = [...SCORES.values()].sort((a, b) => b.score - a.score).slice(0, 10).map(s => ({ who: s.who, score: s.score }));
  const total = [...SCORES.values()].reduce((t, s) => t + s.score, 0);
  return { type: 'snapshot', users, scores, host: HOST, total, champ: CHAMP };
}
function send(msg) {
  remember(msg);
  if (msg.type !== 'viewers') { STATUS.events++; STATUS.lastEvent = msg.type + ' ' + (msg.who ? msg.who.name : '') + ' @ ' + new Date().toISOString(); }
  const s = JSON.stringify(msg);
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(s); });
  if (msg.type === 'like' || msg.type === 'viewers') return;
  console.log('->', msg.type, msg.who ? (msg.who.name || msg.who.id) : '', msg.text || '', msg.coins ? msg.coins + ' เหรียญ' : '');
}
wss.on('connection', ws => {
  console.log('หน้าเกมเชื่อมต่อแล้ว'); ws.send(JSON.stringify(snapshot()));
  ws.on('message', raw => {
    try { const m = JSON.parse(String(raw)); if (m && m.type === 'champ') CHAMP = m.who ? { who: m.who, defenses: m.defenses || 0 } : null; } catch (_) {}
  });
});

/* ---------- แปลงเหตุการณ์ TikTok -> คำสั่งเกม ---------- */
// ไลบรารีเวอร์ชันใหม่: ชื่อบัญชีอยู่ใน displayId, รูปอยู่ใน avatarThumb.urlList (รองรับชื่อช่องแบบเก่าด้วย)
function picOf(u) {
  const lists = [u?.avatarThumb?.urlList, u?.avatarMedium?.urlList, u?.avatarLarge?.urlList, u?.profilePicture?.urls, u?.profilePicture?.url].filter(Array.isArray);
  for (const l of lists) { const ok = l.find(x => x && !/\.heic/i.test(x)); if (ok) return ok; }
  return u?.profilePictureUrl || '';
}
const whoOf = u => ({
  id: String(u?.displayId || u?.uniqueId || u?.id || u?.userId || u?.nickname || 'unknown'),
  name: String(u?.nickname || u?.displayId || u?.uniqueId || 'ผู้ชม'),
  pic: String(picOf(u)),
});

// เลือกคำสั่งจากชื่อของขวัญก่อน ถ้าไม่ตรงค่อยดูจากจำนวนเหรียญรวม
function actionForGift(giftName, coins) {
  const byName = config.giftNameActions || {};
  if (giftName && byName[giftName]) return byName[giftName];
  const tiers = [...(config.giftTiers || [])].sort((a, b) => b.minCoins - a.minCoins);
  const t = tiers.find(t => coins >= t.minCoins);
  return t ? t.action : null;
}

function handleGift(d) {
  const det = d.giftDetails || {};
  // ของขวัญกดรัว (giftType 1) จะส่งมาเป็นชุด ให้นับตอนจบชุดเท่านั้น
  if (det.giftType === 1 && !d.repeatEnd) return;
  const coins = (det.diamondCount || 0) * (d.repeatCount || 1);
  const action = actionForGift(det.giftName, coins);
  if (!action) return;
  send({ type: action, who: whoOf(d.user), coins, gift: det.giftName || '' });
}

const handlers = {
  follow: d => send({ type: 'follow', who: whoOf(d.user) }),
  member: d => send({ type: 'join', who: whoOf(d.user) }),
  chat: d => d.comment && send({ type: 'comment', who: whoOf(d.user), text: String(d.comment).slice(0, 60) }),
  gift: handleGift,
  like: d => send({ type: 'like', count: d.likeCount || 1, who: d.user ? whoOf(d.user) : undefined }),
  roomUser: d => typeof d.viewerCount === 'number' && send({ type: 'viewers', count: d.viewerCount }),
};

/* ---------- เชื่อมต่อ TikTok ---------- */
async function loadLib() {
  if (TikTokLiveConnection) return;
  ({ TikTokLiveConnection, WebcastEvent, ControlEvent } = await import('tiktok-live-connector'));
}

let CONN = null, connecting = false, retryTimer = null;
function scheduleRetry(why, ms = 15000) {
  if (retryTimer) return;
  console.log(why + ' — ลองใหม่ใน ' + Math.round(ms / 1000) + ' วินาที');
  retryTimer = setTimeout(() => { retryTimer = null; connectTikTok(); }, ms);
}
async function connectTikTok() {
  if (connecting) return;
  connecting = true;
  try {
    try { await loadLib(); } catch (e) { console.error('โหลดไลบรารี TikTok ไม่ได้ (ต้องใช้ Node.js 20 ขึ้นไป):', e.message); return; }
    const user = String(process.env.TIKTOK_USERNAME || config.username || '').trim().replace(/^@/, '');
    STATUS.username = user;
    if (!user || user === 'ใส่ชื่อบัญชี') { console.error('ยังไม่ได้ใส่ชื่อบัญชี (TIKTOK_USERNAME)'); return; }
    // ปิดการเชื่อมต่อเก่าก่อน (กันค้างอยู่ห้องเก่า)
    if (CONN) { const old = CONN; CONN = null; try { old.removeAllListeners && old.removeAllListeners(); await old.disconnect(); } catch (_) {} }
    STATUS.connected = false;
    const opts = {};
    const key = process.env.SIGN_API_KEY || config.signApiKey;
    if (key) opts.signApiKey = key;
    const conn = new TikTokLiveConnection(user, opts);
    CONN = conn;
    const on = (ev, fn) => conn.on(ev, d => { if (conn === CONN) fn(d); });
    on(WebcastEvent.FOLLOW, handlers.follow);
    on(WebcastEvent.CHAT, handlers.chat);
    on(WebcastEvent.GIFT, handlers.gift);
    on(WebcastEvent.ROOM_USER, handlers.roomUser);
    on(WebcastEvent.LIKE, handlers.like);
    on(WebcastEvent.MEMBER, handlers.member);
    on(ControlEvent.RAW_DATA, () => { STATUS.lastDataAt = Date.now(); });
    on(ControlEvent.DISCONNECTED, () => { STATUS.connected = false; scheduleRetry('หลุดการเชื่อมต่อ'); });
    on(ControlEvent.ERROR, e => console.error('ข้อผิดพลาด:', e && (e.info || e.exception || e)));
    try {
      const s = await conn.connect();
      if (conn !== CONN) return;
      if (STATUS.roomId && STATUS.roomId !== s.roomId) { ROSTER.clear(); SCORES.clear(); HOST = null; CHAMP = null; }
      STATUS.connected = true; STATUS.roomId = s.roomId; STATUS.lastError = null;
      STATUS.connectedAt = new Date().toISOString(); STATUS.lastDataAt = Date.now();
      console.log('เชื่อมต่อ TikTok สำเร็จ ห้อง', s.roomId);
    } catch (err) {
      if (conn !== CONN) return;
      STATUS.connected = false; STATUS.lastError = String(err && err.message ? err.message : err);
      scheduleRetry('เชื่อมต่อไม่ได้ (' + STATUS.lastError + ') ตรวจว่าเปิดไลฟ์อยู่และชื่อบัญชีถูกต้อง');
    }
  } finally { connecting = false; }
}
// เฝ้าดู: ถ้าต่ออยู่แต่ไม่มีข้อมูลจาก TikTok เกิน 3 นาที (เช่น ค้างห้องเก่า) ให้ต่อใหม่เอง
const SILENT_MS = 3 * 60 * 1000;
function watchdog() {
  if (DEMO || process.env.DEMO === '1') return;
  if (STATUS.connected && STATUS.lastDataAt && Date.now() - STATUS.lastDataAt > SILENT_MS) {
    console.log('ไม่มีข้อมูลจาก TikTok เกิน 3 นาที — ต่อใหม่');
    STATUS.connected = false; connectTikTok();
  }
}
setInterval(watchdog, 30000);

/* ---------- โหมดทดสอบ: สร้างเหตุการณ์ปลอมให้ผ่านทางเดียวกับของจริง ---------- */
function demo() {
  const names = ['น้องมายด์', 'พี่เอ็ม', 'ป้าแดง', 'ลุงหมี', 'แพรว', 'โอ๊ต', 'ส้มโอ', 'เจ๊น้อย'];
  const fakeUser = () => { const n = names[Math.floor(Math.random() * names.length)]; return { uniqueId: 'u_' + n, nickname: n }; };
  const coinsList = [1, 1, 1, 5, 5, 20, 100];
  setInterval(() => {
    const r = Math.random(), user = fakeUser();
    if (r < .08) return handlers.like({ likeCount: 5 });
    if (r < .3) handlers.follow({ user });
    else if (r < .4) handlers.chat({ user, comment: ['แดง', 'น้ำเงิน', 'สู้ๆ', 'แดงงง'][Math.floor(Math.random() * 4)] });
    else handleGift({ user, repeatCount: 1, repeatEnd: true, giftDetails: { giftType: 0, giftName: 'demo', diamondCount: coinsList[Math.floor(Math.random() * coinsList.length)] } });
  }, 1800);
  setInterval(() => handlers.roomUser({ viewerCount: 100 + Math.floor(Math.random() * 60) }), 5000);
}

server.listen(PORT, () => {
  console.log('');
  console.log('เปิดหน้าเกมที่: http://localhost:' + PORT + '/?live=1');
  console.log(DEMO || process.env.DEMO === '1' ? 'โหมดทดสอบ (ไม่ได้ต่อ TikTok)' : 'กำลังเชื่อมต่อ TikTok ของ @' + (process.env.TIKTOK_USERNAME || config.username) + ' ...');
  if (DEMO || process.env.DEMO === '1') demo(); else connectTikTok();
});

module.exports = { handleGift, actionForGift };
